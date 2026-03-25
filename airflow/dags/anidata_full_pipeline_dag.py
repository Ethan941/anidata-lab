from __future__ import annotations

from datetime import datetime
import re
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.email import send_email


DEFAULT_ARGS: dict[str, object] = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "retries": 0,
}

# File used by BranchPythonOperator to decide if we continue the pipeline.
STATUS_FILE = Path("/opt/airflow/output/audit_status.txt")

# --- Email on failure (callback) ---
FAILURE_TO = ["tonmail@example.com"]  # placeholder, identique à EmailOperator
EMAIL_MARKER_DIR = Path("/opt/airflow/output")


def notify_failure_callback(context: dict) -> None:
    """
    Envoie un email dès qu'une tâche échoue.
    Objectif : 1 seul email par run (marker sur filesystem), pas par tâche.
    """
    dag_run = context.get("dag_run")
    run_id = getattr(dag_run, "run_id", "unknown_run")
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(run_id))

    EMAIL_MARKER_DIR.mkdir(parents=True, exist_ok=True)
    marker = EMAIL_MARKER_DIR / f"failure_email_sent_{safe_run_id}.txt"
    if marker.exists():
        return

    task_instance = context.get("task_instance")
    task_id = getattr(task_instance, "task_id", "unknown_task")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    log_url = getattr(task_instance, "log_url", "") if task_instance else ""
    exc = context.get("exception")

    marker.write_text("sent", encoding="utf-8")

    subject = f"[Airflow] Echec DAG {dag_id} - task {task_id}"
    html_content = f"""
    <h3>Airflow : exécution en échec</h3>
    <ul>
      <li><b>DAG</b> : {dag_id}</li>
      <li><b>Task</b> : {task_id}</li>
      <li><b>run_id</b> : {run_id}</li>
      <li><b>Exception</b> : <pre>{exc}</pre></li>
    </ul>
    <p>Logs : <a href="{log_url}">{log_url}</a></p>
    """

    try:
        send_email(FAILURE_TO, subject=subject, html_content=html_content)
    except Exception as e:
        # Ne pas casser le workflow si l'email ne peut pas partir (SMTP non configuré, etc.)
        # On laisse l'erreur initiale de la tâche piloter le FAIL du DAG.
        marker.write_text(f"sent_but_failed:{e}", encoding="utf-8")


def check_audit_status() -> str:
    if not STATUS_FILE.exists():
        return "send_email_audit_failed"

    status = STATUS_FILE.read_text(encoding="utf-8").strip()
    if status == "OK":
        return "02_audit_visuel"
    return "send_email_audit_failed"


with DAG(
    dag_id="anidata_full_pipeline",
    default_args=DEFAULT_ARGS,
    description="Run 01..05 + index into Elasticsearch",
    start_date=datetime(2026, 3, 25),
    schedule=None,  # manual trigger
    catchup=False,
    tags=["anidata", "pipeline"],
) as dag:
    cd_opt_airflow = "cd /opt/airflow"

    run_01_audit_complet = BashOperator(
    task_id="01_audit_complet",
    on_failure_callback=notify_failure_callback,
    bash_command=(
        f"{cd_opt_airflow} && "
        "rm -f /opt/airflow/output/audit_status.txt && "
        "mkdir -p /opt/airflow/output && "
        "rm -f /opt/airflow/output/audit_log.txt /opt/airflow/output/audit_log_1.txt /opt/airflow/output/audit_log_2.txt; "
        # Run 1
        "python /opt/airflow/scripts/01_audit_complet.py > /opt/airflow/output/audit_log_1.txt 2>&1; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then echo FAIL > /opt/airflow/output/audit_status.txt; exit $rc; fi; "
        "grep -q 'Audit terminé avec succès !' /opt/airflow/output/audit_log_1.txt || (echo FAIL > /opt/airflow/output/audit_status.txt; exit 1); "
        "hash1=$(sha256sum /opt/airflow/output/audit_log_1.txt | awk '{print $1}'); "
        # Run 2
        "python /opt/airflow/scripts/01_audit_complet.py > /opt/airflow/output/audit_log_2.txt 2>&1; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then echo FAIL > /opt/airflow/output/audit_status.txt; exit $rc; fi; "
        "grep -q 'Audit terminé avec succès !' /opt/airflow/output/audit_log_2.txt || (echo FAIL > /opt/airflow/output/audit_status.txt; exit 1); "
        "hash2=$(sha256sum /opt/airflow/output/audit_log_2.txt | awk '{print $1}'); "
        # Compare
        "if [ \"$hash1\" != \"$hash2\" ]; then echo FAIL > /opt/airflow/output/audit_status.txt; exit 1; fi; "
        # Canonical log + status
        "cp -f /opt/airflow/output/audit_log_2.txt /opt/airflow/output/audit_log.txt; "
        "echo OK > /opt/airflow/output/audit_status.txt"
    ),
    )

    check_audit = BranchPythonOperator(
        task_id="check_audit_status",
        python_callable=check_audit_status,
        on_failure_callback=notify_failure_callback,
    )

    send_email_audit_failed = EmailOperator(
        task_id="send_email_audit_failed",
        to="tonmail@example.com",
        subject="Échec audit AniData",
        html_content="""
        <h3>L'audit AniData a échoué</h3>
        <p>Le fichier audit_status.txt n'indique pas OK.</p>
        <p>Vérifie les logs Airflow de la tâche 01_audit_complet.</p>
        """,
    )

    run_02_audit_visuel = BashOperator(
    task_id="02_audit_visuel",
    on_failure_callback=notify_failure_callback,
    bash_command=(
        f"{cd_opt_airflow} && "
        "rm -f /opt/airflow/output/audit_visuel_status.txt && "
        "mkdir -p /opt/airflow/output /opt/airflow/output/audit_charts && "
        "rm -f /opt/airflow/output/audit_visuel_log.txt /opt/airflow/output/audit_visuel_log_1.txt; "
        # Run 1
        "rm -rf /opt/airflow/output/audit_charts && mkdir -p /opt/airflow/output/audit_charts; "
        "python /opt/airflow/scripts/02_audit_visuel.py | tee /opt/airflow/output/audit_visuel_log_1.txt; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then echo FAIL > /opt/airflow/output/audit_visuel_status.txt; exit $rc; fi; "
        "png_count_1=$(ls -1 /opt/airflow/output/audit_charts/*.png 2>/dev/null | wc -l); "
        "if [ \"$png_count_1\" -ne \"7\" ] && [ \"$png_count_1\" -ne \"8\" ]; then echo FAIL > /opt/airflow/output/audit_visuel_status.txt; echo \"Expected 7 or 8 png charts, got ${png_count_1}.\"; exit 1; fi; "
        "ls -1 /opt/airflow/output/audit_charts/*score_distribution.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*data_types.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*top_genres.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*top_studios.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*type_distribution.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*boxplots.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*correlation_matrix.png >/dev/null 2>&1 && "
        "hash1=$(ls -1 /opt/airflow/output/audit_charts/*.png 2>/dev/null | sort | xargs sha256sum | sha256sum | awk '{print $1}'); "
        # Run 2
        "rm -rf /opt/airflow/output/audit_charts && mkdir -p /opt/airflow/output/audit_charts; "
        "python /opt/airflow/scripts/02_audit_visuel.py | tee /opt/airflow/output/audit_visuel_log.txt; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then echo FAIL > /opt/airflow/output/audit_visuel_status.txt; exit $rc; fi; "
        "png_count_2=$(ls -1 /opt/airflow/output/audit_charts/*.png 2>/dev/null | wc -l); "
        "if [ \"$png_count_2\" -ne \"7\" ] && [ \"$png_count_2\" -ne \"8\" ]; then echo FAIL > /opt/airflow/output/audit_visuel_status.txt; echo \"Expected 7 or 8 png charts, got ${png_count_2}.\"; exit 1; fi; "
        "ls -1 /opt/airflow/output/audit_charts/*score_distribution.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*data_types.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*top_genres.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*top_studios.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*type_distribution.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*boxplots.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*correlation_matrix.png >/dev/null 2>&1 && "
        "hash2=$(ls -1 /opt/airflow/output/audit_charts/*.png 2>/dev/null | sort | xargs sha256sum | sha256sum | awk '{print $1}'); "
        # Compare hashes
        "if [ \"$hash1\" != \"$hash2\" ]; then echo FAIL > /opt/airflow/output/audit_visuel_status.txt; echo \"Hash mismatch for audit_visuel\"; exit 1; fi; "
        "echo OK > /opt/airflow/output/audit_visuel_status.txt"
    ),
    )

    run_03_nettoyage = BashOperator(
    task_id="03_nettoyage",
    on_failure_callback=notify_failure_callback,
    bash_command=(
        f"{cd_opt_airflow} && "
        "rm -f /opt/airflow/output/nettoyage_status.txt && "
        "mkdir -p /opt/airflow/output && "
        # Run 1
        "rm -f /opt/airflow/output/anime_cleaned.csv /opt/airflow/output/nettoyage_log_1.txt; "
        "python /opt/airflow/scripts/03_nettoyage.py | tee /opt/airflow/output/nettoyage_log_1.txt; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then echo FAIL > /opt/airflow/output/nettoyage_status.txt; exit $rc; fi; "
        "if [ ! -s /opt/airflow/output/anime_cleaned.csv ]; then echo FAIL > /opt/airflow/output/nettoyage_status.txt; exit 1; fi; "
        "hash1=$(sha256sum /opt/airflow/output/anime_cleaned.csv | awk '{print $1}'); "
        # Run 2
        "rm -f /opt/airflow/output/anime_cleaned.csv /opt/airflow/output/nettoyage_log_2.txt; "
        "python /opt/airflow/scripts/03_nettoyage.py | tee /opt/airflow/output/nettoyage_log_2.txt; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then echo FAIL > /opt/airflow/output/nettoyage_status.txt; exit $rc; fi; "
        "if [ ! -s /opt/airflow/output/anime_cleaned.csv ]; then echo FAIL > /opt/airflow/output/nettoyage_status.txt; exit 1; fi; "
        "hash2=$(sha256sum /opt/airflow/output/anime_cleaned.csv | awk '{print $1}'); "
        # Compare hashes
        "if [ \"$hash1\" != \"$hash2\" ]; then "
        "  echo FAIL > /opt/airflow/output/nettoyage_status.txt; "
        "  echo \"Hash mismatch (run1=$hash1, run2=$hash2)\"; "
        "  exit 1; "
        "fi; "
        # Final checks on run 2 log
        "grep -q 'Nettoyage terminé !' /opt/airflow/output/nettoyage_log_2.txt || "
        "(echo FAIL > /opt/airflow/output/nettoyage_status.txt; "
        " echo 'Message de fin non trouvé dans les logs'; exit 1); "
        # Keep run2 log as canonical (optional)
        "cp -f /opt/airflow/output/nettoyage_log_2.txt /opt/airflow/output/nettoyage_log.txt; "
        "echo OK > /opt/airflow/output/nettoyage_status.txt"
    ),
    )

    run_04_feature_engineering = BashOperator(
    task_id="04_feature_engineering",
    on_failure_callback=notify_failure_callback,
    bash_command=(
        f"{cd_opt_airflow} && "
        "rm -f /opt/airflow/output/feature_engineering_status.txt && "
        "mkdir -p /opt/airflow/output && "
        "rm -f /opt/airflow/output/anime_gold.csv /opt/airflow/output/feature_engineering_log_1.txt /opt/airflow/output/feature_engineering_log_2.txt; "
        # Run 1
        "rm -f /opt/airflow/output/anime_gold.csv; "
        "python /opt/airflow/scripts/04_feature_engineering.py | tee /opt/airflow/output/feature_engineering_log_1.txt; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then echo FAIL > /opt/airflow/output/feature_engineering_status.txt; exit $rc; fi; "
        "if [ ! -s /opt/airflow/output/anime_gold.csv ]; then echo FAIL > /opt/airflow/output/feature_engineering_status.txt; echo 'anime_gold.csv missing/empty'; exit 1; fi; "
        "hash1_csv=$(sha256sum /opt/airflow/output/anime_gold.csv | awk '{print $1}'); "
        # Run 2
        "rm -f /opt/airflow/output/anime_gold.csv; "
        "python /opt/airflow/scripts/04_feature_engineering.py | tee /opt/airflow/output/feature_engineering_log_2.txt; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then echo FAIL > /opt/airflow/output/feature_engineering_status.txt; exit $rc; fi; "
        "if [ ! -s /opt/airflow/output/anime_gold.csv ]; then echo FAIL > /opt/airflow/output/feature_engineering_status.txt; echo 'anime_gold.csv missing/empty'; exit 1; fi; "
        "hash2_csv=$(sha256sum /opt/airflow/output/anime_gold.csv | awk '{print $1}'); "
        # Compare hashes
        "if [ \"$hash1_csv\" != \"$hash2_csv\" ]; then "
        "  echo FAIL > /opt/airflow/output/feature_engineering_status.txt; "
        "  echo \"Hash mismatch for feature engineering (anime_gold.csv)\"; "
        "  exit 1; "
        "fi; "
        # Final checks on run 2 logs / outputs
        "grep -qi 'feature engineering' /opt/airflow/output/feature_engineering_log_2.txt || "
        "(echo FAIL > /opt/airflow/output/feature_engineering_status.txt; echo 'Feature engineering logs missing'; exit 1); "
        "header=$(head -n 1 /opt/airflow/output/anime_gold.csv); "
        "echo \"$header\" | grep -q 'weighted_score' || "
        "(echo FAIL > /opt/airflow/output/feature_engineering_status.txt; echo 'Colonne weighted_score absente'; exit 1); "
        "echo \"$header\" | grep -q 'drop_ratio' || "
        "(echo FAIL > /opt/airflow/output/feature_engineering_status.txt; echo 'Colonne drop_ratio absente'; exit 1); "
        "echo \"$header\" | grep -q 'score_category' || "
        "(echo FAIL > /opt/airflow/output/feature_engineering_status.txt; echo 'Colonne score_category absente'; exit 1); "
        "echo \"$header\" | grep -q 'main_studio' || "
        "(echo FAIL > /opt/airflow/output/feature_engineering_status.txt; echo 'Colonne main_studio absente'; exit 1); "
        "echo \"$header\" | grep -q 'studio_tier' || "
        "(echo FAIL > /opt/airflow/output/feature_engineering_status.txt; echo 'Colonne studio_tier absente'; exit 1); "
        "echo \"$header\" | grep -q 'year' || "
        "(echo FAIL > /opt/airflow/output/feature_engineering_status.txt; echo 'Colonne year absente'; exit 1); "
        "echo \"$header\" | grep -q 'decade' || "
        "(echo FAIL > /opt/airflow/output/feature_engineering_status.txt; echo 'Colonne decade absente'; exit 1); "
        "echo \"$header\" | grep -q 'n_genres' || "
        "(echo FAIL > /opt/airflow/output/feature_engineering_status.txt; echo 'Colonne n_genres absente'; exit 1); "
        "echo \"$header\" | grep -q 'main_genre' || "
        "(echo FAIL > /opt/airflow/output/feature_engineering_status.txt; echo 'Colonne main_genre absente'; exit 1); "
        "echo \"$header\" | grep -q 'engagement_ratio' || "
        "(echo FAIL > /opt/airflow/output/feature_engineering_status.txt; echo 'Colonne engagement_ratio absente'; exit 1); "
        "cp -f /opt/airflow/output/feature_engineering_log_2.txt /opt/airflow/output/feature_engineering_log.txt; "
        "echo OK > /opt/airflow/output/feature_engineering_status.txt"
    ),
)

    run_05_validation = BashOperator(
    task_id="05_validation",
    on_failure_callback=notify_failure_callback,
    bash_command=(
        f"{cd_opt_airflow} && "
        "rm -f /opt/airflow/output/validation_status.txt && "
        "mkdir -p /opt/airflow/output && "
        "rm -f /opt/airflow/output/anime_gold_validated.csv /opt/airflow/output/anime_gold.json /opt/airflow/output/rapport_validation.txt "
        "/opt/airflow/output/validation_log_1.txt /opt/airflow/output/validation_log_2.txt; "
        # Run 1
        "rm -f /opt/airflow/output/anime_gold_validated.csv /opt/airflow/output/anime_gold.json /opt/airflow/output/rapport_validation.txt; "
        "python /opt/airflow/scripts/05_validation.py | tee /opt/airflow/output/validation_log_1.txt; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then echo FAIL > /opt/airflow/output/validation_status.txt; exit $rc; fi; "
        "if [ ! -s /opt/airflow/output/anime_gold_validated.csv ]; then echo FAIL > /opt/airflow/output/validation_status.txt; echo 'anime_gold_validated.csv missing/empty'; exit 1; fi; "
        "if [ ! -s /opt/airflow/output/anime_gold.json ]; then echo FAIL > /opt/airflow/output/validation_status.txt; echo 'anime_gold.json missing/empty'; exit 1; fi; "
        "if [ ! -s /opt/airflow/output/rapport_validation.txt ]; then echo FAIL > /opt/airflow/output/validation_status.txt; echo 'rapport_validation.txt missing/empty'; exit 1; fi; "
        "hash1_csv=$(sha256sum /opt/airflow/output/anime_gold_validated.csv | awk '{print $1}'); "
        "hash1_json=$(sha256sum /opt/airflow/output/anime_gold.json | awk '{print $1}'); "
        "hash1_report=$(sha256sum /opt/airflow/output/rapport_validation.txt | awk '{print $1}'); "
        "hash1_combined=$(echo \"$hash1_csv $hash1_json $hash1_report\" | sha256sum | awk '{print $1}'); "
        # Run 2
        "rm -f /opt/airflow/output/anime_gold_validated.csv /opt/airflow/output/anime_gold.json /opt/airflow/output/rapport_validation.txt; "
        "python /opt/airflow/scripts/05_validation.py | tee /opt/airflow/output/validation_log_2.txt; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then echo FAIL > /opt/airflow/output/validation_status.txt; exit $rc; fi; "
        "if [ ! -s /opt/airflow/output/anime_gold_validated.csv ]; then echo FAIL > /opt/airflow/output/validation_status.txt; echo 'anime_gold_validated.csv missing/empty'; exit 1; fi; "
        "if [ ! -s /opt/airflow/output/anime_gold.json ]; then echo FAIL > /opt/airflow/output/validation_status.txt; echo 'anime_gold.json missing/empty'; exit 1; fi; "
        "if [ ! -s /opt/airflow/output/rapport_validation.txt ]; then echo FAIL > /opt/airflow/output/validation_status.txt; echo 'rapport_validation.txt missing/empty'; exit 1; fi; "
        "hash2_csv=$(sha256sum /opt/airflow/output/anime_gold_validated.csv | awk '{print $1}'); "
        "hash2_json=$(sha256sum /opt/airflow/output/anime_gold.json | awk '{print $1}'); "
        "hash2_report=$(sha256sum /opt/airflow/output/rapport_validation.txt | awk '{print $1}'); "
        "hash2_combined=$(echo \"$hash2_csv $hash2_json $hash2_report\" | sha256sum | awk '{print $1}'); "
        # Compare hashes
        "if [ \"$hash1_combined\" != \"$hash2_combined\" ]; then "
        "  echo FAIL > /opt/airflow/output/validation_status.txt; "
        "  echo \"Hash mismatch for validation\"; "
        "  exit 1; "
        "fi; "
        # Final checks on run 2 logs
        "grep -q 'Pipeline Data Refinement terminé !' /opt/airflow/output/validation_log_2.txt || "
        "(echo FAIL > /opt/airflow/output/validation_status.txt; echo 'Message final absent'; exit 1); "
        "cp -f /opt/airflow/output/validation_log_2.txt /opt/airflow/output/validation_log.txt; "
        "echo OK > /opt/airflow/output/validation_status.txt"
    ),
    )

    run_06_indexation = BashOperator(
        task_id="06_indexation_elasticsearch",
        on_failure_callback=notify_failure_callback,
        bash_command=(
            f"{cd_opt_airflow} && "
            "rm -f /opt/airflow/output/indexation_status.txt; "
            "python /opt/airflow/scripts/script_prof.py; "
            "count=$(curl -s \"http://elasticsearch:9200/anime/_count\" | python3 -c \"import sys,json; print(json.load(sys.stdin)['count'])\"); "
            "if [ \"$count\" -gt 0 ]; then "
            "  echo OK > /opt/airflow/output/indexation_status.txt; "
            "else "
            "  exit 1; "
            "fi"
        ),
    )

    run_01_audit_complet >> check_audit
    check_audit >> run_02_audit_visuel
    check_audit >> send_email_audit_failed

    run_02_audit_visuel >> run_03_nettoyage >> run_04_feature_engineering >> run_05_validation >> run_06_indexation

