from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator


DEFAULT_ARGS: dict[str, object] = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "retries": 0,
}

# File used by BranchPythonOperator to decide if we continue the pipeline.
STATUS_FILE = Path("/opt/airflow/output/audit_status.txt")


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
    bash_command=(
        f"{cd_opt_airflow} && "
        "rm -f /opt/airflow/output/audit_status.txt && "
        "mkdir -p /opt/airflow/output && "
        "python /opt/airflow/scripts/01_audit_complet.py | tee /opt/airflow/output/audit_log.txt; "
        "if grep -q 'Audit terminé avec succès !' /opt/airflow/output/audit_log.txt; then "
        "echo OK > /opt/airflow/output/audit_status.txt; "
        "else "
        "echo FAIL > /opt/airflow/output/audit_status.txt; exit 1; "
        "fi"
    ),
    )

    check_audit = BranchPythonOperator(
        task_id="check_audit_status",
        python_callable=check_audit_status,
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
    bash_command=(
        f"{cd_opt_airflow} && "
        "rm -f /opt/airflow/output/audit_visuel_status.txt && "
        "mkdir -p /opt/airflow/output /opt/airflow/output/audit_charts && "
        "python /opt/airflow/scripts/02_audit_visuel.py | tee /opt/airflow/output/audit_visuel_log.txt; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then "
        "  echo FAIL > /opt/airflow/output/audit_visuel_status.txt; "
        "  exit $rc; "
        "fi; "
        "png_count=$(ls -1 /opt/airflow/output/audit_charts/*.png 2>/dev/null | wc -l); "
        "if [ \"$png_count\" -ne \"7\" ] && [ \"$png_count\" -ne \"8\" ]; then "
        "  echo FAIL > /opt/airflow/output/audit_visuel_status.txt; "
        "  echo \"Expected 7 or 8 png charts, got ${png_count}.\"; "
        "  exit 1; "
        "fi; "
        "ls -1 /opt/airflow/output/audit_charts/*score_distribution.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*data_types.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*top_genres.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*top_studios.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*type_distribution.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*boxplots.png >/dev/null 2>&1 && "
        "ls -1 /opt/airflow/output/audit_charts/*correlation_matrix.png >/dev/null 2>&1 && "
        "echo OK > /opt/airflow/output/audit_visuel_status.txt || "
        "(echo FAIL > /opt/airflow/output/audit_visuel_status.txt; exit 1)"
    ),
    )

    run_03_nettoyage = BashOperator(
    task_id="03_nettoyage",
    bash_command=(
        f"{cd_opt_airflow} && "
        "rm -f /opt/airflow/output/nettoyage_status.txt && "
        "mkdir -p /opt/airflow/output && "
        "rm -f /opt/airflow/output/anime_cleaned.csv && "
        "python /opt/airflow/scripts/03_nettoyage.py | tee /opt/airflow/output/nettoyage_log.txt; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then "
        "  echo FAIL > /opt/airflow/output/nettoyage_status.txt; "
        "  exit $rc; "
        "fi; "
        "if [ ! -f /opt/airflow/output/anime_cleaned.csv ]; then "
        "  echo FAIL > /opt/airflow/output/nettoyage_status.txt; "
        "  echo 'Fichier anime_cleaned.csv introuvable'; "
        "  exit 1; "
        "fi; "
        "if [ ! -s /opt/airflow/output/anime_cleaned.csv ]; then "
        "  echo FAIL > /opt/airflow/output/nettoyage_status.txt; "
        "  echo 'Fichier anime_cleaned.csv vide'; "
        "  exit 1; "
        "fi; "
        "grep -q 'Nettoyage terminé !' /opt/airflow/output/nettoyage_log.txt || "
        "(echo FAIL > /opt/airflow/output/nettoyage_status.txt; "
        " echo 'Message de fin non trouvé dans les logs'; exit 1); "
        "echo OK > /opt/airflow/output/nettoyage_status.txt"
    ),
    )

    run_04_feature_engineering = BashOperator(
    task_id="04_feature_engineering",
    bash_command=(
        f"{cd_opt_airflow} && "
        "rm -f /opt/airflow/output/feature_engineering_status.txt && "
        "mkdir -p /opt/airflow/output && "
        "rm -f /opt/airflow/output/anime_gold.csv && "
        "python /opt/airflow/scripts/04_feature_engineering.py | tee /opt/airflow/output/feature_engineering_log.txt; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then "
        "  echo FAIL > /opt/airflow/output/feature_engineering_status.txt; "
        "  exit $rc; "
        "fi; "
        "if [ ! -f /opt/airflow/output/anime_gold.csv ]; then "
        "  echo FAIL > /opt/airflow/output/feature_engineering_status.txt; "
        "  echo 'Fichier anime_gold.csv introuvable'; "
        "  exit 1; "
        "fi; "
        "if [ ! -s /opt/airflow/output/anime_gold.csv ]; then "
        "  echo FAIL > /opt/airflow/output/feature_engineering_status.txt; "
        "  echo 'Fichier anime_gold.csv vide'; "
        "  exit 1; "
        "fi; "
        "grep -q 'Feature engineering terminé !' /opt/airflow/output/feature_engineering_log.txt || "
        "(echo FAIL > /opt/airflow/output/feature_engineering_status.txt; "
        " echo 'Message de succès absent'; exit 1); "
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
        "echo OK > /opt/airflow/output/feature_engineering_status.txt"
    ),
)

    run_05_validation = BashOperator(
    task_id="05_validation",
    bash_command=(
        f"{cd_opt_airflow} && "
        "rm -f /opt/airflow/output/validation_status.txt && "
        "mkdir -p /opt/airflow/output && "
        "rm -f /opt/airflow/output/anime_gold_validated.csv "
        "/opt/airflow/output/anime_gold.json "
        "/opt/airflow/output/rapport_validation.txt && "
        "python /opt/airflow/scripts/05_validation.py | tee /opt/airflow/output/validation_log.txt; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then "
        "  echo FAIL > /opt/airflow/output/validation_status.txt; "
        "  exit $rc; "
        "fi; "
        "if [ ! -f /opt/airflow/output/anime_gold_validated.csv ]; then "
        "  echo FAIL > /opt/airflow/output/validation_status.txt; "
        "  echo 'Fichier anime_gold_validated.csv introuvable'; "
        "  exit 1; "
        "fi; "
        "if [ ! -s /opt/airflow/output/anime_gold_validated.csv ]; then "
        "  echo FAIL > /opt/airflow/output/validation_status.txt; "
        "  echo 'Fichier anime_gold_validated.csv vide'; "
        "  exit 1; "
        "fi; "
        "if [ ! -f /opt/airflow/output/anime_gold.json ]; then "
        "  echo FAIL > /opt/airflow/output/validation_status.txt; "
        "  echo 'Fichier anime_gold.json introuvable'; "
        "  exit 1; "
        "fi; "
        "if [ ! -s /opt/airflow/output/anime_gold.json ]; then "
        "  echo FAIL > /opt/airflow/output/validation_status.txt; "
        "  echo 'Fichier anime_gold.json vide'; "
        "  exit 1; "
        "fi; "
        "if [ ! -f /opt/airflow/output/rapport_validation.txt ]; then "
        "  echo FAIL > /opt/airflow/output/validation_status.txt; "
        "  echo 'Fichier rapport_validation.txt introuvable'; "
        "  exit 1; "
        "fi; "
        "if [ ! -s /opt/airflow/output/rapport_validation.txt ]; then "
        "  echo FAIL > /opt/airflow/output/validation_status.txt; "
        "  echo 'Fichier rapport_validation.txt vide'; "
        "  exit 1; "
        "fi; "
        "grep -q 'Pipeline Data Refinement terminé !' /opt/airflow/output/validation_log.txt || "
        "(echo FAIL > /opt/airflow/output/validation_status.txt; "
        " echo 'Message final absent'; exit 1); "
        "echo OK > /opt/airflow/output/validation_status.txt"
    ),
    )

    run_06_indexation = BashOperator(
        task_id="06_indexation_elasticsearch",
        bash_command=f"{cd_opt_airflow} && python /opt/airflow/scripts/script_prof.py",
    )

    run_01_audit_complet >> check_audit
    check_audit >> run_02_audit_visuel
    check_audit >> send_email_audit_failed

    run_02_audit_visuel >> run_03_nettoyage >> run_04_feature_engineering >> run_05_validation >> run_06_indexation

