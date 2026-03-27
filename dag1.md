# DAG 1 — `anidata_full_pipeline` (explication pas a pas)

Ce document decompose le DAG `airflow/dags/anidata_full_pipeline_dag.py` avec le code et l'explication "au fil de l'eau".

---

## 1) Imports, constantes, et callbacks

```python
from __future__ import annotations

from datetime import datetime
import json
import re
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
```

**Ce que ca fait**
- Charge les operateurs Airflow utilises par le DAG.
- `BashOperator` execute les scripts shell/python.
- `BranchPythonOperator` choisit la branche "OK" ou "FAIL".
- `EmailOperator` envoie un email en cas d'echec audit.

```python
DEFAULT_ARGS: dict[str, object] = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "retries": 0,
}

STATUS_FILE = Path("/opt/airflow/output/audit_status.txt")
FAILURE_TO = ["tonmail@example.com"]
EMAIL_MARKER_DIR = Path("/opt/airflow/output")
```

**Ce que ca fait**
- Parametres generaux des taches.
- `STATUS_FILE` sert a piloter la branche apres l'audit.
- `FAILURE_TO` et `EMAIL_MARKER_DIR` servent au callback d'alerte.

```python
def notify_failure_callback(context: dict) -> None:
    dag_run = context.get("dag_run")
    run_id = getattr(dag_run, "run_id", "unknown_run")
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(run_id))

    EMAIL_MARKER_DIR.mkdir(parents=True, exist_ok=True)
    marker = EMAIL_MARKER_DIR / f"failure_email_sent_{safe_run_id}.txt"
    if marker.exists():
        return
    ...
    send_email(FAILURE_TO, subject=subject, html_content=html_content)
```

**Ce que ca fait**
- Si une tache echoue, envoie un email.
- Le fichier marker evite d'envoyer plusieurs mails pour un meme run.

```python
def check_audit_status() -> str:
    if not STATUS_FILE.exists():
        return "send_email_audit_failed"
    status = STATUS_FILE.read_text(encoding="utf-8").strip()
    if status == "OK":
        return "02_audit_visuel"
    return "send_email_audit_failed"
```

**Ce que ca fait**
- Lit le resultat de l'audit (`OK`/`FAIL`) et decide la branche suivante.

```python
def receive_from_dag2(**context) -> dict:
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    out_dir = Path("/opt/airflow/output")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "full_pipeline_received_from_dag2.json").write_text(
        json.dumps(conf, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return conf
```

**Ce que ca fait**
- Recoit les donnees transmises par DAG2 (`dag_run.conf`).
- Sauvegarde un JSON de debug.
- Retourne la conf en XCom.

---

## 2) Definition du DAG

```python
with DAG(
    dag_id="anidata_full_pipeline",
    default_args=DEFAULT_ARGS,
    description="Run 01..05 + index into Elasticsearch",
    start_date=datetime(2026, 3, 25),
    schedule=None,
    catchup=False,
    tags=["anidata", "pipeline"],
) as dag:
    cd_opt_airflow = "cd /opt/airflow"
```

**Ce que ca fait**
- Cree un DAG manuel (pas de cron), lance a la demande.

---

## 3) Tache 00 — reception depuis DAG2

```python
receive_xcom_from_dag2 = PythonOperator(
    task_id="00_receive_from_dag2",
    python_callable=receive_from_dag2,
    on_failure_callback=notify_failure_callback,
)
```

**Ce que ca fait**
- Point d'entree du DAG1 quand il est declenche par DAG2.

---

## 4) Taches 01 a 06 (pipeline principal)

### 01) Audit complet

```python
run_01_audit_complet = BashOperator(
    task_id="01_audit_complet",
    on_failure_callback=notify_failure_callback,
    bash_command=(
        f"{cd_opt_airflow} && "
        "rm -f /opt/airflow/output/audit_status.txt && "
        "mkdir -p /opt/airflow/output && "
        "rm -f /opt/airflow/output/audit_log.txt /opt/airflow/output/audit_log_1.txt /opt/airflow/output/audit_log_2.txt; "
        "python /opt/airflow/scripts/01_audit_complet.py > /opt/airflow/output/audit_log_1.txt 2>&1; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then echo FAIL > /opt/airflow/output/audit_status.txt; exit $rc; fi; "
        "grep -q 'Audit terminé avec succès !' /opt/airflow/output/audit_log_1.txt || (echo FAIL > /opt/airflow/output/audit_status.txt; exit 1); "
        "python /opt/airflow/scripts/01_audit_complet.py > /opt/airflow/output/audit_log_2.txt 2>&1; "
        "rc=$?; "
        "if [ $rc -ne 0 ]; then echo FAIL > /opt/airflow/output/audit_status.txt; exit $rc; fi; "
        "grep -q 'Audit terminé avec succès !' /opt/airflow/output/audit_log_2.txt || (echo FAIL > /opt/airflow/output/audit_status.txt; exit 1); "
        "cp -f /opt/airflow/output/audit_log_2.txt /opt/airflow/output/audit_log.txt; "
        "echo OK > /opt/airflow/output/audit_status.txt"
    ),
)
```

**Ce que ca fait**
- Lance l'audit 2 fois, verifie le message de succes.
- Ecrit `OK`/`FAIL` dans `audit_status.txt`.

### Branch audit + email

```python
check_audit = BranchPythonOperator(
    task_id="check_audit_status",
    python_callable=check_audit_status,
    on_failure_callback=notify_failure_callback,
)

send_email_audit_failed = EmailOperator(
    task_id="send_email_audit_failed",
    to="tonmail@example.com",
    subject="Échec audit AniData",
    html_content="...",
)
```

**Ce que ca fait**
- Si audit OK: continue vers la suite pipeline.
- Sinon: envoie l'email d'echec.

### 02) Audit visuel

- Execute `02_audit_visuel.py`.
- Verifie que les PNG sont presents.
- Fait un double-run et compare les hashes des graphiques.

### 03) Nettoyage

- Execute `03_nettoyage.py`.
- Verifie `anime_cleaned.csv` + logs.
- Double-run + hash CSV pour controler la reproductibilite.

### 04) Feature engineering

- Execute `04_feature_engineering.py`.
- Verifie `anime_gold.csv` et colonnes attendues.
- Double-run + hash CSV.

### 05) Validation

- Execute `05_validation.py`.
- Verifie `anime_gold_validated.csv`, `anime_gold.json`, `rapport_validation.txt`.
- Double-run + hash combine des 3 fichiers.

### 06) Indexation Elasticsearch

```python
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
```

**Ce que ca fait**
- Lance l'indexation finale dans Elasticsearch.
- Verifie que l'index `anime` contient des documents.

---

## 5) Graphe de dependances

```python
run_01_audit_complet >> check_audit
check_audit >> run_02_audit_visuel
check_audit >> send_email_audit_failed

run_02_audit_visuel >> run_03_nettoyage >> run_04_feature_engineering >> run_05_validation >> run_06_indexation

receive_xcom_from_dag2 >> run_01_audit_complet
```

**Lecture simple**
- Entree: `00_receive_from_dag2`
- Puis audit `01`
- Branch:
  - **OK** -> `02 -> 03 -> 04 -> 05 -> 06`
  - **FAIL** -> `send_email_audit_failed`

