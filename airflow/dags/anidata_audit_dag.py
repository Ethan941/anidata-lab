from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator  

with DAG(
    dag_id="anidata_audit",
    description="Audit qualité des datasets AniData Lab",
    start_date=datetime(2026, 3, 23),
    schedule=None,   # lancement manuel depuis l'UI
    catchup=False,
    tags=["anidata", "audit"],
) as dag:

    run_audit = BashOperator(
        task_id="run_audit_dataset",
        bash_command="python /opt/airflow/scripts/audit_dataset.py",
    )