from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="anidata_refinement",
    description="Nettoyage, enrichissement et export dataset gold AniData Lab",
    start_date=datetime(2026, 3, 23),
    schedule=None,
    catchup=False,
    tags=["anidata", "refinement", "gold"],
) as dag:
    run_refinement = BashOperator(
        task_id="run_refine_gold_dataset",
        bash_command="python /opt/airflow/scripts/refine_gold_dataset.py",
    )
