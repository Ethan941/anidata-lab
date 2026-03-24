from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="anidata_refinement",
    description="Nettoyage, enrichissement et export gold avec règle priorité titres",
    start_date=datetime(2026, 3, 23),
    schedule=None,
    catchup=False,
    tags=["anidata", "refinement", "gold"],
) as dag:
    run_refinement = BashOperator(
        task_id="run_refine_gold_dataset",
        bash_command=(
            "TITLE_WEIGHT_NAME=0.5 "
            "TITLE_WEIGHT_ENGLISH_NAME=0.3 "
            "TITLE_WEIGHT_JAPANESE_NAME=0.2 "
            "TITLE_WEIGHTED_COVERAGE_THRESHOLD=0.8 "
            "python /opt/airflow/scripts/refine_gold_dataset.py"
        ),
    )
