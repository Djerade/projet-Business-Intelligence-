from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["test"],
) as dag:

    hello_task = BashOperator(
        task_id="hello_world",
        bash_command="echo 'Airflow fonctionne correctement 🚀'",
    )
