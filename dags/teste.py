import datetime

from airflow.models.dag import DAG
from airflow.operators.bash_operator import BashOperator


with DAG(
    dag_id="teste",
    description="primeira dag",
    schedule_interval="@hourly"
    # schedule=None,
    start_date=datetime(2024, 12, 1, tz="UTC"),
    catchup=False,
    tags=["primeira dag", "teste"]
) as dag:
    tarefa1 = BashOperator(
        task_id ="print1",
        bash_command="echo primeira dag"
    )

tarefa1
