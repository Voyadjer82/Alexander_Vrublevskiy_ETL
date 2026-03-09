from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="etl_mongo_to_postgres_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    etl_mongo_to_postgres = BashOperator(
        task_id="etl_mongo_to_postgres",
        bash_command="python /opt/airflow/scripts/etl_mongo_to_postgres.py"
    )
