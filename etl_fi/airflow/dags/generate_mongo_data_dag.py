from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="generate_mongo_data_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python /opt/airflow/scripts/generate_mongo_data.py"
    )

    load_to_mongo = BashOperator(
        task_id="load_to_mongo",
        bash_command="python /opt/airflow/scripts/load_to_mongo.py"
    )

    generate_data >> load_to_mongo
