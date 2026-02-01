from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator

INPUT_JSON = Path("/opt/airflow/data/pets-data.json")

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

TARGET_SCHEMA = "public"
TARGET_TABLE = "pets_flat"

def extract_transform(**context):
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_JSON}")

    data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))

    pets = data.get("pets")
    if pets is None:
        raise KeyError("Key 'pets' not found in JSON")
    if not isinstance(pets, list):
        raise TypeError(f"'pets' must be a list, got: {type(pets)}")

    df = pd.json_normalize(pets, sep="__")

    if "favFoods" in df.columns:
        df["favFoods"] = df["favFoods"].apply(
            lambda x: ", ".join(map(str, x)) if isinstance(x, list) else ("" if x is None else str(x))
        )

    context["ti"].xcom_push(key="records", value=df.to_dict(orient="records"))
    context["ti"].xcom_push(key="columns", value=list(df.columns))


def load_to_postgres(**context):
    records = context["ti"].xcom_pull(key="records", task_ids="extract_transform")
    columns = context["ti"].xcom_pull(key="columns", task_ids="extract_transform")

    if not records or not columns:
        raise RuntimeError("No data to load from XCom")

    df = pd.DataFrame.from_records(records, columns=columns)

    engine = create_engine(DB_URL)

    df.to_sql(
        name=TARGET_TABLE,
        con=engine,
        schema=TARGET_SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )


with DAG(
    dag_id="pets_json_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["hw", "etl", "json", "postgres"],
) as dag:
    t1 = PythonOperator(task_id="extract_transform", python_callable=extract_transform)
    t2 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)

    t1 >> t2
