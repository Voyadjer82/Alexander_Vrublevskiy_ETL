from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator


INPUT_XML = Path("/opt/airflow/data/nutrition.xml")

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
SCHEMA = "public"
FOODS_TABLE = "foods_flat"
DAILY_VALUES_TABLE = "daily_values"


def _slug(tag: str) -> str:
    """Приводим имена тегов/атрибутов к безопасным именам колонок."""
    tag = tag.strip()
    tag = tag.replace("-", "_")
    tag = re.sub(r"[^a-zA-Z0-9_]+", "_", tag)
    return tag.lower()


def extract_transform(**context):
    if not INPUT_XML.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_XML}")

    root = ET.parse(INPUT_XML).getroot()

    dv_rows = []
    dv = root.find("daily-values")
    if dv is not None:
        for child in list(dv):
            nutrient = _slug(child.tag)
            units = child.attrib.get("units")
            value_text = (child.text or "").strip()
            dv_rows.append(
                {
                    "nutrient": nutrient,
                    "value": value_text if value_text != "" else None,
                    "units": units,
                }
            )

    food_rows = []
    for food in root.findall("food"):
        row: dict[str, object] = {}

        for child in list(food):
            tag = _slug(child.tag)

            if child.tag == "calories":
                for k, v in child.attrib.items():
                    row[f"calories_{_slug(k)}"] = v
                continue

            if child.tag == "serving":
                row["serving"] = (child.text or "").strip() or None
                row["serving_units"] = child.attrib.get("units")
                continue

            if child.tag == "vitamins":
                for v in list(child):
                    row[f"vitamins_{_slug(v.tag)}"] = (v.text or "").strip() or None
                continue

            if child.tag == "minerals":
                for m in list(child):
                    row[f"minerals_{_slug(m.tag)}"] = (m.text or "").strip() or None
                continue

            row[tag] = (child.text or "").strip() or None

            for k, v in child.attrib.items():
                row[f"{tag}_{_slug(k)}"] = v

        food_rows.append(row)

    df_foods = pd.DataFrame(food_rows)
    df_dv = pd.DataFrame(dv_rows)

    ti = context["ti"]
    ti.xcom_push(key="foods_records", value=df_foods.to_dict(orient="records"))
    ti.xcom_push(key="foods_cols", value=list(df_foods.columns))
    ti.xcom_push(key="dv_records", value=df_dv.to_dict(orient="records"))
    ti.xcom_push(key="dv_cols", value=list(df_dv.columns))


def load_to_postgres(**context):
    ti = context["ti"]
    foods_records = ti.xcom_pull(key="foods_records", task_ids="extract_transform_xml")
    foods_cols = ti.xcom_pull(key="foods_cols", task_ids="extract_transform_xml")
    dv_records = ti.xcom_pull(key="dv_records", task_ids="extract_transform_xml")
    dv_cols = ti.xcom_pull(key="dv_cols", task_ids="extract_transform_xml")

    engine = create_engine(DB_URL)

    df_foods = pd.DataFrame.from_records(foods_records or [], columns=foods_cols or [])
    df_dv = pd.DataFrame.from_records(dv_records or [], columns=dv_cols or [])

    df_foods.to_sql(
        name=FOODS_TABLE,
        con=engine,
        schema=SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )

    df_dv.to_sql(
        name=DAILY_VALUES_TABLE,
        con=engine,
        schema=SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )


with DAG(
    dag_id="nutrition_xml_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["hw", "etl", "xml", "postgres"],
) as dag:
    t1 = PythonOperator(task_id="extract_transform_xml", python_callable=extract_transform)
    t2 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    t1 >> t2
