
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="iot_temp_csv_to_postgres",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["hw2", "csv", "postgres"],
) as dag:

    load_and_clean = PostgresOperator(
        task_id="load_and_clean",
        postgres_conn_id="postgres_default",
        sql="""
        drop table if exists public.iot_temp_clean;

        create table public.iot_temp_clean as
        select
            id,
            room_id,
            to_date(substring(noted_date, 1, 10), 'DD-MM-YYYY') as noted_date,
            temp
        from public.iot_temp_raw
        where out_in = 'In'
          and temp between
            (select percentile_cont(0.05) within group (order by temp) from public.iot_temp_raw)
            and
            (select percentile_cont(0.95) within group (order by temp) from public.iot_temp_raw);
        """,
        autocommit=True
    )

    top_days = PostgresOperator(
        task_id="top_hot_and_cold_days",
        postgres_conn_id="postgres_default",
        sql="""
        drop table if exists public.iot_top_days;

        create table public.iot_top_days as
        (
            select
                noted_date,
                avg(temp) as avg_temp,
                'hot' as type
            from public.iot_temp_clean
            group by noted_date
            order by avg_temp desc
            limit 5
        )
        union all
        (
            select
                noted_date,
                avg(temp) as avg_temp,
                'cold' as type
            from public.iot_temp_clean
            group by noted_date
            order by avg_temp asc
            limit 5
        );
        """
    )

    load_and_clean >> top_days
