create table if not exists public.iot_temp_raw (
    id text,
    room_id text,
    noted_date text,
    temp int,
    out_in text
);

copy public.iot_temp_raw
from '/opt/airflow/data/IOT-temp.csv'
delimiter ','
csv header;
