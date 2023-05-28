from __future__ import annotations

import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator

import snowflake.connector

conn = snowflake.connector.connect(user='user',
                                    host='host.snowflakecomputing.com',
                                    account='acc1234',
                                    region = 'region',
                                    password ='password',
                                    database='database',      
                                    warehouse='warehouse',  
                                    schema ='schema',
                                    autocommit=True)

curs=conn.cursor()

with DAG(
    dag_id="dag_sutji",
    schedule="0 20 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),

) as dag:

    ingest_data = EmptyOperator(
        task_id="ingest_data",
    )

    daily_gr = BashOperator(
        task_id="create_daily_gr",
        bash_command="python3 /root/coba2/sutji/daily_gr.py ",
    )

    monthly_gr_product = BashOperator(
        task_id="create_monthly_gr_product",
        bash_command="python3 /root/coba2/sutji/monthly_gr_product.py ",
    )

    monthly_order_product = BashOperator(
        task_id="create_monthly_order_product",
        bash_command="python3 /root/coba2/sutji/monthly_oder_product.py ",
    )

    monthly_order_category = BashOperator(
        task_id="create_monthly_order_category",
        bash_command="python3 /root/coba2/sutji/monthly_order_category.py ",
    )

    monthly_order_country = BashOperator(
        task_id="create_monthly_order_country",
        bash_command="python3 /root/coba2/sutji/monthly_order_country.py ",
    )

    ingest_data >> [daily_gr,monthly_gr_product,monthly_order_product,monthly_order_category,monthly_order_country]

if __name__ == "__main__":
    dag.test()