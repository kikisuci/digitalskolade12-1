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

curs.execute("""
        INSERT INTO daily_gr
        SELECT *
        FROM SUTJI_DM_TOTAL_GR_PER_DATE;        
        """
            )

curs.execute("""                
        INSERT INTO monthly_gr_product 
        SELECT *
        FROM SUTJI_DM_TOTAL_GR_PER_PRODUCT_PER_MONTH;
        """
            )

curs.execute("""                
        INSERT INTO monthly_order_product
        SELECT *
        FROM SUTJI_DM_TOTAL_ORDER_PER_PRODUCT_PER_MONTH;
        """
            )

curs.execute("""                
        INSERT INTO monthly_order_category
        SELECT *
        FROM SUTJI_DM_TOTAL_ORDER_PER_CATEGORY_PER_MONTH;
        """
            )

curs.execute("""                
        INSERT INTO monthly_order_country 
        SELECT *
        FROM SUTJI_DM_TOTAL_ORDER_PER_COUNTRY_PER_MONTH;
        """
            )