import os
import glob
from sqlite3 import Timestamp
from typing import List
import json
from datetime import datetime
import psycopg2

from airflow.hooks.postgres_hook import PostgresHook

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator


# cat ~/.aws/credentials
aws_access_key_id = 'ASIAQ6I4YHZXLCDPULMT'
aws_secret_access_key = 'yjwx3AWDqKg74fbTy3wsS+rriacSeoiP5Q2w9hV2'
aws_session_token = 'FwoGZXIvYXdzECEaDFkhxgAdKDgy0hysESLIAQjOAoRsx8mRyK0pU3y7/+vXxHaXT1ZqMz9bYv0IpQLi4zvHVtpQMemDJUS+v4LCSFRkqawP3VCmwbcyfeeWWJNKUFbveU+a+GiDi5hJP/gfJMut7mEBVELcl8v0kYkli1V5v9q2suQYKUxov/l80hEYij+RFnO1U2NU+YhT0Y7c+b3qRkX4inUFwiA0eVR2YFzh2QdbuLDFNFbtV9E35tsBwOxakskbUp3z8bDuQnCn/UFAa13PaFkZVKC9/U8zOYk77zLPa5xUKM/Z/JwGMi1C1Pt/ydytIkCPhVhsvh3qll2t0ChmD3eWIUOaW1UkiuQwJMlLgUxl+gWDnuo='


def _create_tables():

    hook = PostgresHook(postgres_conn_id="my-redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    create_table_queries = [
        """
       DROP TABLE IF EXISTS ceramic_sales
        """,
        """
        CREATE TABLE IF NOT EXISTS ceramic_sales (
            year text	
            ,month	text
            ,country text
            ,quotation text	
            ,sales_type text	
            ,customer_name text	
            ,material_code text	
            ,material_name text	
            ,product_hier text	
            ,material_group3 text	
            ,material_group4 text	
            ,launching_date date	
            ,sales_vol_set text
            ,sales_vol_pcs text 
            ,gross_sales_mkt text
            ,net_sales_mkt text 
            ,net_sales_acc text 
            ,std_vc text 
            ,rebate text	
            ,direct_selling text 
            ,contribution_mkt text
            ,contribution_acc text
        )
        """,
        """
       DROP TABLE IF EXISTS ceramic_sales_data_wh
        """,
        """
        CREATE TABLE IF NOT EXISTS ceramic_sales_data_wh (
            year text	
            ,month	text
            ,country text
            ,customer_name text	
            ,material_code text	
            ,material_name text	
            ,product_hier text	
            ,material_group3 text	
            ,material_group4 text	
            ,sales_vol_set decimal 
            ,gross_sales_mkt decimal 
            ,net_sales_mkt decimal 
            ,contribution_mkt decimal
        )
        """
    ]

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def _load_staging_tables():

    hook = PostgresHook(postgres_conn_id="my-redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    copy_table_queries = [
        """
        COPY ceramic_sales
        FROM 's3://mind-capstone/Ceramic_sales.csv'
        ACCESS_KEY_ID '{0}'
        SECRET_ACCESS_KEY '{1}'
        SESSION_TOKEN '{2}'
        CSV
        DELIMITER ','
        DATEFORMAT 'MM/DD/YYYY'
        ACCEPTINVCHARS 
        IGNOREHEADER 1
        """
    ]

    for query in copy_table_queries:
        cur.execute(query.format(aws_access_key_id, aws_secret_access_key, aws_session_token))
        conn.commit()


def _insert_dwh_tables():

    hook = PostgresHook(postgres_conn_id="my-redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    insert_dwh_queries = [
    """
    INSERT INTO ceramic_sales_data_wh
    SELECT year
            ,month
            ,country
            ,customer_name
            ,material_code
            ,material_name
            ,product_hier
            ,material_group3	
            ,material_group4
            ,REPLACE(sales_vol_set, ',', '')
            ,REPLACE(gross_sales_mkt, ',', '')
            ,REPLACE(net_sales_mkt, ',', '')
            ,REPLACE(contribution_mkt, ',', '')
    FROM ceramic_sales
    
    """,
]
    for query in insert_dwh_queries:
        cur.execute(query)
        conn.commit()


with DAG(
    'Capstone',
    start_date = timezone.datetime(2022, 12, 1), # Start of the flow
    schedule = '@monthly', # Run once a month at midnight of the first day of the month
    tags = ['capstone'],
    catchup = False, # No need to catchup the missing run since start_date
) as dag:


    create_tables = PythonOperator(
        task_id = 'create_tables',
        python_callable = _create_tables,
    )

    load_staging_tables = PythonOperator(
       task_id = 'load_staging_tables',
       python_callable = _load_staging_tables,
    )

    insert_dwh_tables = PythonOperator(
       task_id = 'insert_dwh_tables',
       python_callable = _insert_dwh_tables,
    )

    create_tables >> load_staging_tables >> insert_dwh_tables