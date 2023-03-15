from datetime import datetime
import pandas as pd
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

@task.python
def run_cannabis_elt():
    headers = {'Accept': 'application/json'}
    response = requests.get("https://random-data-api.com/api/cannabis/random_cannabis?size=10")
    df = pd.DataFrame(response.json())
    postgres_sql_upload = PostgresHook(postgres_conn_id='postgres_default', schema='postgres') 
    postgres_sql_upload.insert_rows('cannabis', df.to_numpy().tolist())

with DAG(
    dag_id="dag_cannabis",
    start_date=datetime(2023,3,15),
    schedule_interval="0 */12 * * *",
    catchup=False,
) as dag:
    create_cannabis_table = PostgresOperator(
        task_id="create_cannabis_table",
        postgres_conn_id="postgres_default",
        sql="sql/cannabis_create_table.sql",
    )
    truncate_cannabis_table = PostgresOperator(
        task_id="truncate_cannabis_table",
        postgres_conn_id="postgres_default",
        sql="truncate table cannabis;"
    )
    upload_cannabis_data = run_cannabis_elt()
    create_cannabis_table >> truncate_cannabis_table >> upload_cannabis_data
