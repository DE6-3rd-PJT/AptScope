from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
from plugins.apt_trade_monthly import fetch_apt_data, save_to_snowflake
from dateutil.relativedelta import relativedelta

import requests
import pandas as pd
import xml.etree.ElementTree as ET
import logging

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}


def fetch(**kwargs):
    logger = logging.getLogger(__name__)
    execution_date = kwargs['execution_date']
    target_date = execution_date - relativedelta(months=1)
    yyyymm = target_date.strftime("%Y%m")
    
    output_path = f"/opt/airflow/data/apt_trade_{yyyymm}.csv"
    df = fetch_apt_data(yyyymm, output_path=output_path)

    # 저장한 경로를 다음 태스크에 넘겨주기 위해 push
    kwargs['ti'].xcom_push(key='csv_path', value=output_path)

def upload(**kwargs):
    logger = logging.getLogger(__name__)
    
    #fetch함수에서 사용한 csv_path를 pull하는 코드
    csv_path = kwargs['ti'].xcom_pull(key='csv_path', task_ids='fetch_apt_data')

    df = pd.read_csv(csv_path)

    if not df.empty:
        save_to_snowflake(df)
    else:
        logger.warning("No data to upload to Snowflake")


    
with DAG(
    dag_id='collect_seoul_apt_trades',
    default_args=default_args,
    description='서울 아파트 실거래가 수집 후 Snowflake 저장',
    start_date=datetime(2022, 1, 1),
    schedule_interval='0 2 10 * *',
    catchup=True,
    tags=['real_estate', 'seoul']
) as dag:

    fetch = PythonOperator(
        task_id='fetch_apt_data',
        python_callable=fetch,
        provide_context=True
    )

    upload = PythonOperator(
        task_id='upload_to_snowflake',
        python_callable=upload,
        provide_context=True
    )

    fetch >> upload