from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import logging

# ✅ 로깅 설정 추가
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def fetch_base_rate():
    today = datetime.today()
    API_KEY = "********************"
    start_month = "202201"
    end_month = today.strftime("%Y%m")

    logger.info(f"📡 Fetching ECOS API data: {start_month} ~ {end_month}")

    url = (
        f"https://ecos.bok.or.kr/api/StatisticSearch/{API_KEY}/xml/kr/1/1000/"
        f"722Y001/M/{start_month}/{end_month}/0101000"
    )
    res = requests.get(url)
    root = ET.fromstring(res.content)

    time_list, value_list = [], []
    for row in root.iter("row"):
        time = row.find("TIME").text
        value = row.find("DATA_VALUE").text
        time_list.append(time)
        value_list.append(float(value) if value not in ["", None] else None)

    df = pd.DataFrame({"TIME": time_list, "base_rate": value_list})
    df["TIME"] = pd.to_datetime(df["TIME"], format="%Y%m")
    df["year"] = df["TIME"].dt.year
    df["month"] = df["TIME"].dt.month
    final_df = df[["year", "month", "base_rate"]]

    logger.info("📦 Dataframe preview:")
    logger.info(final_df.tail())

    logger.info("🚀 Uploading to Snowflake")
    upload_to_snowflake(final_df)

def upload_to_snowflake(df: pd.DataFrame):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="BASE_RATE",
        database="ECOS_DB",
        schema="RAW_DATA",
        quote_identifiers=False
    )
    logger.info(f"✅ Uploaded {nrows} rows to Snowflake")

with DAG(
    dag_id="fetch_base_rate_to_snowflake_dag",
    default_args=default_args,
    description="Fetch Korean base rate from ECOS API and load into Snowflake",
    schedule_interval="0 9 1 * *",
    start_date=datetime(2024, 6, 1),
    catchup=False,
    tags=["ecos", "base_rate", "snowflake"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_base_rate",
        python_callable=fetch_base_rate,
    )

