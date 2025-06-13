from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API 연결
API_KEY = '7O72EQ764L9SZ8ROR12J'
CYCLE = 'M'

# CSI 지역 코드
CSI_REGION_CODES = [
    'Z11', 'Z12', 'Z19', 'Z14', 'Z16', 'Z24',
    'Z21', 'Z18', 'Z17', 'Z15', 'Z22', 'Z20', 'Z23'
]

# 미분양주택현황 지역 코드
UNSOLD_REGION_CODES = [
    'I410A', 'I410B', 'I410C', 'I410D', 'I410E', 'I410F',
    'I410G', 'I410H', 'I410I', 'I410J', 'I410K', 'I410L',
    'I410M', 'I410N', 'I410O', 'I410P', 'I410Q', 'I410R', 'I410S'
]

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

def fetch_and_upload_direct(api_type, execution_date, **kwargs):
    year_month = execution_date.strftime("%Y%m")
    rows = []

    if api_type == 'CSI':
        STAT_CODE = '511Y004'
        ITEM_CODE1 = 'FMFB'
        REGION_CODES = CSI_REGION_CODES
        VALUE_COL = 'index_value'
        REGION_FIELD = 'ITEM_NAME2'
        TABLE_NAME = 'HOUSING_CSI'
    else:
        STAT_CODE = '901Y074'
        ITEM_CODE1 = None
        REGION_CODES = UNSOLD_REGION_CODES
        VALUE_COL = 'unsold_units'
        REGION_FIELD = 'ITEM_NAME1'
        TABLE_NAME = 'UNSOLD_HOUSE'

    for code in REGION_CODES:
        url = f"https://ecos.bok.or.kr/api/StatisticSearch/{API_KEY}/xml/kr/1/1000/{STAT_CODE}/{CYCLE}/{year_month}/{year_month}"
        if ITEM_CODE1:
            url += f"/{ITEM_CODE1}/{code}"
        else:
            url += f"/{code}"

        response = requests.get(url)
        logger.info(f"[{api_type}] 요청: {code} → 응답 코드: {response.status_code}")
        if response.status_code != 200:
            continue

        root = ET.fromstring(response.text)
        for row in root.findall("row"):
            rows.append({
                'month': row.findtext('TIME'),
                'region': row.findtext(REGION_FIELD),
                VALUE_COL: row.findtext('DATA_VALUE')
            })
        time.sleep(2)

    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning(f"{api_type} 데이터 없음: {year_month}")
        return

    df['year'] = df['month'].str[:4].astype(int)
    df['month'] = df['month'].str[4:].astype(int)
    df[VALUE_COL] = df[VALUE_COL].astype(float).astype('Int64')
    df = df[['year', 'month', 'region', VALUE_COL]]

    logger.info(f"{api_type} 데이터프레임 미리보기:\n{df.tail()}")

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    temp_table = f"TEMP_{TABLE_NAME}"

    # 1. TEMP 테이블에 덮어쓰기
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=temp_table,
        database="ECOS_DB",
        schema="RAW_DATA",
        overwrite=True,
        quote_identifiers=False
    )
    logger.info(f"임시 테이블 적재 완료: {nrows} rows → {temp_table}")

    # 2. MERGE 수행
    with conn.cursor() as cur:
        merge_query = f"""
            MERGE INTO ECOS_DB.RAW_DATA.{TABLE_NAME} AS target
            USING ECOS_DB.RAW_DATA.{temp_table} AS source
            ON target.year = source.year
               AND target.month = source.month
               AND target.region = source.region
            WHEN MATCHED THEN
                UPDATE SET target.{VALUE_COL} = source.{VALUE_COL}
            WHEN NOT MATCHED THEN
                INSERT (year, month, region, {VALUE_COL})
                VALUES (source.year, source.month, source.region, source.{VALUE_COL});
        """
        cur.execute(merge_query)
        logger.info(f"🔁 MERGE 완료: {TABLE_NAME} ← {temp_table}")

with DAG(
    dag_id="ecos_to_snowflake_direct_dag",
    start_date=datetime(2022, 1, 1),
    schedule_interval="0 9 1 * *",
    catchup=True,
    default_args=default_args,
    max_active_runs=1,
    tags=["ecos", "snowflake", "monthly"],
) as dag:

    fetch_upload_csi = PythonOperator(
        task_id="fetch_upload_csi_direct",
        python_callable=fetch_and_upload_direct,
        op_kwargs={'api_type': 'CSI'},
        provide_context=True,
    )

    fetch_upload_unsold = PythonOperator(
        task_id="fetch_upload_unsold_direct",
        python_callable=fetch_and_upload_direct,
        op_kwargs={'api_type': 'UNSOLD'},
        provide_context=True,
    )

    fetch_upload_csi >> fetch_upload_unsold