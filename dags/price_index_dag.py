from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import io, os
import logging


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

# API 호출 및 전처리, 데이터프레임 생성성
@task
def get_price_index_data(start_date: str = "202201") -> pd.DataFrame:

    API_KEY = Variable.get("ecos_api_key")  
    BASE_URL = "https://ecos.bok.or.kr/api/StatisticSearch"
    FREQ = "M"
    PRICE_CODE = "901Y093"     # 매매지수
    ITEM_CODE1 = "H69B"        
    regions = {
        "national": "R70A",    # 전국
        "capital": "R70B",     # 수도권
        "provincial": "R70C",  # 지방
        "seoul": "R70F"        # 서울
    }

    # 종료일 = 현재 월의 -1개월
    now = datetime.now()
    last_month = now.replace(day=1) - timedelta(days=1)
    end = last_month.strftime("%Y%m")
    records = []

    for region_name, region_code in regions.items():
        url = f"{BASE_URL}/{API_KEY}/xml/kr/1/1000/{PRICE_CODE}/{FREQ}/{start_date}/{end}/{ITEM_CODE1}/{region_code}"
        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            root = ET.fromstring(res.content)
        except (requests.RequestException, ET.ParseError) as e:
            raise ValueError(f"API 요청/파싱 실패: {e}")

        for row in root.iter("row"):
            time = row.find("TIME").text
            value = row.find("DATA_VALUE").text
            records.append({
                "TIME": time,
                region_name: float(value) if value not in ["", None] else None
            })

    df = pd.DataFrame(records)
    df = df.groupby("TIME").first().reset_index()
    df["TIME"] = pd.to_datetime(df["TIME"], format="%Y%m")
    df["year"] = df["TIME"].dt.year
    df["month"] = df["TIME"].dt.month
    df = df[["year", "month", "national", "capital", "provincial", "seoul"]]
    
    return df

# CSV 로컬 저장
@task
def save_local_csv(df: pd.DataFrame, ds_nodash: str) -> str:
    path = f"/tmp/price_index/{ds_nodash}.csv"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logging.info(f"로컬 CSV 저장 완료: {path}")
    logger = logging.getLogger(__name__)
    logger.info("CSV 파일 내용:\n%s", df.tail().to_string())
    return path

# Snowflake 업로드
@task
def upload_to_snowflake(csv_path: str):
    
    df = pd.read_csv(csv_path)
    if df.empty:
        logging.warning("업로드할 데이터 없음")

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    logging.info("Snowflake 연결 성공")
    cursor = conn.cursor()
    database = "ECOS_DB"
    schema = "RAW_DATA"
    table = "PRICE_INDEX"

    try:
        cursor.execute("BEGIN;")
        cursor.execute("USE WAREHOUSE COMPUTE_WH;")
        cursor.execute(f"""
            CREATE OR REPLACE TABLE {database}.{schema}.{table}(
                year INT,
                month INT,
                national FLOAT,
                capital FLOAT,
                provincial FLOAT,
                seoul FLOAT
            );
        """)
        cursor.execute("COMMIT;")
        logging.info("Snowflake 테이블 생성 성공")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        logging.error(f"snowflake COPY 실패: {e}")
        raise

    logging.info(f"Snowflake에 {len(df)}개 행 업로드 중...")
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=table,
        schema=schema,
        database=database,
        quote_identifiers=False
    )
    logging.info("업로드 완료")

# DAG 정의
with DAG(
    dag_id="price_index_dag",
    default_args=default_args,
    description='아파트 가격 지수 ETL DAG',
    schedule_interval='@monthly',  # 매월 1일 자정에 실행
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    df_task = get_price_index_data()
    path_task = save_local_csv(df=df_task, ds_nodash="{{ ds_nodash }}")
    upload_task = upload_to_snowflake(csv_path=path_task)

    df_task >> path_task >> upload_task