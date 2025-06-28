import requests
import xml.etree.ElementTree as ET
import pandas as pd
import logging

from datetime import datetime

logger = logging.getLogger(__name__)

API_KEY = '********************************'
BASE_URL = 'http://openapi.seoul.go.kr:8088'
SERVICE = 'tbLnOpendataRtmsV'
TYPE = 'xml'
ROWS_PER_PAGE = 1000

def fetch_apt_data(target_yyyymm: str, output_path: str = None) -> pd.DataFrame:
    start_index = 1
    data = []
    rcpt_year = target_yyyymm[:4]

    logger.info(f"[START] Fetching Seoul apartment trade data for {target_yyyymm[:4]}년 {target_yyyymm[4:]}월")

    # 전체 건수 조회
    preview_url = f"{BASE_URL}/{API_KEY}/{TYPE}/{SERVICE}/1/1/{rcpt_year}"
    preview_resp = requests.get(preview_url)
    preview_root = ET.fromstring(preview_resp.content)
    total_count_elem = preview_root.find('.//list_total_count')
    total_count = int(total_count_elem.text) if total_count_elem is not None else 0
    logger.info(f"[INFO] {rcpt_year}년 전체 거래 수: {total_count}")

    found_target_month = False

    while start_index <= total_count:
        end_index = start_index + ROWS_PER_PAGE - 1
        url = f"{BASE_URL}/{API_KEY}/{TYPE}/{SERVICE}/{start_index}/{end_index}/{rcpt_year}"
        response = requests.get(url)
        root = ET.fromstring(response.content)
        rows = root.findall('.//row')

        if not rows:
            logger.info("No more rows to fetch. Exiting loop.")
            break

        target_month_count = 0

        for row in rows:
            ctrt_day = row.findtext('CTRT_DAY')
            bldg_usg = row.findtext('BLDG_USG')

            if ctrt_day and ctrt_day.startswith(target_yyyymm) and bldg_usg == '아파트':
                data.append({
                    'contract_date': ctrt_day,
                    'district': row.findtext('CGG_NM'),
                    'legal_dong': row.findtext('STDG_NM'),
                    'building_name': row.findtext('BLDG_NM'),
                    'price_krw': row.findtext('THING_AMT')
                })
                target_month_count += 1

        logger.info(f"[PROGRESS] {target_month_count} records saved from rows {start_index} to {end_index}")

        # 타겟 월 데이터를 한 번이라도 찾은 이후부터는, 현재 페이지에 없으면 종료
        if found_target_month and target_month_count == 0:
            logger.info(f"[STOP] No more records for {target_yyyymm} → ending data collection.")
            break

        # 이번에 처음 발견된 경우
        if not found_target_month and target_month_count > 0:
            found_target_month = True

        start_index += ROWS_PER_PAGE

    logger.info(f"[COMPLETE] Total records fetched: {len(data)}")

    df = pd.DataFrame(data)
    
    df['contract_date'] = pd.to_datetime(df['contract_date'], format='%Y%m%d', errors='coerce')
    df['price_krw'] = pd.to_numeric(df['price_krw'].str.replace(",", ""), errors='coerce').astype('Int64')
    df['year'] = df['contract_date'].dt.year
    df['month'] = df['contract_date'].dt.month
    df['day'] = df['contract_date'].dt.day
    
    df = df[['year', 'month', 'day','contract_date', 'district', 'legal_dong', 'building_name', 'price_krw']]

    
    if output_path:
        df.to_csv(output_path, index=False)
        logger.info(f"[SAVED] Saved data to {output_path}")

    return df

def save_to_snowflake(df: pd.DataFrame): #, table_name: str, conn_id: str):
    from snowflake.connector.pandas_tools import write_pandas
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    yyyymm = pd.to_datetime(df['contract_date']).dt.strftime("%Y%m").iloc[0]
    
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn_id2")
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # 중복 제거
    check_sql = f"""
        SELECT COUNT(*) 
        FROM RAW_DATA.APT_TRADE 
        WHERE TO_CHAR(contract_date, 'YYYYMM') = '{yyyymm}'
    """
    cursor.execute(check_sql)
    row = cursor.fetchone()
    deleted_count = row[0] if row else 0

    delete_sql = f"""
        DELETE FROM RAW_DATA.APT_TRADE 
        WHERE TO_CHAR(contract_date, 'YYYYMM') = '{yyyymm}'
    """
    cursor.execute(delete_sql)
    conn.commit()
        
    if deleted_count > 0:
        logger.info(f"[DUPLICATE CLEANUP] {deleted_count} rows deleted for '{yyyymm}'.")
    else:
        logger.info(f"[DUPLICATE CLEANUP] No rows found to delete for '{yyyymm}'.")
    
    database="ECOS_DB"
    schema="RAW_DATA"
    table_name = 'APT_TRADE'
    
    logger.info(f"Inserting {len(df)} rows into {table_name}")
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=table_name,
        schema=schema,
        database=database,
        quote_identifiers=False
    )
    logger.info("Insert completed.")
