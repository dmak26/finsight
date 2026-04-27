import logging
import os
import uuid
import json
import time
from datetime import timezone, datetime

import pandas as pd
import requests
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

from ingestion.snowflake_client import get_snowflake_connection

INGESTION_ID = str(uuid.uuid4())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")
BASE_URL = "https://www.alphavantage.co/query"

INDICATORS = [
    {"name": "REAL_GDP", "function": "REAL_GDP", "interval": "annual"},
    {"name": "CPI", "function": "CPI", "interval": "monthly"},
    {"name": "UNEMPLOYMENT_RATE", "function": "UNEMPLOYMENT", "interval": "monthly"},
]


def fetch_economic_indicator(indicator: dict) -> pd.DataFrame:
    """ Fetch function for all 3 indicators.Sharing same json pattern """

    if not API_KEY:
        raise ValueError("Missing ALPHA_VANTAGE_KEY in environment variables")

    indicator_name = indicator["name"]
    function_name = indicator["function"]
    interval = indicator["interval"]

    logger.info("Fetching economic indicator: %s", indicator_name)

    params = {
        "function": function_name,
        "interval": interval,
        "apikey": API_KEY,
    }

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "Information" in data:
        raise RuntimeError(f"Alpha Vantage rate limit/info message: {data['Information']}")

    if "Note" in data:
        raise RuntimeError(f"Alpha Vantage rate limit note: {data['Note']}")

    if "data" not in data:
        logger.error("Unexpected response for %s: %s", indicator_name, data)
        raise ValueError(f"No economic indicator data returned for {indicator_name}")

    records = []

    for row in data["data"]:
        records.append(
            {
                "INDICATOR_NAME": indicator_name,
                "OBSERVATION_DATE": row.get("date"),
                "OBSERVATION_VALUE": row.get("value"),
                "UNIT": data.get("unit"),
                "INTERVAL": interval,
                "RAW_PAYLOAD": json.dumps(row),
                "INGESTION_ID": INGESTION_ID,
                "SOURCE_SYSTEM": "ALPHA_VANTAGE",
                "ENDPOINT_NAME": function_name,
                "LOADED_AT": datetime.now(timezone.utc),
            }
        )

    df = pd.DataFrame(records)

    df["OBSERVATION_DATE"] = pd.to_datetime(df["OBSERVATION_DATE"], errors="coerce").dt.date
    df["OBSERVATION_VALUE"] = pd.to_numeric(df["OBSERVATION_VALUE"], errors="coerce")

    logger.info("Fetched %s rows for %s", len(df), indicator_name)
    return df


def load_economic_indicator_to_snowflake(df: pd.DataFrame, indicator_name: str) -> None:
    """ Load + Merge function   """

    if df.empty:
        logger.warning("No rows to load for %s", indicator_name)
        return

    conn = get_snowflake_connection()
    cursor = None

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TEMPORARY TABLE STG_ECONOMIC_INDICATORS_SESSION
            LIKE FINSIGHT.RAW.ECONOMIC_INDICATORS_RAW
            """
        )

        logger.info("Loading %s rows for %s into temporary staging", len(df), indicator_name)

        success, nchunks, nrows, output = write_pandas(
            conn=conn,
            df=df,
            table_name="STG_ECONOMIC_INDICATORS_SESSION",
            auto_create_table=False,
        )

        if not success:
            raise RuntimeError(f"write_pandas failed for {indicator_name}: {output}")

        merge_sql = """
            MERGE INTO FINSIGHT.RAW.ECONOMIC_INDICATORS_RAW AS tgt
            USING STG_ECONOMIC_INDICATORS_SESSION AS src
                ON tgt.INDICATOR_NAME = src.INDICATOR_NAME
               AND tgt.OBSERVATION_DATE = src.OBSERVATION_DATE

            WHEN MATCHED THEN UPDATE SET
                tgt.OBSERVATION_VALUE = src.OBSERVATION_VALUE,
                tgt.UNIT = src.UNIT,
                tgt.INTERVAL = src.INTERVAL,
                tgt.RAW_PAYLOAD = PARSE_JSON(src.RAW_PAYLOAD),
                tgt.INGESTION_ID = src.INGESTION_ID,
                tgt.SOURCE_SYSTEM = src.SOURCE_SYSTEM,
                tgt.ENDPOINT_NAME = src.ENDPOINT_NAME,
                tgt.LOADED_AT = src.LOADED_AT

            WHEN NOT MATCHED THEN INSERT (
                INDICATOR_NAME,
                OBSERVATION_DATE,
                OBSERVATION_VALUE,
                UNIT,
                INTERVAL,
                RAW_PAYLOAD,
                INGESTION_ID,
                SOURCE_SYSTEM,
                ENDPOINT_NAME,
                LOADED_AT
            )
            VALUES (
                src.INDICATOR_NAME,
                src.OBSERVATION_DATE,
                src.OBSERVATION_VALUE,
                src.UNIT,
                src.INTERVAL,
                PARSE_JSON(src.RAW_PAYLOAD),
                src.INGESTION_ID,
                src.SOURCE_SYSTEM,
                src.ENDPOINT_NAME,
                src.LOADED_AT
            )
        """

        cursor.execute(merge_sql)

        duplicate_check_sql = """
            SELECT INDICATOR_NAME, OBSERVATION_DATE, COUNT(*) AS CNT
            FROM FINSIGHT.RAW.ECONOMIC_INDICATORS_RAW
            GROUP BY INDICATOR_NAME, OBSERVATION_DATE
            HAVING COUNT(*) > 1
        """

        cursor.execute(duplicate_check_sql)
        duplicates = cursor.fetchall()

        if duplicates:
            raise ValueError(f"Duplicate indicator/date found: {duplicates[:10]}")

        logger.info("Merged %s rows for %s into RAW.ECONOMIC_INDICATORS_RAW", nrows, indicator_name)

    finally:
        if cursor:
            cursor.close()
        conn.close()

def run_ingestion(indicators: list[dict] = INDICATORS) -> None:
    """ Ingestion phase """
    
    logger.info("Starting economic indicators ingestion for %s indicators", len(indicators))

    success_count = 0
    fail_count = 0

    for indicator in indicators:
        indicator_name = indicator["name"]

        try:
            df = fetch_economic_indicator(indicator)
            load_economic_indicator_to_snowflake(df, indicator_name)
            success_count += 1
        except Exception as e:
            logger.error("Failed to process %s: %s", indicator_name, e)
            fail_count += 1

        time.sleep(15)

    logger.info("Ingestion complete. Success: %s Failed: %s", success_count, fail_count)


if __name__ == "__main__":
    run_ingestion()