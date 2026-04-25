import logging
import os
import uuid
from datetime import timezone, datetime

import pandas as pd
import requests
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

from ingestion.snowflake_client import get_snowflake_connection
import time
import json

INGESTION_ID = str(uuid.uuid4())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")
BASE_URL = "https://www.alphavantage.co/query"
TICKERS = ["JPM", "BAC", "GS", "MS", "WFC"]
# TICKERS = ["JPM"]  
ENDPOINT_NAME = "OVERVIEW"
TARGET_TABLE = "COMPANY_OVERVIEW_RAW"


def fetch_company_overview(ticker: str) -> pd.DataFrame:
    if not API_KEY:
        raise ValueError("Missing ALPHA_VANTAGE_KEY in environment variables")

    logger.info("Fetching company overview for %s", ticker)

    params = {
        "function": "OVERVIEW",
        "symbol": ticker,
        "apikey": API_KEY,
    }

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "Information" in data:
        raise RuntimeError(f"Alpha Vantage rate limit/info message: {data['Information']}")

    if "Note" in data:
        raise RuntimeError(f"Alpha Vantage rate limit note: {data['Note']}")

    if "Symbol" not in data or not data.get("Symbol"):
        logger.error("Unexpected overview response for %s: %s", ticker, data)
        raise ValueError(f"No company overview data returned for {ticker}")

    records = []

    records.append(
        {
            "SYMBOL": data.get("Symbol"),
            "ASSET_TYPE": data.get("AssetType"),
            "COMPANY_NAME": data.get("Name"),
            "CIK": data.get("CIK"),
            "EXCHANGE": data.get("Exchange"),
            "CURRENCY": data.get("Currency"),
            "COUNTRY": data.get("Country"),
            "SECTOR": data.get("Sector"),
            "INDUSTRY": data.get("Industry"),
            "LATEST_QUARTER": data.get("LatestQuarter"),
            "MARKET_CAPITALIZATION": data.get("MarketCapitalization"),
            "PERATIO": data.get("PERatio"),
            "EPS": data.get("EPS"),
            "BETA": data.get("Beta"),
            "FIFTY_TWO_WEEK_HIGH": data.get("52WeekHigh"),
            "FIFTY_TWO_WEEK_LOW": data.get("52WeekLow"),

            "RAW_PAYLOAD": json.dumps(data),

            "INGESTION_ID": INGESTION_ID,
            "SOURCE_SYSTEM": "ALPHA_VANTAGE",
            "ENDPOINT_NAME": "OVERVIEW",
            "LOADED_AT": datetime.now(timezone.utc),
        }
    )

    df = pd.DataFrame(records)

    df["LATEST_QUARTER"] = pd.to_datetime(df["LATEST_QUARTER"], errors="coerce").dt.date
    df["MARKET_CAPITALIZATION"] = pd.to_numeric(df["MARKET_CAPITALIZATION"], errors="coerce")
    df["PERATIO"] = pd.to_numeric(df["PERATIO"], errors="coerce")
    df["EPS"] = pd.to_numeric(df["EPS"], errors="coerce")
    df["BETA"] = pd.to_numeric(df["BETA"], errors="coerce")
    df["FIFTY_TWO_WEEK_HIGH"] = pd.to_numeric(df["FIFTY_TWO_WEEK_HIGH"], errors="coerce")
    df["FIFTY_TWO_WEEK_LOW"] = pd.to_numeric(df["FIFTY_TWO_WEEK_LOW"], errors="coerce")

    logger.info("Fetched %s rows for %s", len(df), ticker)
    return df


def load_company_overview_to_snowflake(df: pd.DataFrame, ticker: str) -> None:
    if df.empty:
        logger.warning("No rows to load for %s", ticker)
        return

    conn = get_snowflake_connection()
    cursor = None

    try:
        cursor = conn.cursor()
        # -- Never used temp call like that using like from the raw table to be established, Snowflake can
        cursor.execute(
            """
            CREATE TEMPORARY TABLE STG_COMPANY_OVERVIEW_SESSION
            LIKE FINSIGHT.RAW.COMPANY_OVERVIEW_RAW
            """
        )

        logger.info("Loading %s rows for %s into temporary staging", len(df), ticker)

        success, nchunks, nrows, output = write_pandas(
            conn=conn,
            df=df,
            table_name="STG_COMPANY_OVERVIEW_SESSION",
            auto_create_table=False,
        )

        logger.info(
            "write_pandas result for %s: success=%s, chunks=%s, rows=%s",
            ticker,
            success,
            nchunks,
            nrows,
        )

        if not success:
            raise RuntimeError(f"write_pandas failed for {ticker}: {output}")

        merge_sql = """
            MERGE INTO FINSIGHT.RAW.COMPANY_OVERVIEW_RAW AS tgt
            USING STG_COMPANY_OVERVIEW_SESSION AS src
                ON tgt.SYMBOL = src.SYMBOL

            WHEN MATCHED THEN UPDATE SET
                tgt.ASSET_TYPE = src.ASSET_TYPE,
                tgt.COMPANY_NAME = src.COMPANY_NAME,
                tgt.CIK = src.CIK,
                tgt.EXCHANGE = src.EXCHANGE,
                tgt.CURRENCY = src.CURRENCY,
                tgt.COUNTRY = src.COUNTRY,
                tgt.SECTOR = src.SECTOR,
                tgt.INDUSTRY = src.INDUSTRY,
                tgt.LATEST_QUARTER = src.LATEST_QUARTER,
                tgt.MARKET_CAPITALIZATION = src.MARKET_CAPITALIZATION,
                tgt.PERATIO = src.PERATIO,
                tgt.EPS = src.EPS,
                tgt.BETA = src.BETA,
                tgt.FIFTY_TWO_WEEK_HIGH = src.FIFTY_TWO_WEEK_HIGH,
                tgt.FIFTY_TWO_WEEK_LOW = src.FIFTY_TWO_WEEK_LOW,
                tgt.RAW_PAYLOAD = PARSE_JSON(src.RAW_PAYLOAD),
                tgt.INGESTION_ID = src.INGESTION_ID,
                tgt.SOURCE_SYSTEM = src.SOURCE_SYSTEM,
                tgt.ENDPOINT_NAME = src.ENDPOINT_NAME,
                tgt.LOADED_AT = src.LOADED_AT

            WHEN NOT MATCHED THEN INSERT (
                SYMBOL,
                ASSET_TYPE,
                COMPANY_NAME,
                CIK,
                EXCHANGE,
                CURRENCY,
                COUNTRY,
                SECTOR,
                INDUSTRY,
                LATEST_QUARTER,
                MARKET_CAPITALIZATION,
                PERATIO,
                EPS,
                BETA,
                FIFTY_TWO_WEEK_HIGH,
                FIFTY_TWO_WEEK_LOW,
                RAW_PAYLOAD,
                INGESTION_ID,
                SOURCE_SYSTEM,
                ENDPOINT_NAME,
                LOADED_AT
            )
            VALUES (
                src.SYMBOL,
                src.ASSET_TYPE,
                src.COMPANY_NAME,
                src.CIK,
                src.EXCHANGE,
                src.CURRENCY,
                src.COUNTRY,
                src.SECTOR,
                src.INDUSTRY,
                src.LATEST_QUARTER,
                src.MARKET_CAPITALIZATION,
                src.PERATIO,
                src.EPS,
                src.BETA,
                src.FIFTY_TWO_WEEK_HIGH,
                src.FIFTY_TWO_WEEK_LOW,
                PARSE_JSON(src.RAW_PAYLOAD),
                src.INGESTION_ID,
                src.SOURCE_SYSTEM,
                src.ENDPOINT_NAME,
                src.LOADED_AT
            )
        """

        cursor.execute(merge_sql)
        logger.info("Merged %s staged rows for %s into RAW.COMPANY_OVERVIEW_RAW", nrows, ticker)

        duplicate_check_sql = """
            SELECT SYMBOL, COUNT(*) AS CNT
            FROM FINSIGHT.RAW.COMPANY_OVERVIEW_RAW
            GROUP BY SYMBOL
            HAVING COUNT(*) > 1;
        """

        cursor.execute(duplicate_check_sql)
        duplicates = cursor.fetchall()

        if duplicates:
            raise ValueError(f"Duplicate SYMBOL found: {duplicates[:10]}")

        logger.info("Duplicate guardrail passed for %s", ticker)

    except Exception as e:
        logger.error("Failed to load/merge companies for %s: %s", ticker, e)
        raise

    finally:
        if cursor:
            cursor.close()
        conn.close()


def run_ingestion(tickers: list[str] = TICKERS) -> None:
    logger.info("Starting company overview ingestion for %s tickers", len(tickers))

    success_count = 0
    fail_count = 0

    for ticker in tickers:
        try:
            df = fetch_company_overview(ticker)
            load_company_overview_to_snowflake(df, ticker)
            success_count += 1
        except Exception as e:
            logger.error("Failed to process %s: %s", ticker, e)
            fail_count += 1

        time.sleep(15)

    logger.info("Ingestion complete. Success: %s  Failed: %s", success_count, fail_count)


if __name__ == "__main__":
    run_ingestion()