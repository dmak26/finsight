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

INGESTION_ID = str(uuid.uuid4())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")
BASE_URL = "https://www.alphavantage.co/query"

# TICKERS = ["GS"]
TICKERS = ["JPM", "BAC", "GS", "MS", "WFC"]


def fetch_daily_prices(ticker: str) -> pd.DataFrame:
    if not API_KEY:
        raise ValueError("Missing ALPHA_VANTAGE_KEY in environment variables")

    logger.info("Fetching daily prices for %s", ticker)

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "outputsize": "compact",
        "apikey": API_KEY,
    }

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "Information" in data:
        raise RuntimeError(f"Alpha Vantage rate limit/info message: {data['Information']}")

    if "Note" in data:
        raise RuntimeError(f"Alpha Vantage rate limit note: {data['Note']}")

    if "Time Series (Daily)" not in data:
        logger.error("Unexpected response for %s: %s", ticker, data)
        raise ValueError(f"No time series data returned for {ticker}")

    records = []

    for trade_date, values in data["Time Series (Daily)"].items():
        records.append(
            {
                "TICKER": ticker,
                "TRADE_DATE": trade_date,
                "OPEN_PRICE": values["1. open"],
                "HIGH_PRICE": values["2. high"],
                "LOW_PRICE": values["3. low"],
                "CLOSE_PRICE": values["4. close"],
                "VOLUME": values["5. volume"],
                "LOADED_AT": datetime.now(timezone.utc),
            }
        )

    df = pd.DataFrame(records)

    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"]).dt.date
    df["OPEN_PRICE"] = df["OPEN_PRICE"].astype(float)
    df["HIGH_PRICE"] = df["HIGH_PRICE"].astype(float)
    df["LOW_PRICE"] = df["LOW_PRICE"].astype(float)
    df["CLOSE_PRICE"] = df["CLOSE_PRICE"].astype(float)
    df["VOLUME"] = df["VOLUME"].astype(int)
    df["LOADED_AT"] = pd.to_datetime(df["LOADED_AT"])
    df["INGESTION_ID"] = INGESTION_ID
    df["SOURCE_SYSTEM"] = "ALPHA_VANTAGE"
    df["ENDPOINT_NAME"] = "TIME_SERIES_DAILY"

    logger.info("Fetched %s rows for %s", len(df), ticker)
    return df


def load_prices_to_snowflake(df: pd.DataFrame, ticker: str) -> None:
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
            CREATE TEMPORARY TABLE STG_STOCK_PRICES_SESSION
            LIKE FINSIGHT.RAW.STOCK_PRICES_RAW
            """
        )

        logger.info("Loading %s rows for %s into temporary staging", len(df), ticker)

        success, nchunks, nrows, output = write_pandas(
            conn=conn,
            df=df,
            table_name="STG_STOCK_PRICES_SESSION",
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
            MERGE INTO FINSIGHT.RAW.STOCK_PRICES_RAW AS tgt
            USING STG_STOCK_PRICES_SESSION AS src
                ON tgt.TICKER = src.TICKER
               AND tgt.TRADE_DATE = src.TRADE_DATE

            WHEN MATCHED THEN UPDATE SET
                tgt.OPEN_PRICE = src.OPEN_PRICE,
                tgt.HIGH_PRICE = src.HIGH_PRICE,
                tgt.LOW_PRICE = src.LOW_PRICE,
                tgt.CLOSE_PRICE = src.CLOSE_PRICE,
                tgt.VOLUME = src.VOLUME,
                tgt.LOADED_AT = src.LOADED_AT,
                tgt.INGESTION_ID = src.INGESTION_ID,
                tgt.SOURCE_SYSTEM = src.SOURCE_SYSTEM,
                tgt.ENDPOINT_NAME = src.ENDPOINT_NAME

            WHEN NOT MATCHED THEN INSERT (
                TICKER,
                TRADE_DATE,
                OPEN_PRICE,
                HIGH_PRICE,
                LOW_PRICE,
                CLOSE_PRICE,
                VOLUME,
                LOADED_AT,
                INGESTION_ID,
                SOURCE_SYSTEM,
                ENDPOINT_NAME
            )
            VALUES (
                src.TICKER,
                src.TRADE_DATE,
                src.OPEN_PRICE,
                src.HIGH_PRICE,
                src.LOW_PRICE,
                src.CLOSE_PRICE,
                src.VOLUME,
                src.LOADED_AT,
                src.INGESTION_ID,
                src.SOURCE_SYSTEM,
                src.ENDPOINT_NAME
            )
        """

        cursor.execute(merge_sql)
        logger.info("Merged %s staged rows for %s into RAW.STOCK_PRICES_RAW", nrows, ticker)

        duplicate_check_sql = """
            SELECT TICKER, TRADE_DATE, COUNT(*) AS CNT
            FROM FINSIGHT.RAW.STOCK_PRICES_RAW
            GROUP BY TICKER, TRADE_DATE
            HAVING COUNT(*) > 1
        """

        cursor.execute(duplicate_check_sql)
        duplicates = cursor.fetchall()

        if duplicates:
            raise ValueError(f"Duplicate TICKER + TRADE_DATE found: {duplicates[:10]}")

        logger.info("Duplicate guardrail passed for %s", ticker)

    except Exception as e:
        logger.error("Failed to load/merge prices for %s: %s", ticker, e)
        raise

    finally:
        if cursor:
            cursor.close()
        conn.close()


def run_ingestion(tickers: list[str] = TICKERS) -> None:
    logger.info("Starting stock price ingestion for %s tickers", len(tickers))

    success_count = 0
    fail_count = 0

    for ticker in tickers:
        try:
            df = fetch_daily_prices(ticker)
            load_prices_to_snowflake(df, ticker)
            success_count += 1
        except Exception as e:
            logger.error("Failed to process %s: %s", ticker, e)
            fail_count += 1

        time.sleep(15)

    logger.info("Ingestion complete. Success: %s  Failed: %s", success_count, fail_count)


if __name__ == "__main__":
    run_ingestion()