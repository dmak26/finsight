import requests
import pandas as pd
from datetime import datetime, UTC
from dotenv import load_dotenv
import os
import logging
from ingestion.snowflake_client import get_snowflake_connection, load_dataframe

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")
BASE_URL = "https://www.alphavantage.co/query"

TICKERS = ["JPM", "BAC", "GS", "MS", "WFC"]

def fetch_daily_prices(ticker: str) -> pd.DataFrame:
    """
    Fetch daily stock prices for a given ticker from Alpha Vantage.
    """
    logger.info("Fetching daily prices for %s", ticker)

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "outputsize": "compact",
        "apikey": API_KEY
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    if "Time Series (Daily)" not in data:
        logger.error("Unexpected response for %s: %s", ticker, data)
        raise ValueError(f"No time series data returned for {ticker}")

    time_series = data["Time Series (Daily)"]

    records = []
    for trade_date, values in time_series.items():
        records.append({
            "TICKER":       ticker,
            "TRADE_DATE":   trade_date,
            "OPEN_PRICE":   values["1. open"],
            "HIGH_PRICE":   values["2. high"],
            "LOW_PRICE":    values["3. low"],
            "CLOSE_PRICE":  values["4. close"],
            "VOLUME":       values["5. volume"],
            "LOADED_AT":    datetime.now(UTC)
        })

    df = pd.DataFrame(records)
    logger.info("Fetched %s rows for %s", len(df), ticker)
    return df


def load_prices_to_snowflake(df: pd.DataFrame, ticker: str) -> None:
    """
    Load a DataFrame of stock prices into Snowflake RAW layer.
    """
    conn = get_snowflake_connection()
    try:
        from snowflake.connector.pandas_tools import write_pandas
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            "STOCK_PRICES_RAW",
            database="FINSIGHT",
            schema="RAW",
            auto_create_table=False 
        )
        if success:
            logger.info("Loaded %s rows for %s into RAW.STOCK_PRICES_RAW", nrows, ticker)
        else:
            logger.error("Load failed for %s", ticker)
    finally:
        conn.close()


def run_ingestion(tickers: list = TICKERS) -> None:
    """
    Main ingestion function — fetch and load all tickers.
    """
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

    logger.info("Ingestion complete. Success: %s  Failed: %s", 
                success_count, fail_count)


if __name__ == "__main__":
    run_ingestion()