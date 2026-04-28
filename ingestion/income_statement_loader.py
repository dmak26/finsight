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

ENDPOINT_NAME = "INCOME_STATEMENT"
# TICKERS = ["JPM"]
TICKERS = ["JPM", "BAC", "GS", "MS", "WFC"]
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")
BASE_URL = "https://www.alphavantage.co/query"

def fetch_income_statement(ticker: str) -> pd.DataFrame:
    """ Fetch function for annual and quarterly income statement """

    if not API_KEY:
        raise ValueError("Missing ALPHA_VANTAGE_KEY in environment variables")

    logger.info("Fetching income statement for %s", ticker)

    params = {
        "function": ENDPOINT_NAME,
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

    if "annualReports" not in data or "quarterlyReports" not in data:
            logger.error("Unexpected income statement response for %s: %s", ticker, data)
            raise ValueError(f"No income statement data returned for {ticker}")

    records = []

    # set report_groups for annual and quaterly

    report_groups =[
        ("ANNUAL", "annualReports"),
        ("QUARTERLY", "quarterlyReports")
    ]

    for report_type, key in report_groups:
        for report in data.get(key, []):
            records.append(
                {
                    "TICKER": data.get("symbol", ticker),
                    "REPORT_TYPE": report_type,
                    "FISCAL_DATE_ENDING": report.get("fiscalDateEnding"),
                    "REPORTED_CURRENCY": report.get("reportedCurrency"),
                    "TOTAL_REVENUE": report.get("totalRevenue"),
                    "GROSS_PROFIT": report.get("grossProfit"),
                    "COST_OF_REVENUE": report.get("costOfRevenue"),
                    "OPERATING_INCOME": report.get("operatingIncome"),
                    "OPERATING_EXPENSES": report.get("operatingExpenses"),
                    "EBITDA": report.get("ebitda"),
                    "EBIT": report.get("ebit"),
                    "NET_INCOME": report.get("netIncome"),
                    "RESEARCH_AND_DEVELOPMENT": report.get("researchAndDevelopment"),
                    "SELLING_GENERAL_AND_ADMIN": report.get("sellingGeneralAndAdministrative"),
                    "INTEREST_EXPENSE": report.get("interestExpense"),
                    "INCOME_TAX_EXPENSE": report.get("incomeTaxExpense"),
                    "RAW_PAYLOAD": json.dumps(report),
                    "INGESTION_ID": INGESTION_ID,
                    "SOURCE_SYSTEM": "ALPHA_VANTAGE",
                    "ENDPOINT_NAME": ENDPOINT_NAME,
                    "LOADED_AT": datetime.now(timezone.utc),
                }
            )

    df = pd.DataFrame(records)

    if df.empty:
        return df

    df["FISCAL_DATE_ENDING"] = pd.to_datetime(df["FISCAL_DATE_ENDING"], errors="coerce").dt.date

    numeric_cols = [
        "TOTAL_REVENUE",
        "GROSS_PROFIT",
        "COST_OF_REVENUE",
        "OPERATING_INCOME",
        "OPERATING_EXPENSES",
        "EBITDA",
        "EBIT",
        "NET_INCOME",
        "RESEARCH_AND_DEVELOPMENT",
        "SELLING_GENERAL_AND_ADMIN",
        "INTEREST_EXPENSE",
        "INCOME_TAX_EXPENSE",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info("Fetched %s rows for %s", len(df), ticker)
    return df


def load_income_statement_to_snowflake(df: pd.DataFrame, ticker: str) -> None:
    """ Load income statement   """

    if df.empty:
        logger.warning("No rows to load for %s", ticker)
        return

    conn = get_snowflake_connection()
    cursor = None

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TEMPORARY TABLE STG_INCOME_STATEMENT_SESSION
            LIKE FINSIGHT.RAW.INCOME_STATEMENT_RAW
            """
        )

        logger.info("Loading %s rows for %s into temporary staging", len(df), ticker)

        success, nchunks, nrows, output = write_pandas(
            conn=conn,
            df=df,
            table_name="STG_INCOME_STATEMENT_SESSION",
            auto_create_table=False,
        )

        if not success:
            raise RuntimeError(f"write_pandas failed for {ticker}: {output}")

        merge_sql = """
            MERGE INTO FINSIGHT.RAW.INCOME_STATEMENT_RAW AS tgt
            USING STG_INCOME_STATEMENT_SESSION AS src
                ON tgt.TICKER = src.TICKER
               AND tgt.FISCAL_DATE_ENDING  = src.FISCAL_DATE_ENDING 
               AND tgt.REPORT_TYPE = src.REPORT_TYPE

            WHEN MATCHED THEN UPDATE SET
                tgt.REPORTED_CURRENCY = src.REPORTED_CURRENCY,
                tgt.TOTAL_REVENUE = src.TOTAL_REVENUE,
                tgt.GROSS_PROFIT = src.GROSS_PROFIT,
                tgt.COST_OF_REVENUE = src.COST_OF_REVENUE,
                tgt.OPERATING_INCOME = src.OPERATING_INCOME,
                tgt.OPERATING_EXPENSES = src.OPERATING_EXPENSES,
                tgt.EBITDA = src.EBITDA,
                tgt.EBIT = src.EBIT,
                tgt.NET_INCOME = src.NET_INCOME,
                tgt.RESEARCH_AND_DEVELOPMENT = src.RESEARCH_AND_DEVELOPMENT,
                tgt.SELLING_GENERAL_AND_ADMIN = src.SELLING_GENERAL_AND_ADMIN,
                tgt.INTEREST_EXPENSE = src.INTEREST_EXPENSE,
                tgt.INCOME_TAX_EXPENSE = src.INCOME_TAX_EXPENSE,
                tgt.RAW_PAYLOAD = PARSE_JSON(src.RAW_PAYLOAD),
                tgt.INGESTION_ID = src.INGESTION_ID,
                tgt.SOURCE_SYSTEM = src.SOURCE_SYSTEM,
                tgt.ENDPOINT_NAME = src.ENDPOINT_NAME,
                tgt.LOADED_AT = src.LOADED_AT

            WHEN NOT MATCHED THEN INSERT (
                TICKER,
                REPORT_TYPE,
                FISCAL_DATE_ENDING,
                REPORTED_CURRENCY,
                TOTAL_REVENUE,
                GROSS_PROFIT,
                COST_OF_REVENUE,
                OPERATING_INCOME,
                OPERATING_EXPENSES,
                EBITDA,
                EBIT,
                NET_INCOME,
                RESEARCH_AND_DEVELOPMENT,
                SELLING_GENERAL_AND_ADMIN,
                INTEREST_EXPENSE,
                INCOME_TAX_EXPENSE,
                RAW_PAYLOAD,
                INGESTION_ID,
                SOURCE_SYSTEM,
                ENDPOINT_NAME,
                LOADED_AT
            )
            VALUES (
                src.TICKER,
                src.REPORT_TYPE,
                src.FISCAL_DATE_ENDING,
                src.REPORTED_CURRENCY,
                src.TOTAL_REVENUE,
                src.GROSS_PROFIT,
                src.COST_OF_REVENUE,
                src.OPERATING_INCOME,
                src.OPERATING_EXPENSES,
                src.EBITDA,
                src.EBIT,
                src.NET_INCOME,
                src.RESEARCH_AND_DEVELOPMENT,
                src.SELLING_GENERAL_AND_ADMIN,
                src.INTEREST_EXPENSE,
                src.INCOME_TAX_EXPENSE,
                PARSE_JSON(src.RAW_PAYLOAD),
                src.INGESTION_ID,
                src.SOURCE_SYSTEM,
                src.ENDPOINT_NAME,
                src.LOADED_AT
            )
        """

        cursor.execute(merge_sql)

        duplicate_check_sql = """
            SELECT TICKER, FISCAL_DATE_ENDING, REPORT_TYPE, COUNT(*) AS CNT
            FROM FINSIGHT.RAW.INCOME_STATEMENT_RAW
            GROUP BY TICKER, FISCAL_DATE_ENDING, REPORT_TYPE
            HAVING COUNT(*) > 1
        """

        cursor.execute(duplicate_check_sql)
        duplicates = cursor.fetchall()

        if duplicates:
            raise ValueError(f"Duplicate symbol found: {duplicates[:10]}")

        logger.info("Merged %s rows for %s into RAW.INCOME_STATEMENT_RAW", nrows, ticker)

    finally:
        if cursor:
            cursor.close()
        conn.close()


def run_ingestion(tickers: list[str] = TICKERS) -> None:
    logger.info("Starting income statement ingestion for %s tickers", len(tickers))

    success_count = 0
    fail_count = 0

    for ticker in tickers:
        try:
            df = fetch_income_statement(ticker)
            load_income_statement_to_snowflake(df, ticker)
            success_count += 1
        except Exception as e:
            logger.error("Failed to process %s: %s", ticker, e)
            fail_count += 1

        time.sleep(15)

    logger.info("Ingestion complete. Success: %s  Failed: %s", success_count, fail_count)


if __name__ == "__main__":
    run_ingestion()
