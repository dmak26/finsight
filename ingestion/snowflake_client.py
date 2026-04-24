import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from dotenv import load_dotenv
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def get_snowflake_connection():
    """
    Returns a Snowflake connection using environment variables.
    """
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        logger.info("Snowflake connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise

def test_connection():
    """
    Quick connection test — run this directly to verify credentials.
    """
    conn = get_snowflake_connection()
    cursor = None

    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_USER()"
        )

        version, account, region, user = cursor.fetchone()

        logger.info("Snowflake connection test successful")
        logger.info("Version: %s", version)
        logger.info("Account: %s", account)
        logger.info("Region: %s", region)
        logger.info("User: %s", user)

    except Exception as e:
        logger.error("Snowflake connection test failed: %s", e)
        raise

    finally:
        if cursor:
            cursor.close()
        conn.close()

def execute_query(query: str):
    """
    Execute query reference - reusable
    """
    logger.info("Executing query: %s", query)

    conn = get_snowflake_connection()
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        logger.info("Query returned %s row", len(results))
        return results
    
    except Exception as e:
        logger.error("Query failed: %s", e)
        raise

    finally:
        if cursor:
            cursor.close()
        conn.close()

def execute_non_query(query: str):
    """
    Execute DDL/DML reference - reusable
    """
    logger.info("Executing non-query: %s", query)

    conn = get_snowflake_connection()
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        logger.info("Execution successful")

    except Exception as e:
        logger.error("Execution failed: %s", e)
        raise
        
    finally:
        if cursor:
            cursor.close()
        conn.close()

def load_dataframe(df: pd.DataFrame, table_name: str) -> bool:
    """
    DataFrame loading reference - reusable
    """
    conn = get_snowflake_connection()
    try:
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
        logger.info("Loaded %s rows into %s across %s chunks", nrows, table_name, nchunks)
        return success
    finally:
        conn.close()

if __name__ == "__main__":
    logger.info("Running Snowflake client test...")
    test_connection()