import snowflake.connector
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
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    version = cursor.fetchone()
    logger.info(f"Snowflake version: {version[0]}")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    test_connection()