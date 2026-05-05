import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from ETL_DYNAMIC_SNOWFLAKE.connections.db_connections import (
    get_sql_server_engine,
    get_snowflake_connection
)
from ETL_DYNAMIC_SNOWFLAKE.etl_framework.logger import log_info, log_error


def load_data():

    log_info("Creating SQL Server engine...")
    sql_engine = get_sql_server_engine()

    log_info("Connecting to Snowflake...")
    snowflake_conn = get_snowflake_connection()
    cursor = snowflake_conn.cursor()

    # Clear target table before full reload
    log_info("Truncating Snowflake target table...")
    cursor.execute("TRUNCATE TABLE YELLOW_TRIPDATA_TEST")
    snowflake_conn.commit()

    query = """
            SELECT TOP 1000000 *
            FROM yellow_tripdata_2015_02 \
            """
    # 🔥 Increased chunk size to 5 Lakhs
    chunk_size = 500000

    total_rows = 0

    log_info("Starting high-performance chunk-based load...")

    for chunk in pd.read_sql(query, sql_engine, chunksize=chunk_size):

        log_info(f"Processing new chunk...")

        # Normalize column names
        chunk.columns = chunk.columns.str.upper()

        # ------------------------------------------------
        # DATA CLEANING SECTION (Production Safe)
        # ------------------------------------------------

        # Replace empty strings with None
        chunk.replace("", None, inplace=True)

        # Numeric columns to clean
        numeric_columns = [
            "TRIP_DISTANCE",
            "FARE_AMOUNT",
            "EXTRA",
            "MTA_TAX",
            "TIP_AMOUNT",
            "TOLLS_AMOUNT",
            "IMPROVEMENT_SURCHARGE",
            "TOTAL_AMOUNT",
            "PASSENGER_COUNT",
            "RATECODEID"
        ]

        for col in numeric_columns:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

        # ------------------------------------------------
        # BULK LOAD INTO SNOWFLAKE
        # ------------------------------------------------

        try:
            success, nchunks, nrows, _ = write_pandas(
                snowflake_conn,
                chunk,
                table_name="YELLOW_TRIPDATA_TEST"
            )

            total_rows += nrows
            log_info(f"Loaded chunk of {nrows} rows")

        except Exception as e:
            log_error(f"Chunk load failed: {e}")
            raise

    log_info(f"Total rows loaded into Snowflake: {total_rows}")

    cursor.close()
    snowflake_conn.close()

    return total_rows