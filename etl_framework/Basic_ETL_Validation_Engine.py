import pandas as pd
from ETL_STOREDATA.sql_server import get_sql_server_connection
from ETL_STOREDATA.sf_connection import get_snowflake_connection
from validations.row_count import validate_row_count
from validations.column_validation import validate_columns
from validations.data_quality import validate_nulls, validate_duplicates
from validations.data_mismatch import validate_data_mismatch
from utils.logger import setup_logger


def run_validation():

    logger = setup_logger()
    logger.info("ETL Validation Started")

    # Create DB Connections
    sql_conn = get_sql_server_connection()
    sf_conn = get_snowflake_connection()

    # Hardcoded Queries (Like Your Original Script)
    source_query = "SELECT * FROM dbo.customers"
    target_query = "SELECT * FROM customers"
    primary_key = "id"

    # Load Data
    df_source = pd.read_sql(source_query, sql_conn)
    df_target = pd.read_sql(target_query, sf_conn)

    # Row Count
    result = validate_row_count(df_source, df_target)
    logger.info(f"Row Count Validation: {result}")

    # Column Validation
    result = validate_columns(df_source, df_target)
    logger.info(f"Column Validation: {result}")

    # Null Check
    result = validate_nulls(df_source, df_target)
    logger.info(f"Null Validation: {result}")

    # Duplicate Check
    result = validate_duplicates(df_source, df_target)
    logger.info(f"Duplicate Validation: {result}")

    # Data Mismatch
    result = validate_data_mismatch(df_source, df_target, primary_key)
    logger.info(f"Data Mismatch Validation: {result}")

    logger.info("ETL Validation Completed")


if __name__ == "__main__":
    run_validation()