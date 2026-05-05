from ETL_DYNAMIC_SNOWFLAKE.etl_framework.logger import log_info, log_error


# --------------------------------------------------
# DATE FUNCTION VALIDATION
# --------------------------------------------------

def validate_date_function(snowflake_conn):
    """
    Validate that all pickup dates belong to year 2015
    """

    log_info("Running Date Function Validation...")

    query = """
        SELECT COUNT(*)
        FROM YELLOW_TRIPDATA_TEST
        WHERE YEAR(tpep_pickup_datetime) <> 2015
    """

    cursor = snowflake_conn.cursor()
    cursor.execute(query)
    invalid_count = cursor.fetchone()[0]
    cursor.close()

    if invalid_count == 0:
        log_info("Date Function Validation PASSED")
    else:
        log_error(f"Date Function Validation FAILED - {invalid_count} invalid rows found")


# --------------------------------------------------
# NULL CHECK VALIDATION
# --------------------------------------------------

def validate_nulls(snowflake_conn):
    """
    Check for NULL values in critical numeric column
    """

    log_info("Running Null Validation...")

    query = """
        SELECT COUNT(*)
        FROM YELLOW_TRIPDATA_TEST
        WHERE total_amount IS NULL
    """

    cursor = snowflake_conn.cursor()
    cursor.execute(query)
    null_count = cursor.fetchone()[0]
    cursor.close()

    if null_count == 0:
        log_info("Null Validation PASSED - No NULL values found")
    else:
        log_error(f"Null Validation WARNING - {null_count} NULL values found")


# --------------------------------------------------
# DUPLICATE CHECK VALIDATION
# --------------------------------------------------

def validate_duplicates(snowflake_conn):
    """
    Check for duplicate records based on business keys
    """

    log_info("Running Duplicate Validation...")

    query = """
        SELECT COUNT(*)
        FROM (
            SELECT 
                tpep_pickup_datetime,
                total_amount,
                COUNT(*) OVER (
                    PARTITION BY tpep_pickup_datetime, total_amount
                ) AS dup_cnt
            FROM YELLOW_TRIPDATA_TEST
        ) t
        WHERE dup_cnt > 1
    """

    cursor = snowflake_conn.cursor()
    cursor.execute(query)
    dup_count = cursor.fetchone()[0]
    cursor.close()

    if dup_count == 0:
        log_info("Duplicate Validation PASSED - No duplicate records found")
    else:
        log_info(f"Duplicate records detected: {dup_count}")