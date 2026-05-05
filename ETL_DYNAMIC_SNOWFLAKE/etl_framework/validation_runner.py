from ETL_DYNAMIC_SNOWFLAKE.etl_framework.logger import log_info
from ETL_DYNAMIC_SNOWFLAKE.etl_framework.transformation_validations import *
from ETL_DYNAMIC_SNOWFLAKE.etl_framework.join_validations import *
from ETL_DYNAMIC_SNOWFLAKE.etl_framework.ordering_validations import *
from ETL_DYNAMIC_SNOWFLAKE.etl_framework.data_quality_checks import *


def run_validations(sql_engine, snowflake_conn):

    log_info("========== STARTING FULL TRANSFORMATION VALIDATIONS ==========")

    try:
        validate_aggregates(sql_engine, snowflake_conn)
    except Exception as e:
        log_info(f"Aggregate crashed: {e}")

    try:
        validate_groupby(sql_engine, snowflake_conn)
    except Exception as e:
        log_info(f"Group By crashed: {e}")

    try:
        validate_filter(sql_engine, snowflake_conn)
    except Exception as e:
        log_info(f"Filter crashed: {e}")

    try:
        validate_join_logic(sql_engine, snowflake_conn)
    except Exception as e:
        log_info(f"Join crashed: {e}")

    try:
        validate_order(sql_engine, snowflake_conn)
    except Exception as e:
        log_info(f"Order crashed: {e}")

    try:
        validate_regex(snowflake_conn)
    except Exception as e:
        log_info(f"Regex crashed: {e}")

    try:
        validate_date_function(snowflake_conn)
    except Exception as e:
        log_info(f"Date validation crashed: {e}")

    try:
        validate_nulls(snowflake_conn)
    except Exception as e:
        log_info(f"Null validation crashed: {e}")

    try:
        validate_duplicates(snowflake_conn)
    except Exception as e:
        log_info(f"Duplicate validation crashed: {e}")

    log_info("========== ALL VALIDATIONS COMPLETED ==========")