import pandas as pd
import numpy as np
from ETL_DYNAMIC_SNOWFLAKE.etl_framework.logger import log_info, log_error
from ETL_DYNAMIC_SNOWFLAKE.etl_framework.source_queries import *
from ETL_DYNAMIC_SNOWFLAKE.etl_framework.target_queries import *


def validate_aggregates(sql_engine, snowflake_conn):

    log_info("Running Aggregate Validation...")

    src_df = pd.read_sql(source_aggregates_query(), sql_engine)

    cursor = snowflake_conn.cursor()
    cursor.execute(target_aggregates_query())
    tgt_df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    cursor.close()

    src_df.columns = src_df.columns.str.lower()
    tgt_df.columns = tgt_df.columns.str.lower()

    if np.isclose(src_df.values, tgt_df.values, rtol=1e-4).all():
        log_info("Aggregate Validation PASSED")
    else:
        log_error(f"Aggregate Validation FAILED\nSource:\n{src_df}\nTarget:\n{tgt_df}")


def validate_groupby(sql_engine, snowflake_conn):

    log_info("Running Group By Validation...")

    src_df = pd.read_sql(source_groupby_query(), sql_engine)

    cursor = snowflake_conn.cursor()
    cursor.execute(target_groupby_query())
    tgt_df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    cursor.close()

    # Normalize column names
    src_df.columns = src_df.columns.str.lower()
    tgt_df.columns = tgt_df.columns.str.lower()

    #  TYPE NORMALIZATION (Fix mismatch issue)
    src_df["payment_type"] = pd.to_numeric(src_df["payment_type"], errors="coerce").astype("Int64")
    tgt_df["payment_type"] = pd.to_numeric(tgt_df["payment_type"], errors="coerce").astype("Int64")

    src_df["cnt"] = pd.to_numeric(src_df["cnt"], errors="coerce")
    tgt_df["cnt"] = pd.to_numeric(tgt_df["cnt"], errors="coerce")

    # Sort before comparison
    src_df = src_df.sort_values(by="payment_type").reset_index(drop=True)
    tgt_df = tgt_df.sort_values(by="payment_type").reset_index(drop=True)

    if src_df.equals(tgt_df):
        log_info("Group By Validation PASSED")
    else:
        log_error(
            f"Group By Validation FAILED\n\n"
            f"Source:\n{src_df}\n\n"
            f"Target:\n{tgt_df}"
        )

def validate_filter(sql_engine, snowflake_conn):

    log_info("Running Filter Validation...")

    src = pd.read_sql(source_filter_query(), sql_engine).iloc[0, 0]

    cursor = snowflake_conn.cursor()
    cursor.execute(target_filter_query())
    tgt = cursor.fetchone()[0]
    cursor.close()

    if src == tgt:
        log_info("Filter Validation PASSED")
    else:
        log_error(f"Filter Validation FAILED - Source:{src} Target:{tgt}")


def validate_regex(snowflake_conn):

    log_info("Running Regex Validation...")

    query = """
        SELECT COUNT(*)
        FROM YELLOW_TRIPDATA_TEST
        WHERE REGEXP_LIKE(store_and_fwd_flag, '^[YN]$') = FALSE
    """

    cursor = snowflake_conn.cursor()
    cursor.execute(query)
    invalid_count = cursor.fetchone()[0]
    cursor.close()

    if invalid_count == 0:
        log_info("Regex Validation PASSED")
    else:
        log_error(f"Regex Validation FAILED - {invalid_count} invalid rows")