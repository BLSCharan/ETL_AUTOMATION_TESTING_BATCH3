import pandas as pd
import numpy as np
from ETL_DYNAMIC_SNOWFLAKE.etl_framework.logger import log_info, log_error
from ETL_DYNAMIC_SNOWFLAKE.etl_framework.source_queries import SOURCE_LIMIT


def validate_join_logic(sql_engine, snowflake_conn):

    log_info("Running Join Validation...")

    src_query = f"""
        SELECT payment_type, COUNT(*) AS cnt
        FROM (
            SELECT TOP {SOURCE_LIMIT} *
            FROM yellow_tripdata_2015_02
        ) t
        GROUP BY payment_type
    """

    tgt_query = """
        SELECT payment_type, COUNT(*) AS cnt
        FROM YELLOW_TRIPDATA_TEST
        GROUP BY payment_type
    """

    src_df = pd.read_sql(src_query, sql_engine)

    cursor = snowflake_conn.cursor()
    cursor.execute(tgt_query)
    tgt_df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    cursor.close()

    # Normalize column names
    src_df.columns = src_df.columns.str.lower()
    tgt_df.columns = tgt_df.columns.str.lower()

    # 🔥 Type normalization
    src_df["payment_type"] = pd.to_numeric(src_df["payment_type"], errors="coerce").astype("Int64")
    tgt_df["payment_type"] = pd.to_numeric(tgt_df["payment_type"], errors="coerce").astype("Int64")

    src_df["cnt"] = pd.to_numeric(src_df["cnt"], errors="coerce")
    tgt_df["cnt"] = pd.to_numeric(tgt_df["cnt"], errors="coerce")

    # Sort before compare
    src_df = src_df.sort_values("payment_type").reset_index(drop=True)
    tgt_df = tgt_df.sort_values("payment_type").reset_index(drop=True)

    if src_df.equals(tgt_df):
        log_info("Join Validation PASSED")
    else:
        log_error(
            f"Join Validation FAILED\n\n"
            f"Source:\n{src_df}\n\n"
            f"Target:\n{tgt_df}"
        )