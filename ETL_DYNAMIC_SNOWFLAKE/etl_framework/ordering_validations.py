import numpy as np
import pandas as pd
from ETL_DYNAMIC_SNOWFLAKE.etl_framework.logger import log_info, log_error
from ETL_DYNAMIC_SNOWFLAKE.etl_framework.source_queries import SOURCE_LIMIT


def validate_order(sql_engine, snowflake_conn):

    log_info("Running Order Validation...")

    src_query = f"""
        SELECT TOP 1000 total_amount
        FROM (
            SELECT TOP {SOURCE_LIMIT} *
            FROM yellow_tripdata_2015_02
        ) t
        ORDER BY CAST(total_amount AS FLOAT) DESC
    """

    tgt_query = """
        SELECT total_amount
        FROM YELLOW_TRIPDATA_TEST
        ORDER BY total_amount DESC
        LIMIT 1000
    """

    # Read source
    src_df = pd.read_sql(src_query, sql_engine)
    src_df.columns = src_df.columns.str.lower()

    # Read target
    cursor = snowflake_conn.cursor()
    cursor.execute(tgt_query)
    tgt_df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    cursor.close()

    tgt_df.columns = tgt_df.columns.str.lower()

    #  SAFE COLUMN ACCESS
    if "total_amount" not in src_df.columns or "total_amount" not in tgt_df.columns:
        log_error(f"Order Validation FAILED - Column mismatch\n"
                  f"Source columns: {src_df.columns}\n"
                  f"Target columns: {tgt_df.columns}")
        return

    # Convert to float safely
    src_values = pd.to_numeric(src_df["total_amount"], errors="coerce").astype(float)
    tgt_values = pd.to_numeric(tgt_df["total_amount"], errors="coerce").astype(float)

    # Float tolerance comparison
    if np.allclose(src_values, tgt_values, rtol=1e-4, equal_nan=True):
        log_info("Order Validation PASSED")
    else:
        log_error("Order Validation FAILED")