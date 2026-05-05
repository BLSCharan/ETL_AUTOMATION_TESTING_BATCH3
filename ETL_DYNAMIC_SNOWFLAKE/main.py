from ETL_DYNAMIC_SNOWFLAKE.pipeline.load_sql_to_snowflake import load_data
from ETL_DYNAMIC_SNOWFLAKE.connections.db_connections import (
get_sql_server_engine,get_snowflake_connection )
from ETL_DYNAMIC_SNOWFLAKE.etl_framework.validation_runner import run_validations


if __name__ == "__main__":

    total_rows = load_data()

    sql_engine = get_sql_server_engine()
    snowflake_conn = get_snowflake_connection()

    run_validations(sql_engine, snowflake_conn)

    snowflake_conn.close()