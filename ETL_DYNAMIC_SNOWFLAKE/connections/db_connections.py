from sqlalchemy import create_engine
import snowflake.connector
from ETL_DYNAMIC_SNOWFLAKE.config.config import SQL_SERVER_CONFIG, SNOWFLAKE_CONFIG


def get_sql_server_engine():
    connection_string = (
        f"mssql+pyodbc://@{SQL_SERVER_CONFIG['server']}/"
        f"{SQL_SERVER_CONFIG['database']}?"
        f"driver={SQL_SERVER_CONFIG['driver'].replace(' ', '+')}"
        f"&trusted_connection={SQL_SERVER_CONFIG['trusted_connection']}"
    )
    return create_engine(connection_string)


def get_snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
        role=SNOWFLAKE_CONFIG["role"]
    )