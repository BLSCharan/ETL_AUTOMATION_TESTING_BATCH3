# config/config.py

SQL_SERVER_CONFIG = {
    "driver": "ODBC Driver 17 for SQL Server",
    "server": "localhost\\SQLEXPRESS",
    "database": "ETL_Dynamic",
    "trusted_connection": "yes"
}

SNOWFLAKE_CONFIG = {
    "user": "BLSCHARAN",
    "password": "Scheryy@12345678",
    "account": "myhrjeq-zq60870",
    "warehouse": "ETL_WH_DYNAMIC",
    "database": "ETL_DYNAMIC",
    "schema": "TARGET",
    "role": "ACCOUNTADMIN"
}