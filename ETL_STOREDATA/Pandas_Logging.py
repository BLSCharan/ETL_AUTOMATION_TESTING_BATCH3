import pyodbc
import snowflake.connector
import pandas as pd
import logging
from datetime import datetime

#Logging Configuration

log_file = f"etl_validation_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("ETL Validation Process Started")


#SQL SERVER CONNECTION

try:
    sql_connection = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=ETL_DB;"
        "Trusted_Connection=yes;"
    )
    logging.info("Connected to SQL Server Successfully")

except Exception as e:
    logging.error(f"SQL Server Connection Error: {e}")
    raise


#SNOWFLAKE CONNECTION

try:
    sf_connection = snowflake.connector.connect(
        user='BSDivya',
        password='21092002@SnowFlake',
        account='fdmdkjh-pa09017',
        warehouse='ETL_WH',
        database='ETL_DB',
        schema='PUBLIC',
        role='ACCOUNTADMIN'
    )
    logging.info("Connected to Snowflake Successfully")

except Exception as e:
    logging.error(f"Snowflake Connection Error: {e}")
    raise


#LOAD DATA INTO PANDAS DATAFRAMES

source_query = "SELECT * FROM dbo.customers"
target_query = "SELECT * FROM customers"

try:
    df_source = pd.read_sql(source_query, sql_connection)
    df_target = pd.read_sql(target_query, sf_connection)
    print("df_source",df_source)
    print("df_target",df_target)

    logging.info("Data loaded into Pandas DataFrames successfully")

except Exception as e:
    logging.error(f"Error loading data into Pandas: {e}")
    raise


#VALIDATION CHECKS

def validate_counts():
    source_count = len(df_source)
    print("source_count",source_count)
    target_count = len(df_target)
    print("target_count",target_count)

    logging.info(f"Source Count: {source_count}")
    logging.info(f"Target Count: {target_count}")

    if source_count == target_count:
        logging.info("Row Count Validation Passed")
    else:
        logging.error("Row Count Validation Failed")


#EXECUTE VALIDATION

validate_counts()