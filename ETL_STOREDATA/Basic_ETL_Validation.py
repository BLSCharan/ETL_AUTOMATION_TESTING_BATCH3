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
        user='****',
        password='*******',
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


def validate_columns():
    source_cols = set(col.lower() for col in df_source.columns)
    print("source_cols",source_cols)
    target_cols = set(col.lower() for col in df_target.columns)
    print("target_cols",target_cols)

    if source_cols == target_cols:
        logging.info("Column Validation Passed")
    else:
        logging.error(f"Column Mismatch Found. "
                      f"Missing in Target: {source_cols - target_cols}, "
                      f"Extra in Target: {target_cols - source_cols}")


def validate_nulls():
    source_nulls = df_source.isnull().sum()
    print("source_nulls",source_nulls)
    target_nulls = df_target.isnull().sum()
    print("target_nulls",target_nulls)

    logging.info(f"Source Null Counts:\n{source_nulls}")
    logging.info(f"Target Null Counts:\n{target_nulls}")


def validate_duplicates():
    source_dups = df_source.duplicated().sum()
    print("source_dups",source_dups)
    target_dups = df_target.duplicated().sum()
    print("target_dups",target_dups)

    logging.info(f"Source Duplicate Rows: {source_dups}")
    logging.info(f"Target Duplicate Rows: {target_dups}")


def validate_dtypes():
    logging.info(f"Source Data Types:\n{df_source.dtypes}")
    print(f"Data Types : {df_source.dtypes}")
    logging.info(f"Target Data Types:\n{df_target.dtypes}")
    print(f"Data Types : {df_target.dtypes}")

#
# def validate_data_mismatch(primary_key):
#     """
#     Compare row-level data based on primary key
#     """
#     print("validate data mismatch has started")
#     try:
#         merged = df_source.merge(
#             df_target,
#             on=primary_key,
#             how="outer",
#             suffixes=("_source", "_target"),
#             indicator=True
#         )
#         print("merged",merged)
#
#         # Records missing in target
#         missing_in_target = merged[merged["_merge"] == "left_only"]
#         missing_in_source = merged[merged["_merge"] == "right_only"]
#         print("missing_in_target",missing_in_target)
#         print("missing_in_source",missing_in_source)
#
#         if not missing_in_target.empty:
#             logging.error(f"Records missing in Target:\n{missing_in_target}")
#
#         if not missing_in_source.empty:
#             logging.error(f"Records missing in Source:\n{missing_in_source}")
#
#         # Compare matching records
#         common_records = merged[merged["_merge"] == "both"]
#         print("common_records",common_records)
#
#         mismatches = []
#         print("mismatches",mismatches)
#
#         for col in df_source.columns:
#             if col != primary_key:
#                 source_col = f"{col}_source"
#                 target_col = f"{col}_target"
#                 print("source_col",source_col)
#                 print("target_col",target_col)
#
#                 diff = common_records[common_records[source_col] != common_records[target_col]]
#                 print("diff",diff)
#
#                 if not diff.empty:
#                     mismatches.append((col, len(diff)))
#                     logging.error(f"Mismatch found in column {col}: {len(diff)} rows")
#                     logging.error(f"Mismatch found in column {col}: {diff} rows")
#
#         if not mismatches:
#             logging.info("No Data Mismatches Found")
#
#     except Exception as e:
#         logging.error(f"Data Mismatch Validation Failed: {e}")

def validate_data_mismatch(primary_key):
    """
    Compare row-level data between source and target using primary key
    """

    print("validate data mismatch has started")

    try:
        # Normalize column names
        df_source.columns = df_source.columns.str.lower()
        df_target.columns = df_target.columns.str.lower()
        primary_key = primary_key.lower()

        #Trim string columns (Pandas 2/3 safe)
        for col in df_source.select_dtypes(include=["object", "string"]).columns:
            df_source[col] = df_source[col].str.strip()
            df_target[col] = df_target[col].str.strip()

        #Merge source & target
        merged = df_source.merge(
            df_target,
            on=primary_key,
            how="outer",
            suffixes=("_source", "_target"),
            indicator=True
        )
        print(merged)
        print("Merged shape:", merged.shape)

        #Missing Records
        missing_in_target = merged[merged["_merge"] == "left_only"]
        print(missing_in_target.empty)
        missing_in_source = merged[merged["_merge"] == "right_only"]
        print(missing_in_source.empty)

        if not missing_in_target.empty:
            logging.error(f"Records missing in Target:\n{missing_in_target}")

        if not missing_in_source.empty:
            logging.error(f"Records missing in Source:\n{missing_in_source}")

        #Compare Matching Records
        common_records = merged[merged["_merge"] == "both"]

        mismatches_found = False

        for col in df_source.columns:

            # Skip primary key column
            if col == primary_key:
                continue

            source_col = f"{col}_source"
            target_col = f"{col}_target"

            # Skip if column not present after merge
            if source_col not in common_records.columns:
                continue

            # Safe comparison:
            # - Handle NULL properly
            # - Convert to string for consistent comparison
            diff = common_records[
                ~(common_records[source_col].fillna("NULL").astype(str)
                  ==
                  common_records[target_col].fillna("NULL").astype(str))
            ]
            print(common_records)
            print(common_records[source_col])
            print(common_records[target_col])
            print(common_records[source_col].fillna("NULL"))
            print(common_records[target_col].fillna("NULL"))
            print(common_records[source_col].fillna("NULL").astype(str))
            print(common_records[target_col].fillna("NULL").astype(str))
            print(diff)


            if not diff.empty:
                mismatches_found = True
                print(mismatches_found)
                logging.error(f"Mismatch found in column '{col}': {len(diff)} rows")
                logging.error(
                    diff[[primary_key, source_col, target_col]]
                )

        #Final Result
        if not mismatches_found:
            logging.info("No Data Mismatches Found")
            print("No Data Mismatches Found")
        else:
            print("Data mismatches detected. Check log file.")

    except Exception as e:
        logging.error(f"Data Mismatch Validation Failed: {e}")
        print("Error:", e)


#EXECUTE ALL VALIDATIONS

validate_counts()
validate_columns()
validate_nulls()
validate_duplicates()
validate_dtypes()
validate_data_mismatch(primary_key="id")

#CLOSE CONNECTIONS

sql_connection.close()
sf_connection.close()

logging.info("Connections Closed Successfully")
logging.info("ETL Validation Process Completed")
