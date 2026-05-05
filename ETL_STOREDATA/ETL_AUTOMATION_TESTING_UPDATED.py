#SUPERSTORE ETL TESTING FRAMEWORK (END-TO-END)
from config import ENVIRONMENT, CONFIG
import pyodbc
import snowflake.connector
import pandas as pd
import logging
from datetime import datetime
import re

#CONFIGURATION

SOURCE_TABLE = "dbo.superstore_raw"
TARGET_TABLE = "SUPERSTORE_TRANSFORMED"
PRIMARY_KEY_SOURCE = "Row_ID"
PRIMARY_KEY_TARGET = "ROW_ID"

#LOGGING CONFIGURATION

log_file = f"superstore_etl_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logging.info("========== ETL TEST EXECUTION STARTED ==========")


#CONNECTION LAYER

def get_snowflake_connection():
    env_config = CONFIG[ENVIRONMENT]

    database = env_config["snowflake"]["database"]
    conn = snowflake.connector.connect(
        user="BSDivya",
        password="21092002@SnowFlake",
        account="fdmdkjh-pa09017",
        warehouse="ETL_WH",
        database="ETL_DB",
        schema="PUBLIC",
        role="ACCOUNTADMIN"
    )
    logging.info(f"Snowflake Connection Established | ENV: {ENVIRONMENT}")
    return conn

#DATA LOADING

def load_source_dataframe(sql_conn):
    df = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE}", sql_conn)
    df.columns = df.columns.str.strip().str.replace(" ", "_")
    logging.info(f"Source Rows Loaded: {len(df)}")
    return df


def load_target_dataframe(sf_conn):
    df = pd.read_sql(f"SELECT * FROM {TARGET_TABLE}", sf_conn)
    df.columns = df.columns.str.strip()
    logging.info(f"Target Rows Loaded: {len(df)}")
    return df
def get_sql_server_connection():

    env_config = CONFIG[ENVIRONMENT]

    server = env_config["sql_server"]["server"]
    database = env_config["sql_server"]["database"]

    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )

    logging.info(f"SQL Server Connection Established | ENV: {ENVIRONMENT}")

    return conn




#FULL LOAD VALIDATION

def validate_full_load(df_source, df_target):

    logging.info("---- FULL LOAD VALIDATION STARTED ----")

    df_source["Sales"] = pd.to_numeric(df_source["Sales"], errors="coerce")
    filtered_source = df_source[df_source["Sales"] > 100]

    logging.info(f"Filtered Source Rows (>100): {len(filtered_source)}")
    logging.info(f"Target Rows: {len(df_target)}")

    if len(filtered_source) != len(df_target):
        logging.error("FULL LOAD FAILED - Row count mismatch after filter")
        return False

    logging.info("FULL LOAD VALIDATION PASSED")
    return True


#INCREMENTAL LOAD IDENTIFICATION
def get_incremental_source(df_source, df_target):

    df_source["ORDER_ID"] = df_source["Order_ID"].astype(str).str.strip().str.upper()
    df_target["ORDER_ID"] = df_target["ORDER_ID"].astype(str).str.strip().str.upper()

    df_target["LOAD_DT"] = pd.to_datetime(df_target["LOAD_DT"], errors="coerce").dt.date

    today = datetime.today().date()

    target_incremental = df_target[df_target["LOAD_DT"] == today]

    source_incremental = df_source[
        df_source["ORDER_ID"].isin(target_incremental["ORDER_ID"])
    ]

    return source_incremental, target_incremental

#VALIDATIONS

def validate_filter(df_target):
    logging.info("Filter Validation Started")

    invalid_rows = df_target[df_target["SALES"] <= 100]
    count_invalid = len(invalid_rows)

    logging.info(f"Rows with SALES <= 100: {count_invalid}")

    return count_invalid == 0


def validate_counts(df_source, df_target):
    logging.info("Row Count Validation Started")

    source_count = len(df_source)
    target_count = len(df_target)

    logging.info(f"Source Count: {source_count}")
    logging.info(f"Target Count: {target_count}")

    return source_count == target_count


def validate_columns(df_source, df_target):
    logging.info("Column Validation Started")

    source_cols = set(df_source.columns.str.lower())
    target_cols = set(df_target.columns.str.lower())

    missing = source_cols - target_cols
    extra = target_cols - source_cols

    logging.info(f"Missing Columns: {missing}")
    logging.info(f"Extra Columns: {extra}")

    return len(missing) == 0


def validate_nulls(df_source, df_target):
    logging.info("Null Validation Started")

    source_nulls = df_source.isnull().sum().sum()
    target_nulls = df_target.isnull().sum().sum()

    logging.info(f"Source Null Count: {source_nulls}")
    logging.info(f"Target Null Count: {target_nulls}")

    return source_nulls == target_nulls


def validate_duplicates(df_source, df_target):
    logging.info("Duplicate Validation Started")

    source_dups = df_source.duplicated().sum()
    target_dups = df_target.duplicated().sum()

    logging.info(f"Source Duplicate Rows: {source_dups}")
    logging.info(f"Target Duplicate Rows: {target_dups}")

    return source_dups == target_dups


def validate_dtypes(df_source, df_target):
    logging.info("DataType Validation Started")

    mismatch_cols = []

    for col in df_source.columns:
        if col in df_target.columns:
            if str(df_source[col].dtype) != str(df_target[col].dtype):
                mismatch_cols.append(col)

    logging.info(f"DataType Mismatch Columns: {mismatch_cols}")

    return len(mismatch_cols) == 0


#STRING TRANSFORMATION VALIDATION

def validate_string_transformations(df_source, df_target):

    logging.info("String Transformation Validation Started")

    merged = df_source.merge(
        df_target,
        left_on=PRIMARY_KEY_SOURCE,
        right_on=PRIMARY_KEY_TARGET,
        how="inner"
    )

    mismatch_count = 0

    for _, row in merged.iterrows():

        if str(row["Customer_Name"]).upper() != row["CUSTOMER_NAME"]:
            mismatch_count += 1

        if str(row["Category"]).strip() != row["CATEGORY"]:
            mismatch_count += 1

        if str(row["Ship_Mode"]).replace(" ", "_") != row["SHIP_MODE"]:
            mismatch_count += 1

        if str(row["City"]).title() != row["CITY"]:
            mismatch_count += 1

        if str(row["Product_ID"])[0:3] != row["PRODUCT_PREFIX"]:
            mismatch_count += 1

        if len(str(row["Product_Name"])) != row["PRODUCT_NAME_LENGTH"]:
            mismatch_count += 1

    logging.info(f"String Transformation Mismatch Count: {mismatch_count}")

    return mismatch_count == 0


#REGEX VALIDATION

def validate_regex_cleaning(df_source, df_target):

    logging.info("Regex Cleaning Validation Started")

    merged = df_source.merge(
        df_target,
        left_on=PRIMARY_KEY_SOURCE,
        right_on=PRIMARY_KEY_TARGET,
        how="inner"
    )

    mismatch_count = 0

    for _, row in merged.iterrows():
        cleaned_value = re.sub(r"[^A-Za-z0-9 ]", "", str(row["Product_Name"]))
        if cleaned_value != row["PRODUCT_NAME"]:
            mismatch_count += 1

    logging.info(f"Regex Cleaning Mismatch Count: {mismatch_count}")

    return mismatch_count == 0


#GROUP BY VALIDATION

def validate_region_aggregations(df_target):

    logging.info("Region Aggregation Validation Started")

    grouped = (
        df_target.groupby("REGION")
        .agg(
            TOTAL_REGION_SALES=("SALES", "sum"),
            TOTAL_REGION_PROFIT=("PROFIT", "sum"),
            AVG_REGION_PROFIT=("PROFIT", "mean"),
            MAX_REGION_SALES=("SALES", "max"),
            MIN_REGION_SALES=("SALES", "min"),
            TOTAL_REGION_ORDERS=("ORDER_ID", "count")
        )
        .reset_index()
    )

    merged = grouped.merge(df_target, on="REGION", how="inner")

    mismatch_count = 0

    for _, row in merged.iterrows():
        if row["TOTAL_REGION_SALES_x"] != row["TOTAL_REGION_SALES_y"]:
            mismatch_count += 1

    logging.info(f"Region Aggregation Mismatch Count: {mismatch_count}")

    return mismatch_count == 0

#Date Validations


def validate_date_transformations(df_source, df_target):

    logging.info("Date Transformation Validation Started")

    merged = df_source.merge(
        df_target,
        left_on=PRIMARY_KEY_SOURCE,
        right_on=PRIMARY_KEY_TARGET,
        how="inner"
    )

    mismatch_count = 0

    for _, row in merged.iterrows():

        # Convert source date
        order_date = pd.to_datetime(row["Order_Date"], errors="coerce")

        if pd.isna(order_date):
            continue

        # Derive from source
        src_year = order_date.year
        src_month = order_date.month
        src_day = order_date.day

        # Compare with target
        if src_year != row["ORDER_YEAR"]:
            mismatch_count += 1

        if src_month != row["ORDER_MONTH"]:
            mismatch_count += 1

        if src_day != row["ORDER_DAY"]:
            mismatch_count += 1

    logging.info(f"Date Transformation Mismatch Count: {mismatch_count}")

    return mismatch_count == 0

#WEEK 5 – ADVANCED ETL AUTOMATION & SNOWFLAKE FOCUS

# 1) INCREMENTAL LOAD VALIDATION (ADVANCED)

def validate_incremental_load(source_incremental, target_incremental):

    logging.info("Incremental Validation Started")

    if len(source_incremental) != len(target_incremental):
        logging.error("Row Count Mismatch in Incremental Data")
        return False

    merged = source_incremental.merge(
        target_incremental,
        on="ORDER_ID",
        how="outer",
        indicator=True
    )

    mismatch = merged[merged["_merge"] != "both"]

    logging.info(f"Incremental Mismatch Rows: {len(mismatch)}")

    return len(mismatch) == 0

# 2️) CDC AUTOMATION TESTING

def validate_cdc_inserts(df_source, df_target):

    logging.info("CDC Insert Validation Started")

    # Convert Sales to numeric safely
    df_source["Sales"] = pd.to_numeric(df_source["Sales"], errors="coerce")

    # Apply same ETL filter
    filtered_source = df_source[df_source["Sales"] > 100]

    merged = filtered_source.merge(
        df_target,
        left_on=PRIMARY_KEY_SOURCE,
        right_on=PRIMARY_KEY_TARGET,
        how="left",
        indicator=True
    )

    inserts = merged[merged["_merge"] == "left_only"]

    logging.info(f"Unloaded Insert Records (After Filter): {len(inserts)}")

    return len(inserts) == 0

def validate_cdc_updates(df_source, df_target):

    logging.info("CDC Update Validation Started")

    common = df_source.merge(
        df_target,
        left_on=PRIMARY_KEY_SOURCE,
        right_on=PRIMARY_KEY_TARGET,
        how="inner",
        suffixes=("_SRC", "_TGT")
    )

    mismatch_count = 0

    for col in df_source.columns:
        if col in df_target.columns:
            if not common[f"{col}_SRC"].equals(common[f"{col}_TGT"]):
                mismatch_count += 1

    logging.info(f"Updated Column Mismatch Count: {mismatch_count}")

    return mismatch_count == 0


def validate_cdc_deletes(df_source, df_target):

    logging.info("CDC Delete Validation Started")

    merged = df_target.merge(
        df_source,
        left_on=PRIMARY_KEY_TARGET,
        right_on=PRIMARY_KEY_SOURCE,
        how="left",
        indicator=True
    )

    deleted_records = merged[merged["_merge"] == "left_only"]

    logging.info(f"Orphan Records in Target: {len(deleted_records)}")

    return len(deleted_records) == 0


# 3) DATA RECONCILIATION STRATEGIES

def validate_aggregate_reconciliation(df_source, df_target):

    logging.info("Aggregate Reconciliation Started")

    # Apply same ETL filter (Sales > 100)
    filtered_source = df_source[df_source["Sales"] > 100]

    source_sum = filtered_source["Sales"].sum()
    target_sum = df_target["SALES"].sum()

    logging.info(f"Filtered Source Sales Sum: {source_sum}")
    logging.info(f"Target Sales Sum: {target_sum}")

    return round(source_sum, 2) == round(target_sum, 2)

def validate_hash_reconciliation(df_source, df_target):

    logging.info("Simple Reconciliation Started")

    # Apply ETL filter
    filtered_source = df_source[df_source["Sales"] > 100]

    source_ids = set(filtered_source[PRIMARY_KEY_SOURCE])
    target_ids = set(df_target[PRIMARY_KEY_TARGET])

    logging.info(f"Filtered Source Count: {len(source_ids)}")
    logging.info(f"Target Count: {len(target_ids)}")

    missing_in_target = source_ids - target_ids
    extra_in_target = target_ids - source_ids

    logging.info(f"Missing In Target: {len(missing_in_target)}")
    logging.info(f"Extra In Target: {len(extra_in_target)}")

    return len(missing_in_target) == 0 and len(extra_in_target) == 0


def validate_sampling_reconciliation(df_source, df_target, sample_size=100):

    logging.info("Sampling Reconciliation Started")

    filtered_source = df_source[df_source["Sales"] > 100]

    if len(filtered_source) == 0:
        return False

    sample_source = filtered_source.sample(min(sample_size, len(filtered_source)))

    merged = sample_source.merge(
        df_target,
        left_on=PRIMARY_KEY_SOURCE,
        right_on=PRIMARY_KEY_TARGET,
        how="inner"
    )

    logging.info(f"Sample Rows Compared: {len(merged)}")

    return len(merged) == len(sample_source)

# MASTER EXECUTION

def execute_etl_tests(load_type="CDC"):

    logging.info(f"Execution Started | Load Type: {load_type}")

    sql_conn = get_sql_server_connection()
    sf_conn = get_snowflake_connection()

    df_source = load_source_dataframe(sql_conn)
    df_target = load_target_dataframe(sf_conn)

    test_results = {}
    load_type = load_type.upper()

    # =========================================
    # FULL LOAD
    # =========================================
    if load_type == "FULL":

        validate_full_load(df_source, df_target)

        # Core Validations
        test_results["Row Count"] = validate_counts(df_source, df_target)
        test_results["Column Validation"] = validate_columns(df_source, df_target)
        test_results["Null Validation"] = validate_nulls(df_source, df_target)
        test_results["Duplicate Validation"] = validate_duplicates(df_source, df_target)
        test_results["DataType Validation"] = validate_dtypes(df_source, df_target)
        test_results["Filter Validation"] = validate_filter(df_target)
        test_results["String Transformation"] = validate_string_transformations(df_source, df_target)
        test_results["Regex Cleaning"] = validate_regex_cleaning(df_source, df_target)
        test_results["Region Aggregation"] = validate_region_aggregations(df_target)
        test_results["Date Validation"] = validate_date_transformations(df_source, df_target)

        # FULL Specific
        test_results["Aggregate Reconciliation"] = validate_aggregate_reconciliation(df_source, df_target)
        test_results["Hash Reconciliation"] = validate_hash_reconciliation(df_source, df_target)
        test_results["Sampling Reconciliation"] = validate_sampling_reconciliation(df_source, df_target)

    # =========================================
    # INCREMENTAL LOAD
    # =========================================
    elif load_type == "INCREMENTAL":

        source_incremental, target_incremental = get_incremental_source(df_source, df_target)
        df_source = source_incremental
        df_target = target_incremental

        # Core Validations on Incremental Data
        test_results["Row Count"] = validate_counts(df_source, df_target)
        test_results["Column Validation"] = validate_columns(df_source, df_target)
        test_results["Null Validation"] = validate_nulls(df_source, df_target)
        test_results["Duplicate Validation"] = validate_duplicates(df_source, df_target)
        test_results["DataType Validation"] = validate_dtypes(df_source, df_target)
        test_results["Filter Validation"] = validate_filter(df_target)
        test_results["String Transformation"] = validate_string_transformations(df_source, df_target)
        test_results["Regex Cleaning"] = validate_regex_cleaning(df_source, df_target)
        test_results["Region Aggregation"] = validate_region_aggregations(df_target)
        test_results["Date Validation"] = validate_date_transformations(df_source, df_target)

        # Incremental Specific
        test_results["Incremental Validation"] = validate_incremental_load(df_source, df_target)

    # =========================================
    # CDC LOAD
    # =========================================
    elif load_type == "CDC":

        test_results["CDC Insert"] = validate_cdc_inserts(df_source, df_target)
        test_results["CDC Update"] = validate_cdc_updates(df_source, df_target)
        test_results["CDC Delete"] = validate_cdc_deletes(df_source, df_target)

    else:
        raise ValueError("Invalid load type. Use FULL / INCREMENTAL / CDC")

    # =========================================
    # RESULT LOGGING
    # =========================================

    for test_name, result in test_results.items():
        result_bool = bool(result)
        message = f"{test_name} : {'PASSED' if result_bool else 'FAILED'}"
        print(message)

        if result_bool:
            logging.info(message)
        else:
            logging.error(message)

    overall_status = all(bool(r) for r in test_results.values())

    final_message = "OVERALL ETL TEST PASSED" if overall_status else "OVERALL ETL TEST FAILED"
    print(final_message)
    logging.info(final_message)

    sql_conn.close()
    sf_conn.close()

    logging.info("========== ETL TEST EXECUTION COMPLETED ==========")


# ENTRY POINT

if __name__ == "__main__":
    execute_etl_tests(load_type="CDC")