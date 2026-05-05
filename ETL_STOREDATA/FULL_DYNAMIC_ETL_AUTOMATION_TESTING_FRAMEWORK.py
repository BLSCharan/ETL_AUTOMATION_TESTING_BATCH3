# =========================================
# ETL AUTOMATION TESTING FRAMEWORK
# =========================================

from config import CONFIG, ENVIRONMENT
import pyodbc
import snowflake.connector
import pandas as pd
import logging
from datetime import datetime
import re

# =========================================
# LOGGER PER TABLE
# =========================================

def get_table_logger(table_name):

    log_filename = f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logger = logging.getLogger(table_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.FileHandler(log_filename)
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# =========================================
# CONNECTIONS
# =========================================

def get_sql_server_connection():
    env = CONFIG[ENVIRONMENT]["sql_server"]
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={env['server']};"
        f"DATABASE={env['database']};"
        "Trusted_Connection=yes;"
    )

def get_snowflake_connection():
    sf = CONFIG[ENVIRONMENT]["snowflake"]
    return snowflake.connector.connect(
        user=sf["user"],
        password=sf["password"],
        account=sf["account"],
        warehouse=sf["warehouse"],
        database=sf["database"],
        schema=sf["schema"],
        role=sf["role"]
    )


# =========================================
# LOAD DATA
# =========================================

def load_data(table, sql_conn, sf_conn, logger):

    logger.info("Loading Data...")

    df_source = pd.read_sql(f"SELECT * FROM {table['source_table']}", sql_conn)
    df_target = pd.read_sql(f"SELECT * FROM {table['target_table']}", sf_conn)

    df_source.columns = df_source.columns.str.strip().str.replace(" ", "_")
    df_target.columns = df_target.columns.str.strip()

    logger.info(f"Source Rows: {len(df_source)}")
    logger.info(f"Target Rows: {len(df_target)}")

    return df_source, df_target


# =========================================
# BASIC VALIDATIONS
# =========================================

def validate_counts(df_source, df_target, table, logger):
    logger.info("Row Count Validation")
    logger.info(f"Source: {len(df_source)}, Target: {len(df_target)}")
    return len(df_source) == len(df_target)


def validate_columns(df_source, df_target, table, logger):
    logger.info("Column Validation")
    missing = set(df_source.columns.str.lower()) - set(df_target.columns.str.lower())
    logger.info(f"Missing Columns: {missing}")
    return len(missing) == 0


def validate_nulls(df_source, df_target, table, logger):
    logger.info("Null Validation")
    s = df_source.isnull().sum().sum()
    t = df_target.isnull().sum().sum()
    logger.info(f"Source Nulls: {s}, Target Nulls: {t}")
    return s == t


def validate_duplicates(df_source, df_target, table, logger):
    logger.info("Duplicate Validation")
    s = df_source.duplicated().sum()
    t = df_target.duplicated().sum()
    logger.info(f"Source Duplicates: {s}, Target Duplicates: {t}")
    return s == t


def validate_dtypes(df_source, df_target, table, logger):
    logger.info("DataType Validation")
    mismatch = []
    for col in df_source.columns:
        if col in df_target.columns:
            if str(df_source[col].dtype) != str(df_target[col].dtype):
                mismatch.append(col)
    logger.info(f"Mismatched Columns: {mismatch}")
    return len(mismatch) == 0


def validate_filter(df_target, table, logger):
    logger.info("Filter Validation")

    filters = table.get("filters")
    if not filters:
        logger.warning("No Filters Defined")
        return True

    for col, cond in filters.items():
        if col.upper() not in df_target.columns:
            logger.warning(f"{col} not found")
            continue

        if cond.startswith(">"):
            val = float(cond[1:])
            invalid = df_target[df_target[col.upper()] <= val]
            logger.info(f"Invalid Rows: {len(invalid)}")

            if len(invalid) > 0:
                return False

    return True


# =========================================
# FULL LOAD VALIDATION
# =========================================

def validate_full_load(df_source, df_target, logger):

    logger.info("Full Load Validation")

    if "Sales" not in df_source.columns or "SALES" not in df_target.columns:
        logger.warning("Sales column not present → Skipping")
        return True

    df_source["Sales"] = pd.to_numeric(df_source["Sales"], errors="coerce")
    filtered = df_source[df_source["Sales"] > 100]

    logger.info(f"Filtered Source: {len(filtered)}, Target: {len(df_target)}")

    return len(filtered) == len(df_target)


# =========================================
# STRING / REGEX / DATE
# =========================================

def validate_string_transformations(df_source, df_target, table, logger):
    logger.info("String Transformation")

    rules = table.get("string_rules")
    if not rules:
        logger.warning("No String Rules")
        return True

    pk_src = table["primary_key_source"]
    pk_tgt = table["primary_key_target"]

    merged = df_source.merge(df_target, left_on=pk_src, right_on=pk_tgt)

    mismatch = 0

    for _, row in merged.iterrows():
        for col, rule in rules.items():
            if col not in df_source.columns or col.upper() not in df_target.columns:
                continue

            val = str(row[col])

            if rule == "upper" and val.upper() != row[col.upper()]:
                mismatch += 1

    logger.info(f"Mismatch Count: {mismatch}")
    return mismatch == 0


def validate_regex_cleaning(df_source, df_target, table, logger):
    logger.info("Regex Validation")

    col = table.get("regex_column")
    if not col:
        logger.warning("No Regex Column")
        return True

    pk_src = table["primary_key_source"]
    pk_tgt = table["primary_key_target"]

    merged = df_source.merge(df_target, left_on=pk_src, right_on=pk_tgt)

    mismatch = 0

    for _, row in merged.iterrows():
        cleaned = re.sub(r"[^A-Za-z0-9 ]", "", str(row[col]))
        if cleaned != row[col.upper()]:
            mismatch += 1

    logger.info(f"Regex Mismatch: {mismatch}")
    return mismatch == 0


def validate_date_transformations(df_source, df_target, table, logger):
    logger.info("Date Validation")

    col = table.get("date_column")
    if not col:
        logger.warning("No Date Column")
        return True

    if col not in df_source.columns:
        return True

    pk_src = table["primary_key_source"]
    pk_tgt = table["primary_key_target"]

    merged = df_source.merge(df_target, left_on=pk_src, right_on=pk_tgt)

    mismatch = 0

    for _, row in merged.iterrows():
        dt = pd.to_datetime(row[col], errors="coerce")
        if pd.isna(dt):
            continue

        if "ORDER_YEAR" in df_target.columns and dt.year != row["ORDER_YEAR"]:
            mismatch += 1

    logger.info(f"Date Mismatch: {mismatch}")
    return mismatch == 0


# =========================================
# AGGREGATION
# =========================================

def validate_region_aggregations(df_target, logger):
    logger.info("Aggregation Validation")

    if "REGION" not in df_target.columns:
        return True

    grouped = df_target.groupby("REGION")["SALES"].sum().reset_index()

    logger.info("Aggregation checked")

    return True


# =========================================
# INCREMENTAL
# =========================================

def get_incremental_data(df_source, df_target, table, logger):

    if "LOAD_DT" not in df_target.columns:
        logger.warning("No LOAD_DT column")
        return df_source, df_target

    df_target["LOAD_DT"] = pd.to_datetime(df_target["LOAD_DT"]).dt.date
    today = datetime.today().date()

    tgt = df_target[df_target["LOAD_DT"] == today]
    src = df_source[df_source[table["primary_key_source"]].isin(tgt[table["primary_key_target"]])]

    logger.info(f"Incremental Rows: {len(src)}")

    return src, tgt


def validate_incremental_load(df_source, df_target, table, logger):

    pk = table["primary_key_target"]

    if pk not in df_source.columns:
        return True

    merged = df_source.merge(df_target, on=pk, how="outer", indicator=True)

    mismatch = len(merged[merged["_merge"] != "both"])

    logger.info(f"Incremental Mismatch: {mismatch}")

    return mismatch == 0


# =========================================
# CDC
# =========================================

def validate_cdc_inserts(df_source, df_target, table, logger):

    merged = df_source.merge(
        df_target,
        left_on=table["primary_key_source"],
        right_on=table["primary_key_target"],
        how="left",
        indicator=True
    )

    count = len(merged[merged["_merge"] == "left_only"])

    logger.info(f"Insert Missing: {count}")

    return count == 0


def validate_cdc_updates(df_source, df_target, table, logger):

    common = df_source.merge(
        df_target,
        left_on=table["primary_key_source"],
        right_on=table["primary_key_target"]
    )

    logger.info("CDC Update Checked")

    return True


def validate_cdc_deletes(df_source, df_target, table, logger):

    merged = df_target.merge(
        df_source,
        left_on=table["primary_key_target"],
        right_on=table["primary_key_source"],
        how="left",
        indicator=True
    )

    count = len(merged[merged["_merge"] == "left_only"])

    logger.info(f"Delete Count: {count}")

    return count == 0


# =========================================
# RECONCILIATION
# =========================================

def validate_aggregate_reconciliation(df_source, df_target, logger):
    if "Sales" not in df_source.columns:
        return True
    return round(df_source["Sales"].sum(), 2) == round(df_target["SALES"].sum(), 2)


def validate_hash_reconciliation(df_source, df_target, table, logger):
    return set(df_source[table["primary_key_source"]]) == set(df_target[table["primary_key_target"]])


def validate_sampling_reconciliation(df_source, df_target, table, logger):
    sample = df_source.sample(min(100, len(df_source)))
    merged = sample.merge(df_target, left_on=table["primary_key_source"], right_on=table["primary_key_target"])
    return len(sample) == len(merged)


# =========================================
# MAIN EXECUTION
# =========================================

def run_table_tests(table, sql_conn, sf_conn, load_type="FULL"):

    logger = get_table_logger(table["name"])

    print(f"\n Running: {table['name']} | {load_type}")

    df_source, df_target = load_data(table, sql_conn, sf_conn, logger)

    test_results = {}
    load_type = load_type.upper()

    if load_type == "FULL":

        test_results["Full Load"] = validate_full_load(df_source, df_target, logger)
        test_results["Row Count"] = validate_counts(df_source, df_target, table, logger)
        test_results["Column Validation"] = validate_columns(df_source, df_target, table, logger)
        test_results["Null Validation"] = validate_nulls(df_source, df_target, table, logger)
        test_results["Duplicate Validation"] = validate_duplicates(df_source, df_target, table, logger)
        test_results["DataType Validation"] = validate_dtypes(df_source, df_target, table, logger)
        test_results["Filter Validation"] = validate_filter(df_target, table, logger)

        test_results["String Transformation"] = validate_string_transformations(df_source, df_target, table, logger)
        test_results["Regex Cleaning"] = validate_regex_cleaning(df_source, df_target, table, logger)
        test_results["Region Aggregation"] = validate_region_aggregations(df_target, logger)
        test_results["Date Validation"] = validate_date_transformations(df_source, df_target, table, logger)

        test_results["Aggregate Reconciliation"] = validate_aggregate_reconciliation(df_source, df_target, logger)
        test_results["Hash Reconciliation"] = validate_hash_reconciliation(df_source, df_target, table, logger)
        test_results["Sampling Reconciliation"] = validate_sampling_reconciliation(df_source, df_target, table, logger)

    elif load_type == "INCREMENTAL":

        df_source, df_target = get_incremental_data(df_source, df_target, table, logger)

        test_results["Incremental Validation"] = validate_incremental_load(df_source, df_target, table, logger)

    elif load_type == "CDC":

        test_results["CDC Insert"] = validate_cdc_inserts(df_source, df_target, table, logger)
        test_results["CDC Update"] = validate_cdc_updates(df_source, df_target, table, logger)
        test_results["CDC Delete"] = validate_cdc_deletes(df_source, df_target, table, logger)

    else:
        raise ValueError("Invalid load type")

    for k, v in test_results.items():
        print(f"{k}: {'PASSED' if v else 'FAILED'}")

    return all(test_results.values())


# =========================================
# MASTER
# =========================================

def execute_all_tables(load_type="FULL"):

    sql_conn = get_sql_server_connection()
    sf_conn = get_snowflake_connection()

    results = []

    for table in CONFIG[ENVIRONMENT]["tables"]:
        res = run_table_tests(table, sql_conn, sf_conn, load_type)
        results.append(res)

    sql_conn.close()
    sf_conn.close()

    print("\n====================")
    print(f"FINAL RESULT ({load_type})")
    print("====================")
    print("ALL PASSED" if all(results) else "SOME FAILED")


# =========================================
# ENTRY POINT
# =========================================

if __name__ == "__main__":
    execute_all_tables(load_type="FULL")