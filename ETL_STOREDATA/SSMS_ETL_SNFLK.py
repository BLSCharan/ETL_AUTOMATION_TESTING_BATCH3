import pandas as pd
import re
from datetime import datetime   # ✅ NEW

from sql_server import get_sql_server_engine
from sf_connection import get_snowflake_engine

# =========================================================
# 1️⃣ EXTRACT
# =========================================================

def extract_data():
    engine = get_sql_server_engine()
    query = "SELECT * FROM dbo.superstore_raw"
    df = pd.read_sql(query, engine)
    print(f"Extracted {len(df)} rows from SQL Server")
    return df


# =========================================================
# 2️⃣ TRANSFORMATIONS
# =========================================================

# -------- STRING FUNCTIONS --------
def apply_string_transformations(df: pd.DataFrame) -> pd.DataFrame:

    df["Customer_Name"] = df["Customer_Name"].str.upper()
    df["Category"] = df["Category"].str.strip()
    df["Ship_Mode"] = df["Ship_Mode"].str.replace(" ", "_")
    df["City"] = df["City"].str.title()
    df["Product_Prefix"] = df["Product_ID"].str.slice(0, 3)
    df["Product_Name_Length"] = df["Product_Name"].str.len()

    return df


# -------- REGEX CLEANING --------
def apply_regex_cleaning(df: pd.DataFrame) -> pd.DataFrame:

    cleaned_values = []

    for value in df["Product_Name"]:
        cleaned_value = re.sub(r"[^A-Za-z0-9 ]", "", str(value))
        cleaned_values.append(cleaned_value)

    df["Product_Name"] = cleaned_values

    return df


# -------- DATE FUNCTIONS --------
def apply_date_transformations(df: pd.DataFrame) -> pd.DataFrame:

    df["Order_Date"] = pd.to_datetime(df["Order_Date"])
    df["Ship_Date"] = pd.to_datetime(df["Ship_Date"])

    df["Order_Year"] = df["Order_Date"].dt.year
    df["Order_Month"] = df["Order_Date"].dt.month
    df["Order_Day"] = df["Order_Date"].dt.day
    df["Order_Weekday"] = df["Order_Date"].dt.day_name()
    df["Shipping_Days"] = (df["Ship_Date"] - df["Order_Date"]).dt.days
    df["Year_Month"] = df["Order_Date"].dt.to_period("M").astype(str)

    return df


# -------- FILTER --------
def apply_filter(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["Sales"] > 100]


# -------- GROUP BY + AGGREGATE --------
def create_region_dimension(df: pd.DataFrame) -> pd.DataFrame:

    region_dim = (
        df.groupby("Region")
        .agg(
            TOTAL_REGION_SALES=("Sales", "sum"),
            TOTAL_REGION_PROFIT=("Profit", "sum"),
            AVG_REGION_PROFIT=("Profit", "mean"),
            MAX_REGION_SALES=("Sales", "max"),
            MIN_REGION_SALES=("Sales", "min"),
            TOTAL_REGION_ORDERS=("Order_ID", "count")
        )
        .reset_index()
    )

    return region_dim


# -------- JOIN (FACT + DIMENSION STYLE) --------
def join_fact_with_dimension(df: pd.DataFrame, region_dim: pd.DataFrame) -> pd.DataFrame:

    joined_df = pd.merge(
        df,
        region_dim,
        on="Region",
        how="left"
    )

    return joined_df


# -------- SORT --------
def apply_sorting(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(by="Profit", ascending=False)
def convert_numeric_columns(df):

    df["Sales"] = pd.to_numeric(df["Sales"], errors="coerce")
    df["Profit"] = pd.to_numeric(df["Profit"], errors="coerce")
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["Discount"] = pd.to_numeric(df["Discount"], errors="coerce")

    return df

# -------- MASTER TRANSFORMATION --------
def apply_all_transformations(df: pd.DataFrame) -> pd.DataFrame:
    df = convert_numeric_columns(df)
    df = apply_string_transformations(df)
    df = apply_regex_cleaning(df)
    df = apply_date_transformations(df)
    df = apply_filter(df)

    region_dimension = create_region_dimension(df)
    df = join_fact_with_dimension(df, region_dimension)

    df = apply_sorting(df)

    print("All transformations applied successfully")

    return df


# =========================================================
# 3️⃣ LOAD (UPDATED HERE ✅)
# =========================================================

def load_to_snowflake(df: pd.DataFrame, table_name: str):

    # ✅ Add consistent LOAD_DT for entire run
    load_dt = datetime.now()
    df["LOAD_DT"] = load_dt

    df.columns = df.columns.str.upper()  # Snowflake safe

    engine = get_snowflake_engine()

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000
    )

    print(f"Data loaded into Snowflake successfully with LOAD_DT = {load_dt}")


# =========================================================
# 4️⃣ MAIN ETL PIPELINE
# =========================================================

def run_etl():

    # STEP 1: Extract
    raw_df = extract_data()

    # STEP 2: Transform
    transformed_df = apply_all_transformations(raw_df)

    # STEP 3: Load
    load_to_snowflake(transformed_df, "SUPERSTORE_TRANSFORMED")


if __name__ == "__main__":
    run_etl()