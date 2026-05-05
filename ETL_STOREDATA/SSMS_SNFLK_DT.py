import pandas as pd
from sql_server import get_sql_server_engine
from sf_connection import get_snowflake_engine


# ---------------------------------
# EXTRACT FROM SQL SERVER
# ---------------------------------
def extract_data():
    engine = get_sql_server_engine()
    data = pd.read_sql("SELECT * FROM superstore_raw", engine)
    print(f"Extracted {len(data)} rows from SQL Server")
    return data


# ---------------------------------
# LOAD TO SNOWFLAKE RAW
# ---------------------------------
def load_raw_to_snowflake(data):
    # 🔥 Convert column names to uppercase
    data.columns = data.columns.str.upper()

    engine = get_snowflake_engine()

    data.to_sql(
        name="SUPERSTORE",  # your table name
        con=engine,
        if_exists="append",
        index=False,
        method="multi"
    )

    print("Data loaded successfully into Snowflake 🚀")


# ---------------------------------
# MAIN
# ---------------------------------
if __name__ == "__main__":
    dataset = extract_data()
    load_raw_to_snowflake(dataset)
    print("RAW LOAD COMPLETED ✅")