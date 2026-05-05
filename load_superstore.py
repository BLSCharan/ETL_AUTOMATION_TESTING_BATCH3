import pandas as pd
from sqlalchemy import create_engine
import urllib

csv_path = r"C:\ETL_Project\superstore.csv"

# Read CSV
df = pd.read_csv(csv_path, encoding="latin1")

print("CSV Loaded Successfully")
print("Total Rows:", len(df))

# 🔥 IMPORTANT FIX: Rename columns to match SQL table
df.columns = df.columns.str.replace(" ", "_").str.replace("-", "_")

print("Columns After Rename:")
print(df.columns)

# SQL Server connection
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=LENOVO\\SQLEXPRESS;"
    "DATABASE=ETL_SUPERSTORE;"
    "Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Insert data
df.to_sql(
    name="superstore_raw",
    con=engine,
    if_exists="append",
    index=False
)

print("Data loaded into SQL Server successfully!")