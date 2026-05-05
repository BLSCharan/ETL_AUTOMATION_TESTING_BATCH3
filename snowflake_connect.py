import snowflake.connector

conn = snowflake.connector.connect(
    user='BLSCHARAN',
    password='Scheryy@12345678',
    account='myhrjeq-zq60870',
    warehouse='ETL_WH',
    database='ETL_DB',
    schema='PUBLIC',
    role= "ACCOUNTADMIN",
)

cursor = conn.cursor()

cursor.execute("SELECT * FROM customers")

rows = cursor.fetchall()

for row in rows:
    print(row)

cursor.close()
conn.close()
