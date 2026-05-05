import pyodbc
import snowflake.connector

try:
    sql_connection = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=ETL_DB;"
        "Trusted_Connection=yes;"
    )
    sql_cursor = sql_connection.cursor()
    print("Connected to SQL Server Successfully")

except Exception as e:
    print("SQL Server Connection Error:", e)


try:
    sf_connection = snowflake.connector.connect(
        user='***',
        password='****',
        account='fdmdkjh-pa09017',
        warehouse='ETL_WH',
        database='ETL_DB',
        schema='PUBLIC',
        role='ACCOUNTADMIN'
    )

    sf_cursor = sf_connection.cursor()
    print("Connected to Snowflake Successfully")

except Exception as e:
    print("Snowflake Connection Error:", e)


print("\n--- SQL Server Data ---")
sql_cursor.execute("SELECT * FROM dbo.customers")
sql_rows = sql_cursor.fetchmany(4)

for row in sql_rows:
    print(row)

print("\n--- Snowflake Data ---")
sf_cursor.execute("SELECT * FROM customers")
sf_rows = sf_cursor.fetchall()

for row in sf_rows:
    print(row)


customer_id = 6

# SQL Server (uses ?)
sql_cursor.execute(
    "SELECT * FROM dbo.customers WHERE id = ?",
    (customer_id,)
)
print("\nSQL Server - ID 1:", sql_cursor.fetchone())

# Snowflake (uses %s)
sf_cursor.execute(
    "SELECT * FROM customers WHERE id = %s",
    (customer_id,)
)
print("Snowflake - ID 1:", sf_cursor.fetchone())

#MULTIPLE CONDITIONS


name = 'Rahul'
age =25
name = 'sai'
age = 22

sql_cursor.execute(
    "SELECT * FROM dbo.customers WHERE name = ? AND age = ?",
    (name, age)
)
print("\nSQL Server Multi Condition:", sql_cursor.fetchall())

sf_cursor.execute(
    "SELECT * FROM customers WHERE name = %s AND age = %s",
    (name, age)
)
print("Snowflake Multi Condition:", sf_cursor.fetchall())


#BETWEEN CLAUSE

min_age = 25
max_age = 30

sql_cursor.execute(
    "SELECT * FROM dbo.customers WHERE age BETWEEN ? AND ?",
    (min_age, max_age)
)
print("\nSQL Server BETWEEN:", sql_cursor.fetchone())

sf_cursor.execute(
    "SELECT * FROM customers WHERE age BETWEEN %s AND %s",
    (min_age, max_age)
)
print("Snowflake BETWEEN:", sf_cursor.fetchone())

# fetchone()
one_row = sf_cursor.fetchone()
print("one_row",one_row)

# fetchone()
two_row = sql_cursor.fetchone()
print("two_row",two_row)

# fetchmany(n)
sf_cursor.execute("SELECT * FROM customers")
many_rows = sf_cursor.fetchmany(5)
print("many_rows",many_rows)

# fetchall()
sf_cursor.execute("SELECT * FROM customers")
all_rows = sf_cursor.fetchall()
print("all_rows",all_rows)

# fetchmany(n)
sql_cursor.execute("SELECT * FROM customers")
many_rows = sql_cursor.fetchmany(5)
print("many_rows",many_rows)

# fetchall()
sql_cursor.execute("SELECT * FROM customers")
all_rows = sql_cursor.fetchall()
print("all_rows",all_rows)

sql_cursor.close()
sql_connection.close()

sf_cursor.close()
sf_connection.close()

print("\n Connections Closed Successfully")


