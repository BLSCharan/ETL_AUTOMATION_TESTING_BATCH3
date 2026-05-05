import pyodbc

try:
    connection = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=ETL_DB;"
        "Trusted_Connection=yes;"
    )

    cursor = connection.cursor()
    print("Connected Successfully")

    # First Query
    cursor.execute("SELECT * FROM dbo.customers")
    rows = cursor.fetchall()

    print("All Customers:")
    for row in rows:
        print(row)

    # Second Query
    cursor.execute("SELECT COUNT(*) FROM dbo.customers")
    count = cursor.fetchone()
    print(count)
    print("Total Records:", count)

    # Third Query (Parameterized)
    customer_id = 1
    cursor.execute(
        "SELECT * FROM dbo.customers WHERE id = ?",
        customer_id
    )
    result = cursor.fetchone()
    print("Customer with ID 1:", result)

    cursor.execute(
        "INSERT INTO customers VALUES (?, ?, ?, ?)",
        (8, 'param', 'mumbai', 22)
    )

    connection.commit()

    cursor.execute("SELECT * FROM dbo.customers")
    rows = cursor.fetchall()

    print("All Customers:")
    for row in rows:
        print(row)

except Exception as e:
    print("Error:", e)

finally:
    cursor.close()
    connection.close()