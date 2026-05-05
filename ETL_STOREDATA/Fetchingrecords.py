import pyodbc

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

#retriveing top values
sql_cursor.execute("SELECT TOP 5 * FROM dbo.customers")
sql_rows = sql_cursor.fetchall()

for row in sql_rows:
    print(row)

#check for requried records
for i in sql_rows:
    if 2 in i or 3 in i :
        print(2 in i)
        print(3 in i)
        print(i)

customer_id = (5,6)

# SQL Server (uses ?)
sql_cursor.execute("SELECT * FROM dbo.customers WHERE id IN (?,?)",(customer_id))
print("result", sql_cursor.fetchall())

#multiple arguements
def args(*args):
    for arg in args:
        print(arg)
args(1,2,3,4,5,6,)

#multiple conditions
name = ('Rahul','Kiran')
age =(25,28)
city=('Hyderabad')

sql_cursor.execute("SELECT * FROM dbo.customers WHERE name IN (?,?) AND age IN (?,?) AND city IN (?)",
                   (*name, *age, city))
print("SQL Server Multi Condition Result:", sql_cursor.fetchall())


#BETWEEN CLAUSE
min_age = 25
max_age = 30

sql_cursor.execute(
    "SELECT * FROM dbo.customers WHERE age BETWEEN ? AND ?",
    (min_age, max_age)
)
print("\nSQL Server BETWEEN:", sql_cursor.fetchone())