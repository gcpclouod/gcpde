import mysql.connector
from mysql.connector import Error

def fetch_records_from_db(host, database, user, password):
    """Connect to MySQL and fetch records from Products and Orders tables"""
    try:
        # Establishing the connection
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        
        if connection.is_connected():
            print("Successfully connected to MySQL database")
            cursor = connection.cursor()

            # Querying the Products table
            print("\nFetching records from Products table:")
            cursor.execute("SELECT * FROM Products")
            products = cursor.fetchall()

            # Displaying the products
            for row in products:
                print(f"ProductID: {row[0]}, ProductName: {row[1]}, Price: {row[2]}, Stock: {row[3]}")
            
            # Querying the Orders table
            print("\nFetching records from Orders table:")
            cursor.execute("SELECT * FROM Orders")
            orders = cursor.fetchall()

            # Displaying the orders
            for row in orders:
                print(f"OrderID: {row[0]}, CustomerID: {row[1]}, ProductID: {row[2]}, OrderDate: {row[3]}, "
                      f"Quantity: {row[4]}, TotalAmount: {row[5]}")

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    finally:
        # Close the connection if it is open
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("\nMySQL connection is closed")

# Replace the following with your actual MySQL connection details
fetch_records_from_db(
    host='34.171.34.164',       # e.g., '127.0.0.1' or your MySQL server IP
    database='ecommerce',      # Your MySQL database name
    user='root',    # Your MySQL username
    password='hani' # Your MySQL password
)