import mysql.connector
from mysql.connector import Error

def fetch_all_records():
    try:
        # Set up the database connection
        connection = mysql.connector.connect(
            host='34.66.73.145',
            user='root',
            password='Hani',
            database='customers'
        )
        
        if connection.is_connected():
            print("Connected to the database")
            
            cursor = connection.cursor()
            query = "SELECT ProductID, ProductName, Price, Description, ImageURL FROM Products"  # Replace 'your_table_name' with the actual table name
            cursor.execute(query)
            
            # Fetch all rows from the executed query
            rows = cursor.fetchall()
            print(type(rows))
            print(rows)
            
            # Process the results
            for row in rows:
                print(row)
            
            cursor.close()
        else:
            print("Failed to connect to the database")
    
    except Error as e:
        print(f"Error: {e}")
    
    finally:
        if connection.is_connected():
            connection.close()
            print("Database connection closed")

# Run the function
fetch_all_records()
