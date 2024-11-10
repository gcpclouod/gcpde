import mysql.connector # downloading --
from mysql.connector import Error # function, method, class object -  

def connect_to_mysql(host, database, user, password):
    """ Connect to MySQL database """
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
            # Fetch and print MySQL server info
            db_info = connection.get_server_info()
            print(f"MySQL Server version: {db_info}")
            
            # Create a cursor object to interact with the database
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM `ecommerce`.`orders`;")
            db = cursor.fetchone()
            print(f"You're connected to the database: {db}")

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    finally:
        # Close the connection if it is open
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Replace the following with your actual MySQL connection details
connect_to_mysql(
    host='34.171.34.164',       # e.g., '127.0.0.1' or your MySQL server IP
    database='ecommerce',      # Your MySQL database name
    user='root',    # Your MySQL username
    password='hani' # Your MySQL password
)
