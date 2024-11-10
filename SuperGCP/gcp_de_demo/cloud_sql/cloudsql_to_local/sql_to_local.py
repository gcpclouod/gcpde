import mysql.connector

# Database connection configuration
config = {
    'host': '34.66.73.145',
    'user': 'root',
    'password': 'Hani',
    'database': 'customers'
}

# Create a connection to the database
try:
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()

    # Query to fetch data from the Customers table
    query = "SELECT * FROM Customers"
    cursor.execute(query)

    # Fetch all results
    results = cursor.fetchall()

    print(results) # list of tuples

    # Print the results
    for row in results:
        print(row)

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()