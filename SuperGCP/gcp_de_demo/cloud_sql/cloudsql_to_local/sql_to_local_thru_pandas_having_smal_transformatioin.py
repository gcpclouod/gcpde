import mysql.connector
import pandas as pd

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

    # Fetch column names
    columns = [desc[0] for desc in cursor.description]

    # Fetch all results
    results = cursor.fetchall()

    # Convert results to a DataFrame
    df = pd.DataFrame(results, columns=columns)

    # Filter DataFrame to include only Male gender
    df = df[df['Gender'] == 'Male']

    # Join FirstName and LastName into a new column FullName
    df['FullName'] = df['FirstName'] + ' ' + df['LastName']

    # Drop FirstName and LastName columns
    df = df.drop(columns=['FirstName', 'LastName'])

    # Print the DataFrame with the new column
    print(df)

    # Save DataFrame to a CSV file
    df.to_csv('customers_data_with_fullname_male.csv', index=False)

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()
