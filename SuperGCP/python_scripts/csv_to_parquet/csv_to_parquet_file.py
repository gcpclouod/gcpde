import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def csv_to_parquet(csv_file_path, parquet_file_path):
    # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv(csv_file_path)

    # Convert the DataFrame to a PyArrow Table
    table = pa.Table.from_pandas(df)

    # Write the Table to a Parquet file
    pq.write_table(table, parquet_file_path)

    print(f"Successfully converted {csv_file_path} to {parquet_file_path}")


# Example usage
csv_file_path = "python_scripts/csv_to_parquet/input.csv"
parquet_file_path = "output_data.parquet"

csv_to_parquet(csv_file_path, parquet_file_path)
