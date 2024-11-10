import pandas as pd

# Load the Parquet file
parquet_file = r'D:\SuperGCPClasses\python_scripts\csv_to_parquet\output.parquet'
df = pd.read_parquet(parquet_file)

# Print all column names to check the exact name of 'cid'
print(df.columns)

# If 'cid' has additional characters, rename it
df.columns = df.columns.str.strip()  # Strips any leading/trailing whitespace
df.rename(columns=lambda x: x.split(',')[0] if 'cid' in x else x, inplace=True)

# Now apply the cleaning operation
if 'cid' in df.columns:
    df['cid'] = df['cid'].str.strip(',')

# Save the cleaned DataFrame back to Parquet
clean_parquet_file = r'D:\SuperGCPClasses\python_scripts\csv_to_parquet\output2.parquet'
df.to_parquet(clean_parquet_file)

# Verify the cleaned column
print(df['cid'].head())

