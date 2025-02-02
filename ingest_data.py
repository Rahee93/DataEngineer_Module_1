import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table_name = params.table_name
    file_path = params.file_path  # Now pointing to CSV file

    
    print("Reading data...")
    # Read CSV instead of Parquet
    green_taxis = pd.read_csv(file_path, low_memory=False, nrows=1000)  # Only the first 1000 rows
    print("Reading data...")


    # Connecting to the PostgreSQL database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    engine.connect()

    # Transform the dataframe to a DDL (data definition language) to specify the schema
    print(pd.io.sql.get_schema(green_taxis, name=table_name, con=engine))

    BATCH_SIZE = 10000
    with engine.begin() as connection:  # Ensures transaction handling
        for start in range(0, len(green_taxis), BATCH_SIZE):
            batch = green_taxis.iloc[start:start + BATCH_SIZE]  # Get batch of rows
            batch.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
            print(f"Inserted batch {start} to {start + len(batch)}")
    
    print("All data inserted successfully!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='User name for postgres')
    parser.add_argument('--password', help='Password for postgres')
    parser.add_argument('--host', help='Host for postgres')
    parser.add_argument('--port', help='Port for postgres')
    parser.add_argument('--database', help='Database name for postgres')
    parser.add_argument('--table_name', help='Table name for postgres')
    parser.add_argument('--file_path', help='Path to the CSV file')  # Changed to file path argument

    args = parser.parse_args()
    main(args)
