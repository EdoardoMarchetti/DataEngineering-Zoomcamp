#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def ingest_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
) -> pd.DataFrame:
    """
    Ingest parquet data into PostgreSQL database in chunks.
    
    Args:
        url: URL or path to the parquet file
        engine: SQLAlchemy database engine
        target_table: Name of the target table in PostgreSQL
        chunksize: Number of rows to process in each chunk
    """
    # Read the entire parquet file into memory
    df = pd.read_parquet(url)
    
    # Calculate total number of chunks needed
    total_rows = len(df)
    num_chunks = (total_rows + chunksize - 1) // chunksize  # Ceiling division
    
    # Process the first chunk separately to create the table schema
    first_chunk = df.iloc[0:chunksize]
    
    # Create table schema without inserting data (head(0) returns empty DataFrame with schema)
    first_chunk.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )
    
    print(f"Table {target_table} created")
    
    # Insert the first chunk
    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )
    
    print(f"Inserted first chunk: {len(first_chunk)} rows")
    
    # Iterate over remaining chunks and insert them into the database
    # tqdm provides a progress bar for the iteration
    for i in tqdm(range(chunksize, total_rows, chunksize), desc="Processing chunks"):
        # Extract chunk: from index i to i+chunksize (or end of dataframe)
        end_idx = min(i + chunksize, total_rows)
        df_chunk = df.iloc[i:end_idx]
        
        # Insert chunk into database
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        
        print(f"Inserted chunk: {len(df_chunk)} rows")
    
    print(f'Done ingesting {total_rows} rows to {target_table}')

@click.command()
@click.option('--year', required=True, type=int, help='Year of the data (e.g., 2021)')
@click.option('--month', required=True, type=int, help='Month of the data (1-12)')
@click.option('--pg-user', default='postgres', help='PostgreSQL user')
@click.option('--pg-pass', default='postgres', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for data ingestion')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--url-prefix', default='https://d37ci6vzurychx.cloudfront.net/trip-data', help='URL prefix for data files')
@click.option('--taxi-type', default='yellow', help='Taxi type (yellow, green, fhv)')


def main(year, month, pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, target_table, url_prefix, taxi_type):
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    url = f'{url_prefix}/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'

    ingest_data(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )

if __name__ == '__main__':
    main()