

import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# Define data types for zone lookup CSV columns
dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}


def ingest_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
) -> pd.DataFrame:
    """
    Ingest CSV zone data into PostgreSQL database in chunks.
    
    Args:
        url: URL or path to the CSV file
        engine: SQLAlchemy database engine
        target_table: Name of the target table in PostgreSQL
        chunksize: Number of rows to process in each chunk
    """
    # Read CSV file with iterator to process in chunks
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        iterator=True,
        chunksize=chunksize
    )

    # Get the first chunk to create the table schema
    first_chunk = next(df_iter)

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
        if_exists="append"
    )

    print(f"Inserted first chunk: {len(first_chunk)} rows")

    # Iterate over remaining chunks and insert them into the database
    # tqdm provides a progress bar for the iteration
    for df_chunk in tqdm(df_iter, desc="Processing chunks"):
        # Insert chunk into database
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        print(f"Inserted chunk: {len(df_chunk)} rows")

    print(f'Done ingesting to {target_table}')

@click.command()
@click.option('--pg-user', default='postgres', help='PostgreSQL user')
@click.option('--pg-pass', default='postgres', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for data ingestion')
@click.option('--target-table', default='zones', help='Target table name')
@click.option('--url', default='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv', help='URL to the zone lookup CSV file')


def main(pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, target_table, url):
    """
    Main function to ingest taxi zone lookup data into PostgreSQL.
    """
    # Create database engine connection
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Ingest data from CSV URL
    ingest_data(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )

if __name__ == '__main__':
    main()
