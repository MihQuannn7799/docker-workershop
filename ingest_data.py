#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import argparse
import requests

dtype = { "VendorID": "Int64",
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


def download_and_convert_parquet(parquet_url, parquet_file, csv_file):
    print("Downloading parquet from web...")
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    r = requests.get(parquet_url, headers=headers)
    r.raise_for_status()

    with open(parquet_file, "wb") as f:
        f.write(r.content)

    print("Reading parquet...")
    df = pd.read_parquet(parquet_file)

    print("Converting to CSV...")
    df.to_csv(csv_file, index=False)



def ingest_data(csv_file, engine, target_table, chunksize = 100000):
    df_iter = pd.read_csv(
        csv_file,
        dtype=dtype,
        iterator=True,
        chunksize=chunksize
    )

    first_chunk = next(df_iter)

    first_chunk.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )

    print(f"Table {target_table} created")

    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append"
    )

    print(f"Inserted first chunk: {len(first_chunk)}")

    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )

    print(f'done ingesting to {target_table}')

def main(params):
    pg_user = params.pg_user
    pg_pass = params.pg_pass
    pg_host = params.pg_host
    pg_port = params.pg_port
    pg_db = params.pg_db
    year = params.year
    month = params.month
    chunksize = params.chunksize
    target_table = params.target_table

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}', pool_pre_ping=True)
    url_prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    
    url = f'{url_prefix}/green_tripdata_{year}-{month}.parquet'
    
    parquet_file = "data.parquet"
    csv_file = "output.csv"
    
    download_and_convert_parquet(parquet_url = url,
                                parquet_file=parquet_file,
                                csv_file=csv_file )
    
    ingest_data(
        csv_file=csv_file,
        engine=engine,
        target_table=target_table,
        chunksize=int(chunksize)
    )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    # pg_user 
    # pg_pass 
    # pg_host
    # pg_port
    # pg_db
    # year
    # month
    # chunksize
    # target_table
    
    parser.add_argument('--pg-user', help="user name for postgres")
    parser.add_argument('--pg-pass', help="password name for postgres")
    parser.add_argument('--pg-host', help="host for postgres")
    parser.add_argument('--pg-port', help="port for postgres")
    parser.add_argument('--pg-db', help="database name for postgres")
    parser.add_argument('--year', help="year of the data")
    parser.add_argument('--month', help="month of the data")
    parser.add_argument('--chunksize', help="Chunk size for ingestion")
    parser.add_argument('--target-table', help="name of the table")
    
    args = parser.parse_args()
    main(args)