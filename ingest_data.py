import os
import argparse
from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(params):
  file_url = params["url"]
  db_host = params["host"]
  db_user = params["user"]
  db_password = params["password"]
  db_name = params["db"]
  table_name = params["table_name"]
  port = params["port"]

  if file_url.endswith('.csv.gz'):
    csv_name = 'output.csv.gz'
  else:
    csv_name = 'output.csv'

  # url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-09.parquet'

  os.system(f'wget {file_url} -O {csv_name}')

  engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{port}/{db_name}')

  df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

  df = next(df_iter)

  if 'lpep_pickup_datetime' in df.columns:
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

  df.head(0).to_sql(table_name, engine, if_exists='replace')

  while True:
    try:
        t_start = time()

        if 'lpep_pickup_datetime' in df.columns:
          df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
          df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

        df = next(df_iter)

    except StopIteration:
        print("Finished ingesting data into the postgres database")
        break

if __name__ == "__main__":
  print(os.environ)
  # parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')

  # parser.add_argument('--user', required=True, help='user name for postgres')
  # parser.add_argument('--password', required=True, help='password for postgres')
  # parser.add_argument('--host', required=True, help='host for postgres')
  # parser.add_argument('--port', required=True, help='port for postgres')
  # parser.add_argument('--db', required=True, help='database name for postgres')
  # parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
  # parser.add_argument('--url', required=True, help='url of the parquet file')

  # args = parser.parse_args()

  main(os.environ)
  print('Done!')
