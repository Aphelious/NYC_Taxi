import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()

df = pd.read_csv('/Users/mike/Desktop/Main/Programming/Projects/Tutorials/Datazoomcamp/week1/2022_jan_green.csv')
# df_iter = pd.read_csv('/Users/mike/Desktop/Main/Programming/Projects/Tutorials/Datazoomcamp/week1/2022_jan_yellow.csv', iterator=True, chunksize=100000)
# df = next(df_iter)
# print(df.info())

# while True:
#     df = next(df_iter)
#
#     df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
#     df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
#
#     df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
#     print("Inserted another chunk...")

# print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))
def ingest_green_data():
    df = pd.read_csv('/Users/mike/Desktop/Main/Programming/Projects/Tutorials/Datazoomcamp/week1/2022_jan_green.csv')

    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    df.head(0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')
    df.to_sql(name='green_taxi_data', con=engine, if_exists='replace')
