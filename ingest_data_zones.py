import pandas as pd
from sqlalchemy import create_engine

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

df= pd.read_csv(url, nrows=1)
engine = create_engine(f'postgresql://root:root@localhost:5432/ny_taxi', pool_pre_ping=True)


engine.connect()

df.to_sql(name="zones",
            con=engine,
            if_exists='replace',
            index=False)

df_iter = pd.read_csv(url, chunksize=50)

for chunk in df_iter:
    chunk.to_sql(name="zones",con=engine,if_exists='append',index=False)

print(pd.io.sql.get_schema(df, name="yellow_tripdata_2021-01", con = engine ))