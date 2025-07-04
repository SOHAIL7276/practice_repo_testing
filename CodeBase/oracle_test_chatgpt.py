#this code i generated from chartgpt to checck connection with racle and running query in it.
# results is fine

import pandas as pd
from sqlalchemy import create_engine

oracle_engine = create_engine("oracle+cx_oracle://system:NewPassword123@localhost:1521/orcl")

query =  "SELECT * FROM STORES_CITY"

try:
    df = pd.read_sql(query, oracle_engine)
    print("Fetched rows:", len(df))
    print(df.head())
except Exception as e:
    print("Error:", e)
