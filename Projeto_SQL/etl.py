import pandas as pd
import sqlalchemy

engine = sqlalchemy.create_engine("sqlite:///database.db")

# %%
with open("etl_projeto.sql") as open_file:
    query = open_file.read()

print(query)

# %%
print(query.format(date = '2025-02-01'))
# %%
dates = [
    '2025-01-01',
    '2025-02-01',
    '2025-03-01',
    '2025-04-01',
    '2025-05-01',
    '2025-06-01'
]

for i in dates:
    df = pd.read_sql(query.format(date=i),engine)
    df.to_sql("feature_store_cliente",engine, index=False,if_exists="append")
    