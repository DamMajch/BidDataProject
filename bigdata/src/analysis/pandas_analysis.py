import pandas as pd
from sqlalchemy import create_engine

df = pd.read_sql("SELECT * FROM alcohol_consumption", create_engine("postgresql+psycopg2://postgres:polaki@localhost:5432/postgres"))

print(df.groupby("country")["servings"].sum().sort_values(ascending=False).head())
