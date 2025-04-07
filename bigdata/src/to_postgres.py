import pandas as pd
from sqlalchemy import create_engine

# Dane do logowania
db_url = "postgresql+psycopg2://postgres:HASLO@localhost:5432/postgres"

# Wczytaj dane przetworzone wcześniej
df = pd.read_csv("data/processed/alcohol_data.csv")

# Połączenie z bazą
engine = create_engine(db_url)

# Zapis do tabeli
df.to_sql("alcohol_consumption", engine, if_exists="replace", index=False)

print("✅ Dane zapisane do PostgreSQL.")
