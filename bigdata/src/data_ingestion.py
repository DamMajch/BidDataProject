import os
import pandas as pd

# Wczytanie danych o alkoholu
url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/alcohol-consumption/drinks.csv"
df = pd.read_csv(url)

# Wczytanie danych o PKB i populacji z poprawioną ścieżką
gdp_pop_df = pd.read_csv("C:/Users/spoxm/Desktop/bigdata/data/raw/GDP_Data.csv")

# Sprawdzenie rzeczywistej nazwy kolumny z krajami
print(gdp_pop_df.columns)  # To pomoże upewnić się, że znamy poprawną nazwę

# Zmiana nazwy kolumny na "country", jeśli to konieczne
gdp_pop_df.rename(columns={"Country Name": "country"}, inplace=True)

# Połączenie danych na podstawie kraju
df = df.merge(gdp_pop_df, on="country", how="left")

# Tworzenie folderu, jeśli nie istnieje
output_dir = "data/processed"
os.makedirs(output_dir, exist_ok=True)

# Zapis do pliku przetworzonego
df.to_csv(f"{output_dir}/alcohol_data.csv", index=False)

print("Dane zostały przetworzone i zapisane.")
