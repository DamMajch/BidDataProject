import os
import pandas as pd

# Wczytanie danych o alkoholu
alcohol_url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/alcohol-consumption/drinks.csv"
alcohol_df = pd.read_csv(alcohol_url)

# Sprawdzenie poprawności kolumn
alcohol_df.rename(columns={"country": "Country Name"}, inplace=True)

# Wczytanie danych makroekonomicznych (PKB, populacja itd.)
gdp_pop_df = pd.read_csv("C:/Users/spoxm/Desktop/bigdata/data/raw/GDP_Data.csv")

# Filtrowanie na rok 2010
gdp_pop_df = gdp_pop_df[gdp_pop_df["Time"] == 2010]

# Wybór istotnych kolumn
selected_columns = [
    'Country Name',
    'Population, total [SP.POP.TOTL]',
    'Surface area (sq. km) [AG.SRF.TOTL.K2]',
    'Mortality rate, under-5 (per 1,000 live births) [SH.DYN.MORT]',
    'GDP (current US$) [NY.GDP.MKTP.CD]',
    'GDP per capita (current US$) [NY.GDP.PCAP.CD]'
]
gdp_pop_df = gdp_pop_df[selected_columns]

# Połączenie danych na podstawie kraju
merged_df = alcohol_df.merge(gdp_pop_df, on="Country Name", how="left")

# Tworzenie folderu, jeśli nie istnieje
output_dir = "data/processed"
os.makedirs(output_dir, exist_ok=True)

# Zapis do pliku przetworzonego
merged_df.to_csv(f"{output_dir}/alcohol_data.csv", index=False)

print("✅ Dane zostały przetworzone i zapisane do 'data/processed/alcohol_data.csv'")
