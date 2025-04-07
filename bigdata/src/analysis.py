import pandas as pd

# Wczytanie przetworzonych danych
df = pd.read_csv("../data/processed/alcohol_data.csv")

# Podstawowe statystyki
print(df.describe())

# Znalezienie krajów z najwyższym i najniższym spożyciem alkoholu
top_countries = df.nlargest(10, 'total_litres_of_pure_alcohol')
bottom_countries = df.nsmallest(10, 'total_litres_of_pure_alcohol')

print("Kraje z najwyższym spożyciem alkoholu:")
print(top_countries[['country', 'total_litres_of_pure_alcohol']])

print("Kraje z najniższym spożyciem alkoholu:")
print(bottom_countries[['country', 'total_litres_of_pure_alcohol']])
