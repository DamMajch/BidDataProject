import pandas as pd

# Wczytanie danych o spożyciu alkoholu
url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/alcohol-consumption/drinks.csv"
df = pd.read_csv(url)

# Wyświetlenie pierwszych wierszy
print(df.head())
