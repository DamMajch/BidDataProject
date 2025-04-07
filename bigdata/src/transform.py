import pandas as pd

# Wczytaj przetworzone dane
df = pd.read_csv("data/processed/alcohol_data.csv")

# Upewnij się, że są kolumny z rodzajami alkoholu
alcohol_cols = ['beer_servings', 'spirit_servings', 'wine_servings']

# Zamień z formatu szerokiego na długi
df_long = df.melt(
    id_vars=["country"],
    value_vars=alcohol_cols,
    var_name="alcohol_type",
    value_name="servings"
)

# Na przykład możesz dodać kolumnę kontynent, jeśli chcesz rozszerzyć analizy
# (na razie pomijamy, ale można dorzucić później)

# Zapisz dane przekształcone
df_long.to_csv("data/processed/alcohol_data_long.csv", index=False)

print("✅ Dane przekształcone do formatu long i zapisane.")
