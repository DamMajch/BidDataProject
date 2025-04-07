from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import os
import sys

# Debugowanie ścieżek modułów
print("🔍 Ścieżki modułów Pythona:", sys.path)

# Sprawdź import modułu typing
try:
    import typing

    print("✅ Moduł typing zaimportowany pomyślnie z:", typing.__file__)
except Exception as e:
    print("❌ Błąd importu modułu typing:", e)
    raise ImportError("Moduł typing jest uszkodzony lub konfliktuje z lokalnym plikiem/folderem.")

# Konfiguracja zmiennych środowiskowych
os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk-11.0.25.9-hotspot"
os.environ["SPARK_HOME"] = "C:\\spark-3.5.5-bin-hadoop3"

# Debugowanie zmiennych środowiskowych
print(f"🔍 JAVA_HOME ustawiony na: {os.environ['JAVA_HOME']}")
print(f"🔍 SPARK_HOME ustawiony na: {os.environ['SPARK_HOME']}")

# Weryfikacja zmiennych środowiskowych
if not os.path.exists(os.environ["JAVA_HOME"]):
    raise FileNotFoundError(f"Błąd: Ścieżka JAVA_HOME ({os.environ['JAVA_HOME']}) nie istnieje.")
if not os.path.exists(os.environ["SPARK_HOME"]):
    raise FileNotFoundError(f"Błąd: Ścieżka SPARK_HOME ({os.environ['SPARK_HOME']}) nie istnieje.")

# Inicjalizacja SparkSession
builder = SparkSession.builder \
    .appName("DeltaSave") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

try:
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    print("✅ SparkSession został zainicjalizowany pomyślnie.")
except Exception as e:
    print("❌ Błąd inicjalizacji SparkSession.")
    raise RuntimeError(
        "Błąd inicjalizacji SparkSession. "
        "Upewnij się, że JAVA_HOME i SPARK_HOME są poprawnie skonfigurowane oraz że Spark działa.") from e

# Załaduj dane z CSV
csv_path = "data/processed/alcohol_data.csv"
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"Plik CSV ({csv_path}) nie został znaleziony.")

df = spark.read.option("header", True).csv(csv_path)

# Zapis jako Delta Lake
delta_path = "data/delta/alcohol"
# noinspection PyPackageRequirements
try:
    df.write.format("delta").mode("overwrite").save(delta_path)
    print(f"✅ Dane zapisane jako Delta Lake w: {delta_path}")
except Exception as e:
    raise RuntimeError(f"Nie udało się zapisać danych w Delta Lake pod ścieżką: {delta_path}.") from e
