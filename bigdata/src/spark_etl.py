from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Konfiguracja SparkSession dla obsługi Delta Lake
builder = SparkSession.builder \
    .appName("ReadDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Ścieżka do danych w formacie Delta Lake
delta_path = "data/delta/alcohol"

try:
    # Ładowanie danych Delta
    df = spark.read.format("delta").load(delta_path)
    df.show()
except Exception as e:
    print(f"Error reading Delta table from path {delta_path}: {e}")
