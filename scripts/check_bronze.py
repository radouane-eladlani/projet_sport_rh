from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckBronze") \
    .master("local[*]") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Lecture des données déjà stockées dans ta couche Bronze
df = spark.read.format("delta").load("data/lake/bronze/activites_sportives")

# .show() est l'équivalent du "print" pour afficher le contenu d'un DataFrame
df.show(20, truncate=False)