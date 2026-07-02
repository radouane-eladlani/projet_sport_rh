# ==============================================================================
# 07 - INGESTION STREAMING : REDPANDA → SPARK → DELTA LAKE (BRONZE LAYER)
# ==============================================================================

# OBJECTIF GLOBAL :
# Lire en continu (Streaming) les messages de Redpanda (via Debezium)
# et les stocker de manière fiable dans la couche BRONZE du Data Lake (Format Delta).

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from dotenv import load_dotenv

# ==============================================================================
# 1. INITIALISATION DE SPARK (MOTEUR DE STREAMING BIG DATA)
# ==============================================================================

load_dotenv()

spark = SparkSession.builder \
    .appName("SPORT_RH_STREAMING_INGESTION") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Démarrage du flux Spark Streaming...")

# ==============================================================================
# 2. LECTURE DU FLUX EN CONTINU DEPUIS REDPANDA (KAFKA-LIKE)
# ==============================================================================

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "dbserver1.public.activites_sportives") \
    .option("startingOffsets", "earliest") \
    .load()

# ==============================================================================
# 3. CONVERSION DE LA VALEUR BINAIRE KAFKA EN CHAÎNE DE CARACTÈRES (JSON)
# ==============================================================================

df_string = df_kafka.selectExpr("CAST(value AS STRING) as json_payload")

# ==============================================================================
# 4. DÉFINITION DU SCHÉMA JSON
# ==============================================================================

schema_after = StructType([
    StructField("id", IntegerType(), True),
    StructField("id_salarie", IntegerType(), True),
    StructField("type_sport", StringType(), True),
    StructField("distance", DoubleType(), True),
    StructField("date_debut", StringType(), True),
    StructField("date_fin", StringType(), True),
    StructField("commentaire", StringType(), True)
])

schema_debezium = StructType([
    StructField("payload", StructType([
        StructField("after", schema_after, True)
    ]), True)
])

# ==============================================================================
# 5. DECODAGE DU JSON ET EXTRACTION DES DONNÉES (CORRIGÉ EN MINUSCULES)
# ==============================================================================

df_parsed = df_string \
    .withColumn("data", from_json(col("json_payload"), schema_debezium)) \
    .select("data.payload.after.*") \
    .filter(col("id").isNotNull())

# ==============================================================================
# 6. CALCUL EN TEMPS RÉEL : DURÉE DE L'ACTIVITÉ 
# ==============================================================================

# Puisque ce sont des microsecondes, la soustraction directe donne des microsecondes.
# On divise par 1 000 000 pour obtenir des secondes.
df_final = df_parsed.withColumn(
    "duration_sec",
    ((col("date_fin").cast("long") - col("date_debut").cast("long")) / 1000000).cast("long")
)

# ==============================================================================
# 7. CHEMINS DE STOCKAGE (DATA LAKE ARCHITECTURE)
# ==============================================================================

chemin_delta_bronze = "data/lake/bronze/activites_sportives"

chemin_checkpoint = "data/lake/checkpoints/activites_sportives"

# ==============================================================================
# 8. ÉCRITURE CONTINUE (STREAMING WRITE) VERS DELTA LAKE (COUCHE BRONZE)
# ==============================================================================

print("Démarrage de l'écriture en continu dans Delta Lake (Bronze)...")

query = df_final.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", chemin_checkpoint) \
    .start(chemin_delta_bronze)

# ==============================================================================
# 9. MAINTIEN DU STREAMING ACTIF
# ==============================================================================

query.awaitTermination()