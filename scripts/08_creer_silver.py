# ==============================================================================
# SCRIPT 08 : PASSAGE BRONZE → SILVER (NETTOYAGE + ENRICHISSEMENT + CONTRÔLE)
# ==============================================================================
# OBJECTIF GLOBAL :
# Transformer des données BRUTES (Bronze) en données PROPRES et UTILISABLES (Silver)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, coalesce, lit, when
import pandas as pd

# ==============================================================================
# 1. INITIALISATION SPARK (MOTEUR DE TRAITEMENT BIG DATA)
# ==============================================================================
spark = SparkSession.builder \
    .appName("SportRH_Bronze_to_Silver") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# CHEMINS DES DONNÉES (ARCHITECTURE DATA LAKE)
chemin_bronze = "data/lake/bronze/activites_sportives"
chemin_silver = "data/lake/silver/activites_sportives"
chemin_rh = "data/donnees_fusionnees.csv"

# ==============================================================================
# 2. CHARGEMENT DU RÉFÉRENTIEL RH 
# ==============================================================================
print(f"Lecture du référentiel RH : {chemin_rh}")
pdf_rh = pd.read_csv(chemin_rh)
df_rh_spark = spark.createDataFrame(pdf_rh.astype(str))

df_rh_lookup = df_rh_spark.select(
    col("employee_id").cast("integer").alias("id_salarie_rh"),
    col("moyen_transport").alias("mode_transport_declare"),
    col("distance_domicile_entreprise_km").cast("double").alias("distance_domicile_travail_km"),
    col("erreur_declaration_transport").alias("erreur_flag")
).distinct()

df_google_lookup = df_rh_lookup.withColumn(
    "alerte_transport",
    when(col("erreur_flag") == "True", "FRAUDE").otherwise("OK")
).drop("erreur_flag")

# ==============================================================================
# 3. LECTURE BRONZE + NETTOYAGE
# ==============================================================================
print("Lecture des données Bronze...")
df_bronze = spark.read.format("delta").load(chemin_bronze)

df_silver_base = df_bronze \
    .withColumn("date_debut_clean", (col("date_debut").cast("long") / 1000000).cast("timestamp")) \
    .withColumn("date_fin_clean", (col("date_fin").cast("long") / 1000000).cast("timestamp")) \
    .withColumn("distance_clean", coalesce(col("distance").cast("double"), lit(0.0))) \
    .withColumn("id_salarie_int", col("id_salarie").cast("integer")) \
    .dropDuplicates(["id_salarie_int", "type_sport", "date_debut"])

# ==============================================================================
# 4. ENRICHISSEMENT MÉTIER (JOINTURE RH + SPORT)
# ==============================================================================
df_silver_final = df_silver_base.join(
    df_google_lookup,
    df_silver_base.id_salarie_int == df_google_lookup.id_salarie_rh,
    "left"
).select(
    col("id").alias("id_evenement"),
    col("id_salarie_int").alias("id_salarie"),
    col("type_sport"),
    col("distance_clean").alias("distance_metres"),
    (col("duration_sec") / 60).cast("integer").alias("duration_min"),
    col("date_debut_clean").alias("date_debut"),
    col("date_fin_clean").alias("date_fin"),
    col("mode_transport_declare"),
    col("distance_domicile_travail_km"),
    col("alerte_transport"),
    col("commentaire")
)

print("Aperçu des données converties en Silver :")
df_silver_final.select("id_evenement", "date_debut", "date_fin", "duration_min").show(5)

# ==============================================================================
# 5. GESTION DES DONNÉES MANQUANTES 
# ==============================================================================
df_silver_final = df_silver_final.fillna({
    "distance_metres": 0.0,
    "duration_min": 0,
    "alerte_transport": "INCONNU"
})

# ==============================================================================
# 6. CONTRÔLE QUALITÉ (DATA QUALITY CHECK)
# ==============================================================================
print("Vérification qualité des données...")
nb_negatif = df_silver_final.filter(col("distance_metres") < 0).count()
if nb_negatif > 0:
    print(f"ALERTE : {nb_negatif} lignes avec distances négatives")

# ==============================================================================
# 7. SAUVEGARDE SILVER
# ==============================================================================
print(f"Écriture vers Silver : {chemin_silver}")
df_silver_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(chemin_silver)

print("Étape Silver terminée : données prêtes pour analyse KPI / Gold")