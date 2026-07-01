# ==============================================================================
# SCRIPT 08 : PASSAGE BRONZE → SILVER (NETTOYAGE + ENRICHISSEMENT + CONTRÔLE)
# ==============================================================================

# OBJECTIF GLOBAL :
# Transformer des données BRUTES (Bronze) en données PROPRES et UTILISABLES (Silver)
# pour analyse métier, KPI et reporting.

# Ce script fait 3 choses principales :
# 1. Nettoyage des données sportives (formats, types, doublons)
# 2. Enrichissement avec les données RH (transport + fraude)
# 3. Contrôle qualité + sauvegarde dans Delta Lake


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, coalesce, lit, when
import pandas as pd


# ==============================================================================
# 1. INITIALISATION SPARK (MOTEUR DE TRAITEMENT BIG DATA)
# ==============================================================================

# SparkSession = point d’entrée obligatoire pour utiliser Spark
# Ici on configure Spark avec Delta Lake (stockage type Data Lake)

spark = SparkSession.builder \
.appName("SportRH_Bronze_to_Silver") \
.config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
.getOrCreate()

# Réduction des logs Spark pour garder uniquement les erreurs importantes
spark.sparkContext.setLogLevel("WARN")


# ==============================================================================
# CHEMINS DES DONNÉES (ARCHITECTURE DATA LAKE)
# ==============================================================================

# Bronze = données brutes issues du streaming (Redpanda / Debezium)
chemin_bronze = "data/lake/bronze/activites_sportives"

# Silver = données nettoyées et prêtes pour analyse
chemin_silver = "data/lake/silver/activites_sportives"

# Référentiel RH enrichi (venant du script 03)
chemin_rh = "data/donnees_fusionnees.csv"


# ==============================================================================
# 2. CHARGEMENT DU RÉFÉRENTIEL RH (SOURCE DE VÉRITÉ MÉTIER)
# ==============================================================================

# OBJECTIF :
# Charger les données RH (salariés + transport + fraude + distance domicile)

print(f"📋 Lecture du référentiel RH : {chemin_rh}")

# Lecture du fichier CSV avec Pandas (simple pour fichier local)
pdf_rh = pd.read_csv(chemin_rh)

# Conversion Pandas → Spark pour traitement distribué
df_rh_spark = spark.createDataFrame(pdf_rh.astype(str))

# On ne garde que les colonnes utiles pour la jointure métier
df_rh_lookup = df_rh_spark.select(
    col("employee_id").cast("integer").alias("id_salarie_rh"),
    col("moyen_transport").alias("mode_transport_declare"),
    col("distance_domicile_entreprise_km").cast("double").alias("distance_domicile_travail_km"),
    col("erreur_declaration_transport").alias("erreur_flag")
).distinct()

# Transformation métier :
# On transforme un booléen technique en lecture business
df_google_lookup = df_rh_lookup.withColumn(
    "alerte_transport",
    when(col("erreur_flag") == "True", "FRAUDE").otherwise("OK")
).drop("erreur_flag")


# ==============================================================================
# 3. LECTURE BRONZE + NETTOYAGE (CLEANING DATA)
# ==============================================================================

# OBJECTIF :
# Transformer des données instables (formats multiples) en données propres

print("📖 Lecture des données Bronze...")

df_bronze = spark.read.format("delta").load(chemin_bronze)

# Nettoyage des données :
# - dates incohérentes (plusieurs formats)
# - valeurs nulles
# - types incorrects
# - doublons

df_silver_base = df_bronze \
.withColumn("date_debut_clean", coalesce(
    to_timestamp(col("date_debut"), "yyyy-MM-dd HH:mm:ss"),
    to_timestamp(col("date_debut"), "yyyy-MM-dd'T'HH:mm:ss"),
    to_timestamp(col("date_debut"), "dd/MM/yyyy HH:mm"),
    col("date_debut").cast("timestamp")
)) \
.withColumn("date_fin_clean", coalesce(
    to_timestamp(col("date_fin"), "yyyy-MM-dd HH:mm:ss"),
    to_timestamp(col("date_fin"), "yyyy-MM-dd'T'HH:mm:ss"),
    to_timestamp(col("date_fin"), "dd/MM/yyyy HH:mm"),
    col("date_fin").cast("timestamp")
)) \
.withColumn("distance_clean", coalesce(col("distance").cast("double"), lit(0.0))) \
.withColumn("id_salarie_int", col("id_salarie").cast("integer")) \
.dropDuplicates(["id_salarie_int", "type_sport", "date_debut"])


# ==============================================================================
# 4. ENRICHISSEMENT MÉTIER (JOINTURE RH + SPORT)
# ==============================================================================

# OBJECTIF :
# Ajouter les informations RH à chaque activité sportive

df_silver_final = df_silver_base.join(
    df_google_lookup,
    df_silver_base.id_salarie_int == df_google_lookup.id_salarie_rh,
    "left"  # on ne perd aucune activité sportive même sans RH
).select(
    col("id").alias("id_evenement"),
    col("id_salarie_int").alias("id_salarie"),
    col("type_sport"),
    col("distance_clean").alias("distance_metres"),
    col("duration_sec").cast("long"),
    col("date_debut_clean").alias("date_debut"),
    col("date_fin_clean").alias("date_fin"),
    col("mode_transport_declare"),
    col("distance_domicile_travail_km"),
    col("alerte_transport"),
    col("commentaire")
)


# ==============================================================================
# 5. GESTION DES DONNÉES MANQUANTES
# ==============================================================================

# OBJECTIF :
# Sécuriser les données pour éviter les erreurs dans les KPI futurs

df_silver_final = df_silver_final.fillna({
    "distance_metres": 0.0,
    "duration_sec": 0,
    "alerte_transport": "INCONNU"
})


# ==============================================================================
# 6. CONTRÔLE QUALITÉ (DATA QUALITY CHECK)
# ==============================================================================

# OBJECTIF :
# Détecter des anomalies évidentes avant exploitation BI

print("Vérification qualité des données...")

nb_negatif = df_silver_final.filter(col("distance_metres") < 0).count()

if nb_negatif > 0:
    print(f"ALERTE : {nb_negatif} lignes avec distances négatives")


# ==============================================================================
# 7. SAUVEGARDE SILVER (DATA READY FOR ANALYTICS)
# ==============================================================================

# OBJECTIF :
# Sauvegarder la version propre et enrichie des données

print(f"Écriture vers Silver : {chemin_silver}")

df_silver_final.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.save(chemin_silver)

print("Étape Silver terminée : données prêtes pour analyse KPI / Gold")