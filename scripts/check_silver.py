from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# ==============================================================================
# 1. INITIALISATION
# ==============================================================================
spark = SparkSession.builder \
    .appName("Check_Silver_Compliance") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==============================================================================
# 2. LECTURE & ANALYSE
# ==============================================================================
df_silver = spark.read.format("delta").load("data/lake/silver/activites_sportives")
print("✅ Chargement de la table Silver effectué.")

# ==============================================================================
# APERÇU DES DONNÉES
# ==============================================================================
print("\n--- 🏁 APERÇU DES DONNÉES (5 LIGNES) ---")
# n=5 pour 5 lignes, truncate=False pour afficher le texte complet
df_silver.show(5, truncate=False)

print("\n--- 📋 SCHEMA ET VOLUMÉTRIE ---")
df_silver.printSchema()
total_rows = df_silver.count()
print(f"📊 Nombre total d'activités enregistrées : {total_rows}")

# ==============================================================================
# 3. CONTRÔLE QUALITÉ (Conformité "Note de Cadrage")
# ==============================================================================
print("\n--- 🔍 CONTRÔLE DE CONFORMITÉ (Sanity Checks) ---")
issues_found = 0

# Test A : Distances négatives ou aberrantes
neg_dist = df_silver.filter(col("distance_metres") < 0).count()
if neg_dist > 0:
    print(f"❌ ALERTE : {neg_dist} activités ont une distance négative !")
    issues_found += 1

# Test B : Dates incohérentes (Fin avant début)
bad_dates = df_silver.filter(col("date_fin") < col("date_debut")).count()
if bad_dates > 0:
    print(f"❌ ALERTE : {bad_dates} activités ont une date de fin antérieure à la date de début.")
    issues_found += 1

# Test C : Salariés non trouvés dans le référentiel RH
missing_rh = df_silver.filter(col("mode_transport_declare").isNull()).count()
if missing_rh > 0:
    print(f"⚠️ ATTENTION : {missing_rh} activités sont liées à des salariés absents du référentiel RH (Jointure impossible).")
    issues_found += 1

if issues_found == 0:
    print("✅ OK : Aucune anomalie de structure détectée.")

# ==============================================================================
# 4. KPI MÉTIER (Préparation pour la Prime Gold)
# ==============================================================================
print("\n--- 📊 KPI POUR LE POC (Règles de Cadrage) ---")

# Répartition des fraudes
df_silver.groupBy("alerte_transport").agg(count("*").alias("nb_activites")).show()

# Focus sur les fraudes (Détail)
fraudes = df_silver.filter(col("alerte_transport") == "FRAUDE")
if fraudes.count() > 0:
    print("🕵️ Détail des fraudes détectées (exemple) :")
    fraudes.select("id_salarie", "type_sport", "distance_domicile_travail_km", "alerte_transport").show(5)
else:
    print("🎉 Aucune fraude détectée sur les distances déclarées.")

print("\n--- 🏁 TEST TERMINÉ ---")