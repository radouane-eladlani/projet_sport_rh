from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialisation
spark = SparkSession.builder \
    .appName("Check_Gold") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Lecture de la couche Gold 
chemin_gold = "data/lake/gold/indicateurs_rh"
print(f"🔍 Lecture des indicateurs RH depuis : {chemin_gold} ...")
df_gold = spark.read.format("delta").load(chemin_gold)

# 2. Vérification des KPI de la note de cadrage
print("\n---  RAPPORT D'ÉLIGIBILITÉ (KPI MÉTIERS) ---")

# Calculs globaux
nb_total = df_gold.count()

nb_prime = df_gold.filter(col("montant_prime_sportive") > 0).count()
nb_bien_etre = df_gold.filter(col("eligible_jours_bien_etre") == True).count()

print(f"Total salariés analysés : {nb_total}")
print(f"Salariés éligibles à la PRIME SPORTIVE (5%) : {nb_prime}")
print(f"Salariés éligibles aux JOURS BIEN-ÊTRE : {nb_bien_etre}")

# 3. Aperçu des données pour valider les calculs
print("\n--- APERÇU DES RÉSULTATS (Top 10 éligibles prime) ---")
df_gold.select("id_salarie", "nb_total_activites", "montant_prime_sportive", "eligible_jours_bien_etre") \
       .orderBy(col("nb_total_activites").desc()) \
       .show(10, truncate=False)

# 4. Test de cohérence 
if df_gold.filter(col("id_salarie").isNull()).count() > 0:
    print(" ALERTE : Il y a des ID salariés manquants dans la couche Gold !")
else:
    print(" OK : Intégrité des données Gold vérifiée.")