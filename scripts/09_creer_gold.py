# ==============================================================================
# SCRIPT 09 : CALCUL DES INDICATEURS DÉCISIONNELS RH (SILVER ➜ GOLD)
# ==============================================================================
# OBJECTIF GLOBAL :
# Transformer des données SILVER (propres + enrichies)
# en INDICATEURS MÉTIER (KPI RH) exploitables pour la décision.
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum, current_date, add_months

# ==============================================================================
# 1. INITIALISATION SPARK (MOTEUR DE CALCUL DISTRIBUÉ)
# ==============================================================================
spark = SparkSession.builder \
    .appName("SportRH_Gold_ToutEnUn") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==============================================================================
# 2. CHARGEMENT DES DONNÉES (SILVER + RH)
# ==============================================================================
df_rh = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("data/donnees_fusionnees.csv")

print("Colonnes disponibles après nettoyage :", df_rh.columns)

# Chargement des activités sportives nettoyées (Silver)
df_silver = spark.read.format("delta").load("data/lake/silver/activites_sportives")

# ==============================================================================
# NORMALISATION DES COLONNES RH
# ==============================================================================
new_columns = [
    c.replace(" ", "_")
    .replace("(", "")
    .replace(")", "")
    .replace("é", "e")
    .lower()
    for c in df_rh.columns
]
df_rh = df_rh.toDF(*new_columns)

# ==============================================================================
# 3. FILTRAGE TEMPOREL (CORRIGÉ POUR ACCEPTER LES DONNÉES SIMULÉES DE 2025)
# ==============================================================================
# On commente la ligne restrictive des 12 mois glissants qui excluait l'année 2025
# date_limite = add_months(current_date(), -12)
# df_silver_12m = df_silver.filter(col("date_debut") >= date_limite)

# On conserve toutes les données générées
df_silver_12m = df_silver

# ==============================================================================
# 4. AGRÉGATION MÉTIER (TRANSFORMATION EN KPI RH)
# ==============================================================================
df_gold = df_silver_12m.groupBy("id_salarie").agg(
    # KPI 1 : nombre total d'activités sportives
    count("id_evenement").alias("nb_total_activites"),
    # KPI 2 : nombre d'activités valides (sans fraude transport)
    sum(
        when(col("alerte_transport") == "OK", 1).otherwise(0)
    ).alias("nb_activites_valides")
)

# ==============================================================================
# 5. JOINTURE RH + KPI SPORTIFS
# ==============================================================================
df_final = df_gold.join(
    df_rh,
    df_gold.id_salarie == df_rh.employee_id,
    "inner"
)

# ==============================================================================
# 6. CALCUL DES INDICATEURS DÉCISIONNELS
# ==============================================================================
df_final_gold = df_final.withColumn(
    "montant_prime_sportive",
    when(
        (col("moyen_transport") == "VÉLO/TROTTINETTE/AUTRES") | (col("moyen_transport") == "MARCHE/RUNNING"),
        col("salaire_brut") * 0.05
    ).otherwise(0.0)
).withColumn(
    # ÉLIGIBILITÉ BIEN-ÊTRE : Si salarié ≥ 15 activités total → obtient ses jours bien-être
    "eligible_jours_bien_etre",
    when(
        col("nb_total_activites") >= 15,
        True
    ).otherwise(False)
)

df_final_gold.select("montant_prime_sportive").show()
df_final_gold.printSchema()

# ==============================================================================
# 7. EXPORT GOLD (COUCHE DÉCISIONNELLE)
# ==============================================================================
df_final_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("data/lake/gold/indicateurs_rh")

print("Traitement Gold terminé : KPI RH prêts pour décision")

# ==============================================================================
# 7B. EXPORT DÉFINITIF POUR TABLEAU PUBLIC
# ==============================================================================
df_final_gold.coalesce(1).write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save("data/lake/gold/export_tableau_csv")

print("Export CSV unique pour Tableau généré dans : data/lake/gold/export_tableau_csv/")

# Affichage rapide pour contrôle qualité
df_final_gold.show()

# ==============================================================================
# 8. AFFICHAGE DE L'IMPACT FINANCIER GLOBAL 
# ==============================================================================
print("\n" + "="*50)
print(" BILAN FINANCIER DE L'OPÉRATION POUR JULIETTE")
print("="*50)

# Sécurité if/else pour remplacer la valeur par 0.0 si l'agrégation renvoie None
res_primes = df_final_gold.agg(sum("montant_prime_sportive")).collect()[0][0]
total_primes = res_primes if res_primes is not None else 0.0

nb_salaries_primes = df_final_gold.filter(col("montant_prime_sportive") > 0).count()

print(f" Coût total des primes pour l'entreprise : {total_primes:,.2f} €")
print(f" Nombre de salariés bénéficiaires : {nb_salaries_primes}")
print("="*50 + "\n")