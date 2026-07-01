# ==============================================================================
# SCRIPT 09 : CALCUL DES INDICATEURS DÉCISIONNELS RH (SILVER ➜ GOLD)
# ==============================================================================

# OBJECTIF GLOBAL :
# Transformer des données SILVER (propres + enrichies)
# en INDICATEURS MÉTIER (KPI RH) exploitables pour la décision.

# Ici on passe du niveau :
# -> “données techniques” (activités sportives)
# -> vers “données business” (prime, éligibilité, performance RH)


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum, current_date, add_months


# ==============================================================================
# 1. INITIALISATION SPARK (MOTEUR DE CALCUL DISTRIBUÉ)
# ==============================================================================

# SparkSession = point d'entrée pour exécuter des requêtes distribuées
# Delta Lake = stockage fiable type Data Lake moderne

spark = SparkSession.builder \
.appName("SportRH_Gold_ToutEnUn") \
.config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
.getOrCreate()

# Réduction des logs pour ne garder que les alertes importantes
spark.sparkContext.setLogLevel("WARN")


# ==============================================================================
# 2. CHARGEMENT DES DONNÉES (SILVER + RH)
# ==============================================================================

# OBJECTIF :
# Combiner 2 univers :
# - RH (salaires, contrat)
# - Activités sportives (historique + KPI)

df_rh = spark.read.format("csv") \
.option("header", "true") \
.option("inferSchema", "true") \
.load("data/donnees_fusionnees.csv")

# Debug : vérifier les colonnes disponibles après lecture
print("Colonnes disponibles après nettoyage :", df_rh.columns)


# Chargement des activités sportives nettoyées (Silver)
df_silver = spark.read.format("delta").load("data/lake/silver/activites_sportives")


# ==============================================================================
# NORMALISATION DES COLONNES RH
# ==============================================================================

# OBJECTIF :
# Standardiser les noms de colonnes pour éviter erreurs de jointure

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
# 3. FILTRAGE TEMPOREL (RÈGLE MÉTIER)
# ==============================================================================

# OBJECTIF :
# Ne garder que les activités des 12 derniers mois (vision RH annuelle)

date_limite = add_months(current_date(), -12)

df_silver_12m = df_silver.filter(col("date_debut") >= date_limite)


# ==============================================================================
# 4. AGRÉGATION MÉTIER (TRANSFORMATION EN KPI RH)
# ==============================================================================

# OBJECTIF :
# Transformer une liste d'activités en indicateurs par salarié

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

# OBJECTIF :
# Reconstituer une vision complète salarié :
# RH (salaire) + Sport (activité)

df_final = df_gold.join(
    df_rh,
    df_gold.id_salarie == df_rh.employee_id,
    "inner"
)


# ==============================================================================
# 6. CALCUL DES INDICATEURS DÉCISIONNELS 
# ==============================================================================

# OBJECTIF :
# Transformer les KPI en décisions RH concrètes

df_final_gold = df_final.withColumn(
    "montant_prime_sportive",
    when(
        # On utilise le nom complet exact pour chaque catégorie
        (col("moyen_transport") == "VÉLO/TROTTINETTE/AUTRES") | (col("moyen_transport") == "MARCHE/RUNNING"), 
        col("salaire_brut") * 0.05
    ).otherwise(0.0)
).withColumn(

    # ÉLIGIBILITÉ BIEN-ÊTRE :
    # Si salarié ≥ 15 activités total → obtient ses jours bien-être
    "eligible_jours_bien_etre",
    when(
        col("nb_total_activites") >= 15,
        True
    ).otherwise(False)
)


# ==============================================================================
# 7. EXPORT GOLD (COUCHE DÉCISIONNELLE)
# ==============================================================================

# OBJECTIF :
# Sauvegarder les KPI finaux pour Power BI / dashboards RH

df_final_gold.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.save("data/lake/gold/indicateurs_rh")


print("Traitement Gold terminé : KPI RH prêts pour décision")


# ==============================================================================
# 7B. EXPORT DÉFINITIF POUR TABLEAU PUBLIC (CORRECTION ET SÉCURISATION)
# ==============================================================================

# OBJECTIF :
# Forcer Spark à rassembler les calculs distribués (.coalesce(1)) 
# pour générer un fichier CSV unique et propre directement exploitable par Tableau.

df_final_gold.coalesce(1).write \
.format("csv") \
.mode("overwrite") \
.option("header", "true") \
.save("data/lake/gold/export_tableau_csv")

print("Export CSV unique pour Tableau généré dans : data/lake/gold/export_tableau_csv/")


# Affichage rapide pour contrôle qualité
df_final_gold.show()

# ==============================================================================
# 8. AFFICHAGE DE L'IMPACT FINANCIER GLOBAL DANS LE TERMINAL
# ==============================================================================
print("\n" + "="*50)
print(" BILAN FINANCIER DE L'OPÉRATION POUR JULIETTE")
print("="*50)

# 1. On calcule la somme totale de la colonne 'montant_prime_sportive'
# .collect() permet d'extraire la valeur hors de Spark pour l'isoler en Python
total_primes = df_final_gold.agg(sum("montant_prime_sportive")).collect()[0][0]

# 2. On compte le nombre de salariés qui vont toucher cette prime
nb_salaries_primes = df_final_gold.filter(col("montant_prime_sportive") > 0).count()

# 3. On affiche proprement les résultats dans le terminal
print(f" Coût total des primes pour l'entreprise : {total_primes:,.2f} €")
print(f" Nombre de salariés bénéficiaires : {nb_salaries_primes}")
print("="*50 + "\n")