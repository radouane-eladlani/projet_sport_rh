# ==============================================================================
# 07a - INGESTION STREAMING : REDPANDA → SPARK → DELTA LAKE (BRONZE LAYER)
# ==============================================================================
#
# OBJECTIF DU SCRIPT
#
# Ce script représente la couche BRONZE du Data Lake.
#
# Son rôle est de :
#
# 1. Lire en temps réel les événements envoyés dans Redpanda/Kafka.
# 2. Récupérer les activités sportives produites par Debezium.
# 3. Transformer le JSON complexe Debezium en colonnes exploitables.
# 4. Stocker les données brutes dans Delta Lake.

# Chargement des fonctions système Python
import os

# Création et pilotage du moteur Spark
from pyspark.sql import SparkSession

# Fonctions Spark utilisées pour manipuler les colonnes
from pyspark.sql.functions import col, from_json

# Définition des schémas de données
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

# Lecture des variables d'environnement
from dotenv import load_dotenv

# ==============================================================================
# 1. INITIALISATION DE SPARK
# ==============================================================================

# Chargement du fichier .env
load_dotenv()

# Création de la session Spark
#
# Cette session est le point d'entrée principal de Spark.
# Toutes les opérations de traitement passent par elle.
spark = (
    SparkSession.builder

    # Nom visible dans l'interface Spark
    .appName("SPORT_RH_STREAMING_INGESTION")

    # Exécution locale sur tous les cœurs CPU disponibles
    .master("local[*]")

    # Bibliothèques nécessaires :
    #
    # - kafka : lecture du topic Redpanda
    # - delta : écriture Delta Lake
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
        "io.delta:delta-core_2.12:2.4.0"
    )

    # Activation des fonctionnalités Delta Lake
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )

    # Configuration du catalogue Delta
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

    .getOrCreate()
)

# Réduit les messages techniques affichés par Spark
# pour rendre les logs plus lisibles.
spark.sparkContext.setLogLevel("WARN")

print("🚀 Démarrage du flux Spark Streaming...")

# ==============================================================================
# 2. LECTURE DU TOPIC REDPANDA
# ==============================================================================

# Connexion au broker Redpanda
#
# Spark va écouter le topic Kafka en continu.
#
# Chaque nouvel événement envoyé par Debezium
# sera automatiquement récupéré.
df_kafka = (
    spark.readStream

    # Source = Kafka
    .format("kafka")

    # Adresse du broker Redpanda
    .option(
        "kafka.bootstrap.servers",
        "localhost:19092"
    )

    # Topic surveillé
    .option(
        "subscribe",
        "dbserver1.public.activites_sportives"
    )

    # Lecture de tous les événements existants
    .option(
        "startingOffsets",
        "earliest"
    )

    .load()
)

# Fonction permettant de convertir une date en timestamp
from pyspark.sql.functions import unix_timestamp

# ==============================================================================
# 3. CONVERSION DU MESSAGE KAFKA EN TEXTE
# ==============================================================================

# Kafka stocke les messages au format binaire.
#
# Cette étape convertit le contenu en chaîne de caractères.
df_string = df_kafka.selectExpr(
    "CAST(value AS STRING) as json_payload"
)

# ==============================================================================
# 4. DESCRIPTION DU FORMAT DEBEZIUM
# ==============================================================================

# Debezium envoie un JSON complexe.
#
# Exemple simplifié :
#
# {
#   "payload": {
#      "after": {
#          ...
#      }
#   }
# }
#
# Spark doit connaître à l'avance la structure
# du JSON afin de pouvoir le lire correctement.

schema_after = StructType([

    # Identifiant technique de l'activité
    StructField("ID", IntegerType(), True),

    # Identifiant salarié
    StructField("ID salarié", IntegerType(), True),

    # Sport pratiqué
    StructField("Type", StringType(), True),

    # Distance parcourue
    StructField("Distance", DoubleType(), True),

    # Date de début
    StructField(
        "Date de début de l'activité",
        StringType(),
        True
    ),

    # Date de fin
    StructField(
        "Date de fin de l'activité",
        StringType(),
        True
    ),

    StructField(
        "Commentaire",
        StringType(),
        True
    )
])

# Structure complète Debezium
schema_debezium = StructType([
    StructField(
        "payload",
        StructType([
            StructField(
                "after",
                schema_after,
                True
            )
        ]),
        True
    )
])

# ==============================================================================
# 5. EXTRACTION DU CONTENU UTILE
# ==============================================================================

# Transformation du JSON en colonnes Spark
#
# Objectif :
# récupérer directement les données métier.
df_parsed = (
    df_string

    # Conversion JSON → colonnes
    .withColumn(
        "data",
        from_json(
            col("json_payload"),
            schema_debezium
        )
    )

    # Extraction du bloc payload.after
    .select("data.payload.after.*")

    # Ignore les suppressions Debezium
    .filter(col("ID").isNotNull())

    # Renommage des colonnes
    # pour faciliter les traitements futurs
    .withColumnRenamed("ID", "id")
    .withColumnRenamed("ID salarié", "id_salarie")
    .withColumnRenamed("Type", "type_sport")
    .withColumnRenamed("Distance", "distance")
    .withColumnRenamed(
        "Date de début de l'activité",
        "date_debut"
    )
    .withColumnRenamed(
        "Date de fin de l'activité",
        "date_fin"
    )
    .withColumnRenamed(
        "Commentaire",
        "commentaire"
    )
)

# ==============================================================================
# 6. CALCUL DE LA DUREE DE L'ACTIVITE
# ==============================================================================

# Création d'un KPI simple :
# - durée = date_fin - date_debut
# - Le résultat est exprimé en secondes.
df_final = df_parsed.withColumn(

    "duration_sec",

    unix_timestamp(
        col("date_fin"),
        "dd/MM/yyyy HH:mm"
    )

    -

    unix_timestamp(
        col("date_debut"),
        "dd/MM/yyyy HH:mm"
    )
)

# ==============================================================================
# 7. CHEMINS DU DATA LAKE
# ==============================================================================

# Emplacement de stockage des données Bronze
chemin_delta_bronze = (
    "data/lake/bronze/activites_sportives"
)

# Les checkpoints permettent à Spark
# de reprendre automatiquement après un arrêt.
chemin_checkpoint = (
    "data/lake/checkpoints/activites_sportives"
)

# ==============================================================================
# 8. ECRITURE DANS DELTA LAKE
# ==============================================================================

print(
    "🚀 Démarrage de l'écriture dans Delta Lake..."
)

# Création du job streaming
query = (

    df_final.writeStream

    # Format Delta Lake
    .format("delta")

    # Ajout des nouvelles lignes uniquement
    .outputMode("append")

    # Sauvegarde de l'état du streaming
    .option(
        "checkpointLocation",
        chemin_checkpoint
    )

    # Démarrage de l'écriture continue
    .start(chemin_delta_bronze)
)

# ==============================================================================
# 9. MAINTIEN DU STREAMING ACTIF
# ==============================================================================

# Le script reste actif en permanence.
#
# Il continuera à écouter Redpanda
# et à écrire dans Delta Lake
# jusqu'à son arrêt manuel.
query.awaitTermination()