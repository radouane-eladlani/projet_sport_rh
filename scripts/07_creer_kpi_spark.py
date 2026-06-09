import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, coalesce, lit, round, trim
from dotenv import load_dotenv

# =========================
# 1. ENV
# =========================
load_dotenv()

# =========================
# 2. SPARK INIT
# =========================
spark = (
    SparkSession.builder
    .appName("KPI_SPORT_RH")
    .master("local[*]")

    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3,io.delta:delta-core_2.12:2.4.0"
    )

    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

    .getOrCreate()
)


# =========================
# 3. CONFIG JDBC
# =========================
url = f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

props = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

# =========================
# 4. READ DATA
# =========================
df_employes = spark.read.jdbc(url, "employes", properties=props)
df_sessions = spark.read.jdbc(url, "activites_sportives", properties=props)

# =========================
# 5. NORMALISATION
# =========================

df_sessions = (
    df_sessions
    .withColumnRenamed("ID salarié", "id_salarie")
    .withColumnRenamed("Type", "type_sport")
    .withColumn("type_sport", trim(col("type_sport")))   # 🔥 FIX espaces invisibles
    .withColumn("Distance", coalesce(col("Distance"), lit(0)))
)

# =========================
# 6. KPI
# =========================

df_kpi = (
    df_sessions
    .groupBy("type_sport")
    .agg(
        round(_sum("Distance"), 2).alias("distance_totale_km"),
        round(avg("Distance"), 2).alias("distance_moyenne_km"),
        count("*").alias("total_sessions")
    )
)

print("--- RÉSULTAT KPI ---")
df_kpi.orderBy("type_sport").show(truncate=False)

# =========================
# 7. SAVE DELTA
# =========================

output_path = "data/lakehouse/kpi_bu"

(
    df_kpi.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(output_path)
)

print(f"✔ KPI sauvegardé en Delta : {output_path}")

spark.stop()