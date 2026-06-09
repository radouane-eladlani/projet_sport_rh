# ==============================================================================
# SCRIPT 05 : CHARGEMENT DES DONNÉES DANS LE DATA SYSTEM (POSTGRESQL)
# ==============================================================================

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Integer, String, Float, Boolean, Text

# 1. CHARGEMENT DES CONFIGURATIONS (.env)
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "sport_rh_db")

# Création de la connexion vers PostgreSQL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

print("=" * 60)
print("        IMPORTATION DES DONNÉES VERS LA BASE DE DONNÉES        ")
print("=" * 60)

# 2. LECTURE DES FICHIERS EXCEL
print("Lecture des fichiers Excel locaux...")
try:
    df_employes = pd.read_excel(os.path.join(os.path.dirname(__file__), "../data/donnees_fusionnees.xlsx"))
    df_activites = pd.read_excel(os.path.join(os.path.dirname(__file__), "../data/activites_sportives.xlsx"))
except Exception as e:
    print(f"Erreur de lecture des fichiers Excel : {e}")
    print("Vérifie que tes fichiers sont bien générés dans le dossier 'data'.")
    exit(1)

# 3. ENVOI DES SALARIÉS DANS LA TABLE 'employes'
print("Insertion des salariés dans la table 'employes'...")
dtype_employes = {
    "employee_id": Integer(),
    "Nom": String(100),
    "Prénom": String(100),
    "salaire_brut": Float(),
    "domicile_address": String(255),
    "moyen_transport": String(50),
    "pratique_sport": String(50),
    "distance_domicile_entreprise_km": Float(),
    "erreur_declaration_transport": Boolean()
}

df_employes.to_sql(
    name="employes",
    con=engine,
    if_exists="replace", 
    index=False,
    dtype=dtype_employes
)

# 4. ENVOI DES ACTIVITÉS DANS LA TABLE 'activites_sportives'
print("Insertion des activités dans la table 'activites_sportives'...")
dtype_activites = {
    "ID": Integer(),
    "ID salarié": Integer(),
    "Date de début de l'activité": String(50),
    "Type": String(50),
    "Distance": Float(),
    "Date de fin de l'activité": String(50),
    "Commentaire": Text()
}

df_activites.to_sql(
    name="activites_sportives",
    con=engine,
    if_exists="replace",
    index=False,
    dtype=dtype_activites
)

# 5. AJOUT DES CLÉS PRIMAIRES (Indispensable pour le CDC Debezium)
print("Configuration des clés primaires pour Debezium...")
try:
    with engine.connect() as conn:
        conn.execute("ALTER TABLE employes ADD PRIMARY KEY (employee_id);")
        conn.execute("ALTER TABLE activites_sportives ADD PRIMARY KEY (\"ID\");")
    print("Clés primaires configurées avec succès.")
except Exception as e:
    print(f"⚠️ Note sur les clés primaires : {e}")

print("\nTOUTES LES DONNÉES ONT ÉTÉ ENVOYÉES EN BASE AVEC SUCCÈS !")
print("=" * 60)