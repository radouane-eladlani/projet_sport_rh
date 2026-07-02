import os

# create_engine : connexion à PostgreSQL
# text : permet d’exécuter du SQL brut proprement
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


# Construction de la connexion PostgreSQL
# on récupère les informations de connexion depuis le fichier .env
engine = create_engine(

    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

)

print("Création des tables CDC en cours")

# Ouverture d’une connexion sécurisée à la base de données
# engine.begin() garantit que les requêtes sont validées
with engine.begin() as conn:

# ==========================================================
# TABLE 1 : employes
# ==========================================================

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS employes (
                      
        employee_id INT PRIMARY KEY,
        nom TEXT,
        prenom TEXT,
        salaire_brut FLOAT,
        domicile_address TEXT,
        moyen_transport TEXT,
        pratique_sport TEXT,
        distance_domicile_entreprise_km FLOAT
                      
    );
    """))

# ==========================================================
# TABLE 2 : activites_sportives
# ==========================================================

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS activites_sportives (
        id SERIAL PRIMARY KEY,
        id_salarie INT NOT NULL,
        type_sport TEXT NOT NULL,
        distance FLOAT,
        duration_sec INT,
        date_debut TIMESTAMP,
        date_fin TIMESTAMP,
        commentaire TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

    );
    """))

print("Tables créées et prêtes pour le CDC")