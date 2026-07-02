# ==============================================================================
# 05b - API INGESTION FASTAPI (ENTRY POINT PIPELINE)
# ==============================================================================

# la librairie FastAPI permet de créer une API web.
# Ici, elle va recevoir les activités sportives générées par le script# 04_generer_activites.py.


from fastapi import FastAPI

# Permet de lancer le serveur FastAPI en local
import uvicorn






# Import de SQLAlchemy
# create_engine : crée une connexion vers PostgreSQL.
# text : permet d'exécuter des requêtes SQL écrites manuellement.
from sqlalchemy import create_engine, text

from dotenv import load_dotenv

# Import du module os
# Permet de lire les variables d'environnement.
import os

# ==============================================================================
# 1. CHARGEMENT DES VARIABLES D'ENVIRONNEMENT
# ==============================================================================


load_dotenv()





# Lecture des paramètres de connexion PostgreSQL.
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


# ==============================================================================
# 2. CRÉATION DE L'APPLICATION FASTAPI
# ==============================================================================


# Création de l'API.
# Cette variable "app" représente le serveur web.
app = FastAPI(title="Sport Data Ingestion API")

# ==============================================================================
# 3. CONNEXION À POSTGRESQL
# ==============================================================================

# Création de la connexion à PostgreSQL.
# La chaîne de connexion est construite à partir des variables du .env.
engine = create_engine(

    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

)

print("API INGESTION ACTIVE")

# ==============================================================================
# 4. ENDPOINT D'INGESTION
# ==============================================================================

# Création d'une route HTTP POST.
# Lorsque quelqu'un envoie une requête POST vers : http://localhost:8000/ingest

# FastAPI exécute automatiquement la fonction ingest().
@app.post("/ingest")

def ingest(activity: dict):

# ==========================================================================
# VALIDATION DES DONNÉES
# ==========================================================================

    # Liste des champs obligatoires pour enregistrer une activité.
    required_fields = [

        "id_salarie",
        "type_sport",
        "distance",
        "date_debut",
        "date_fin"

    ]


    # Vérifie que tous les champs obligatoires sont présents.
    for field in required_fields:

        # Si un champ est absent :
        if field not in activity:

            # Retourne immédiatement une erreur.
            return {

                "status": "error",
                "message": f"Missing field: {field}"

            }


# ==========================================================================
# REQUÊTE SQL D'INSERTION
# ==========================================================================

    # Préparation de la requête SQL.
    # Les ":" représentent des paramètres qui seront remplacés
    # par les valeurs du dictionnaire activity.

    query = text("""

        INSERT INTO activites_sportives (
                 
            id_salarie,
            type_sport,
            distance,
            date_debut,
            date_fin,
            commentaire

        )

        VALUES (
            :id_salarie,
            :type_sport,
            :distance,
            :date_debut,
            :date_fin,
            :commentaire

        )
    """)


# ==========================================================================
# INSERTION DANS LA BASE DE DONNÉES
# ==========================================================================

    try:

        # Ouvre une transaction PostgreSQL.
        # Si tout se passe bien COMMIT automatique
        # Si une erreur survient : ROLLBACK automatique
        with engine.begin() as conn:

            # Exécution de la requête SQL.
            conn.execute(query, {

                # ID du salarié concerné.
                "id_salarie": activity["id_salarie"],

                # Sport pratiqué.
                "type_sport": activity["type_sport"],

                # Distance parcourue.
                # Si le champ n'existe pas valeur par défaut = 0
                "distance": activity.get("distance", 0),

                # Date de début de l'activité.
                "date_debut": activity["date_debut"],


                # Date de fin de l'activité.
                "date_fin": activity["date_fin"],

                # Commentaire si absent : chaîne vide.
                "commentaire": activity.get("commentaire", "")

            })


# ==========================================================================
# RÉPONSE EN CAS DE SUCCÈS
# ==========================================================================

        return {
            "status": "ok",
            "message": "activity stored"
        }

# ==========================================================================
# GESTION DES ERREURS
# ==========================================================================

    except Exception as e:
        # Si une erreur SQL ou Python se produit on retourne le détail de l'erreur.

        return {

            "status": "error",
            "message": str(e)

        }


# ==========================================================================
# LANCEMENT API 
# ==========================================================================


if __name__ == "__main__":

    uvicorn.run(

        "05b_API_ingestion:app",
        host="0.0.0.0",
        port=8000,
        reload=True

    )
