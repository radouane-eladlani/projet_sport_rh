import pandas as pd
import random
import time
import requests
from datetime import datetime, timedelta

# Chargement du fichier Excel RH
# contient les employés + leur sport déclaré
df = pd.read_csv("./data/donnees_fusionnees.csv")
# Nettoyage des noms de colonnes (supprime espaces invisibles)
df.columns = df.columns.str.strip()

# Définition de la période de simulation (1 an complet)
START = datetime(2025, 1, 1)
END = datetime(2025, 12, 31)

# Fonction qui génère une date aléatoire dans l'année
def random_date():
    return START + timedelta(
        seconds=random.randint(
            0,
            int((END - START).total_seconds())
        )
    )

# Message de démarrage du producer
print("STREAM PRODUCER ACTIVÉ")

# Boucle infinie = simulation temps réel (streaming type Strava)
while True:

    # Sélection d'un salarié aléatoire dans le fichier RH
    row = df.sample(1).iloc[0]

    # Récupération du sport pratiqué (normalisation)
    sport = str(row["pratique_sport"]).upper().strip()

    # Si aucun sport déclaré, on ignore cette ligne
    if sport == "AUCUN":
        continue

    # Génération de la date de début de l'activité
    start_dt = random_date()

    # Durée aléatoire entre 20 min et 2h (en secondes)
    duration_sec = random.randint(20 * 60, 120 * 60)

    # Calcul automatique de la date de fin
    end_dt = start_dt + timedelta(seconds=duration_sec)

    # Gestion de la distance selon le sport
    # certains sports n'ont pas de distance (yoga, muscu)
    if sport in ["YOGA", "MUSCULATION"]:
        distance_m = 0
    else:
        distance_m = random.randint(2000, 15000)

    # Construction de l'événement (format API / Strava simulé)
    activity = {
        # ID salarié
        "id_salarie": int(row["employee_id"]),

        # type de sport
        "type_sport": sport,

        # distance en mètres
        "distance": distance_m,

        # durée en secondes
        "duration_sec": duration_sec,

        # date de début format SQL
        "date_debut": start_dt.strftime("%Y-%m-%d %H:%M:%S"),

        # date de fin format SQL
        "date_fin": end_dt.strftime("%Y-%m-%d %H:%M:%S"),

        # commentaire humain pour debug / Slack
        "commentaire": f"{row['Prénom']} {row['Nom']} - {sport}"
    }

    # Envoi de l'événement vers ton API FastAPI
    requests.post("http://localhost:8000/ingest", json=activity)

    # ✔ log de confirmation dans le terminal
    print(" Event envoyé")

    # pause aléatoire pour simuler un flux réel
    time.sleep(random.randint(2, 5))