# =========================
# IMPORT DES LIBRAIRIES
# =========================

import pandas as pd
import random # random → génération de valeurs aléatoires
from datetime import datetime, timedelta


# =========================
# CHARGEMENT DES DONNÉES FUSIONNÉES
# =========================

# On charge le fichier issu de l’étape 3 (RH + sport)
# Ce fichier contient toutes les informations des salariés
df = pd.read_excel("../data/donnees_fusionnees.xlsx")


# =========================
# LISTE POUR STOCKER LES ACTIVITÉS SIMULÉES
# =========================

# Cette liste contiendra toutes les activités sportives générées
# Objectif : créer un historique fictif pour analyse + futur Slack
activities_fictives = []


# =========================
# FONCTION DE GÉNÉRATION DE DATE
# =========================

# Permet de simuler une activité sur l’année 2025
def random_date():
    start = datetime(2025, 1, 1)
    return start + timedelta(days=random.randint(0, 365))


# =========================
# PARCOURS DES SALARIÉS
# =========================

# Chaque ligne = un salarié
for _, row in df.iterrows():

    employee_id = row["employee_id"]
    pratique_sport = row["pratique_sport"]


    # =========================
    # CAS : PAS DE SPORT
    # =========================

    # Si le salarié ne pratique pas de sport → aucune activité simulée
    if pratique_sport == "AUCUN":
        continue


    # =========================
    # SIMULATION D’ACTIVITÉS SPORTIVES
    # =========================

    # On simule un historique d’activités pour les salariés sportifs
    # ⚠️ Ceci est uniquement une simulation (POC), pas une règle métier

    nb_activites = random.randint(5, 15)


    for _ in range(nb_activites):

        activities_fictives.append({
            "employee_id": employee_id,
            "pratique_sport": pratique_sport,
            "date": random_date()
        })


# =========================
# CONVERSION EN DATAFRAME
# =========================

df_activities = pd.DataFrame(activities_fictives)


# =========================
# EXPORT DU FICHIER
# =========================

df_activities.to_excel("../data/activites_sportives.xlsx", index=False)


# =========================
# AFFICHAGE POUR CONTRÔLE
# =========================

print(df_activities.head())