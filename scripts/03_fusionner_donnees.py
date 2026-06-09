# ==============================================================================
# SCRIPT 03 : FUSION ET VALIDATION DES DISTANCES (MIS À JOUR DATETIME)
# ==============================================================================

import pandas as pd
import requests
import os
from dotenv import load_dotenv

# Chargement des variables d'environnement (pour la clé API Google)
load_dotenv("../.env")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# 1. CHARGEMENT DES DONNÉES
df_rh = pd.read_excel("../data/Données+RH.xlsx")
df_sport = pd.read_excel("../data/Données+Sportive.xlsx")

# 2. RENOMMAGE, CLEANING STANDARD & CONVERSION DATETIME
df_rh = df_rh.rename(columns={
    "ID salarié": "employee_id",
    "Salaire brut": "salaire_brut",
    "Adresse du domicile": "domicile_address",
    "Moyen de déplacement": "moyen_transport"
})

df_sport = df_sport.rename(columns={
    "ID salarié": "employee_id",
    "Pratique d'un sport": "pratique_sport"
})

# --- AJOUT & STANDARDISATION DES FORMATS TEMPORELS ---

df_sport["pratique_sport"] = df_sport["pratique_sport"].fillna("AUCUN").str.upper().str.strip()
df_rh["moyen_transport"] = df_rh["moyen_transport"].astype(str).str.upper().str.strip()

# 3. FUSION DES REFERENTIELS
df = df_rh.merge(df_sport, on="employee_id", how="left")

# 4. FONCTION DE CALCUL VIA GOOGLE MAPS DISTANCE MATRIX
ADRESSE_ENTREPRISE = "1362 Av. des Platanes, 34970 Lattes"

def obtenir_distance_google_maps(origin, destination, mode_transport):
    """
    Appelle l'API Google Maps pour obtenir la distance en kilomètres.
    Si la clé API n'est pas configurée, renvoie une simulation par défaut.
    """
    if not GOOGLE_MAPS_API_KEY or GOOGLE_MAPS_API_KEY == "TA_CLE_API":
        # Mode Simulation (au cas où la clé n'est pas encore activée dans le .env)
        return 12.5 # Valeur fictive par défaut pour le test
        
    # Mapping des modes de transport pour l'API Google Maps
    mode_mapping = {
        "VÉLO": "bicycling",
        "TROTTINETTE": "bicycling",
        "MARCHE": "walking",
        "COURSE À PIED": "walking",
        "RUNNING": "walking"
    }
    google_mode = mode_mapping.get(mode_transport, "driving")

    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": origin,
        "destinations": destination,
        "mode": google_mode,
        "key": GOOGLE_MAPS_API_KEY
    }
    
    try:
        response = requests.get(url, params=params).json()
        if response["status"] == "OK":
            element = response["rows"][0]["elements"][0]
            if element["status"] == "OK":
                distance_m = element["distance"]["value"]
                return distance_m / 1000
    except Exception as e:
        print(f"Erreur API Google Maps : {e}")
    
    return None

# 5. APPLICATION DU CALCUL ET DES RÈGLES DE CADRAGE
print("Calcul des distances domicile-entreprise en cours...")

distances_calculées = []
flags_erreur = []

for idx, row in df.iterrows():
    adresse_salarie = row["domicile_address"]
    transport = row["moyen_transport"]
    
    # Appel API
    distance_km = obtenir_distance_google_maps(adresse_salarie, ADRESSE_ENTREPRISE, transport)
    distances_calculées.append(distance_km)
    
    # Validation des règles de la note de cadrage
    erreur_declaration = False
    if distance_km is not None:
        if transport in ["MARCHE", "COURSE À PIED", "RUNNING"] and distance_km > 15:
            erreur_declaration = True
        elif transport in ["VÉLO", "TROTTINETTE"] and distance_km > 25:
            erreur_declaration = True
            
    flags_erreur.append(erreur_declaration)

# Ajout des nouvelles colonnes de validation
df["distance_domicile_entreprise_km"] = distances_calculées
df["erreur_declaration_transport"] = flags_erreur

# 6. SAUVEGARDE DU FICHIER ENRICHI
df.to_excel("../data/donnees_fusionnees.xlsx", index=False)
print("=== ÉTAPE A TERMINÉE AVEC SUCCÈS ===")
print(f"Nombre d'anomalies de déclaration détectées : {df['erreur_declaration_transport'].sum()}")