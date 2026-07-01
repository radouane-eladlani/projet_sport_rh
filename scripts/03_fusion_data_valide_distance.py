# ==============================================================================
# SCRIPT 03 : FUSION ET VALIDATION DES DISTANCES DOMICILE-ENTREPRISE
# ==============================================================================

# OBJECTIF DU SCRIPT :
#
# Ce script sert à enrichir les données RH en combinant plusieurs sources :
#
# 1. Données RH (salariés : adresse, salaire, transport…)
# 2. Données sportives (activité physique)
# 3. Fusion des deux datasets sur l’identifiant salarié
# 4. Calcul de la distance domicile ↔ entreprise via Google Maps API
# 5. Détection d’anomalies (règles métier sur mobilité douce)
#
# Résultat final : un dataset enrichi avec distance + contrôle de cohérence

import pandas as pd
import requests
import os
from dotenv import load_dotenv

# ==============================================================================
# 1. LOCALISATION DU PROJET ET CHARGEMENT DU .ENV
# ==============================================================================

# Récupère le dossier du script actuel (scripts/)
base_dir = os.path.dirname(os.path.abspath(__file__))

# Remonte à la racine du projet
project_root = os.path.dirname(base_dir)

# Construit le chemin vers le fichier .env
env_path = os.path.join(project_root, '.env')

# Charge les variables d’environnement (clé API Google Maps, etc.)
load_dotenv(dotenv_path=env_path)

# Récupération de la clé API Google Maps depuis le .env
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Debug pour vérifier que la config est bien chargée
print(f"DEBUG: Chargement du .env depuis {env_path}")
print(f"DEBUG: Clé API trouvée : {'Oui' if GOOGLE_MAPS_API_KEY else 'Non'}")

# ==============================================================================
# 2. CHARGEMENT DES DONNÉES SOURCES
# ==============================================================================

# Données RH (salariés)
df_rh = pd.read_excel("./data/Données+RH.xlsx")

# Données sportives
df_sport = pd.read_excel("./data/Données+Sportive.xlsx")

# ==============================================================================
# 3. NORMALISATION DES DONNÉES
# ==============================================================================

# Harmonisation des noms de colonnes RH pour faciliter la fusion
df_rh = df_rh.rename(columns={
    "ID salarié": "employee_id",
    "Salaire brut": "salaire_brut",
    "Adresse du domicile": "domicile_address",
    "Moyen de déplacement": "moyen_transport"
})

# Harmonisation des colonnes sport
df_sport = df_sport.rename(columns={
    "ID salarié": "employee_id",
    "Pratique d'un sport": "pratique_sport"
})

# Nettoyage des valeurs sport (évite null, casse et espaces)
df_sport["pratique_sport"] = df_sport["pratique_sport"].fillna("AUCUN").str.upper().str.strip()

# Normalisation du mode de transport
df_rh["moyen_transport"] = df_rh["moyen_transport"].astype(str).str.upper().str.strip()

# ==============================================================================
# 4. FUSION DES DEUX DATASETS
# ==============================================================================

# Jointure RH + sport sur l'identifiant salarié
df = df_rh.merge(df_sport, on="employee_id", how="left")

# ==============================================================================
# 5. CONFIGURATION GOOGLE MAPS
# ==============================================================================

# Adresse fixe de l'entreprise (point d'arrivée)
ADRESSE_ENTREPRISE = "1362 Av. des Platanes, 34970 Lattes"

# Fonction de calcul de distance via Google Maps API
def obtenir_distance_google_maps(origin, destination, mode_transport):

    # Mode simulation si clé API absente
    if not GOOGLE_MAPS_API_KEY or GOOGLE_MAPS_API_KEY == "CLE_API":
        print("MODE SIMULATION ACTIF : clé API manquante")
        return 12.5

    # Correspondance entre transport RH et mode Google Maps
    mode_mapping = {
        "VÉLO": "bicycling",
        "TROTTINETTE": "bicycling",
        "MARCHE": "walking",
        "COURSE À PIED": "walking",
        "RUNNING": "walking"
    }

    google_mode = mode_mapping.get(mode_transport, "driving")

    # Appel API Google Distance Matrix
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
                return distance_m / 1000  # conversion mètres → km

    except Exception as e:
        print(f"Erreur API Google Maps : {e}")

    return None

# ==============================================================================
# 6. CALCUL DES DISTANCES + CACHE OPTIMISATION
# ==============================================================================

print("\nDémarrage du calcul des distances domicile-entreprise...")
print(f"Nombre total de lignes à traiter : {len(df)}")
print("-" * 80)

# Si fichier déjà existant → on recharge les distances calculées (cache)
if os.path.exists("../data/donnees_fusionnees.csv"):
    df_existant = pd.read_excel("../data/donnees_fusionnees.csv")

    # Dictionnaire pour éviter de recalculer les distances déjà connues
    cache_distances = dict(zip(
        df_existant["employee_id"],
        df_existant["distance_domicile_entreprise_km"]
    ))
else:
    cache_distances = {}

# Listes de stockage des résultats
distances_calculees = []
flags_erreur = []

# Parcours ligne par ligne des salariés
for idx, row in df.iterrows():

    salarie_id = row["employee_id"]
    adresse_salarie = row["domicile_address"]
    transport = row["moyen_transport"]

    # ==========================================================
    # CACHE : évite appel API si distance déjà connue
    # ==========================================================
    if salarie_id in cache_distances and pd.notna(cache_distances[salarie_id]):
        distance_km = cache_distances[salarie_id]

        if idx < 10:
            print(f"Salarié {salarie_id} | Cache utilisé | {distance_km} km")
    else:
        # Sinon appel API Google Maps
        distance_km = obtenir_distance_google_maps(
            adresse_salarie,
            ADRESSE_ENTREPRISE,
            transport
        )

    distances_calculees.append(distance_km)

    # ==========================================================
    # RÈGLES MÉTIER (contrôle de cohérence mobilité douce)
    # ==========================================================
    erreur_declaration = False

    if distance_km is not None:

        # Si marche / course → distance max autorisée 15 km
        if transport in ["MARCHE", "COURSE À PIED", "RUNNING"] and distance_km > 15:
            erreur_declaration = True

        # Si vélo / trottinette → distance max autorisée 25 km
        elif transport in ["VÉLO", "TROTTINETTE"] and distance_km > 25:
            erreur_declaration = True

    flags_erreur.append(erreur_declaration)

    # Affichage debug pour les 10 premières lignes
    if idx < 10:
        print(f"Salarié {salarie_id} | {transport} | {distance_km} km | erreur={erreur_declaration}")

print("-" * 80)
print("Traitement terminé")

# ==============================================================================
# 7. ENRICHISSEMENT DU DATASET FINAL
# ==============================================================================

df["distance_domicile_entreprise_km"] = distances_calculees
df["erreur_declaration_transport"] = flags_erreur

# ==============================================================================
# 8. SAUVEGARDE FINALE
# ==============================================================================

df.to_csv("./data/donnees_fusionnees.csv", index=False, encoding='utf-8', sep=',')

print("\n=== ÉTAPE 03 TERMINÉE AVEC SUCCÈS ===")
print(f"Anomalies détectées : {df['erreur_declaration_transport'].sum()}")
print("Fichier sauvegardé : ./data/donnees_fusionnees.csv")