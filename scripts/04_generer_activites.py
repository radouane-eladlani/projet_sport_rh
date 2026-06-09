import pandas as pd
import random
from datetime import datetime, timedelta

# Chargement
df = pd.read_excel("../data/donnees_fusionnees.xlsx")

activities = []

# Génération de dates ET heures sur les 12 derniers mois
def random_date_complete():
    start = datetime(2025, 6, 1) # Pour couvrir les 12 derniers mois par rapport à juin 2026
    random_minutes = random.randint(0, 365 * 24 * 60)
    return start + timedelta(minutes=random_minutes)

for _, row in df.iterrows():
    sport = str(row.get("pratique_sport", "AUCUN")).upper().strip()
    if sport == "AUCUN" or pd.isna(row.get("pratique_sport")):
        continue
    
    # Simulation incluant des salariés sous la barre des 15 activités et d'autres au-dessus
    nb_activities = random.randint(5, 35) 
    
    for _ in range(nb_activities):
        start_dt = random_date_complete()
        duration_min = random.randint(20, 120)
        end_dt = start_dt + timedelta(minutes=duration_min)
        
        # Règle de la note de cadrage : vide si non pertinent (ex: Escalade, Yoga...)
        if sport in ["ESCALADE", "YOGA", "MUSCULATION"]:
            distance_m = None
            km_text = ""
        else:
            distance_m = random.randint(2000, 15000)
            km_text = f"de {round(distance_m / 1000, 1)} km "

        # Formatage des messages Slack comme demandé dans l'énoncé
        # "Bravo [Nom]! Tu viens de [sport] [distance] en [temps]!"
        nom_complet = f"{row.get('Prénom', '')} {row.get('Nom', '')}".strip()
        commentaire = f"Bravo {nom_complet} ! Une session de {sport.lower()} {km_text}en {duration_min} min ! Quelle énergie ! 🔥"
        
        activities.append({
            "ID": len(activities) + 1,
            "ID salarié": row["employee_id"],
            "Date de début de l'activité": start_dt.strftime("%d/%m/%Y %H:%M"),
            "Type": sport,
            "Distance": distance_m if distance_m is not None else "",
            "Date de fin de l'activité": end_dt.strftime("%d/%m/%Y %H:%M"),
            "Commentaire": commentaire
        })

df_activities = pd.DataFrame(activities)
df_activities.to_excel("../data/activites_sportives.xlsx", index=False)
print(f"Génération terminée : {len(df_activities)} lignes générées de façon conforme.")