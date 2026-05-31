import pandas as pd


# =========================
# CHARGEMENT DES DONNÉES
# =========================
df_rh = pd.read_excel("../data/Données+RH.xlsx")
df_sport = pd.read_excel("../data/Données+Sportive.xlsx")

# =========================
# RENOMMAGE
# =========================
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


# =========================
# NETTOYAGE SPORT
# =========================
df_sport["pratique_sport"] = df_sport["pratique_sport"].fillna("AUCUN").str.upper().str.strip()

# =========================
# FUSION DONNEES 
# =========================
df = df_rh.merge(df_sport, on="employee_id", how="left")

# =========================
# CONVERSATION DES DATES
# =========================

df["Date de naissance"] = df["Date de naissance"].dt.strftime("%Y-%m-%d")
df["Date d'embauche"] = df["Date d'embauche"].dt.strftime("%Y-%m-%d")

# =========================
# SAUVEGARDE DU FICHIER FUSIONNÉ
# =========================
df.to_excel("../data/donnees_fusionnees.xlsx", index=False)

print("Fichier fusionné créé avec succès !")

# =========================
# VÉRIFICATION
# =========================
print(df.head())