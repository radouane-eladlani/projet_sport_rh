# ==============================================================================
# 02_nettoyer_donnees
# ==============================================================================
import pandas as pd

# Fichier RH (informations salariés : salaire, adresse, transport, etc.)

df_rh = pd.read_excel("./data/Données+RH.xlsx")

#Fichier Sport (activité sportive des salariés)
df_sport = pd.read_excel("./data/Données+Sportive.xlsx")

#Objectif :
#→ Avoir des noms de colonnes simples, identiques et exploitables dans tout le projet

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

#On normalise les modes de transport :
#suppression espaces inutiles mise en majuscules pour uniformiser les valeurs
df_rh["moyen_transport"] = df_rh["moyen_transport"].str.strip().str.upper()

#Remplacement des valeurs manquantes par "AUCUN"

df_sport["pratique_sport"] = df_sport["pratique_sport"].fillna("AUCUN")

#Uniformisation des valeurs : majuscules, suppression espaces
df_sport["pratique_sport"] = df_sport["pratique_sport"].str.strip().str.upper()

# On affiche un aperçu pour vérifier que tout est correct

print("=== RH ===")
print(df_rh.head())

print("\n=== SPORT ===")
print(df_sport.head())
print(df_rh["moyen_transport"].unique())