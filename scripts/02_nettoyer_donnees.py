# =========================
# IMPORT DE LA LIBRAIRIE
# =========================
import pandas as pd


# =========================
# CHARGEMENT DES FICHIERS EXCEL
# =========================

# Fichier RH (informations salariés : salaire, adresse, transport, etc.)
df_rh = pd.read_excel("../data/Données+RH.xlsx")

# Fichier Sport (activité sportive des salariés)
df_sport = pd.read_excel("../data/Données+Sportive.xlsx")


# =========================
# RENOMMAGE DES COLONNES (STANDARDISATION)
# =========================

# Objectif :
# → Avoir des noms de colonnes simples, identiques et exploitables dans tout le projet

df_rh = df_rh.rename(columns={
    "ID salarié": "employee_id",
    "Salaire brut": "salaire_brut",
    "Adresse du domicile": "address_domicile",
    "Moyen de déplacement": "transport_mode"
})

df_sport = df_sport.rename(columns={
    "ID salarié": "employee_id",
    "Pratique d'un sport": "pratique_sport"
})


# =========================
# NETTOYAGE DES DONNÉES RH
# =========================

# On normalise les modes de transport :
# - suppression espaces inutiles
# - mise en majuscules pour uniformiser les valeurs
df_rh["transport_mode"] = df_rh["transport_mode"].str.strip().str.upper()


# =========================
# NETTOYAGE DES DONNÉES SPORT
# =========================

# Remplacement des valeurs manquantes par "AUCUN"
# → signifie que le salarié ne pratique pas de sport
df_sport["pratique_sport"] = df_sport["pratique_sport"].fillna("AUCUN")

# Uniformisation des valeurs :
# - majuscules
# - suppression espaces
df_sport["pratique_sport"] = df_sport["pratique_sport"].str.strip().str.upper()


# =========================
# CONTRÔLE DES DONNÉES
# =========================

# On affiche un aperçu pour vérifier que tout est correct
print("=== RH ===")
print(df_rh.head())

print("\n=== SPORT ===")
print(df_sport.head())