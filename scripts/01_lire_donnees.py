import pandas as pd

df_rh = pd.read_excel("../data/Données+RH.xlsx")

df_sport = pd.read_excel("../data/Données+Sportive.xlsx")

# Afficher les premières lignes RH
print("=== RH ===")
print(df_rh.head())

# Afficher les colonnes RH
print("\nColonnes RH :")
print(df_rh.columns)

# Afficher les premières lignes Sport
print("\n=== SPORT ===")
print(df_sport.head())

# Afficher les colonnes Sport
print("\nColonnes Sport :")
print(df_sport.columns)