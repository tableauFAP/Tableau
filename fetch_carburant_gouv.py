import requests
import pandas as pd

URL = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-carburants-quotidien/exports/xlsx?select=median%28prix_valeur%29%20as%20prix&where=prix_nom%20IS%20NOT%20NULL%20AND%20prix_valeur%20IS%20NOT%20NULL%20AND%20prix_maj%20is%20not%20null&group_by=prix_nom%20as%20category%2C%20date_format%28prix_maj%2C%20%27yyyy-MM-dd%27%29%20as%20date&limit=-1&timezone=UTC&use_labels=false&compressed=false&epsg=4326"

OUTPUT_CSV = "data_carburant_gouv.csv"

print("Téléchargement des données carburant...")

# Télécharger le fichier Excel
response = requests.get(URL)
response.raise_for_status()

with open("data.xlsx", "wb") as f:
    f.write(response.content)

print("Fichier Excel téléchargé")

# Lire le Excel
df = pd.read_excel("data.xlsx")

print(f"✅ {len(df)} lignes chargées")

# Renommer colonnes (important pour Tableau)
df.columns = ["carburant", "date", "prix"]

# Nettoyage
df = df.dropna()

# Conversion types
df["date"] = pd.to_datetime(df["date"])
df["prix"] = pd.to_numeric(df["prix"], errors="coerce")

# Tri
df = df.sort_values(by=["carburant", "date"])

# Sauvegarde CSV
df.to_csv(OUTPUT_CSV, index=False)

print("✅ CSV généré :", OUTPUT_CSV)
