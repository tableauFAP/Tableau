import requests
import pandas as pd

URL = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-carburants-quotidien/records"

params = {
    "select": "prix_nom,prix_valeur,prix_maj",
    "where": "prix_nom IS NOT NULL AND prix_valeur IS NOT NULL AND prix_maj IS NOT NULL",
    "limit": 100000
}

print("Téléchargement des données API...")

response = requests.get(URL, params=params)

if response.status_code != 200:
    raise Exception(f"Erreur API: {response.status_code}")

data = response.json()["results"]

df = pd.DataFrame(data)

print(f"Données récupérées : {len(df)} lignes")

# --- CLEAN DATA ---
df["date"] = df["prix_maj"].str[:10]

# garder dernier prix par station/jour si dispo (optionnel)
# df = df.sort_values("prix_maj").drop_duplicates(["id", "prix_nom", "date"], keep="last")

# moyenne nationale
df_avg = df.groupby(["prix_nom", "date"])["prix_valeur"].mean().reset_index()

# sauvegarde
df_avg.to_csv("data_carburant_gouv.csv", index=False)

print("✅ CSV généré")
