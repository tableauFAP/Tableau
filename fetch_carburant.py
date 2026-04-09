import requests
import pandas as pd
import io

# URL directe du fichier Excel INSEE
URL = "https://www.insee.fr/fr/statistiques/fichier/000442588/prix_carburants.xlsx"

print("Téléchargement du fichier INSEE...")
response = requests.get(URL)

if response.status_code != 200:
    raise Exception(f"Impossible de télécharger le fichier: {response.status_code}")

# Lire le fichier Excel depuis la mémoire
excel_data = pd.read_excel(io.BytesIO(response.content), sheet_name=None)  # None = toutes les feuilles

# Vérifier les feuilles
print("Feuilles disponibles:", excel_data.keys())

# Exemple: choisir la feuille pertinente (vérifie le nom exact dans le fichier)
sheet_name = list(excel_data.keys())[0]  # première feuille par défaut
df = excel_data[sheet_name]

# Nettoyage simple si nécessaire
df = df.dropna(how='all')  # enlever les lignes vides

# Sauvegarder en CSV
df.to_csv("data_carburant.csv", index=False)
print("CSV généré ✅")
