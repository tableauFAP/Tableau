import requests
import zipfile
import io
import pandas as pd

# URL officielle INSEE
URL = "https://bdm.insee.fr/series/000442588/xlsx"

# Télécharger le ZIP
print("Téléchargement du ZIP...")
response = requests.get(URL)
if response.status_code != 200:
    raise Exception(f"Impossible de télécharger le fichier: {response.status_code}")

# Ouvrir le ZIP depuis la mémoire
with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    # Liste des fichiers contenus
    print("Fichiers dans le ZIP:", z.namelist())
    # On prend le premier fichier Excel trouvé
    excel_filename = [f for f in z.namelist() if f.endswith(".xlsx")][0]
    print("Extraction du fichier:", excel_filename)
    with z.open(excel_filename) as f:
        # Lire l'Excel
        df = pd.read_excel(f, sheet_name=None)  # None = toutes les feuilles

# Vérifier les feuilles
print("Feuilles disponibles:", df.keys())

# Exemple : prendre la première feuille
sheet_name = list(df.keys())[0]
data = df[sheet_name]

# Nettoyage simple
data = data.dropna(how='all')

# Sauvegarder en CSV
data.to_csv("data_carburant.csv", index=False)
print("CSV généré ✅")
