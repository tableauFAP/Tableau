# publish_tableau.py
import os
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_projects_dataframe

# ⚠️ Ces variables seront passées depuis GitHub Actions secrets
TABLEAU_SERVER_URL = os.environ['TABLEAU_SERVER_URL']  # ex: https://10ax.online.tableau.com
TABLEAU_SITE_ID = os.environ['TABLEAU_SITE_ID']        # ex: Default
TABLEAU_API_TOKEN_NAME = os.environ['TABLEAU_API_TOKEN_NAME']
TABLEAU_API_TOKEN_VALUE = os.environ['TABLEAU_API_TOKEN_VALUE']

# Fichier CSV à publier
CSV_FILE_PATH = "data_carburant.csv"
DATASOURCE_NAME = "Carburant"
PROJECT_NAME = "Default"  # Nom du projet Tableau Cloud où publier

# --- Connexion à Tableau Cloud ---
tableau_config = {
    "server": TABLEAU_SERVER_URL,
    "api_version": "3.20",
    "personal_access_token_name": TABLEAU_API_TOKEN_NAME,
    "personal_access_token_secret": TABLEAU_API_TOKEN_VALUE,
    "site_id": TABLEAU_SITE_ID
}

connection = TableauServerConnection.from_config_dict(config_json=tableau_config)
connection.sign_in()
print("✅ Connecté à Tableau Cloud")

# --- Récupérer l'ID du projet ---
projects_df = get_projects_dataframe(connection)
if PROJECT_NAME not in projects_df['name'].values:
    raise Exception(f"Projet '{PROJECT_NAME}' introuvable sur Tableau Cloud")
project_id = projects_df.loc[projects_df.name == PROJECT_NAME, 'id'].values[0]

# --- Publier le CSV comme datasource ---
connection.publish_data_source(
    datasource_file_path=CSV_FILE_PATH,
    datasource_name=DATASOURCE_NAME,
    project_id=project_id,
    connection_credentials=None,
    overwrite=True
)
print(f"✅ CSV '{CSV_FILE_PATH}' publié comme datasource '{DATASOURCE_NAME}' dans le projet '{PROJECT_NAME}'")

# --- Déconnexion ---
connection.sign_out()
print("✅ Déconnecté de Tableau Cloud")
