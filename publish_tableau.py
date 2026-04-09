import os
import tableauserverclient as TSC

# Récupération des secrets
TABLEAU_SERVER_URL = os.environ["TABLEAU_SERVER_URL"]  # https://10ax.online.tableau.com
TABLEAU_SITE_ID = os.environ["TABLEAU_SITE_ID"]        # ex: Default
TABLEAU_API_TOKEN_NAME = os.environ["TABLEAU_API_TOKEN_NAME"]
TABLEAU_API_TOKEN_VALUE = os.environ["TABLEAU_API_TOKEN_VALUE"]

CSV_PATH = "data_carburant.csv"
DATASOURCE_NAME = "Carburant"

# Authentification
tableau_auth = TSC.PersonalAccessTokenAuth(
    token_name=TABLEAU_API_TOKEN_NAME,
    personal_access_token=TABLEAU_API_TOKEN_VALUE,
    site_id=TABLEAU_SITE_ID
)

server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)

with server.auth.sign_in(tableau_auth):

    # Trouver l'ID du projet "z_Autres DB"
    all_projects, _ = server.projects.get()
    project = next(p for p in all_projects if p.name == "z_Autres DB")
    project_id = project.id

    # Préparer le fichier CSV
    new_datasource = TSC.FileUploadItem(CSV_PATH, DATASOURCE_NAME)

    # Publier / mettre à jour
    print("Publication en cours sur Tableau Cloud...")
    server.datasources.update(new_datasource, project_id=project_id, overwrite=True)

    print("✅ Publication terminée")
