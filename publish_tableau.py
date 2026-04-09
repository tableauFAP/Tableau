import os
import tableauserverclient as TSC

# Récupération des secrets
TABLEAU_SERVER_URL = os.environ["TABLEAU_SERVER_URL"]
TABLEAU_SITE_ID = os.environ["TABLEAU_SITE_ID"]
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

    # Lister les projets
    all_projects, _ = server.projects.get()
    project_name = "GitHubDB"  # ou le nom exact du projet existant
    project = next((p for p in all_projects if p.name == project_name), None)
    if project is None:
        # Créer le projet si inexistant
        new_project = TSC.ProjectItem(name=project_name)
        project = server.projects.create(new_project)
    project_id = project.id

    # Préparer le fichier CSV pour publication
    file_item = TSC.FileuploadItem(CSV_PATH)  # <-- seulement le chemin du fichier

    # Préparer la datasource
    datasource = TSC.DatasourceItem(project_id, name=DATASOURCE_NAME)

    # Publier ou mettre à jour la datasource
    print("Publication en cours sur Tableau Cloud...")
    server.datasources.publish(datasource, file_item, mode=TSC.Server.PublishMode.Overwrite)

    print("✅ Publication terminée")
