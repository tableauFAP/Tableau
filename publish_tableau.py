import os
import pandas as pd
import unicodedata
from tableauhyperapi import (
    HyperProcess,
    Connection,
    TableDefinition,
    SqlType,
    Telemetry,
    TableName,
    Inserter,
    CreateMode,
)
import tableauserverclient as TSC

# --- Variables & Secrets ---
TABLEAU_SERVER_URL = os.environ["TABLEAU_SERVER_URL"]
TABLEAU_SITE_ID = os.environ["TABLEAU_SITE_ID"]
TABLEAU_API_TOKEN_NAME = os.environ["TABLEAU_API_TOKEN_NAME"]
TABLEAU_API_TOKEN_VALUE = os.environ["TABLEAU_API_TOKEN_VALUE"]

CSV_PATH = "Insee_Gazole.csv"
HYPER_PATH = "Insee_Gazole.hyper"
DATASOURCE_NAME = "Insee_Gazole"

# --- Nettoyage des noms de colonnes ---
def clean_colname(col):
    nfkd_form = unicodedata.normalize("NFKD", col)
    no_accents = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    cleaned = (
        no_accents.replace(" ", "_")
        .replace("-", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "_")
    )
    return cleaned

# --- Lecture et nettoyage CSV ---
print("Chargement CSV...")
data = pd.read_csv(CSV_PATH, skiprows=[1, 2, 3])  # sauter lignes 2,3,4
data.columns = [clean_colname(c) for c in data.columns]
print(f"✅ CSV nettoyé : {len(data)} lignes, colonnes : {list(data.columns)}")

# --- Création du fichier Hyper ---
print("Création du fichier Hyper...")
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(
        endpoint=hyper.endpoint, database=HYPER_PATH, create_mode=CreateMode.CREATE_AND_REPLACE
    ) as conn:

        # Définition des colonnes avec types adaptés
        columns = []
        for col in data.columns:
            if pd.api.types.is_integer_dtype(data[col]):
                columns.append(TableDefinition.Column(col, SqlType.int()))
            elif pd.api.types.is_float_dtype(data[col]):
                columns.append(TableDefinition.Column(col, SqlType.double()))
            else:
                columns.append(TableDefinition.Column(col, SqlType.text()))

        # Créer le schéma "public" si inexistant
        try:
            conn.catalog.create_schema("public")
        except Exception:
            pass  # schéma existe déjà

        # Définir la table dans le schéma "public"
        table = TableDefinition(table_name=TableName("public", DATASOURCE_NAME), columns=columns)

        # Créer la table
        conn.catalog.create_table(table)

        # Insérer les données
        with Inserter(conn, table) as inserter:
            inserter.add_rows(data.values.tolist())
            inserter.execute()

print(f"✅ Fichier Hyper créé : {HYPER_PATH}")

# --- Publication sur Tableau Cloud ---
print("Connexion à Tableau Cloud...")

tableau_auth = TSC.PersonalAccessTokenAuth(
    token_name=TABLEAU_API_TOKEN_NAME,
    personal_access_token=TABLEAU_API_TOKEN_VALUE,
    site_id=TABLEAU_SITE_ID,
)
server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)

with server.auth.sign_in(tableau_auth):
    # Trouver ou créer projet
    all_projects, _ = server.projects.get()
    project_name = "GitHubDB"
    project = next((p for p in all_projects if p.name == project_name), None)
    if project is None:
        new_project = TSC.ProjectItem(name=project_name)
        project = server.projects.create(new_project)

    # Préparer datasource Hyper à publier
    datasource = TSC.DatasourceItem(project.id, name=DATASOURCE_NAME)

    print("Publication sur Tableau Cloud...")
    server.datasources.publish(datasource, HYPER_PATH, mode=TSC.Server.PublishMode.Overwrite)

print("✅ Publication terminée")
