import os
import pandas as pd
from tableauhyperapi import (
    HyperProcess, Connection, TableDefinition, SqlType,
    TableName, Inserter, Telemetry, CreateMode
)
import tableauserverclient as TSC

# --- CONFIGURATION Tableau Cloud via secrets GitHub ---
TABLEAU_SERVER_URL = os.environ["TABLEAU_SERVER_URL"]
TABLEAU_SITE_ID = os.environ["TABLEAU_SITE_ID"]
TABLEAU_API_TOKEN_NAME = os.environ["TABLEAU_API_TOKEN_NAME"]
TABLEAU_API_TOKEN_VALUE = os.environ["TABLEAU_API_TOKEN_VALUE"]

CSV_PATH = "data_carburant.csv"
DATASOURCE_NAME = "Carburant"
HYPER_PATH = "data_carburant.hyper"
PROJECT_NAME = "GitHubDB"

# --- CHARGEMENT DU CSV ---
data = pd.read_csv(CSV_PATH)
print(f"✅ CSV chargé : {len(data)} lignes, {len(data.columns)} colonnes")

# --- CREATION DU FICHIER HYPER ---
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint, database=HYPER_PATH, create_mode=CreateMode.CREATE_AND_REPLACE) as conn:
        
        # Définir les colonnes Hyper dynamiquement
        columns = []
        for col in data.columns:
            if pd.api.types.is_integer_dtype(data[col]):
                columns.append(TableDefinition.Column(col, SqlType.int()))
            elif pd.api.types.is_float_dtype(data[col]):
                columns.append(TableDefinition.Column(col, SqlType.double()))
            else:
                columns.append(TableDefinition.Column(col, SqlType.text()))
        
        table = TableDefinition(
            table_name=TableName("Extract", DATASOURCE_NAME),
            columns=columns
        )
        
        conn.catalog.create_table(table)
        
        # Insérer les données
        with Inserter(conn, table) as inserter:
            inserter.add_rows(data.values.tolist())
            inserter.execute()
print(f"✅ Fichier Hyper créé : {HYPER_PATH}")

# --- PUBLICATION SUR TABLEAU CLOUD ---
tableau_auth = TSC.PersonalAccessTokenAuth(
    token_name=TABLEAU_API_TOKEN_NAME,
    personal_access_token=TABLEAU_API_TOKEN_VALUE,
    site_id=TABLEAU_SITE_ID
)

server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)

with server.auth.sign_in(tableau_auth):
    # Récupérer ou créer le projet
    all_projects, _ = server.projects.get()
    project = next((p for p in all_projects if p.name == PROJECT_NAME), None)
    if project is None:
        project = server.projects.create(TSC.ProjectItem(name=PROJECT_NAME))
    project_id = project.id

    # Préparer la datasource Hyper
    datasource_item = TSC.DatasourceItem(project_id, name=DATASOURCE_NAME)

    # Publier ou mettre à jour la datasource
    print("Publication en cours sur Tableau Cloud...")
    server.datasources.publish(datasource_item, HYPER_PATH, mode=TSC.Server.PublishMode.Overwrite)
    print("✅ Publication terminée sur Tableau Cloud")
