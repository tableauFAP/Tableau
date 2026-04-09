import os
import pandas as pd
from tableauhyperapi import (
    HyperProcess, Connection, TableDefinition, TableName, SqlType,
    Telemetry, Inserter, CreateMode
)
import tableauserverclient as TSC

# ----------------------------
# CONFIGURATION via secrets
# ----------------------------
TABLEAU_SERVER_URL = os.environ["TABLEAU_SERVER_URL"]
TABLEAU_SITE_ID = os.environ["TABLEAU_SITE_ID"]
TABLEAU_API_TOKEN_NAME = os.environ["TABLEAU_API_TOKEN_NAME"]
TABLEAU_API_TOKEN_VALUE = os.environ["TABLEAU_API_TOKEN_VALUE"]

CSV_PATH = "data_carburant.csv"
HYPER_PATH = "data_carburant.hyper"
DATASOURCE_NAME = "Carburant"
PROJECT_NAME = "GitHubDB"  # Nom du projet Tableau où publier

# ----------------------------
# 1️⃣ Lire le CSV
# ----------------------------
data = pd.read_csv(CSV_PATH)
print(f"✅ CSV chargé : {len(data)} lignes, {len(data.columns)} colonnes")

# ----------------------------
# 2️⃣ Créer le fichier Hyper
# ----------------------------
with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint,
                    database=HYPER_PATH,
                    create_mode=CreateMode.CREATE_AND_REPLACE) as conn:

        # Définir les colonnes Hyper en fonction du CSV
        columns = []
        for col in data.columns:
            if pd.api.types.is_integer_dtype(data[col]):
                columns.append((col, SqlType.int()))
            elif pd.api.types.is_float_dtype(data[col]):
                columns.append((col, SqlType.double()))
            else:
                columns.append((col, SqlType.text()))

        table = TableDefinition(table_name=TableName("Extract", DATASOURCE_NAME),
                                columns=columns)

        conn.catalog.create_table(table)

        # Insérer les données
        with Inserter(conn, table) as inserter:
            inserter.add_rows(data.values.tolist())
            inserter.execute()

print(f"✅ Fichier Hyper créé : {HYPER_PATH}")

# ----------------------------
# 3️⃣ Publier sur Tableau Cloud
# ----------------------------
tableau_auth = TSC.PersonalAccessTokenAuth(
    token_name=TABLEAU_API_TOKEN_NAME,
    personal_access_token=TABLEAU_API_TOKEN_VALUE,
    site_id=TABLEAU_SITE_ID
)

server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)

with server.auth.sign_in(tableau_auth):
    # Vérifier / créer le projet
    all_projects, _ = server.projects.get()
    project = next((p for p in all_projects if p.name == PROJECT_NAME), None)
    if project is None:
        project = server.projects.create(TSC.ProjectItem(name=PROJECT_NAME))
    project_id = project.id

    # Préparer la datasource Hyper
    datasource = TSC.DatasourceItem(project_id, name=DATASOURCE_NAME)
    print(f"Publication de {HYPER_PATH} sur Tableau Cloud...")
    server.datasources.publish(datasource, HYPER_PATH, mode=TSC.Server.PublishMode.Overwrite)
    print("✅ Publication terminée")
