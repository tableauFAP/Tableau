import os
import tableauserverclient as TSC
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, TableName, Inserter

# Récupération des secrets
TABLEAU_SERVER_URL = os.environ["TABLEAU_SERVER_URL"]
TABLEAU_SITE_ID = os.environ["TABLEAU_SITE_ID"]
TABLEAU_API_TOKEN_NAME = os.environ["TABLEAU_API_TOKEN_NAME"]
TABLEAU_API_TOKEN_VALUE = os.environ["TABLEAU_API_TOKEN_VALUE"]

CSV_PATH = "data_carburant.csv"
HYPER_PATH = "data_carburant.hyper"
DATASOURCE_NAME = "Carburant"

# 1️⃣ Convertir CSV en Hyper
with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint, database=HYPER_PATH, create_mode=True) as conn:
        # Déterminer la table
        table = TableDefinition(table_name=TableName("Extract", "Sheet1"), columns=[])
        
        # Lire CSV
        import pandas as pd
        df = pd.read_csv(CSV_PATH)
        
        # Créer les colonnes selon le CSV
        for col_name, dtype in zip(df.columns, df.dtypes):
            if pd.api.types.is_integer_dtype(dtype):
                table.columns.append(TSC.tableauhyperapi.TableDefinition.Column(col_name, SqlType.big_int()))
            elif pd.api.types.is_float_dtype(dtype):
                table.columns.append(TSC.tableauhyperapi.TableDefinition.Column(col_name, SqlType.double()))
            else:
                table.columns.append(TSC.tableauhyperapi.TableDefinition.Column(col_name, SqlType.text()))
        
        conn.catalog.create_table(table)
        
        # Insérer les données
        with Inserter(conn, table) as inserter:
            inserter.add_rows(df.values.tolist())
            inserter.execute()

# 2️⃣ Publier sur Tableau Cloud
tableau_auth = TSC.PersonalAccessTokenAuth(
    token_name=TABLEAU_API_TOKEN_NAME,
    personal_access_token=TABLEAU_API_TOKEN_VALUE,
    site_id=TABLEAU_SITE_ID
)

server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)

with server.auth.sign_in(tableau_auth):

    # Projet
    all_projects, _ = server.projects.get()
    project_name = "GitHubDB"
    project = next((p for p in all_projects if p.name == project_name), None)
    if project is None:
        new_project = TSC.ProjectItem(name=project_name)
        project = server.projects.create(new_project)
    project_id = project.id

    # Préparer la datasource Hyper
    datasource = TSC.DatasourceItem(project_id, name=DATASOURCE_NAME)
    
    print("Publication en cours sur Tableau Cloud...")
    server.datasources.publish(datasource, HYPER_PATH, mode=TSC.Server.PublishMode.Overwrite)
    print("✅ Publication terminée")
