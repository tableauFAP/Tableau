from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_projects_dataframe

connection = TableauServerConnection(
    host='https://10ax.online.tableau.com',             # ton URL Tableau Cloud
    site_id='Default',                                  # ton site Tableau
    personal_access_token_name='TON_TOKEN_NAME',
    personal_access_token_secret='TON_TOKEN_VALUE',
    api_version='3.20'
)

connection.sign_in()

# Exemple : publier le CSV
csv_file_path = "data_carburant.csv"
project_name = "Default"

projects_df = get_projects_dataframe(connection)
project_id = projects_df.loc[projects_df.name == project_name, 'id'].values[0]

connection.publish_data_source(
    datasource_file_path=csv_file_path,
    datasource_name='Carburant',
    project_id=project_id,
    connection_credentials=None,
    overwrite=True
)

connection.sign_out()
print("CSV publié sur Tableau Cloud ✅")
