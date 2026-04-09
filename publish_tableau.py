from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_projects_dataframe

connection = TableauServerConnection(
    server='https://your-site.tableau.com',
    api_version='3.20',
    personal_access_token_name='YOUR_TOKEN_NAME',
    personal_access_token_secret='YOUR_TOKEN_VALUE',
    site_id='YOUR_SITE_ID'
)
connection.sign_in()

# Publier le CSV comme datasource
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
