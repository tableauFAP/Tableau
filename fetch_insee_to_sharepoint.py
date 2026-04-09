import requests
import pandas as pd
import os

# ========================
# CONFIG
# ========================
INSEE_CLIENT_ID = os.environ["INSEE_CLIENT_ID"]
INSEE_CLIENT_SECRET = os.environ["INSEE_CLIENT_SECRET"]

TENANT_ID = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]

SITE_ID = os.environ["SHAREPOINT_SITE_ID"]
DRIVE_ID = os.environ["SHAREPOINT_DRIVE_ID"]

FILE_PATH = "/Shared Documents/insee_000442588.csv"

# ========================
# 1. TOKEN INSEE
# ========================
token_resp = requests.post(
    "https://api.insee.fr/token",
    data={"grant_type": "client_credentials"},
    auth=(INSEE_CLIENT_ID, INSEE_CLIENT_SECRET)
)

insee_token = token_resp.json()["access_token"]

# ========================
# 2. DATA INSEE
# ========================
url = "https://api.insee.fr/series/BDM/V1/data/SERIES_BDM/000442588"

headers = {
    "Authorization": f"Bearer {insee_token}",
    "Accept": "application/json"
}

data = requests.get(url, headers=headers).json()

obs = data["Series"][0]["Obs"]

rows = []
for o in obs:
    rows.append({
        "date": o["@TIME_PERIOD"],
        "value": o["@OBS_VALUE"]
    })

df = pd.DataFrame(rows)

# ========================
# 3. TOKEN MICROSOFT
# ========================
ms_token_resp = requests.post(
    f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
    data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default"
    }
)

ms_token = ms_token_resp.json()["access_token"]

# ========================
# 4. UPLOAD SHAREPOINT
# ========================
upload_url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:{FILE_PATH}:/content"

csv_data = df.to_csv(index=False)

upload_resp = requests.put(
    upload_url,
    headers={
        "Authorization": f"Bearer {ms_token}",
        "Content-Type": "text/csv"
    },
    data=csv_data
)

print(upload_resp.status_code, upload_resp.text)
