import requests
import pandas as pd
import os

CLIENT_ID = os.environ["INSEE_CLIENT_ID"]
CLIENT_SECRET = os.environ["INSEE_CLIENT_SECRET"]

# 🔐 Token
response = requests.post(
    "https://api.insee.fr/token",
    data={"grant_type": "client_credentials"},
    auth=(CLIENT_ID, CLIENT_SECRET)
)

print("STATUS:", response.status_code)
print("RESPONSE:", response.text)

data = response.json()
token = data["access_token"]

# 📊 Data
url = "https://api.insee.fr/series/BDM/V1/data/SERIES_BDM/000442588"

data = requests.get(
    url,
    headers={"Authorization": f"Bearer {token}"}
).json()

obs = data["Series"][0]["Obs"]

rows = [
    {"date": o["@TIME_PERIOD"], "value": o["@OBS_VALUE"]}
    for o in obs
]

df = pd.DataFrame(rows)

# 💾 Sauvegarde
df.to_csv("data.csv", index=False)
