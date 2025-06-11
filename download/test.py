import os
import requests
from dotenv import load_dotenv

load_dotenv()

KEYCLOAK_TOKEN_URL = "http://40.84.231.179:8080/realms/master/protocol/openid-connect/token"
ORION_LD_URL = "http://40.84.231.179:8000/orion/ngsi-ld/v1/entityOperations/query"
ENTITY_TYPE = "videoRecorded"

CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID")
CLIENT_SECRET = os.getenv("KEYCLOAK_CLIENT_SECRET")

def get_token():
    response = requests.post(KEYCLOAK_TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    })
    response.raise_for_status()
    return response.json()["access_token"]

def query_entities(token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/ld+json",
        "Link": '<https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
    }

    body = {
        "entities": [{"type": ENTITY_TYPE}],
        "limit": 10
    }

    response = requests.post(ORION_LD_URL, headers=headers, json=body)
    response.raise_for_status()
    data = response.json()
    print(f"✅ Recibidas {len(data)} entidades tipo '{ENTITY_TYPE}'")
    for e in data:
        print("🟢", e.get("id"), e.get("path", {}).get("value", ""))

if __name__ == "__main__":
    token = get_token()
    query_entities(token)
