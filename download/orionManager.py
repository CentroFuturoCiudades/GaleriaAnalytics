import os
import time
import requests
import json
from typing import List
from dotenv import load_dotenv


class OrionManager:
    def __init__(self, client_id: str, client_secret: str, keycloak_url: str, orion_url: str):
        """
        Initialize the OrionManager class.

        Args:
            client_id (str): Keycloak client ID.
            client_secret (str): Keycloak client secret.
            keycloak_url (str): Keycloak token URL.
            orion_url (str): Orion Context Broker base URL.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.keycloak_url = keycloak_url.rstrip("/")
        self.orion_url = orion_url.rstrip("/")
        self.token = None
        self.token_expiry = 0

    def obtain_token(self):
        """
        Obtain an access token from Keycloak using client credentials.
        """
        token_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        if self.token_expiry < time.time():
            try:
                response = requests.post(self.keycloak_url, data=token_data)
                response.raise_for_status()
                token_json = response.json()
                self.token = token_json.get("access_token")
                expires_in = token_json.get("expires_in", 0)
                self.token_expiry = time.time() + expires_in - 10
                print("Access token obtained successfully.")
            except requests.exceptions.RequestException as e:
                print(f"Error obtaining token: {e}")
                raise

    def get_token(self):
        """
        Get a valid token, refreshing it if necessary.
        """
        if not self.token or time.time() > self.token_expiry:
            self.obtain_token()
        return self.token

    def fetch_and_filter_entities(self, entity_type: str, path_prefix: str = "galeria", batch_size: int = 1000) -> List[str]:
        """
        Fetch and filter entities based on a prefix for the 'path' field.

        Args:
            entity_type (str): Type of entities to fetch.
            path_prefix (str): Prefix for filtering 'path' values.
            batch_size (int): Maximum number of entities per batch.

        Returns:
            List[str]: List of filtered paths.
        """
        offset = 0
        filtered_paths = []

        while True:
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {self.get_token()}"
            }

            params = {"type": entity_type, "limit": batch_size, "offset": offset}
            try:
                response = requests.get(f"{self.orion_url}/v1/entities", params=params, headers=headers)
                if response.status_code == 401:
                    print("⚠️ Token expired, refreshing token and retrying...")
                    self.obtain_token()
                    headers["Authorization"] = f"Bearer {self.token}"
                    response = requests.get(f"{self.orion_url}/v1/entities", params=params, headers=headers)

                response.raise_for_status()
                entities = response.json()

                if not entities:
                    break  # No more entities available

                for entity in entities:
                    path_value = entity.get("path", {}).get("value", "")
                    if path_value.startswith(path_prefix):
                        filtered_paths.append(path_value)

                offset += batch_size
                print(f"Processed {offset} entities...")

            except requests.exceptions.RequestException as e:
                print(f"Error fetching entities: {e}")
                break

        return filtered_paths

    def save_filtered_paths(self, paths: List[str], output_file: str = "filtered_paths.json") -> None:
        """
        Save filtered paths to a JSON file.

        Args:
            paths (List[str]): List of paths to save.
            output_file (str): Output file name.
        """
        try:
            with open(output_file, "w") as json_file:
                json.dump(paths, json_file, indent=4)
            print(f"Filtered paths saved to '{output_file}'.")
        except IOError as e:
            print(f"Error saving paths to file: {e}")


if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()

    client_id = os.getenv("KEYCLOAK_CLIENT_ID")
    client_secret = os.getenv("KEYCLOAK_CLIENT_SECRET")
    keycloak_url = "http://40.84.231.179:8080/realms/master/protocol/openid-connect/token"
    orion_url = "http://40.84.231.179:8000/orion/ngsi-ld"

    if not client_id or not client_secret:
        print("Client ID and Client Secret must be provided in the .env file.")
        exit(1)

    manager = OrionManager(client_id, client_secret, keycloak_url, orion_url)

    # Fetch and filter paths
    entity_type = "videoRecorded"
    filtered_paths = manager.fetch_and_filter_entities(entity_type=entity_type, path_prefix="galeria")

    print(f"Total filtered paths: {len(filtered_paths)}")

    # Save the paths to a file
    manager.save_filtered_paths(filtered_paths)