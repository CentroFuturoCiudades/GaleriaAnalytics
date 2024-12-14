# # code with video limit for debugging
# import os
# import json
# from azure.storage.blob import BlobServiceClient
# from dotenv import load_dotenv
# from tqdm import tqdm
# import psycopg2


# class AzureVideos:
#     def __init__(self, output_dir: str, sas_token=None, account_url=None, default_container="crowdcounting", verbose=False, db_config=None):
#         self.output_dir = output_dir
#         self.sas_token = sas_token
#         self.account_url = account_url or "https://cienciaciudades2024.blob.core.windows.net"
#         self.default_container = default_container
#         self.blob_service_client = BlobServiceClient(self.account_url, credential=sas_token)
#         self.verbose = verbose

#         db_name = os.getenv("DB_NAME")
#         user = os.getenv("DB_USER")
#         password = os.getenv("DB_PASSWORD")
#         host = os.getenv("DB_HOST")
#         port = os.getenv("DB_PORT")
#         # Database configuration
#         self.db_config = db_config or {
#             "dbname": db_name,
#             "user": user,
#             "password": password,
#             "host": host,
#             "port": port
#         }

#     def _connect_to_db(self):
#         """Connect to PostgreSQL database."""
#         return psycopg2.connect(**self.db_config)

#     def is_video_downloaded(self, video_id):
#         """Check if a video has already been downloaded."""
#         try:
#             conn = self._connect_to_db()
#             cursor = conn.cursor()

#             # Query to check if the video exists in the table
#             query = "SELECT id FROM video_recorded WHERE id = %s;"
#             cursor.execute(query, (video_id,))
#             result = cursor.fetchone()

#             cursor.close()
#             conn.close()

#             return result is not None
#         except Exception as e:
#             print(f"Error checking video in database: {e}")
#             return False

#     def mark_video_as_downloaded(self, video_id):
#         """Mark a video as downloaded in the database."""
#         try:
#             conn = self._connect_to_db()
#             cursor = conn.cursor()

#             # Insert or update the record in the video_recorded table
#             query = """
#             INSERT INTO video_recorded (id, camera, date_observed, path)
#             VALUES (%s, NULL, CURRENT_TIMESTAMP, %s)
#             ON CONFLICT (id) DO NOTHING;
#             """
#             cursor.execute(query, (video_id, video_id))
#             conn.commit()

#             cursor.close()
#             conn.close()
#         except Exception as e:
#             print(f"Error updating database: {e}")

#     def download_videos_by_paths(self, paths, container_name: str = "", batch_size: int = 4, max_videos: int = None):
#         """Download videos from Azure Blob Storage."""
#         container_name = self.default_container if not container_name else container_name
#         container_client = self.blob_service_client.get_container_client(container_name)

#         total_videos = len(paths)
#         downloaded_count = 0

#         for i in range(0, total_videos, batch_size):
#             if max_videos and downloaded_count >= max_videos:
#                 print(f"Reached the maximum limit of {max_videos} videos. Stopping download.")
#                 break

#             batch_paths = paths[i:i + batch_size]
#             print(f"Downloading batch {i // batch_size + 1} ({len(batch_paths)} videos)...")

#             for path in tqdm(batch_paths, desc="Downloading videos in batch"):
#                 if max_videos and downloaded_count >= max_videos:
#                     break

#                 video_id = os.path.basename(path)

#                 # Check if the video is already downloaded
#                 if self.is_video_downloaded(video_id):
#                     print(f"Video {video_id} is already downloaded. Skipping.")
#                     continue

#                 download_file_path = os.path.join(self.output_dir, path)
#                 os.makedirs(os.path.dirname(download_file_path), exist_ok=True)

#                 try:
#                     blob_client = container_client.get_blob_client(path)

#                     # Download the blob
#                     with open(download_file_path, "wb") as download_file:
#                         download_stream = blob_client.download_blob()
#                         download_file.write(download_stream.readall())
#                     if self.verbose:
#                         print(f"Blob '{path}' has been downloaded to '{download_file_path}'.")

#                     # Mark video as downloaded in the database
#                     self.mark_video_as_downloaded(video_id)
#                     downloaded_count += 1
#                 except Exception as e:
#                     print(f"Error downloading blob {path}: {e}")

#             print(f"Batch {i // batch_size + 1} completed.")


# if __name__ == "__main__":
#     load_dotenv()

#     # Load paths from the JSON file
#     with open("filtered_paths.json", "r") as json_file:
#         filtered_paths = json.load(json_file)

#     print(f"Total paths to download: {len(filtered_paths)}")

#     # Configure Azure Blob Storage
#     account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
#     sas_token = os.getenv("AZURE_STORAGE_SAS_TOKEN")
#     db_config = {
#         "dbname": os.getenv("DB_NAME"),
#         "user": os.getenv("DB_USER"),
#         "password": os.getenv("DB_PASSWORD"),
#         "host": os.getenv("DB_HOST"),
#         "port": os.getenv("DB_PORT")
#     }

#     azure_client = AzureVideos(
#         output_dir="videosGaleria",
#         sas_token=sas_token,
#         account_url=account_url,
#         verbose=True,
#         db_config=db_config
#     )

#     # Download videos using paths
#     azure_client.download_videos_by_paths(filtered_paths, batch_size=4, max_videos=5)




# Code without a video limit

import os
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from tqdm import tqdm
import psycopg2


class AzureVideos:
    def __init__(self, output_dir: str, sas_token=None, account_url=None, default_container="crowdcounting", verbose=False, db_config=None):
        self.output_dir = output_dir
        self.sas_token = sas_token
        self.account_url = account_url or "https://cienciaciudades2024.blob.core.windows.net"
        self.default_container = default_container
        self.blob_service_client = BlobServiceClient(self.account_url, credential=sas_token)
        self.verbose = verbose
        db_name = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        # Database configuration
        self.db_config = db_config or {
            "dbname": db_name,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }

    def _connect_to_db(self):
        """Connect to PostgreSQL database."""
        return psycopg2.connect(**self.db_config)

    def is_video_downloaded(self, video_id):
        """Check if a video has already been downloaded."""
        try:
            conn = self._connect_to_db()
            cursor = conn.cursor()

            # Query to check if the video exists in the table
            query = "SELECT id FROM video_recorded WHERE id = %s;"
            cursor.execute(query, (video_id,))
            result = cursor.fetchone()

            cursor.close()
            conn.close()

            return result is not None
        except Exception as e:
            print(f"Error checking video in database: {e}")
            return False

    def mark_video_as_downloaded(self, video_id):
        """Mark a video as downloaded in the database."""
        try:
            conn = self._connect_to_db()
            cursor = conn.cursor()

            # Insert or update the record in the video_recorded table
            query = """
            INSERT INTO video_recorded (id, camera, date_observed, path)
            VALUES (%s, NULL, CURRENT_TIMESTAMP, %s)
            ON CONFLICT (id) DO NOTHING;
            """
            cursor.execute(query, (video_id, video_id))
            conn.commit()

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error updating database: {e}")

    def download_videos_by_paths(self, paths, container_name: str = "", batch_size: int = 4):
        """Download videos from Azure Blob Storage."""
        container_name = self.default_container if not container_name else container_name
        container_client = self.blob_service_client.get_container_client(container_name)

        total_videos = len(paths)
        downloaded_count = 0

        for i in range(0, total_videos, batch_size):
            batch_paths = paths[i:i + batch_size]
            print(f"Downloading batch {i // batch_size + 1} ({len(batch_paths)} videos)...")

            for path in tqdm(batch_paths, desc="Downloading videos in batch"):
                video_id = os.path.basename(path)

                # Check if the video is already downloaded
                if self.is_video_downloaded(video_id):
                    print(f"Video {video_id} is already downloaded. Skipping.")
                    continue

                download_file_path = os.path.join(self.output_dir, path)
                os.makedirs(os.path.dirname(download_file_path), exist_ok=True)

                try:
                    blob_client = container_client.get_blob_client(path)

                    # Download the blob
                    with open(download_file_path, "wb") as download_file:
                        download_stream = blob_client.download_blob()
                        download_file.write(download_stream.readall())
                    if self.verbose:
                        print(f"Blob '{path}' has been downloaded to '{download_file_path}'.")

                    # Mark video as downloaded in the database
                    self.mark_video_as_downloaded(video_id)
                    downloaded_count += 1
                except Exception as e:
                    print(f"Error downloading blob {path}: {e}")

            print(f"Batch {i // batch_size + 1} completed.")


if __name__ == "__main__":
    load_dotenv()

    # Load paths from the JSON file
    with open("filtered_paths.json", "r") as json_file:
        filtered_paths = json.load(json_file)

    print(f"Total paths to download: {len(filtered_paths)}")

    # Configure Azure Blob Storage
    account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
    sas_token = os.getenv("AZURE_STORAGE_SAS_TOKEN")
    db_config = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT")
    }

    azure_client = AzureVideos(
        output_dir="videosGaleria",
        sas_token=sas_token,
        account_url=account_url,
        verbose=True,
        db_config=db_config
    )

    # Download videos using paths
    azure_client.download_videos_by_paths(filtered_paths, batch_size=4)



# CREATE TABLE video_recorded (
#     id VARCHAR(255) PRIMARY KEY,
#     camera VARCHAR(255),
#     date_observed TIMESTAMP NOT NULL,
#     path VARCHAR(255) NOT NULL
# );

# CREATE TABLE tracks (
#     id SERIAL PRIMARY KEY,
#     video_id INT NOT NULL,
#     track_id VARCHAR(255) ,
#     duration FLOAT ,
#     direction VARCHAR(255),
#     FOREIGN KEY (video_id) REFERENCES videos (id)
# );