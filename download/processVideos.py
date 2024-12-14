import json
import logging
import os
import cv2
import numpy as np
from ultralytics import YOLO
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from typing import List
import multiprocessing

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
_LOGGER = logging.getLogger('video_processor')

# Initialize the YOLO model
model = YOLO('best.pt')

# Configurations
Base = declarative_base()
videos_galeria_path = 'videosGaleria'

dbname = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')

# Database setup (PostgreSQL)
DB_CONFIG = {
    'dbname': dbname,
    'user': user,
    'password': password,
    'host': host,
    'port': port
}
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

class VideoRecorded(Base):
    __tablename__ = 'video_recorded'

    id = Column(String(255), primary_key=True)
    path = Column(String(255), nullable=False)
    date_observed = Column(String(255), nullable=False)

class Track(Base):
    __tablename__ = 'tracks'
    
    id = Column(Integer, primary_key=True)
    video_id = Column(String(255), ForeignKey("video_recorded.id"), nullable=False)
    track_id = Column(String)
    duration = Column(Float)
    direction = Column(String)

Base.metadata.create_all(engine)

def calculate_angle(vector):
    """Calculate the angle of a vector in degrees."""
    return np.degrees(np.arctan2(vector[1], vector[0]))

def classify_direction(angle, threshold):
    """Classify the direction based on the determined threshold."""
    return "forward" if angle <= threshold else "backward"

def delete_video_file(video_path):
    """Delete the video file from the file system."""
    try:
        os.remove(video_path)
        _LOGGER.info(f"Deleted video file: {video_path}")
    except FileNotFoundError:
        _LOGGER.warning(f"Video file not found: {video_path}")
    except OSError as e:
        _LOGGER.error(f"Error deleting video file {video_path}: {e}")

def process_video(video_path, video_id) -> List[dict]:
    """Process a single video file."""
    _LOGGER.info(f"Processing video: {video_path} (ID: {video_id})")
    input_video = cv2.VideoCapture(video_path)
    if not input_video.isOpened():
        _LOGGER.error(f"Could not open video: {video_path}")
        return []

    fps = int(input_video.get(cv2.CAP_PROP_FPS))
    if fps <= 0:  # Validate FPS
        _LOGGER.error(f"Invalid FPS ({fps}) for video: {video_path}")
        input_video.release()
        return []

    results = model.track(video_path, classes=0, show=False, conf=0.55, iou=0.6, tracker='botsort.yaml', stream=True)

    previous_positions = {}
    video_tracking_data = []

    for frame_idx, result in enumerate(results):
        timestamp = (frame_idx / fps) * 1000  # Frame timestamp in milliseconds

        if frame_idx % (fps // 5) == 0:  # Process every 5th frame
            if result.boxes.is_track:
                track_ids = result.boxes.id.int().cpu().tolist()

                for j, track_id in enumerate(track_ids):
                    xyxy_ = [int(x) for x in result.boxes.xyxy[j]]
                    center_x = (xyxy_[0] + xyxy_[2]) // 2
                    center_y = (xyxy_[1] + xyxy_[3]) // 2
                    current_position = (center_x, center_y)

                    unique_track_id = f"{track_id}_{os.path.basename(video_path).split('.')[0]}"
                    angle = None

                    if unique_track_id in previous_positions:
                        prev_position = previous_positions[unique_track_id]
                        dx = current_position[0] - prev_position[0]
                        dy = current_position[1] - prev_position[1]
                        displacement_vector = (dx, dy)
                        angle = calculate_angle(displacement_vector)

                    previous_positions[unique_track_id] = current_position

                    video_tracking_data.append({
                        'video_id': video_id,
                        'track_id': unique_track_id,
                        'duration': timestamp,
                        'direction': classify_direction(angle, 0) if angle is not None else 'unknown'
                    })

    input_video.release()
    return video_tracking_data

def load_results_to_db(results):
    """Save tracking results to the database."""
    session = Session()
    try:
        for result in results:
            track = Track(
                video_id=result['video_id'],  # Use `VARCHAR` ID
                track_id=result['track_id'],
                duration=result['duration'],
                direction=result['direction']
            )
            session.add(track)
        session.commit()
        _LOGGER.info(f"Inserted {len(results)} tracking results into the database.")
    except SQLAlchemyError as e:
        session.rollback()
        _LOGGER.error(f"Error while inserting into database: {e}")
    finally:
        session.close()

def split_list(lst, n):
    """Split the list into n partitions."""
    k, m = divmod(len(lst), n)
    return (lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def process_video_batch(video_files, video_ids):
    """Process a batch of videos."""
    local_video_data = []
    processed_files = []
    for video_path, video_id in zip(video_files, video_ids):
        try:
            data = process_video(video_path, video_id)
            if data:
                local_video_data.extend(data)
                _LOGGER.info(f"Attempting to delete video file: {video_path}")
                processed_files.append(video_path)

        except Exception as e:
            _LOGGER.error(f"Error processing video {video_path}: {e}")
    for video_path in processed_files:
        delete_video_file(video_path)
    return local_video_data

def main():
    num_processes = 3
    session = Session()

    try:
        query = text("SELECT id, path FROM video_recorded")
        videos = session.execute(query).fetchall()

        if not videos:
            _LOGGER.info("No videos found in the database.")
            return

        video_files = [os.path.join(videos_galeria_path, video.path) for video in videos]
        video_ids = [video.id for video in videos]

        file_partitions = list(split_list(video_files, num_processes))
        id_partitions = list(split_list(video_ids, num_processes))

        with multiprocessing.Pool(processes=num_processes) as pool:
            results = pool.starmap(process_video_batch, zip(file_partitions, id_partitions))

        all_video_data = [item for sublist in results for item in sublist]
        load_results_to_db(all_video_data)
        _LOGGER.info("Processing completed and data saved to the database.")
    except Exception as e:
        _LOGGER.error(f"Error during processing: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    main()





# -- Crear la tabla video_recorded
# CREATE TABLE public.video_recorded (
#     id character varying(255) NOT NULL,
#     camera character varying(255),
#     date_observed timestamp without time zone NOT NULL,
#     path character varying(255) NOT NULL,
#     CONSTRAINT video_recorded_pkey PRIMARY KEY (id)
# );

# -- Crear la tabla tracks
# CREATE TABLE public.tracks (
#     id integer NOT NULL DEFAULT nextval('tracks_id_seq'::regclass),
#     video_id character varying(255) NOT NULL,
#     track_id character varying(255),
#     duration double precision,
#     direction character varying(255),
#     CONSTRAINT tracks_pkey PRIMARY KEY (id),
#     CONSTRAINT tracks_video_id_fkey FOREIGN KEY (video_id)
#         REFERENCES public.video_recorded(id)
# );
