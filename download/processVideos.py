import os
import cv2
import numpy as np
import logging
import multiprocessing
from collections import defaultdict, Counter
from ultralytics import YOLO
from sqlalchemy import create_engine, Column, String, Float, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from dotenv import load_dotenv

# Configuracion de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
_LOGGER = logging.getLogger('video_processor')

# Cargar variables de entorno
load_dotenv('.env')

# Paths
videos_galeria_path = 'videosGaleria'

# Configuracion de base de datos
Base = declarative_base()
dbname = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Modelos de datos
class Track(Base):
    __tablename__ = 'tracks'
    track_id = Column(String, primary_key=True)
    video_id = Column(String, nullable=False)
    duration = Column(Float)
    direction = Column(String)

Base.metadata.create_all(engine)

# Utilidades

def calculate_angle(vector):
    return np.degrees(np.arctan2(vector[1], vector[0]))

def classify_direction(angle, threshold):
    return "forward" if angle <= threshold else "backward"

def delete_video_file(video_path):
    try:
        os.remove(video_path)
        _LOGGER.info(f"Deleted video file: {video_path}")
    except FileNotFoundError:
        _LOGGER.warning(f"Video file not found: {video_path}")
    except OSError as e:
        _LOGGER.error(f"Error deleting video file {video_path}: {e}")

def process_video(video_path, video_id):
    _LOGGER.info(f"Processing video: {video_path} (ID: {video_id})")
    try:
        model = YOLO('best.pt')
    except Exception as e:
        _LOGGER.error(f"Failed to load YOLO model: {e}")
        return []

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        _LOGGER.error(f"Could not open video: {video_path}")
        return []

    fps = int(cap.get(cv2.CAP_PROP_FPS))
    if fps <= 0:
        _LOGGER.error(f"Invalid FPS ({fps}) for video: {video_path}")
        cap.release()
        return []

    try:
        results = model.track(video_path, classes=0, show=False, conf=0.55, iou=0.6, tracker='botsort.yaml', stream=True)
    except Exception as e:
        _LOGGER.error(f"YOLO tracking failed for {video_path}: {e}")
        cap.release()
        return []

    previous_positions = {}
    track_history = defaultdict(list)

    for frame_idx, result in enumerate(results):
        timestamp = (frame_idx / fps) 

        if frame_idx % (fps // 5) == 0 and result.boxes.is_track:
            ids = result.boxes.id.int().cpu().tolist()
            for j, track_id in enumerate(ids):
                xyxy = [int(x) for x in result.boxes.xyxy[j]]
                cx = (xyxy[0] + xyxy[2]) // 2
                cy = (xyxy[1] + xyxy[3]) // 2
                position = (cx, cy)
                unique_id = f"{track_id}_{os.path.basename(video_path).split('.')[0]}"
                angle = None

                if unique_id in previous_positions:
                    dx = position[0] - previous_positions[unique_id][0]
                    dy = position[1] - previous_positions[unique_id][1]
                    angle = calculate_angle((dx, dy))

                previous_positions[unique_id] = position
                direction = classify_direction(angle, 0) if angle is not None else 'unknown'
                track_history[unique_id].append((timestamp, direction))

    cap.release()

    video_tracking_data = []
    for track_id, entries in track_history.items():
        last_time = max(t for t, _ in entries)
        common_dir = Counter(d for _, d in entries).most_common(1)[0][0]
        video_tracking_data.append({
            'track_id': track_id,
            'video_id': video_id,
            'duration': last_time,
            'direction': common_dir
        })

    return video_tracking_data

def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return (lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def process_video_batch(video_files, video_ids):
    session = Session()
    processed_files = []
    for video_path, video_id in zip(video_files, video_ids):
        try:
            data = process_video(video_path, video_id)
            if data:
                for entry in data:
                    session.add(Track(**entry))
                session.commit()
                _LOGGER.info(f"Inserted {len(data)} tracks for video {video_path}")
                processed_files.append(video_path)
            else:
                _LOGGER.info(f"No detections for video {video_path}")
        except IntegrityError:
            session.rollback()
            _LOGGER.warning(f"Duplicate entries for {video_path} skipped.")
        except SQLAlchemyError as e:
            session.rollback()
            _LOGGER.error(f"DB error for {video_path}: {e}")
        except Exception as e:
            _LOGGER.error(f"Error for {video_path}: {e}")
    for path in processed_files:
        delete_video_file(path)
    session.close()

def main():
    num_processes = 20
    session = Session()
    try:
        rows = session.execute(text("SELECT id, path FROM video_recorded")).fetchall()
        if not rows:
            _LOGGER.info("No videos found.")
            return
        video_files = [os.path.join(videos_galeria_path, row.path) for row in rows]
        video_ids = [row.id for row in rows]
        parts_files = list(split_list(video_files, num_processes))
        parts_ids = list(split_list(video_ids, num_processes))
        with multiprocessing.Pool(processes=num_processes) as pool:
            pool.starmap(process_video_batch, zip(parts_files, parts_ids))
        _LOGGER.info("✅ Processing completed.")
    except Exception as e:
        _LOGGER.error(f"❌ Error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    main()
