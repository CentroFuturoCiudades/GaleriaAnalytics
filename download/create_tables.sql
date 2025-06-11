-- Crear la tabla video_recorded
CREATE TABLE video_recorded (
    id VARCHAR(255) NOT NULL,
    camera VARCHAR(255),
    date_observed TIMESTAMP NOT NULL,
    path VARCHAR(255) NOT NULL,
    CONSTRAINT video_recorded_pkey PRIMARY KEY (id)
);

-- Crear la tabla tracks
CREATE TABLE tracks (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    track_id VARCHAR(255),
    duration DOUBLE PRECISION,
    direction VARCHAR(255),
    CONSTRAINT tracks_video_id_fkey FOREIGN KEY (video_id)
        REFERENCES video_recorded(id)
);
