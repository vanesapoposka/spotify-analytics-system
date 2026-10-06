-- Medallion staging schemas. This project performs full rebuilds, so rerunning
-- this file intentionally replaces the prior Bronze, Silver, and Gold layers.
DROP SCHEMA IF EXISTS gold CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS bronze CASCADE;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

-- BRONZE: preserve each source CSV record as received, with source lineage.
CREATE TABLE bronze.spotify_songs_raw (
    raw_row_id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(40) NOT NULL,
    source_file VARCHAR(200) NOT NULL,
    source_row_number BIGINT NOT NULL,
    raw_record JSONB NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (batch_id, source_row_number)
);

CREATE TABLE bronze.listening_stats_raw (
    raw_row_id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(40) NOT NULL,
    source_file VARCHAR(200) NOT NULL,
    source_row_number BIGINT NOT NULL,
    raw_record JSONB NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (batch_id, source_row_number)
);

-- SILVER: typed, deduplicated, validated records used by the Gold warehouse.
CREATE TABLE silver.spotify_songs (
    source_row_number BIGINT NOT NULL,
    source_batch_id VARCHAR(40) NOT NULL,
    track_id VARCHAR(64) PRIMARY KEY,
    track_name VARCHAR(400) NOT NULL,
    track_artist TEXT NOT NULL,
    track_popularity SMALLINT,
    track_album_id VARCHAR(64) NOT NULL,
    track_album_name VARCHAR(400),
    track_album_release_date DATE,
    playlist_name TEXT,
    playlist_id VARCHAR(64),
    playlist_genre VARCHAR(100) NOT NULL,
    playlist_subgenre VARCHAR(100),
    danceability NUMERIC(6,4),
    energy NUMERIC(6,4),
    key SMALLINT,
    loudness NUMERIC(7,3),
    mode SMALLINT,
    speechiness NUMERIC(6,4),
    acousticness NUMERIC(6,4),
    instrumentalness NUMERIC(7,5),
    liveness NUMERIC(6,4),
    valence NUMERIC(6,4),
    tempo NUMERIC(7,2),
    duration_ms INTEGER,
    cleaned_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE silver.listening_stats (
    source_row_number BIGINT PRIMARY KEY,
    source_batch_id VARCHAR(40) NOT NULL,
    listening_date DATE NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    country VARCHAR(2) NOT NULL,
    age_bracket VARCHAR(10) NOT NULL,
    subscription_tier VARCHAR(20) NOT NULL,
    user_segment VARCHAR(30) NOT NULL,
    track_id VARCHAR(64) NOT NULL,
    stream_count INTEGER NOT NULL CHECK (stream_count >= 0),
    ms_played BIGINT NOT NULL CHECK (ms_played >= 0),
    skipped_count INTEGER NOT NULL CHECK (skipped_count >= 0),
    completed_count INTEGER NOT NULL CHECK (completed_count >= 0),
    cleaned_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (listening_date, user_id, track_id)
);
CREATE INDEX idx_silver_listening_track ON silver.listening_stats(track_id);
CREATE INDEX idx_silver_listening_user ON silver.listening_stats(user_id);

CREATE TABLE silver.dq_rejects (
    reject_id BIGSERIAL PRIMARY KEY,
    source_name VARCHAR(200) NOT NULL,
    source_batch_id VARCHAR(40) NOT NULL,
    source_row_number BIGINT NOT NULL,
    natural_key VARCHAR(200),
    rule_violated VARCHAR(200) NOT NULL,
    raw_record JSONB NOT NULL,
    rejected_at TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_silver_reject_source ON silver.dq_rejects(source_name, source_row_number);
