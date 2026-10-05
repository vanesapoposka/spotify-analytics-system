DROP SCHEMA IF EXISTS dwh CASCADE;
CREATE SCHEMA dwh;
SET search_path TO dwh;

CREATE TABLE dim_date (
    date_key        INT PRIMARY KEY,           -- YYYYMMDD
    full_date       DATE NOT NULL UNIQUE,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    day_of_month    SMALLINT,
    week_of_year    SMALLINT,
    month_num       SMALLINT,
    month_name      VARCHAR(10),
    quarter         SMALLINT,
    year            SMALLINT,
    is_weekend      BOOLEAN
);

CREATE TABLE dim_artist (
    artist_sk           BIGSERIAL PRIMARY KEY,     -- surrogate key (versioned)
    artist_bk            VARCHAR(200) NOT NULL,     -- business/natural key (artist name, normalized)
    artist_name          VARCHAR(300) NOT NULL,
    primary_genre        VARCHAR(100),
    popularity_tier       VARCHAR(20),               -- e.g. 'Emerging','Mid','Mainstream','Superstar' (changes over time -> SCD2)
    followers_bucket     VARCHAR(20),
    -- SCD2 audit columns
    eff_start_date       DATE NOT NULL,
    eff_end_date          DATE,                       -- NULL = current
    is_current            BOOLEAN NOT NULL DEFAULT TRUE,
    version_no            INT NOT NULL DEFAULT 1,
    row_hash               CHAR(64),                   -- hash of tracked attributes, used to detect change
    load_ts                TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_dim_artist_bk_current ON dim_artist(artist_bk, is_current);

CREATE TABLE dim_song (
    song_sk               BIGSERIAL PRIMARY KEY,
    song_bk                VARCHAR(64) NOT NULL,        -- track_id (Spotify ID) = durable business key
    track_name             VARCHAR(400) NOT NULL,
    duration_ms            INT,
    explicit                BOOLEAN,
    mood_label              VARCHAR(20),                  -- derived (ML classification) e.g. Happy/Sad/Energetic/Calm
    danceability            NUMERIC(5,4),
    energy                  NUMERIC(5,4),
    key_signature           SMALLINT,
    loudness                NUMERIC(6,3),
    mode                    SMALLINT,
    speechiness              NUMERIC(5,4),
    acousticness             NUMERIC(5,4),
    instrumentalness         NUMERIC(6,5),
    liveness                 NUMERIC(5,4),
    valence                  NUMERIC(5,4),
    tempo                    NUMERIC(6,2),
    -- SCD2 audit columns
    eff_start_date           DATE NOT NULL,
    eff_end_date              DATE,
    is_current                BOOLEAN NOT NULL DEFAULT TRUE,
    version_no                INT NOT NULL DEFAULT 1,
    row_hash                   CHAR(64),
    load_ts                     TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_dim_song_bk_current ON dim_song(song_bk, is_current);

CREATE TABLE dim_album (
    album_sk        BIGSERIAL PRIMARY KEY,
    album_bk         VARCHAR(64) NOT NULL UNIQUE,   -- track_album_id
    album_name       VARCHAR(400),
    release_date     DATE,
    load_ts           TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE dim_genre (
    genre_sk        BIGSERIAL PRIMARY KEY,
    genre_bk         VARCHAR(150) NOT NULL UNIQUE,  -- genre|subgenre concat
    playlist_genre   VARCHAR(100),
    playlist_subgenre VARCHAR(100)
);

CREATE TABLE dim_user (
    user_sk          BIGSERIAL PRIMARY KEY,
    user_bk           VARCHAR(64) NOT NULL,          -- listener ID from listening-statistics source
    signup_date        DATE,
    country             VARCHAR(2),
    age_bracket          VARCHAR(10),
    subscription_tier    VARCHAR(20),                 -- Free / Premium / Family / Student (can change -> SCD2)
    user_segment          VARCHAR(30),                  -- e.g. 'Casual','Power Listener' (derived, changes -> SCD2)
    eff_start_date        DATE NOT NULL,
    eff_end_date            DATE,
    is_current               BOOLEAN NOT NULL DEFAULT TRUE,
    version_no                INT NOT NULL DEFAULT 1,
    row_hash                    CHAR(64),
    load_ts                      TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_dim_user_bk_current ON dim_user(user_bk, is_current);

CREATE TABLE bridge_song_artist (
    song_sk       BIGINT NOT NULL REFERENCES dim_song(song_sk),
    artist_sk     BIGINT NOT NULL REFERENCES dim_artist(artist_sk),
    artist_seq     SMALLINT NOT NULL,           -- credit order (1 = primary/lead)
    weighting_factor NUMERIC(6,5) NOT NULL,     -- e.g. 0.5 for a 2-artist collab
    PRIMARY KEY (song_sk, artist_sk)
);

CREATE TABLE fact_stream (
    stream_fact_sk   BIGSERIAL PRIMARY KEY,
    date_key           INT NOT NULL REFERENCES dim_date(date_key),
    user_sk             BIGINT NOT NULL REFERENCES dim_user(user_sk),
    song_sk             BIGINT NOT NULL REFERENCES dim_song(song_sk),
    album_sk             BIGINT NOT NULL REFERENCES dim_album(album_sk),
    genre_sk              BIGINT NOT NULL REFERENCES dim_genre(genre_sk),
    stream_count           INT NOT NULL,           -- # plays that user/song/day
    ms_played               BIGINT NOT NULL,        -- total ms listened
    skipped_count            INT NOT NULL DEFAULT 0,
    completed_count           INT NOT NULL DEFAULT 0,
    load_ts                    TIMESTAMP NOT NULL DEFAULT now(),
    batch_id                    VARCHAR(40) NOT NULL   -- ETL run / incremental batch tag
);
CREATE INDEX idx_fact_stream_date ON fact_stream(date_key);
CREATE INDEX idx_fact_stream_song ON fact_stream(song_sk);
CREATE INDEX idx_fact_stream_user ON fact_stream(user_sk);
CREATE INDEX idx_fact_stream_genre ON fact_stream(genre_sk);

CREATE TABLE fact_song_artist_daily (
    date_key             INT NOT NULL REFERENCES dim_date(date_key),
    song_sk               BIGINT NOT NULL REFERENCES dim_song(song_sk),
    artist_sk             BIGINT NOT NULL REFERENCES dim_artist(artist_sk),
    unweighted_streams     BIGINT NOT NULL,   -- = total streams of the song (no split)
    weighted_streams       NUMERIC(14,4) NOT NULL,  -- = total streams * weighting_factor
    PRIMARY KEY (date_key, song_sk, artist_sk)
);
CREATE INDEX idx_fsad_date ON fact_song_artist_daily(date_key);
CREATE INDEX idx_fsad_artist ON fact_song_artist_daily(artist_sk);

CREATE TABLE etl_batch_log (
    batch_id        VARCHAR(40) PRIMARY KEY,
    source_name       VARCHAR(100),
    started_at         TIMESTAMP,
    finished_at         TIMESTAMP,
    rows_read             INT,
    rows_rejected           INT,
    rows_loaded               INT,
    status                      VARCHAR(20)
);

CREATE TABLE dq_reject_log (
    reject_id     BIGSERIAL PRIMARY KEY,
    batch_id        VARCHAR(40) REFERENCES etl_batch_log(batch_id),
    source_table      VARCHAR(100),
    natural_key         VARCHAR(200),
    rule_violated         VARCHAR(200),
    raw_row                 JSONB,
    rejected_at               TIMESTAMP DEFAULT now()
);
