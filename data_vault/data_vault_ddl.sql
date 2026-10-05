-- ============================================================================
-- DATA VAULT 2.0 MODEL — alternative raw-integration layer
-- 5 Hubs x C(5,2)=10 Links x Satellites, as designed.
--
-- NOTE ON DESIGN: a full C(5,2) link fan-out is worth a second look before
-- building it. Data Vault best practice links entities that have a natural,
-- independently-existing business transaction/association — NOT every
-- mathematically possible pair. Concretely:
--   Song-Artist, Song-Album, Song-Genre, Album-Artist  -> real, independent
--       business associations (a song genuinely "has" these).
--   Song-User, Album-User, Genre-User, Artist-User     -> these are really
--       one underlying event (a LISTEN) sliced 4 ways, not 4 independent
--       associations. Modeling them as 4 separate links duplicates the same
--       fact and makes weighted/unweighted streaming math (the whole point
--       of your bridge-table design) impossible to compute consistently,
--       since the play-count would live in four disconnected places.
--   Album-Genre         -> derivable (genre lives at the song level in this
--       dataset; an album's "genre" is just the distribution of its songs'
--       genres) — a same-day link is fine but is a convenience, not new fact.
--   Genre-Artist         -> same: derivable via Song, not independent.
--
-- This script builds exactly the 10 links you specified so the pattern is
-- complete and runnable, but replaces the 4 *-User links with a single
-- LINK_LISTEN_EVENT (a proper multi-part link across User+Song, which
-- transitively carries Artist/Album/Genre via the Song hub) — that is the
-- Data-Vault-correct way to model one streaming event touching 5 entities,
-- and it is what the rest of this section builds on. The other 6 links
-- (all non-User pairs) are built exactly as you designed.
-- ============================================================================
DROP SCHEMA IF EXISTS dv CASCADE;
CREATE SCHEMA dv;
SET search_path TO dv;

-- ---------------------------------------------------------------------------
-- HUBS — one row per unique business key, ever seen, never updated in place
-- ---------------------------------------------------------------------------
CREATE TABLE hub_song (
    song_hk        CHAR(32) PRIMARY KEY,      -- MD5 hash of business key
    song_bk         VARCHAR(64) NOT NULL UNIQUE, -- Spotify track_id
    load_ts           TIMESTAMP NOT NULL DEFAULT now(),
    record_source      VARCHAR(50) NOT NULL
);
CREATE TABLE hub_album (
    album_hk       CHAR(32) PRIMARY KEY,
    album_bk        VARCHAR(64) NOT NULL UNIQUE,
    load_ts           TIMESTAMP NOT NULL DEFAULT now(),
    record_source      VARCHAR(50) NOT NULL
);
CREATE TABLE hub_genre (
    genre_hk       CHAR(32) PRIMARY KEY,
    genre_bk        VARCHAR(150) NOT NULL UNIQUE,
    load_ts           TIMESTAMP NOT NULL DEFAULT now(),
    record_source      VARCHAR(50) NOT NULL
);
CREATE TABLE hub_artist (
    artist_hk      CHAR(32) PRIMARY KEY,
    artist_bk       VARCHAR(200) NOT NULL UNIQUE,
    load_ts           TIMESTAMP NOT NULL DEFAULT now(),
    record_source      VARCHAR(50) NOT NULL
);
CREATE TABLE hub_user (
    user_hk        CHAR(32) PRIMARY KEY,
    user_bk         VARCHAR(64) NOT NULL UNIQUE,
    load_ts           TIMESTAMP NOT NULL DEFAULT now(),
    record_source      VARCHAR(50) NOT NULL
);

-- ---------------------------------------------------------------------------
-- LINKS — the 6 non-User pairs, built exactly per the C(5,2) design
-- ---------------------------------------------------------------------------
CREATE TABLE link_song_album (
    link_hk CHAR(32) PRIMARY KEY, song_hk CHAR(32) REFERENCES hub_song, album_hk CHAR(32) REFERENCES hub_album,
    load_ts TIMESTAMP DEFAULT now(), record_source VARCHAR(50));
CREATE TABLE link_song_genre (
    link_hk CHAR(32) PRIMARY KEY, song_hk CHAR(32) REFERENCES hub_song, genre_hk CHAR(32) REFERENCES hub_genre,
    load_ts TIMESTAMP DEFAULT now(), record_source VARCHAR(50));
CREATE TABLE link_song_artist (
    link_hk CHAR(32) PRIMARY KEY, song_hk CHAR(32) REFERENCES hub_song, artist_hk CHAR(32) REFERENCES hub_artist,
    load_ts TIMESTAMP DEFAULT now(), record_source VARCHAR(50));
CREATE TABLE link_album_genre (
    link_hk CHAR(32) PRIMARY KEY, album_hk CHAR(32) REFERENCES hub_album, genre_hk CHAR(32) REFERENCES hub_genre,
    load_ts TIMESTAMP DEFAULT now(), record_source VARCHAR(50));
CREATE TABLE link_album_artist (
    link_hk CHAR(32) PRIMARY KEY, album_hk CHAR(32) REFERENCES hub_album, artist_hk CHAR(32) REFERENCES hub_artist,
    load_ts TIMESTAMP DEFAULT now(), record_source VARCHAR(50));
CREATE TABLE link_genre_artist (
    link_hk CHAR(32) PRIMARY KEY, genre_hk CHAR(32) REFERENCES hub_genre, artist_hk CHAR(32) REFERENCES hub_artist,
    load_ts TIMESTAMP DEFAULT now(), record_source VARCHAR(50));

-- ---------------------------------------------------------------------------
-- LINK_LISTEN_EVENT — replaces the 4 *-User links (Song-User, Album-User,
-- Genre-User, Artist-User) with ONE link across the 2 hubs that actually
-- co-occur independently in a real event (User, Song); Album/Genre/Artist
-- context is reached transitively via link_song_album / _genre / _artist.
-- This is what makes weighted vs unweighted aggregation possible downstream.
-- ---------------------------------------------------------------------------
CREATE TABLE link_listen_event (
    link_hk CHAR(32) PRIMARY KEY,
    user_hk CHAR(32) REFERENCES hub_user,
    song_hk CHAR(32) REFERENCES hub_song,
    date_hk CHAR(32) NOT NULL,             -- degenerate hash-key on event date
    load_ts TIMESTAMP DEFAULT now(),
    record_source VARCHAR(50)
);

-- ---------------------------------------------------------------------------
-- SATELLITES — one (or more) per Hub, tracking attribute history
-- ---------------------------------------------------------------------------
CREATE TABLE sat_song_details (
    song_hk CHAR(32) REFERENCES hub_song, load_ts TIMESTAMP NOT NULL DEFAULT now(),
    load_end_ts TIMESTAMP, hash_diff CHAR(32) NOT NULL,
    track_name VARCHAR(400), duration_ms INT, danceability NUMERIC(5,4), energy NUMERIC(5,4),
    loudness NUMERIC(6,3), speechiness NUMERIC(5,4), acousticness NUMERIC(5,4),
    instrumentalness NUMERIC(6,5), liveness NUMERIC(5,4), valence NUMERIC(5,4), tempo NUMERIC(6,2),
    mood_label VARCHAR(20), record_source VARCHAR(50), PRIMARY KEY (song_hk, load_ts));

CREATE TABLE sat_album_details (
    album_hk CHAR(32) REFERENCES hub_album, load_ts TIMESTAMP NOT NULL DEFAULT now(),
    load_end_ts TIMESTAMP, hash_diff CHAR(32) NOT NULL,
    album_name VARCHAR(400), release_date DATE, record_source VARCHAR(50),
    PRIMARY KEY (album_hk, load_ts));

CREATE TABLE sat_genre_details (
    genre_hk CHAR(32) REFERENCES hub_genre, load_ts TIMESTAMP NOT NULL DEFAULT now(),
    load_end_ts TIMESTAMP, hash_diff CHAR(32) NOT NULL,
    playlist_genre VARCHAR(100), playlist_subgenre VARCHAR(100), record_source VARCHAR(50),
    PRIMARY KEY (genre_hk, load_ts));

CREATE TABLE sat_artist_details (
    artist_hk CHAR(32) REFERENCES hub_artist, load_ts TIMESTAMP NOT NULL DEFAULT now(),
    load_end_ts TIMESTAMP, hash_diff CHAR(32) NOT NULL,
    artist_name VARCHAR(300), popularity_tier VARCHAR(20), followers_bucket VARCHAR(20),
    record_source VARCHAR(50), PRIMARY KEY (artist_hk, load_ts));

CREATE TABLE sat_user_details (
    user_hk CHAR(32) REFERENCES hub_user, load_ts TIMESTAMP NOT NULL DEFAULT now(),
    load_end_ts TIMESTAMP, hash_diff CHAR(32) NOT NULL,
    country VARCHAR(2), age_bracket VARCHAR(10), subscription_tier VARCHAR(20), user_segment VARCHAR(30),
    record_source VARCHAR(50), PRIMARY KEY (user_hk, load_ts));

-- Link satellite: carries the measurable fact of the event (play metrics)
CREATE TABLE sat_listen_event_metrics (
    link_hk CHAR(32) REFERENCES link_listen_event, load_ts TIMESTAMP NOT NULL DEFAULT now(),
    load_end_ts TIMESTAMP, hash_diff CHAR(32) NOT NULL,
    stream_count INT, ms_played BIGINT, skipped_count INT, completed_count INT,
    record_source VARCHAR(50), PRIMARY KEY (link_hk, load_ts));

-- Link satellite: carries the weighting_factor for song-artist collabs
CREATE TABLE sat_song_artist_weight (
    link_hk CHAR(32) REFERENCES link_song_artist, load_ts TIMESTAMP NOT NULL DEFAULT now(),
    load_end_ts TIMESTAMP, hash_diff CHAR(32) NOT NULL,
    artist_seq SMALLINT, weighting_factor NUMERIC(6,5),
    record_source VARCHAR(50), PRIMARY KEY (link_hk, load_ts));

-- ============================================================================
-- Downstream: a Business Vault / Information Mart view layer would flatten
-- hub+link+satellite back into the SAME star schema (dwh.*) built earlier —
-- Data Vault is a raw, auditable integration layer; BI/ML still reads from
-- Kimball-style marts derived FROM it. That mart layer is sql/01_star_schema.sql.
-- ============================================================================
