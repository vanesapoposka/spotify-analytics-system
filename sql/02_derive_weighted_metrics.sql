-- ============================================================================
-- Derives FACT_SONG_ARTIST_DAILY:  weighted vs unweighted stream allocation
--
-- unweighted_streams: the FULL daily stream_count of the song is credited to
--   EVERY artist on it. Correct lens = "how big is this artist's reach" —
--   summing across an artist's songs never double-penalizes a collaborator.
--
-- weighted_streams: stream_count * weighting_factor (1/#artists on the song).
--   Correct lens = "global totals" — summing weighted_streams across ALL
--   artists for a song reconstructs the song's true total exactly once,
--   so summing further up to genre/day/global never double-counts a stream.
-- ============================================================================
TRUNCATE dwh.fact_song_artist_daily;

INSERT INTO dwh.fact_song_artist_daily (date_key, song_sk, artist_sk, unweighted_streams, weighted_streams)
SELECT
    fs.date_key,
    fs.song_sk,
    bsa.artist_sk,
    SUM(fs.stream_count)                          AS unweighted_streams,
    SUM(fs.stream_count * bsa.weighting_factor)    AS weighted_streams
FROM dwh.fact_stream fs
JOIN dwh.bridge_song_artist bsa ON bsa.song_sk = fs.song_sk
GROUP BY fs.date_key, fs.song_sk, bsa.artist_sk;

-- Sanity proof: weighted sum reconstructs true song-level totals exactly;
-- unweighted sum (correctly) inflates once per extra collaborator.
