-- OLAP ANALYTICAL LAYER — rollup / drill-down / slice-dice / advanced KPIs

-- (A) ROLL-UP: total (correct, non-duplicated) streams by month -> quarter,
--     using GROUPING SETS to get several aggregation levels in one pass.
SELECT
    dd.year, dd.quarter,
    CASE WHEN GROUPING(dd.month_num) = 0 THEN dd.month_num END AS month_num,
    SUM(fs.stream_count) AS total_streams
FROM dwh.fact_stream fs
JOIN dwh.dim_date dd ON dd.date_key = fs.date_key
GROUP BY GROUPING SETS ((dd.year, dd.quarter, dd.month_num), (dd.year, dd.quarter), (dd.year))
ORDER BY dd.year, dd.quarter, month_num NULLS FIRST;

-- (B) DRILL-DOWN: from genre -> subgenre -> song, top tracks per subgenre
SELECT genre, subgenre, track_name, streams FROM (
    SELECT dg.playlist_genre AS genre, dg.playlist_subgenre AS subgenre,
           ds.track_name, SUM(fs.stream_count) AS streams,
           RANK() OVER (PARTITION BY dg.playlist_genre, dg.playlist_subgenre
                         ORDER BY SUM(fs.stream_count) DESC) AS rnk
    FROM dwh.fact_stream fs
    JOIN dwh.dim_genre dg ON dg.genre_sk = fs.genre_sk
    JOIN dwh.dim_song ds ON ds.song_sk = fs.song_sk
    GROUP BY dg.playlist_genre, dg.playlist_subgenre, ds.track_name
) t
WHERE rnk = 1
ORDER BY genre, subgenre;

-- (C) SLICE: single genre ("pop"), DICE: cross by weekend vs weekday x subscription tier
SELECT dg.playlist_subgenre, dd.is_weekend, du.subscription_tier,
       SUM(fs.stream_count) AS streams, SUM(fs.ms_played)/60000.0 AS minutes_played
FROM dwh.fact_stream fs
JOIN dwh.dim_genre dg ON dg.genre_sk = fs.genre_sk
JOIN dwh.dim_date dd ON dd.date_key = fs.date_key
JOIN dwh.dim_user du ON du.user_sk = fs.user_sk
WHERE dg.playlist_genre = 'pop'
GROUP BY dg.playlist_subgenre, dd.is_weekend, du.subscription_tier
ORDER BY dg.playlist_subgenre, dd.is_weekend, du.subscription_tier;

-- (D) ADVANCED KPI 1: correct Top-N Artists by GLOBAL reach — MUST use
--     weighted_streams to avoid double-counting collab streams at platform level
SELECT da.artist_name, ROUND(SUM(fsad.weighted_streams)) AS true_global_streams
FROM dwh.fact_song_artist_daily fsad
JOIN dwh.dim_artist da ON da.artist_sk = fsad.artist_sk AND da.is_current
GROUP BY da.artist_name
ORDER BY true_global_streams DESC
LIMIT 10;

-- (E) ADVANCED KPI 2: Top-N Artists by OWN POPULARITY/reach — use unweighted
--     (an artist's own promotional value includes the FULL reach of every
--     collab they're credited on, uncapped by how many co-artists it has)
SELECT da.artist_name, SUM(fsad.unweighted_streams) AS artist_total_exposure
FROM dwh.fact_song_artist_daily fsad
JOIN dwh.dim_artist da ON da.artist_sk = fsad.artist_sk AND da.is_current
GROUP BY da.artist_name
ORDER BY artist_total_exposure DESC
LIMIT 10;

-- (F) ADVANCED KPI 3: skip rate & completion rate by mood label (engagement KPI)
SELECT ds.mood_label,
       ROUND(100.0 * SUM(fs.skipped_count) / NULLIF(SUM(fs.stream_count + fs.skipped_count),0), 2) AS skip_rate_pct,
       ROUND(100.0 * SUM(fs.completed_count) / NULLIF(SUM(fs.stream_count + fs.skipped_count),0), 2) AS completion_rate_pct
FROM dwh.fact_stream fs
JOIN dwh.dim_song ds ON ds.song_sk = fs.song_sk
GROUP BY ds.mood_label
ORDER BY skip_rate_pct DESC;

-- (G) ADVANCED KPI 4: DAU/MAU stickiness ratio (user-engagement KPI)
WITH dau AS (
    SELECT date_key, COUNT(DISTINCT user_sk) AS d_active FROM dwh.fact_stream GROUP BY date_key
), mau AS (
    SELECT COUNT(DISTINCT user_sk) AS m_active FROM dwh.fact_stream
)
SELECT ROUND(AVG(d_active),0) AS avg_dau, (SELECT m_active FROM mau) AS mau,
       ROUND(100.0*AVG(d_active)/(SELECT m_active FROM mau),2) AS stickiness_pct
FROM dau;

-- (H) ADVANCED KPI 5: genre affinity by user segment (behavioral cross-tab)
SELECT user_segment, playlist_genre, streams, genre_rank FROM (
    SELECT du.user_segment, dg.playlist_genre, SUM(fs.stream_count) AS streams,
           RANK() OVER (PARTITION BY du.user_segment ORDER BY SUM(fs.stream_count) DESC) AS genre_rank
    FROM dwh.fact_stream fs
    JOIN dwh.dim_user du ON du.user_sk = fs.user_sk
    JOIN dwh.dim_genre dg ON dg.genre_sk = fs.genre_sk
    GROUP BY du.user_segment, dg.playlist_genre
) t
WHERE genre_rank <= 3
ORDER BY user_segment, genre_rank;
