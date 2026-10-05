"""Personalized hybrid recommendations for a warehouse user.

The model combines a standardized audio-feature profile with collaborative
signals from users who listened to many of the same tracks.
"""
from functools import lru_cache

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text

from etl.database_configuration import get_engine


ENGINE = get_engine()
FEATURES = [
    "danceability", "energy", "loudness", "speechiness", "acousticness",
    "instrumentalness", "liveness", "valence", "tempo",
]


@lru_cache(maxsize=1)
def _catalog_model():
    """Load and scale the current catalog once per dashboard-server process."""
    query = text("""
        SELECT s.song_sk, s.track_name, s.danceability, s.energy, s.loudness,
               s.speechiness, s.acousticness, s.instrumentalness, s.liveness,
               s.valence, s.tempo, a.album_name,
               COALESCE(g.playlist_genre, 'Unknown') AS genre,
               COALESCE((
                   SELECT array_agg(DISTINCT ar.artist_name ORDER BY ar.artist_name)
                   FROM dwh.bridge_song_artist b
                   JOIN dwh.dim_artist ar ON ar.artist_sk = b.artist_sk
                   WHERE b.song_sk = s.song_sk AND ar.is_current
               ), ARRAY[]::varchar[]) AS artists
        FROM dwh.dim_song s
        LEFT JOIN dwh.dim_genre g ON g.genre_sk = (
            SELECT fs.genre_sk FROM dwh.fact_stream fs
            WHERE fs.song_sk = s.song_sk ORDER BY fs.stream_count DESC LIMIT 1
        )
        LEFT JOIN dwh.dim_album a ON a.album_sk = (
            SELECT fs.album_sk FROM dwh.fact_stream fs
            WHERE fs.song_sk = s.song_sk GROUP BY fs.album_sk ORDER BY SUM(fs.stream_count) DESC LIMIT 1
        )
        WHERE s.is_current
    """)
    with ENGINE.begin() as connection:
        songs = pd.read_sql(query, connection)
    if songs.empty:
        raise RuntimeError("No current songs were found in the warehouse.")
    songs[FEATURES] = songs[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(0)
    songs["artists"] = songs["artists"].apply(lambda value: list(value or []))
    scaler = StandardScaler()
    matrix = scaler.fit_transform(songs[FEATURES])
    return songs, matrix, {int(song_sk): index for index, song_sk in enumerate(songs.song_sk)}


def _collaborative_scores(user_sk: int) -> dict[int, float]:
    """Rank unseen tracks by listening overlap with the most similar users."""
    query = text("""
        WITH history AS (
            SELECT song_sk, SUM(stream_count)::float AS plays
            FROM dwh.fact_stream
            WHERE user_sk = :user_sk
            GROUP BY song_sk
            ORDER BY plays DESC
            LIMIT 30
        ), neighbors AS (
        SELECT other.user_sk, SUM(h.plays) AS overlap
            FROM history h
            JOIN (
                SELECT user_sk, song_sk, SUM(stream_count) AS plays
                FROM dwh.fact_stream GROUP BY user_sk, song_sk
            ) other ON other.song_sk = h.song_sk
            WHERE other.user_sk <> :user_sk
            GROUP BY other.user_sk
            ORDER BY overlap DESC
            LIMIT 100
        ), neighbor_items AS (
            SELECT fs.user_sk, fs.song_sk, SUM(fs.stream_count)::float AS plays
            FROM dwh.fact_stream fs
            JOIN neighbors n ON n.user_sk = fs.user_sk
            GROUP BY fs.user_sk, fs.song_sk
        )
        SELECT ni.song_sk,
               SUM(n.overlap * LN(1 + ni.plays))::float AS score
        FROM neighbor_items ni
        JOIN neighbors n ON n.user_sk = ni.user_sk
        WHERE ni.song_sk NOT IN (SELECT song_sk FROM history)
        GROUP BY ni.song_sk
        ORDER BY score DESC
        LIMIT 2500
    """)
    with ENGINE.begin() as connection:
        rows = pd.read_sql(query, connection, params={"user_sk": user_sk})
    return {int(row.song_sk): float(row.score) for row in rows.itertuples(index=False)}


def get_recommendations(user_sk: int, limit: int = 10) -> dict:
    songs, matrix, index_by_song = _catalog_model()
    history_query = text("""
        SELECT s.song_sk, SUM(fs.stream_count)::float AS plays
        FROM dwh.fact_stream fs
        JOIN dwh.dim_song s ON s.song_sk = fs.song_sk AND s.is_current
        WHERE fs.user_sk = :user_sk
        GROUP BY s.song_sk
        ORDER BY plays DESC
        LIMIT 100
    """)
    with ENGINE.begin() as connection:
        history = pd.read_sql(history_query, connection, params={"user_sk": user_sk})
    heard = set(history.song_sk.astype(int))

    if history.empty:
        # Cold-start fallback: use the most popular tracks, still scored against
        # the population's learned audio-feature center.
        profile = matrix.mean(axis=0, keepdims=True)
        with ENGINE.begin() as connection:
            popular = pd.read_sql(text("""
                SELECT song_sk, SUM(stream_count)::float AS plays
                FROM dwh.fact_stream GROUP BY song_sk
                ORDER BY plays DESC LIMIT 2500
            """), connection)
        collaborative = {int(row.song_sk): float(row.plays) for row in popular.itertuples(index=False)}
    else:
        usable_history = history[history.song_sk.astype(int).isin(index_by_song)].copy()
        positions = [index_by_song[int(song_sk)] for song_sk in usable_history.song_sk]
        weights = usable_history["plays"].to_numpy(dtype=float)
        if not positions:
            profile = matrix.mean(axis=0, keepdims=True)
        else:
            weights = np.maximum(weights, 1)
            profile = np.average(matrix[positions], axis=0, weights=weights).reshape(1, -1)
        collaborative = _collaborative_scores(user_sk)

    if not collaborative:
        with ENGINE.begin() as connection:
            popular = pd.read_sql(text("""
                SELECT song_sk, SUM(stream_count)::float AS plays
                FROM dwh.fact_stream GROUP BY song_sk
                ORDER BY plays DESC LIMIT 2500
            """), connection)
        collaborative = {int(row.song_sk): float(row.plays) for row in popular.itertuples(index=False)}

    candidate_ids = [song_sk for song_sk in collaborative if song_sk in index_by_song and song_sk not in heard]
    if not candidate_ids:
        candidate_ids = [int(sk) for sk in songs.song_sk if int(sk) not in heard]
        collaborative = {song_sk: 0.0 for song_sk in candidate_ids}
    indexes = np.array([index_by_song[song_sk] for song_sk in candidate_ids], dtype=int)
    content = cosine_similarity(profile, matrix[indexes]).ravel()
    collab = np.array([collaborative.get(song_sk, 0.0) for song_sk in candidate_ids], dtype=float)
    content = (content + 1.0) / 2.0
    collab = np.log1p(np.maximum(collab, 0))
    if collab.max(initial=0) > 0:
        collab /= collab.max()
    combined = 0.55 * content + 0.45 * collab
    top_indices = np.argsort(-combined)[:max(limit, 30)]

    recommendations = []
    for i in top_indices:
        row = songs.iloc[indexes[i]]
        recommendations.append({
            "song_sk": int(row.song_sk), "track_name": str(row.track_name),
            "album_name": str(row.album_name or "Unknown album"),
            "genre": str(row.genre or "Unknown"), "artists": row.artists,
            "score": round(float(combined[i]), 4),
        })

    albums, artists = {}, {}
    for rank, item in enumerate(recommendations, 1):
        contribution = item["score"] / rank
        album_name = item["album_name"]
        albums[album_name] = albums.get(album_name, 0) + contribution
        for artist in item["artists"]:
            artists[artist] = artists.get(artist, 0) + contribution

    return {
        "songs": recommendations[:limit],
        "albums": [{"name": name, "score": round(score, 4)} for name, score in
                   sorted(albums.items(), key=lambda entry: entry[1], reverse=True)[:5]],
        "artists": [{"name": name, "score": round(score, 4)} for name, score in
                    sorted(artists.items(), key=lambda entry: entry[1], reverse=True)[:5]],
        "model": "hybrid collaborative filtering + audio-feature similarity",
    }
