"""Load listening statistics from Silver into the Gold warehouse.

etl/prepare_medallion.py validates the source CSV and materializes
silver.listening_stats. This loader maps its business keys to warehouse keys,
loads demo users, and appends Gold fact_stream rows.

Run after applying the schema and loading the Spotify catalog:
    python etl/load_listening_stats.py
"""
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db_config import get_engine

ENGINE = get_engine()
BATCH_ID = "LISTEN_STATS_V1"
REQUIRED_COLUMNS = {
    "listening_date", "user_id", "country", "age_bracket",
    "subscription_tier", "user_segment", "track_id", "stream_count",
    "ms_played", "skipped_count", "completed_count",
}


def load_listening_stats():
    with ENGINE.begin() as conn:
        stats = pd.read_sql(text("""
            SELECT listening_date, user_id, country, age_bracket, subscription_tier,
                   user_segment, track_id, stream_count, ms_played, skipped_count,
                   completed_count
            FROM silver.listening_stats
            ORDER BY source_row_number
        """), conn)
    missing = REQUIRED_COLUMNS - set(stats.columns)
    if missing:
        raise ValueError(f"Listening dataset is missing columns: {sorted(missing)}")
    if stats.empty:
        raise ValueError("Silver listening data is empty. Run etl/prepare_medallion.py first.")
    stats["listening_date"] = pd.to_datetime(stats["listening_date"], errors="raise")

    # The checked-in dataset is a fixed snapshot. The stable batch key makes
    # repeated manual/Airflow runs idempotent instead of duplicating facts.
    with ENGINE.begin() as conn:
        already_loaded = conn.execute(text(
            "SELECT 1 FROM dwh.etl_batch_log WHERE batch_id = :batch AND status = 'SUCCESS'"
        ), {"batch": BATCH_ID}).first()
    if already_loaded:
        print(f"[fact_stream] Silver listening snapshot is already loaded (batch {BATCH_ID}); skipping")
        return

    for col in ("stream_count", "ms_played", "skipped_count", "completed_count"):
        stats[col] = pd.to_numeric(stats[col], errors="raise").astype("int64")
        if (stats[col] < 0).any():
            raise ValueError(f"Listening dataset column {col} cannot contain negatives")

    # One current dimension row per user business key. Dataset profiles are
    # stable, so rerunning this loader against an already-loaded warehouse is safe.
    user_cols = ["user_id", "country", "age_bracket", "subscription_tier", "user_segment"]
    profile_counts = stats.groupby("user_id", dropna=False)[user_cols[1:]].nunique(dropna=False)
    if (profile_counts > 1).any().any():
        raise ValueError("Each user_id must map to one profile in the CSV")
    user_profiles = stats[user_cols].drop_duplicates("user_id").rename(columns={"user_id": "user_bk"})
    with ENGINE.begin() as conn:
        existing = pd.read_sql(text("SELECT user_bk FROM dwh.dim_user WHERE is_current"), conn)
        new_users = user_profiles[~user_profiles.user_bk.isin(existing.user_bk)]
        if not new_users.empty:
            new_users = new_users.copy()
            new_users["signup_date"] = pd.Timestamp(stats.listening_date.min()).date()
            new_users["eff_start_date"] = pd.Timestamp(stats.listening_date.min()).date()
            new_users["eff_end_date"] = None
            new_users["is_current"] = True
            new_users["version_no"] = 1
            new_users["row_hash"] = "csv-profile"
            new_users.to_sql("dim_user", conn, schema="dwh", if_exists="append", index=False)

        user_map = pd.read_sql(
            text("SELECT user_sk, user_bk FROM dwh.dim_user WHERE is_current"), conn)
        song_map = pd.read_sql(
            text("SELECT song_sk, song_bk FROM dwh.dim_song WHERE is_current"), conn)
        album_map = pd.read_sql(text("SELECT album_sk, album_bk FROM dwh.dim_album"), conn)
        genre_map = pd.read_sql(text("SELECT genre_sk, genre_bk FROM dwh.dim_genre"), conn)
    # Resolve catalog business keys using the validated Silver catalog.
    with ENGINE.begin() as conn:
        catalog = pd.read_sql(text("""
            SELECT track_id AS song_bk, track_album_id AS album_bk,
                   playlist_genre, playlist_subgenre
            FROM silver.spotify_songs
        """), conn)
    catalog["genre_bk"] = catalog["playlist_genre"].fillna("") + "|" + catalog["playlist_subgenre"].fillna("")
    song_refs = catalog.merge(album_map, on="album_bk", how="left").merge(genre_map, on="genre_bk", how="left")

    facts = (stats.rename(columns={"user_id": "user_bk", "track_id": "song_bk"})
        .merge(user_map, on="user_bk", how="left", validate="many_to_one")
        .merge(song_map, on="song_bk", how="left", validate="many_to_one")
        .merge(song_refs[["song_bk", "album_sk", "genre_sk"]], on="song_bk", how="left", validate="many_to_one"))
    unresolved = facts[["user_sk", "song_sk", "album_sk", "genre_sk"]].isna().any(axis=1)
    if unresolved.any():
        examples = facts.loc[unresolved, ["user_bk", "song_bk"]].head(5).to_dict("records")
        raise ValueError(f"{int(unresolved.sum())} CSV rows do not resolve to loaded dimensions; examples: {examples}")

    facts["date_key"] = facts["listening_date"].dt.strftime("%Y%m%d").astype("int64")
    facts["batch_id"] = BATCH_ID
    fact_columns = ["date_key", "user_sk", "song_sk", "album_sk", "genre_sk",
                    "stream_count", "ms_played", "skipped_count", "completed_count", "batch_id"]
    fact_count = 0
    with ENGINE.begin() as conn:
        # Migrate existing project databases too: prior versions wrote generated
        # rows with STREAM_ batch IDs. Replace those rows in the same transaction.
        removed_legacy_rows = conn.execute(text(
            "DELETE FROM dwh.fact_stream WHERE batch_id LIKE 'STREAM_%'"
        )).rowcount
        for start in range(0, len(facts), 20_000):
            chunk = facts.iloc[start:start + 20_000][fact_columns]
            chunk.to_sql("fact_stream", conn, schema="dwh", if_exists="append", index=False,
                         method="multi", chunksize=1_000)
            fact_count += len(chunk)
        conn.execute(text("""
            INSERT INTO dwh.etl_batch_log
                (batch_id, source_name, started_at, finished_at, rows_read, rows_rejected, rows_loaded, status)
            VALUES (:batch, 'spotify_listening_stats.csv', now(), now(), :n, 0, :n, 'SUCCESS')
        """), {"batch": BATCH_ID, "n": fact_count})

    print(f"[dim_user] available profiles: {len(user_profiles)} ({len(new_users)} newly loaded)")
    if removed_legacy_rows:
        print(f"[fact_stream] replaced {removed_legacy_rows} rows from the former generator")
    print(f"[fact_stream] loaded {fact_count} validated Silver rows as batch {BATCH_ID}")


if __name__ == "__main__":
    load_listening_stats()
