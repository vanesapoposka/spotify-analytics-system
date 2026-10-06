"""Load the Gold dimensional warehouse from the Silver catalog tables.

etl/prepare_medallion.py must materialize Bronze and Silver first. Listening
statistics are loaded separately by etl/load_listening_stats.py.

Run: python etl/load_dwh.py
"""

import sys, os, hashlib, uuid, random
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dq_rules import split_artists, row_hash
from db_config import get_engine

ENGINE = get_engine()
SONG_TRACKED_COLS = ["mood_label","danceability","energy","key_signature","loudness","mode",
                      "speechiness","acousticness","instrumentalness","liveness","valence","tempo"]
ARTIST_TRACKED_COLS = ["primary_genre","popularity_tier","followers_bucket"]
USER_TRACKED_COLS = ["subscription_tier","user_segment"]

random.seed(42); np.random.seed(42)

def build_dim_date(engine, start="2018-01-01", end="2020-12-31"):
    dates = pd.date_range(start, end, freq="D")
    df = pd.DataFrame({"full_date": dates})
    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["day_of_week"] = df["full_date"].dt.dayofweek
    df["day_name"] = df["full_date"].dt.day_name()
    df["day_of_month"] = df["full_date"].dt.day
    df["week_of_year"] = df["full_date"].dt.isocalendar().week.astype(int)
    df["month_num"] = df["full_date"].dt.month
    df["month_name"] = df["full_date"].dt.month_name()
    df["quarter"] = df["full_date"].dt.quarter
    df["year"] = df["full_date"].dt.year
    df["is_weekend"] = df["day_of_week"].isin([5, 6])
    df.to_sql("dim_date", engine, schema="dwh", if_exists="append", index=False)
    print(f"[dim_date] loaded {len(df)} rows")

def mood_from_audio(row):
    v, e = row["valence"], row["energy"]
    if v >= 0.5 and e >= 0.5: return "Happy/Energetic"
    if v >= 0.5 and e < 0.5:  return "Calm/Positive"
    if v < 0.5 and e >= 0.5:  return "Angry/Tense"
    return "Sad/Melancholic"

def popularity_tier(pop):
    if pop >= 80: return "Superstar"
    if pop >= 60: return "Mainstream"
    if pop >= 35: return "Mid-tier"
    return "Emerging"

def followers_bucket(pop):
    if pop >= 80: return "10M+"
    if pop >= 60: return "1M-10M"
    if pop >= 35: return "100K-1M"
    return "<100K"

def scd2_upsert(engine, table, business_key_col, staging_df, tracked_cols, as_of_date):
    with engine.begin() as conn:
        current = pd.read_sql(
            text(f"SELECT * FROM dwh.{table} WHERE is_current = TRUE"), conn)

    staging_df = staging_df.copy()
    staging_df["row_hash"] = staging_df.apply(lambda r: row_hash(r, tracked_cols), axis=1)

    if current.empty:
        staging_df["eff_start_date"] = as_of_date
        staging_df["eff_end_date"] = None
        staging_df["is_current"] = True
        staging_df["version_no"] = 1
        cols = [c for c in staging_df.columns]
        staging_df[cols].to_sql(table, ENGINE, schema="dwh", if_exists="append", index=False)
        return len(staging_df), 0, len(staging_df)

    cur_hash_map = current.set_index(business_key_col)["row_hash"].to_dict()
    cur_ver_map = current.set_index(business_key_col)["version_no"].to_dict()

    new_rows, changed_keys = [], []
    for _, r in staging_df.iterrows():
        bk = r[business_key_col]
        if bk not in cur_hash_map:
            new_rows.append(r)
        elif cur_hash_map[bk] != r["row_hash"]:
            changed_keys.append(bk)
            new_rows.append(r)

    if changed_keys:
        with engine.begin() as conn:
            conn.execute(text(f"""
                UPDATE dwh.{table} SET is_current = FALSE, eff_end_date = :d
                WHERE {business_key_col} = ANY(:keys) AND is_current = TRUE
            """), {"d": as_of_date, "keys": list(changed_keys)})

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        new_df["eff_start_date"] = as_of_date
        new_df["eff_end_date"] = None
        new_df["is_current"] = True
        new_df["version_no"] = new_df[business_key_col].map(lambda k: cur_ver_map.get(k, 0) + 1)
        new_df.to_sql(table, ENGINE, schema="dwh", if_exists="append", index=False)

    return len(new_rows) - len(changed_keys), len(changed_keys), len(new_rows)

def load_catalog_incrementally(n_batches=5):
    with ENGINE.begin() as conn:
        raw = pd.read_sql(text("SELECT * FROM silver.spotify_songs ORDER BY source_row_number"), conn)
        silver_rejected = conn.execute(text("""
            SELECT COUNT(*) FROM silver.dq_rejects
            WHERE source_name = 'spotify_songs.csv'
        """)).scalar_one()
    if raw.empty:
        raise ValueError("Silver catalog is empty. Run etl/prepare_medallion.py first.")
    idx_to_batch = {}
    shuffled_idx = raw.sample(frac=1, random_state=1).index.to_numpy()
    for i, chunk_idx in enumerate(np.array_split(shuffled_idx, n_batches)):
        for idx in chunk_idx:
            idx_to_batch[idx] = i
    raw["batch_no"] = raw.index.map(idx_to_batch)

    total_read = total_loaded = 0

    for b in range(n_batches):
        batch_id = f"CATALOG_B{b+1}_{uuid.uuid4().hex[:6]}"
        chunk = raw[raw["batch_no"] == b].drop(columns=["batch_no"])
        clean = chunk.copy()
        total_read += len(chunk)

        as_of = pd.Timestamp("2018-01-01") + pd.Timedelta(days=b)  # simulate arrival day

        # DIM_GENRE (Type 1)
        g = clean[["playlist_genre", "playlist_subgenre"]].drop_duplicates()
        g["genre_bk"] = g["playlist_genre"] + "|" + g["playlist_subgenre"]
        with ENGINE.begin() as conn:
            existing = pd.read_sql(text("SELECT genre_bk FROM dwh.dim_genre"), conn)["genre_bk"].tolist()
        g_new = g[~g["genre_bk"].isin(existing)]
        if len(g_new):
            g_new.to_sql("dim_genre", ENGINE, schema="dwh", if_exists="append", index=False)

        # DIM_ALBUM (Type 1)
        alb = clean[["track_album_id", "track_album_name", "track_album_release_date"]].drop_duplicates("track_album_id")
        alb = alb.rename(columns={"track_album_id": "album_bk", "track_album_name": "album_name",
                                   "track_album_release_date": "release_date"})
        with ENGINE.begin() as conn:
            existing = pd.read_sql(text("SELECT album_bk FROM dwh.dim_album"), conn)["album_bk"].tolist()
        alb_new = alb[~alb["album_bk"].isin(existing)]
        if len(alb_new):
            alb_new.to_sql("dim_album", ENGINE, schema="dwh", if_exists="append", index=False)

        # DIM_ARTIST (Type 2) — explode many-to-many artist credits
        artist_rows = []
        for _, r in clean.iterrows():
            for name in split_artists(r["track_artist"]):
                artist_rows.append({"artist_bk": name.lower(), "artist_name": name,
                                     "primary_genre": r["playlist_genre"],
                                     "popularity_tier": popularity_tier(r["track_popularity"]),
                                     "followers_bucket": followers_bucket(r["track_popularity"])})
        art_df = pd.DataFrame(artist_rows).drop_duplicates("artist_bk")
        n_new, n_chg, n_tot = scd2_upsert(ENGINE, "dim_artist", "artist_bk", art_df, ARTIST_TRACKED_COLS, as_of)

        song_df = clean.copy()
        song_df["mood_label"] = song_df.apply(mood_from_audio, axis=1)
        song_df = song_df.rename(columns={"track_id": "song_bk", "track_name": "track_name",
                                           "key": "key_signature"})
        song_df["explicit"] = False  # not present in source; default (documented DQ assumption)
        song_cols = ["song_bk", "track_name", "duration_ms", "explicit", "mood_label",
                     "danceability", "energy", "key_signature", "loudness", "mode",
                     "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo"]
        song_df = song_df[song_cols].drop_duplicates("song_bk")
        scd2_upsert(ENGINE, "dim_song", "song_bk", song_df, SONG_TRACKED_COLS, as_of)

        total_loaded += len(clean)
        with ENGINE.begin() as conn:
            conn.execute(text("""
                INSERT INTO dwh.etl_batch_log(batch_id, source_name, started_at, finished_at,
                                               rows_read, rows_rejected, rows_loaded, status)
                VALUES (:bid,'spotify_songs.csv', now(), now(), :r, :rej, :l, 'SUCCESS')
            """), {"bid": batch_id, "r": len(chunk), "rej": 0, "l": len(clean)})
        print(f"[batch {b+1}/{n_batches}] Silver rows={len(chunk)} loaded={len(clean)} "
              f"| new_artists={n_new} changed_artists={n_chg}")

    print(f"\nGOLD CATALOG LOAD: silver_rows={total_read}, loaded={total_loaded}; "
          f"rejected earlier in Silver={silver_rejected}")

def build_bridge_song_artist():
    with ENGINE.begin() as conn:
        raw = pd.read_sql(text("""
            SELECT track_id, track_artist FROM silver.spotify_songs
            ORDER BY source_row_number
        """), conn).drop_duplicates("track_id")
        song_map = pd.read_sql(text("SELECT song_sk, song_bk FROM dwh.dim_song WHERE is_current"), conn)
        artist_map = pd.read_sql(text("SELECT artist_sk, artist_bk FROM dwh.dim_artist WHERE is_current"), conn)
    song_lookup = song_map.set_index("song_bk")["song_sk"].to_dict()
    artist_lookup = artist_map.set_index("artist_bk")["artist_sk"].to_dict()

    rows = []
    for _, r in raw.iterrows():
        song_sk = song_lookup.get(r["track_id"])
        if song_sk is None:
            continue
        names = split_artists(r["track_artist"])
        if not names:
            continue
        w = round(1.0 / len(names), 5)
        for seq, name in enumerate(names, start=1):
            a_sk = artist_lookup.get(name.lower())
            if a_sk is None:
                continue
            rows.append({"song_sk": song_sk, "artist_sk": a_sk, "artist_seq": seq, "weighting_factor": w})

    bridge = pd.DataFrame(rows).drop_duplicates(subset=["song_sk", "artist_sk"])
    with ENGINE.begin() as conn:
        conn.execute(text("DELETE FROM dwh.bridge_song_artist"))
        bridge.to_sql("bridge_song_artist", conn, schema="dwh", if_exists="append", index=False)
    n_collabs = (bridge.groupby("song_sk").size() > 1).sum()
    print(f"[bridge_song_artist] loaded {len(bridge)} links ({n_collabs} multi-artist collabs)")

if __name__ == "__main__":
    with ENGINE.begin() as conn:
        conn.execute(text("TRUNCATE dwh.dim_date CASCADE"))
    build_dim_date(ENGINE)
    load_catalog_incrementally(n_batches=5)
    build_bridge_song_artist()
