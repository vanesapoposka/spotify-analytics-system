"""Materialize the Bronze and Silver layers from the project's CSV sources.

Bronze keeps source records as JSONB with source filename and row number.
Silver applies the catalog DQ rules and validates/types listening statistics.
The Gold warehouse loaders then read only the Silver tables.
"""
import json
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text

from db_config import get_engine, PROJECT_ROOT
from dq_rules import clean_and_validate

ENGINE = get_engine()
CATALOG_CSV = PROJECT_ROOT / "data" / "spotify_songs.csv"
LISTENING_CSV = PROJECT_ROOT / "data" / "spotify_listening_stats.csv"
BATCH_ID = f"MED_{pd.Timestamp.now():%Y%m%d%H%M%S}_{uuid.uuid4().hex[:6]}"
LISTENING_COLUMNS = [
    "listening_date", "user_id", "country", "age_bracket", "subscription_tier",
    "user_segment", "track_id", "stream_count", "ms_played", "skipped_count",
    "completed_count",
]
COUNT_COLUMNS = ["stream_count", "ms_played", "skipped_count", "completed_count"]


def _load_raw_records(path: Path):
    # dtype=str + keep_default_na=False preserves blank and textual source values.
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    records = []
    for row_number, record in enumerate(frame.to_dict(orient="records"), start=2):
        records.append({"source_row_number": row_number, "record": record})
    return frame, records


def _write_bronze(conn, table: str, source_file: str, records):
    statement = text(f"""
        INSERT INTO bronze.{table}
            (batch_id, source_file, source_row_number, raw_record)
        VALUES (:batch_id, :source_file, :source_row_number, CAST(:raw_record AS JSONB))
    """)
    for start in range(0, len(records), 1000):
        params = [{
            "batch_id": BATCH_ID,
            "source_file": source_file,
            "source_row_number": row["source_row_number"],
            "raw_record": json.dumps(row["record"], ensure_ascii=False),
        } for row in records[start:start + 1000]]
        conn.execute(statement, params)


def _write_rejects(conn, source_file: str, rejects):
    if not rejects:
        return
    statement = text("""
        INSERT INTO silver.dq_rejects
            (source_name, source_batch_id, source_row_number, natural_key,
             rule_violated, raw_record)
        VALUES (:source_name, :batch_id, :row_number, :natural_key,
                :rule, CAST(:raw_record AS JSONB))
    """)
    for start in range(0, len(rejects), 1000):
        conn.execute(statement, rejects[start:start + 1000])


def prepare_catalog(raw_records):
    parsed = pd.read_csv(CATALOG_CSV)
    parsed["_source_row_number"] = np.arange(2, len(parsed) + 2)
    cleaned, dq_rejects = clean_and_validate(parsed)
    cleaned["playlist_subgenre"] = cleaned["playlist_subgenre"].replace("", np.nan).fillna("Unknown")

    raw_by_line = {row["source_row_number"]: row["record"] for row in raw_records}
    rejects = []
    for item in dq_rejects.to_dict(orient="records"):
        row_number = item.get("source_row_number")
        if pd.isna(row_number):
            row_number = 0
        row_number = int(row_number)
        rejects.append({
            "source_name": CATALOG_CSV.name,
            "batch_id": BATCH_ID,
            "row_number": row_number,
            "natural_key": str(item.get("natural_key") or "")[:200],
            "rule": str(item["rule_violated"]),
            "raw_record": json.dumps(raw_by_line.get(row_number, {}), ensure_ascii=False),
        })

    silver = cleaned.rename(columns={"_source_row_number": "source_row_number"}).copy()
    silver["source_batch_id"] = BATCH_ID
    silver["track_album_release_date"] = pd.to_datetime(
        silver["track_album_release_date"], errors="coerce").dt.date
    columns = [
        "source_row_number", "source_batch_id", "track_id", "track_name", "track_artist",
        "track_popularity", "track_album_id", "track_album_name", "track_album_release_date",
        "playlist_name", "playlist_id", "playlist_genre", "playlist_subgenre", "danceability",
        "energy", "key", "loudness", "mode", "speechiness", "acousticness",
        "instrumentalness", "liveness", "valence", "tempo", "duration_ms",
    ]
    return silver[columns], rejects


def prepare_listening_stats(listening_source: pd.DataFrame, raw_records, valid_track_ids):
    missing_columns = set(LISTENING_COLUMNS) - set(listening_source.columns)
    if missing_columns:
        raise ValueError(f"Listening CSV is missing columns: {sorted(missing_columns)}")

    frame = listening_source[LISTENING_COLUMNS].copy()
    frame["source_row_number"] = np.arange(2, len(frame) + 2)
    frame["source_batch_id"] = BATCH_ID
    reasons = [[] for _ in range(len(frame))]

    def reject(mask, reason):
        for idx in np.flatnonzero(np.asarray(mask)):
            reasons[idx].append(reason)

    required = ["listening_date", "user_id", "country", "age_bracket", "subscription_tier",
                "user_segment", "track_id"]
    for column in required:
        reject(frame[column].astype(str).str.strip().eq(""), f"MISSING_REQUIRED:{column}")

    parsed_dates = pd.to_datetime(frame["listening_date"], errors="coerce")
    reject(parsed_dates.isna(), "INVALID_DATE:listening_date")
    numeric = {}
    for column in COUNT_COLUMNS:
        values = pd.to_numeric(frame[column], errors="coerce")
        reject(values.isna(), f"INVALID_NUMBER:{column}")
        reject(values.notna() & (values < 0), f"NEGATIVE_VALUE:{column}")
        reject(values.notna() & ((values % 1) != 0), f"NON_INTEGER:{column}")
        numeric[column] = values

    reject(~frame["track_id"].isin(valid_track_ids), "UNKNOWN_TRACK_ID")
    normalized_date_key = parsed_dates.dt.strftime("%Y-%m-%d")
    duplicate_key = pd.DataFrame({
        "date": normalized_date_key,
        "user_id": frame["user_id"],
        "track_id": frame["track_id"],
    })
    reject(duplicate_key.duplicated(keep="first"),
           "DUPLICATE_USER_TRACK_DATE")

    profile_fields = ["country", "age_bracket", "subscription_tier", "user_segment"]
    profile_variants = frame.groupby("user_id", dropna=False)[profile_fields].nunique(dropna=False)
    inconsistent_users = profile_variants.index[(profile_variants > 1).any(axis=1)]
    reject(frame["user_id"].isin(inconsistent_users), "INCONSISTENT_USER_PROFILE")

    length_limits = {"user_id": 64, "country": 2, "age_bracket": 10,
                     "subscription_tier": 20, "user_segment": 30, "track_id": 64}
    for column, limit in length_limits.items():
        reject(frame[column].astype(str).str.len() > limit, f"VALUE_TOO_LONG:{column}")

    raw_by_line = {row["source_row_number"]: row["record"] for row in raw_records}
    rejects = []
    for idx, row_reasons in enumerate(reasons):
        if not row_reasons:
            continue
        row_number = int(frame.iloc[idx]["source_row_number"])
        rejects.append({
            "source_name": LISTENING_CSV.name,
            "batch_id": BATCH_ID,
            "row_number": row_number,
            "natural_key": str(frame.iloc[idx]["user_id"]),
            "rule": ";".join(row_reasons),
            "raw_record": json.dumps(raw_by_line[row_number], ensure_ascii=False),
        })

    keep = np.array([not row_reasons for row_reasons in reasons])
    silver = frame.loc[keep].copy()
    silver["listening_date"] = parsed_dates.loc[keep].dt.date
    for column in COUNT_COLUMNS:
        silver[column] = numeric[column].loc[keep].astype("int64")
    for column in ["user_id", "country", "age_bracket", "subscription_tier", "user_segment", "track_id"]:
        silver[column] = silver[column].astype(str).str.strip()
    silver["country"] = silver["country"].str.upper()
    silver = silver[[
        "source_row_number", "source_batch_id", "listening_date", "user_id", "country",
        "age_bracket", "subscription_tier", "user_segment", "track_id", *COUNT_COLUMNS,
    ]]
    return silver, rejects


def main():
    for path in (CATALOG_CSV, LISTENING_CSV):
        if not path.is_file():
            raise FileNotFoundError(f"Source dataset not found: {path}")

    _, catalog_raw = _load_raw_records(CATALOG_CSV)
    listening_source, listening_raw = _load_raw_records(LISTENING_CSV)
    silver_catalog, catalog_rejects = prepare_catalog(catalog_raw)
    silver_listening, listening_rejects = prepare_listening_stats(
        listening_source, listening_raw, set(silver_catalog["track_id"]))

    with ENGINE.begin() as conn:
        conn.execute(text("""
            TRUNCATE bronze.spotify_songs_raw, bronze.listening_stats_raw,
                     silver.spotify_songs, silver.listening_stats, silver.dq_rejects
            RESTART IDENTITY
        """))
        _write_bronze(conn, "spotify_songs_raw", CATALOG_CSV.name, catalog_raw)
        _write_bronze(conn, "listening_stats_raw", LISTENING_CSV.name, listening_raw)
        _write_rejects(conn, CATALOG_CSV.name, catalog_rejects + listening_rejects)
        silver_catalog.to_sql("spotify_songs", conn, schema="silver", if_exists="append",
                              index=False, chunksize=1000, method="multi")
        silver_listening.to_sql("listening_stats", conn, schema="silver", if_exists="append",
                                index=False, chunksize=1000, method="multi")

    print(f"[bronze] retained {len(catalog_raw)} catalog and {len(listening_raw)} listening source rows")
    print(f"[silver] accepted {len(silver_catalog)} catalog rows; rejected {len(catalog_rejects)} catalog issues")
    print(f"[silver] accepted {len(silver_listening)} listening rows; rejected {len(listening_rejects)} listening rows")


if __name__ == "__main__":
    main()
