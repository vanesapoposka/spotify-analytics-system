from idlelib.iomenu import errors

import pandas as pd
import hashlib

AUDIO_FEATURE_BOUNDS = {
    "danceability" : (0,1),
    "energy" : (0,1),
    "speechiness" : (0,1),
    "acousticness" : (0,1),
    "instrumentalness" : (0,1),
    "liveness" : (0,1),
    "valence" : (0,1),
    "loudness" : (-60, 5),
    "tempo" : (0, 250),
    "duration_ms" : (1000, 3600000),
}

def row_hash(row, cols):
    payload = "|".join(str(row[c] for c in cols))
    return hashlib.sha256(payload.encode()).hexdigest()

def clean_and_validate(df: pd.DataFrame):
    rejects = []
    df = df.copy()
    df["_orig_idx"] = df.index

    mandatory_fields = ["track_id", "track_name", "track_artist", "track_album_id", "playlist_genre"]

    for col in mandatory_fields:
        bad  = df[df[col].isna() | (df[col].astype(str).str.strip() == "")]
        for _, r in bad.iterrows():
            rejects.append(
                {
                "natural_key" : r.get("track_id", "UNKNOWN"),
                "rule_violated" : f"NULL_MANDATORY_FIELD:{col}"
                }
        )
        df = df.drop(bad.index)

    df["_completeness"] = df.notna().sum(axis=1)
    df = df.sort_values("_completeness", ascending = False)
    dup_mask = df.duplicated(subset=["track_id"], keep="first")
    for _, r in df[dup_mask].iterrows():
        rejects.append(
            {"natural_key" : r["track_id"],
             "rule_violated" : "DUPLICATE_TRACK_ID"}
        )
    df = df[~dup_mask]

    for col, (lo, hi) in AUDIO_FEATURE_BOUNDS.items():
        if col not in df.columns:
            continue
        bad = df[(df[col] < lo) | (df[col] > hi) | df[col].isna()]
        for _, r in bad.iterrows():
            rejects.append({
                "natural_key" : r["track_id"],
                "rule_violated" : f"OUT_OF_RANGE:{col}"
            })
        df = df.drop(bad.index, errors = "ignore")

    bad = df[(df["track_popularity"] < 0) | (df["track_popularity"] > 100)]
    for _, r in bad.iterrows():
        rejects.append({
            "natural_key" : r["track_id"],
            "rule_violated" : "OUT_OF_RANGE:popularity"
        })

    df = df.drop(bad.index, errors="ignore")

    bad = df[(df["track_popularity"] < 0) | (df["track_popularity"] > 100)]
    for _, r in bad.iterrows():
        rejects.append({
            "natural_key" : r["track_id"],
            "rule_violated" : "OUT_OF_RANGE:popularity"
        })
    df = df.drop(bad.index, errors="ignore")

    df["track_album_release_date"] = pd.to_datetime(df["track_album_release_date"], errors="coerce")
    bad = df[df["track_album_release_date"].isna() | (df["track_album_release_date"].dt.year < 1900) |
             (df["track_album_release_date"] > pd.Timestamp.today())]
    for _, r in bad.iterrows():
        rejects.append({"natural_key": r["track_id"], "rule_violated": "INVALID_RELEASE_DATE"})
    df.loc[df["track_album_release_date"].isna(), "track_album_release_date"] = pd.Timestamp("1900-01-01")

    for col in ["track_name", "track_artist", "track_album_name"]:
        df[col] = df[col].astype(str).str.strip()

    rejects_df = pd.DataFrame(rejects)
    df = df.drop(columns=["_orig_idx", "_completeness"], errors="ignore")
    return df.reset_index(drop=True), rejects_df

def split_artists(artist_field: str):
    if not isinstance(artist_field, str):
        return []
    parts = artist_field.replace(" feat. ", ",").replace(" Feat. ", ",") \
                         .replace(" & ", ",").replace(";", ",").split(",")
    return [p.strip() for p in parts if p.strip()]