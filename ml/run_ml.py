"""Run the project's three ML/analytics tasks and save dashboard results.

1. Segment listeners with KMeans using their listening behavior.
2. Predict next-day song streams with Gradient Boosting and rank trend candidates.
3. Aggregate streams by weekday for the dashboard.

Run after loading the catalog and listening statistics:
    python ml/run_ml.py
"""
import json
import os
import sys

import pandas as pd
from sqlalchemy import text
from sklearn.cluster import KMeans
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "etl"))
from db_config import get_engine, PROJECT_ROOT

ENGINE = get_engine()
OUT = {}

# 1. Listener clustering: group users by aggregate listening behavior.
with ENGINE.begin() as conn:
    user_features = pd.read_sql(text("""
        SELECT fs.user_sk,
               AVG(ds.valence) AS avg_valence,
               AVG(ds.energy) AS avg_energy,
               AVG(ds.danceability) AS avg_dance,
               AVG(ds.tempo) AS avg_tempo,
               SUM(fs.stream_count) AS total_plays,
               SUM(fs.skipped_count)::float /
                   NULLIF(SUM(fs.stream_count + fs.skipped_count), 0) AS skip_rate,
               COUNT(DISTINCT dg.playlist_genre) AS genre_diversity
        FROM gold.fact_stream fs
        JOIN gold.dim_song ds ON ds.song_sk = fs.song_sk
        JOIN gold.dim_genre dg ON dg.genre_sk = fs.genre_sk
        GROUP BY fs.user_sk
    """), conn)

if len(user_features) < 5:
    raise ValueError("KMeans listener clustering needs at least five users with listening data")

feature_columns = ["avg_valence", "avg_energy", "avg_dance", "avg_tempo",
                   "total_plays", "skip_rate", "genre_diversity"]
scaled_features = StandardScaler().fit_transform(user_features[feature_columns].fillna(0))
user_features["cluster"] = KMeans(n_clusters=5, random_state=42, n_init=10).fit_predict(scaled_features)

cluster_profiles = (user_features.groupby("cluster")
    .agg(listeners=("user_sk", "count"),
         avg_valence=("avg_valence", "mean"),
         avg_energy=("avg_energy", "mean"),
         avg_dance=("avg_dance", "mean"),
         total_plays=("total_plays", "mean"),
         skip_rate=("skip_rate", "mean"),
         genre_diversity=("genre_diversity", "mean"))
    .round(3).reset_index())
OUT["user_clusters"] = json.loads(cluster_profiles.to_json(orient="records"))
print("=== LISTENER CLUSTERS ===\n", cluster_profiles.to_string(index=False), "\n")

# 2. Trend prediction: use recent daily stream counts to forecast the next day.
with ENGINE.begin() as conn:
    daily = pd.read_sql(text("""
        SELECT fs.song_sk, fs.date_key, SUM(fs.stream_count) AS streams
        FROM gold.fact_stream fs
        GROUP BY fs.song_sk, fs.date_key
    """), conn)
    song_names = pd.read_sql(text("""
        SELECT song_sk, track_name
        FROM gold.dim_song
        WHERE is_current = TRUE
    """), conn)

daily = daily.sort_values(["song_sk", "date_key"]).copy()
song_groups = daily.groupby("song_sk")["streams"]
daily["lag1"] = song_groups.shift(1)
daily["lag2"] = song_groups.shift(2)
daily["roll3"] = song_groups.transform(lambda values: values.shift(1).rolling(3).mean())
daily["target_next"] = song_groups.shift(-1)
training_rows = daily.dropna(subset=["lag1", "lag2", "roll3", "target_next"])
if training_rows.empty:
    raise ValueError("Not enough consecutive daily listening data to train the trend model")

model_features = ["streams", "lag1", "lag2", "roll3"]
x_train, x_test, y_train, _ = train_test_split(
    training_rows[model_features], training_rows["target_next"],
    test_size=0.2, random_state=42)
trend_model = GradientBoostingRegressor(n_estimators=150, max_depth=3, random_state=42)
trend_model.fit(x_train, y_train)

latest_rows = daily.groupby("song_sk", sort=False).tail(1).dropna(subset=["lag1", "lag2", "roll3"]).copy()
latest_rows["predicted_next"] = trend_model.predict(latest_rows[model_features])
latest_rows["predicted_growth"] = latest_rows["predicted_next"] - latest_rows["streams"]
trending = (latest_rows.sort_values("predicted_growth", ascending=False)
    .merge(song_names, on="song_sk", how="inner")
    .head(10)[["track_name", "streams", "predicted_next", "predicted_growth"]]
    .round(2))
OUT["top_trending_predicted"] = json.loads(trending.to_json(orient="records"))
print("=== TOP PREDICTED TRENDING SONGS ===\n", trending.to_string(index=False), "\n")

# 3. Weekday stream totals for the dashboard.
with ENGINE.begin() as conn:
    weekday_totals = pd.read_sql(text("""
        SELECT dd.day_name, SUM(fs.stream_count) AS streams
        FROM gold.fact_stream fs
        JOIN gold.dim_date dd ON dd.date_key = fs.date_key
        GROUP BY dd.day_of_week, dd.day_name
        ORDER BY dd.day_of_week
    """), conn)
OUT["temporal_dow_totals"] = {
    row.day_name: int(row.streams) for row in weekday_totals.itertuples(index=False)
}
print("=== STREAMS BY WEEKDAY ===\n", weekday_totals.to_string(index=False), "\n")

results_path = PROJECT_ROOT / "ml" / "ml_results.json"
results_path.write_text(json.dumps(OUT, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"Saved dashboard results to {results_path}")
