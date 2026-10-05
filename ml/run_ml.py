"""
ML layer: (1) user clustering, (2) song mood classification, (3) trending
prediction, (4) hybrid recommender, (5) temporal pattern analysis.
Pulls features straight from the warehouse (dwh schema).
"""
import sys, os
import pandas as pd, numpy as np, json
from sqlalchemy import create_engine, text
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "etl"))
from database_configuration import get_engine, PROJECT_ROOT

ENGINE = get_engine()
OUT = {}

# ---------------------------------------------------------------------------
# 1) USER CLUSTERING — behavioral segments from listening habits
# ---------------------------------------------------------------------------
with ENGINE.begin() as conn:
    user_feat = pd.read_sql(text("""
        SELECT fs.user_sk,
               AVG(ds.valence) avg_valence, AVG(ds.energy) avg_energy,
               AVG(ds.danceability) avg_dance, AVG(ds.tempo) avg_tempo,
               COUNT(*) total_plays,
               SUM(fs.skipped_count)::float/NULLIF(SUM(fs.stream_count+fs.skipped_count),0) skip_rate,
               COUNT(DISTINCT dg.playlist_genre) genre_diversity
        FROM dwh.fact_stream fs
        JOIN dwh.dim_song ds ON ds.song_sk = fs.song_sk
        JOIN dwh.dim_genre dg ON dg.genre_sk = fs.genre_sk
        GROUP BY fs.user_sk
    """), conn)

X = user_feat.drop(columns=["user_sk"]).fillna(0)
Xs = StandardScaler().fit_transform(X)
km_u = KMeans(n_clusters=5, random_state=42, n_init=10).fit(Xs)
user_feat["cluster"] = km_u.labels_
profile = user_feat.groupby("cluster")[["avg_valence","avg_energy","avg_dance","total_plays","skip_rate","genre_diversity"]].mean().round(3)
OUT["user_clusters"] = profile.reset_index().to_dict("records")
print("=== USER CLUSTERS ===\n", profile, "\n")

# ---------------------------------------------------------------------------
# 2) SONG MOOD CLASSIFICATION — supervised model trained on the rule-based
#    mood_label (from ETL) to prove audio features predict mood well, then
#    used going forward to classify NEW incoming songs w/o manual rules.
# ---------------------------------------------------------------------------
with ENGINE.begin() as conn:
    song_feat = pd.read_sql(text("""
        SELECT mood_label, danceability, energy, loudness, speechiness,
               acousticness, instrumentalness, liveness, valence, tempo
        FROM dwh.dim_song WHERE is_current
    """), conn)

Xs2 = song_feat.drop(columns=["mood_label"])
ys2 = song_feat["mood_label"]
Xtr, Xte, ytr, yte = train_test_split(Xs2, ys2, test_size=0.2, random_state=42, stratify=ys2)
clf = RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42, n_jobs=-1).fit(Xtr, ytr)
acc = accuracy_score(yte, clf.predict(Xte))
OUT["mood_classifier_accuracy"] = round(acc, 4)
print(f"=== MOOD CLASSIFIER accuracy={acc:.4f} ===")
print(classification_report(yte, clf.predict(Xte)))

# ---------------------------------------------------------------------------
# 3) TRENDING-SONG PREDICTION — forecast next-day stream growth per song
#    using lag features (regression on daily aggregated stream counts)
# ---------------------------------------------------------------------------
with ENGINE.begin() as conn:
    daily = pd.read_sql(text("""
        SELECT fs.song_sk, fs.date_key, SUM(fs.stream_count) streams
        FROM dwh.fact_stream fs GROUP BY fs.song_sk, fs.date_key
    """), conn)
daily = daily.sort_values(["song_sk","date_key"])
daily["lag1"] = daily.groupby("song_sk")["streams"].shift(1)
daily["lag2"] = daily.groupby("song_sk")["streams"].shift(2)
daily["roll3"] = daily.groupby("song_sk")["streams"].transform(lambda s: s.shift(1).rolling(3).mean())
daily["target_next"] = daily.groupby("song_sk")["streams"].shift(-1)
train = daily.dropna()
Xtr3 = train[["streams","lag1","lag2","roll3"]]
ytr3 = train["target_next"]
Xtr3_tr, Xtr3_te, ytr3_tr, ytr3_te = train_test_split(Xtr3, ytr3, test_size=0.2, random_state=42)
gbr = GradientBoostingRegressor(n_estimators=150, max_depth=3, random_state=42).fit(Xtr3_tr, ytr3_tr)
r2 = gbr.score(Xtr3_te, ytr3_te)
OUT["trend_model_r2"] = round(r2, 4)
print(f"=== TREND PREDICTOR R^2={r2:.4f} ===")

# rank songs by predicted next-day growth to surface "trending" candidates
latest = daily.groupby("song_sk").tail(1).dropna(subset=["lag1","lag2","roll3"])
latest = latest.copy()
latest["predicted_next"] = gbr.predict(latest[["streams","lag1","lag2","roll3"]])
latest["predicted_growth"] = latest["predicted_next"] - latest["streams"]
top_trending = latest.sort_values("predicted_growth", ascending=False).head(10)
with ENGINE.begin() as conn:
    names = pd.read_sql(text("SELECT song_sk, track_name FROM dwh.dim_song WHERE is_current"), conn)
top_trending = top_trending.merge(names, on="song_sk")
OUT["top_trending_predicted"] = top_trending[["track_name","streams","predicted_next","predicted_growth"]].round(2).to_dict("records")
print("=== TOP PREDICTED TRENDING SONGS ===\n", top_trending[["track_name","streams","predicted_next"]])

# ---------------------------------------------------------------------------
# 4) HYBRID RECOMMENDER — content-based (audio-feature cosine similarity)
#    blended with collaborative signal (co-listen matrix from fact_stream)
# ---------------------------------------------------------------------------
with ENGINE.begin() as conn:
    songs_all = pd.read_sql(text("""
        SELECT song_sk, song_bk, track_name, danceability, energy, loudness,
               speechiness, acousticness, instrumentalness, liveness, valence, tempo
        FROM dwh.dim_song WHERE is_current
    """), conn)
    cowatch = pd.read_sql(text("""
        SELECT user_sk, song_sk FROM dwh.fact_stream GROUP BY user_sk, song_sk
    """), conn)

# Restrict the candidate pool to the songs that actually have listening
# activity (top-listened ~1500) so both similarity computations stay
# tractable in memory (28k^2 dense matrix would need ~6GB and isn't needed:
# a real system would use ANN / pgvector indexes instead of a dense matrix).
top_songs = cowatch["song_sk"].value_counts().head(1500).index
songs_all = songs_all[songs_all["song_sk"].isin(top_songs)].reset_index(drop=True)

feat_cols = ["danceability","energy","loudness","speechiness","acousticness","instrumentalness","liveness","valence","tempo"]
content_matrix = StandardScaler().fit_transform(songs_all[feat_cols])
content_sim = cosine_similarity(content_matrix)

# collaborative: users x songs sparse co-occurrence -> item-item cosine sim
cw_small = cowatch[cowatch["song_sk"].isin(top_songs)]
pivot = pd.crosstab(cw_small["user_sk"], cw_small["song_sk"])
collab_sim = cosine_similarity(pivot.T.values)
collab_song_ids = pivot.columns.tolist()
collab_idx = {sid: i for i, sid in enumerate(collab_song_ids)}
song_idx = {sid: i for i, sid in enumerate(songs_all["song_sk"])}

def hybrid_recommend(song_sk, k=5, alpha=0.5):
    i = song_idx[song_sk]
    c_sim = content_sim[i]
    if song_sk in collab_idx:
        j = collab_idx[song_sk]
        cb_sim_full = np.zeros(len(songs_all))
        for sid, jj in collab_idx.items():
            cb_sim_full[song_idx[sid]] = collab_sim[j][jj]
        blended = alpha * c_sim + (1 - alpha) * cb_sim_full
    else:
        blended = c_sim
    top = np.argsort(-blended)[1:k+1]
    return songs_all.iloc[top][["track_name"]].assign(score=blended[top]).round(3)

seed_song = songs_all.iloc[0]
recs = hybrid_recommend(seed_song["song_sk"])
OUT["hybrid_recommendation_example"] = {"seed_song": seed_song["track_name"], "recommendations": recs.to_dict("records")}
print(f"\n=== HYBRID RECS for '{seed_song['track_name']}' ===\n", recs)

# ---------------------------------------------------------------------------
# 5) TEMPORAL PATTERN ANALYSIS — hour-of-week-equivalent (day-of-week here,
#    since events are daily-grain) + genre seasonality
# ---------------------------------------------------------------------------
with ENGINE.begin() as conn:
    temporal = pd.read_sql(text("""
        SELECT dd.day_name, dd.is_weekend, dg.playlist_genre,
               SUM(fs.stream_count) streams
        FROM dwh.fact_stream fs
        JOIN dwh.dim_date dd ON dd.date_key = fs.date_key
        JOIN dwh.dim_genre dg ON dg.genre_sk = fs.genre_sk
        GROUP BY dd.day_name, dd.is_weekend, dg.playlist_genre
    """), conn)
dow_totals = temporal.groupby("day_name")["streams"].sum().sort_values(ascending=False)
OUT["temporal_dow_totals"] = dow_totals.to_dict()
print("\n=== STREAMS BY DAY OF WEEK ===\n", dow_totals)

# with open(str(PROJECT_ROOT / "ml" / "ml_results.json"), "w") as f:
#     json.dump(OUT, f, indent=2, default=str)
# print("\nSaved ml_results.json")
