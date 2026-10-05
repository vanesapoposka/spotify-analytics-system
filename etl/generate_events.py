"""
The source catalog has no real per-user streaming log (Spotify's actual
listening logs are private). To exercise the fact table, weighted/unweighted
bridge metrics, user clustering, temporal-pattern and trend-prediction
pieces of the project, we generate a realistic SYNTHETIC listening-event
log: user affinities are drawn from real audio-feature clusters, and each
song's daily play volume is biased by its real Spotify `track_popularity`
score (power-law distribution, matching real streaming behavior).

Loaded DAY BY DAY into fact_stream to demonstrate genuine incremental
(daily) loading, each day tagged with its own batch_id.
"""
import sys, os
import numpy as np, pandas as pd, uuid
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_config import get_engine, PROJECT_ROOT

ENGINE = get_engine()
np.random.seed(7)

N_USERS = 4000
N_DAYS = 60  # rolling window: 2020-11-02 .. 2020-12-31
START_DATE = pd.Timestamp("2020-11-02")

COUNTRIES = ["US","GB","DE","FR","BR","IN","MX","CA","AU","NL"]
AGE_BRACKETS = ["13-17","18-24","25-34","35-44","45-54","55+"]
TIERS = ["Free","Premium","Family","Student"]
SEGMENTS = ["Casual","Regular","Power Listener","Binger"]

def gen_users(n):
    rows = []
    for i in range(n):
        rows.append({
            "user_bk": f"U{i:06d}",
            "signup_date": (START_DATE - pd.Timedelta(days=int(np.random.exponential(400)))).date(),
            "country": np.random.choice(COUNTRIES, p=[.22,.10,.09,.08,.10,.12,.07,.06,.06,.10]),
            "age_bracket": np.random.choice(AGE_BRACKETS, p=[.08,.28,.27,.18,.12,.07]),
            "subscription_tier": np.random.choice(TIERS, p=[.45,.35,.13,.07]),
            "user_segment": np.random.choice(SEGMENTS, p=[.35,.35,.20,.10]),
        })
    return pd.DataFrame(rows)

def load_users():
    df = gen_users(N_USERS)
    df["eff_start_date"] = START_DATE.date()
    df["eff_end_date"] = None
    df["is_current"] = True
    df["version_no"] = 1
    df["row_hash"] = "seed"
    df.to_sql("dim_user", ENGINE, schema="dwh", if_exists="append", index=False)
    print(f"[dim_user] loaded {len(df)} users")

def main():
    load_users()

    with ENGINE.begin() as conn:
        songs = pd.read_sql(text("""SELECT song_sk, song_bk, valence, energy, danceability, tempo
                                     FROM dwh.dim_song WHERE is_current"""), conn)
        users = pd.read_sql(text("SELECT user_sk, user_bk FROM dwh.dim_user WHERE is_current"), conn)
        genre_map = pd.read_sql(text("SELECT genre_sk, genre_bk FROM dwh.dim_genre"), conn)
        raw_pop = pd.read_csv(str(PROJECT_ROOT / "data" / "spotify_songs.csv"))[
            ["track_id","track_popularity","playlist_genre","playlist_subgenre","track_album_id"]
        ].drop_duplicates("track_id")

    songs = songs.merge(raw_pop, left_on="song_bk", right_on="track_id")
    songs["genre_bk"] = songs["playlist_genre"] + "|" + songs["playlist_subgenre"]
    songs = songs.merge(genre_map, on="genre_bk", how="left")

    with ENGINE.begin() as conn:
        album_map = pd.read_sql(text("SELECT album_sk, album_bk FROM dwh.dim_album"), conn)
    songs = songs.merge(album_map, left_on="track_album_id", right_on="album_bk", how="left")

    # popularity -> sampling weight (power law: popular songs streamed far more)
    songs["weight"] = (songs["track_popularity"].clip(lower=1) / 100) ** 3 + 0.001

    # give each user a "taste vector" (valence/energy/danceability affinity) to bias song choice
    user_taste = pd.DataFrame({
        "user_sk": users["user_sk"],
        "pref_valence": np.random.beta(2, 2, len(users)),
        "pref_energy": np.random.beta(2, 2, len(users)),
        "activity_level": np.random.gamma(2, 2, len(users))  # avg songs/day
    })

    songs_sk = songs["song_sk"].values
    song_weight = songs["weight"].values
    song_valence = songs["valence"].fillna(0.5).values
    song_energy = songs["energy"].fillna(0.5).values
    song_genre = songs["genre_sk"].values
    song_album = songs["album_sk"].values

    total_days_loaded = 0
    for d in range(N_DAYS):
        day = START_DATE + pd.Timedelta(days=d)
        date_key = int(day.strftime("%Y%m%d"))
        batch_id = f"STREAM_{day.strftime('%Y%m%d')}_{uuid.uuid4().hex[:6]}"

        # each day, a random ~35-55% of users are active
        active = user_taste.sample(frac=np.random.uniform(0.35, 0.55), random_state=d)
        day_rows = []
        for _, u in active.iterrows():
            n_plays = max(1, int(np.random.poisson(u["activity_level"])))
            taste_affinity = 1 - (np.abs(song_valence - u["pref_valence"]) +
                                   np.abs(song_energy - u["pref_energy"])) / 2
            p = song_weight * (0.3 + 0.7 * taste_affinity)
            p = p / p.sum()
            choices = np.random.choice(len(songs_sk), size=n_plays, p=p)
            for c in choices:
                skip = np.random.rand() < 0.15
                day_rows.append((date_key, u["user_sk"], songs_sk[c], song_album[c], song_genre[c],
                                  0 if skip else 1, int(np.random.uniform(20000, 240000)) if not skip
                                  else int(np.random.uniform(2000, 20000)),
                                  1 if skip else 0, 0 if skip else 1))

        day_df = pd.DataFrame(day_rows, columns=["date_key","user_sk","song_sk","album_sk","genre_sk",
                                                   "stream_count","ms_played","skipped_count","completed_count"])
        # aggregate to (date, user, song) grain
        day_df = day_df.groupby(["date_key","user_sk","song_sk","album_sk","genre_sk"], as_index=False).agg(
            stream_count=("stream_count","sum"), ms_played=("ms_played","sum"),
            skipped_count=("skipped_count","sum"), completed_count=("completed_count","sum"))
        day_df = day_df.dropna(subset=["album_sk","genre_sk"])
        day_df["batch_id"] = batch_id
        day_df.to_sql("fact_stream", ENGINE, schema="dwh", if_exists="append", index=False)

        with ENGINE.begin() as conn:
            conn.execute(text("""INSERT INTO dwh.etl_batch_log
                (batch_id, source_name, started_at, finished_at, rows_read, rows_rejected, rows_loaded, status)
                VALUES (:b,'synthetic_listening_events', now(), now(), :n, 0, :n, 'SUCCESS')"""),
                {"b": batch_id, "n": len(day_df)})
        total_days_loaded += 1
        if d % 10 == 0 or d == N_DAYS - 1:
            print(f"[fact_stream] day {d+1}/{N_DAYS} ({day.date()}) -> {len(day_df)} rows")

    print(f"\nLoaded {total_days_loaded} daily incremental batches into fact_stream.")

if __name__ == "__main__":
    main()
