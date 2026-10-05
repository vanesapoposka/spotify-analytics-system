drop schema if exists dwh cascade;
create schema dwh;
set search_path to dwh;

create table dim_date(
    date_key int primary key,
    full_date date not null unique,
    day_of_week smallint,
    day_name varchar(10),
    day_of_month smallint,
    week_of_year smallint,
    month_num smallint,
    month_name varchar(10),
    quarter smallint,
    year smallint,
    is_weekend boolean
);

create table dim_artist(
    artist_sk bigserial primary key,
    artist_bk varchar(200) not null,
    artist_name varchar(300) not null,
    primary_genre varchar(100),
    popularity_tier varchar(20),
    followers_bucket varchar(20),
    eff_start_date date not null,
    eff_end_date date,
    is_current boolean not null default true,
    version_no int not null default 1,
    row_hash char(64),
    load_ts timestamp not null default now()
);

create index idx_dim_artist_bk_current on dim_artist(artist_bk, is_current);

create table dim_song(
    song_sk bigserial primary key,
    song_bk varchar(64) not null,
    track_name varchar(400) not null,
    duration_ms int,
    explicit boolean,
    mood_label varchar(20),
    danceability numeric(5,4),
    energy numeric(5,4),
    key_signature smallint,
    loudness numeric(6,3),
    mode smallint,
    speechiness numeric(5,4),
    acousticness numeric(5,4),
    instrumentalness numeric(6,5),
    liveness numeric(5,4),
    valence numeric(5,4),
    tempo numeric(6,2),
    eff_start_date date not null,
    eff_end_date date,
    is_current boolean not null default true,
    version_no int not null default 1,
    row_hash char(64),
    load_ts timestamp not null default now()
);

create index idx_dim_song_bk_current on dim_song(song_bk, is_current);

create table dim_album(
    album_sk bigserial primary key,
    album_bk varchar(64) not null unique,
    album_name varchar(400),
    release_date date,
    load_ts timestamp not null default now()
);

create table dim_genre(
    genre_sk bigserial primary key,
    genre_bk varchar(150) not null unique,
    playlist_genre varchar(100),
    playlist_subgenre varchar(100)
);

create table dim_user(
    user_sk bigserial primary key,
    user_bk varchar(64) not null,
    signup_date date,
    country varchar(2),
    age_bracket varchar(10),
    subscription_tier varchar(20),
    user_segment varchar(30),
    eff_start_date date not null,
    eff_end_date date,
    is_current boolean not null default true,
    version_no int not null default 1,
    row_hash char(64),
    load_ts timestamp not null default now()
);

create index idx_dim_user_bk_current on dim_user(user_bk, is_current);

create table bridge_song_artist(
    song_sk bigint not null references dim_song(song_sk),
    artist_sk bigint not null references dim_artist(artist_sk),
    artist_seq smallint not null,
    weighting_factor numeric(6,5) not null,
    primary key (song_sk, artist_sk)
);

create table fact_stream(
    stream_fact_sk bigserial primary key,
    date_key int not null references dim_date(date_key),
    user_sk bigint not null references dim_user(user_sk),
    song_sk bigint not null references dim_song(song_sk),
    album_sk bigint not null references dim_album(album_sk),
    genre_sk bigint not null references dim_genre(genre_sk),
    stream_count int not null,
    ms_played bigint not null,
    skipped_count int not null default 0,
    completed_count int not null default 0,
    load_ts timestamp not null default now(),
    batch_id varchar(40) not null
);

create index idx_fact_stream_date on fact_stream(date_key);
create index idx_fact_stream_song on fact_stream(song_sk);
create index idx_fact_stream_user on fact_stream(user_sk);
create index idx_fact_stream_genre on fact_stream(genre_sk);

create table fact_song_artist_daily(
    date_key int not null references dim_date(date_key),
    song_sk bigint not null references dim_song(song_sk),
    artist_sk bigint not null references dim_artist(artist_sk),
    unweighted_streams bigint not null,
    weighted_streams numeric(14,4) not null,
    primary key (date_key, song_sk, artist_sk)
);

create index idx_fsad_date on fact_song_artist_daily(date_key);
create index idx_fsad_artist on fact_song_artist_daily(artist_sk);

create table etl_batch_log(
    batch_id varchar(40) primary key,
    source_name varchar(100),
    started_at timestamp,
    finished_at timestamp,
    rows_read int,
    rows_rejected int,
    rows_loaded int,
    status varchar(20)
);

create table dq_reject_log(
    reject_id bigserial primary key,
    batch_id varchar(40)  references etl_batch_log(batch_id),
    source_table varchar(100),
    natural_key varchar(200),
    rule_violated varchar(200),
    raw_row jsonb,
    rejected_at timestamp default now()
);