import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')

# DROP TABLES
nodist_schema_table_drop = "DROP SCHEMA IF EXISTS nodist CASCADE;"
dist_schema_table_drop  = "DROP SCHEMA IF EXISTS dist CASCADE;"

# CREATE TABLES
schema_create = "CREATE SCHEMA IF NOT EXISTS dist;"

staging_events_table_create= ("""
SET search_path TO dist;

CREATE TABLE staging_events (
    artist        varchar,
    auth          varchar,
    firstName     varchar,
    gender        varchar(1),
    itemInSession int,
    lastName      varchar,
    length        decimal,
    level         varchar,
    location      varchar,
    method        varchar,
    page          varchar,
    registration  decimal,
    sessionId     int,
    song          varchar,
    status        int,
    ts            bigint,
    userAgent     varchar,
    userId        int)
""")

staging_songs_table_create = ("""
SET search_path TO dist;

CREATE TABLE staging_songs (
    artist_id        varchar, 
    artist_latitude  decimal, 
    artist_location  varchar, 
    artist_longitude decimal, 
    artist_name      varchar, 
    duration         decimal, 
    num_songs        int, 
    song_id          varchar, 
    title            varchar, 
    year             smallint)
""")

## SONGPLAYS TABLE

## SORTKEY : start_time
## REASON : When loading user's play list,
## it's likely to be queried in the order of descending 'start_time'.

## DISTKEY : song_id
## REASON : The 'song_id' column has the highest cardinality among foreign keys.

songplay_table_create = ("""
SET search_path TO dist;

CREATE TABLE songplays(
    songplay_id int       IDENTITY(0,1)       PRIMARY KEY, 
    start_time  timestamp REFERENCES time     SORTKEY, 
    user_id     int       REFERENCES users, 
    level       varchar,   
    song_id     varchar   REFERENCES songs    DISTKEY, 
    artist_id   varchar   REFERENCES artists, 
    session_id  int,       
    location    varchar, 
    user_agent  varchar)
""")

## USERS TABLE

## DISTSTYLE : all
## REASON : Table size is small.

user_table_create = ("""
SET search_path TO dist;

CREATE TABLE users(
    user_id    int    PRIMARY KEY SORTKEY, 
    first_name varchar, 
    last_name  varchar, 
    gender     varchar(1), 
    level      varchar)
    DISTSTYLE all;
""")

## SONGS TABLE

## DISTSTYPE : key
## REASON : 'song_id' column is DISTKEY.

song_table_create = ("""
SET search_path TO dist;

CREATE TABLE songs(
    song_id   varchar  PRIMARY KEY DISTKEY, 
    title     varchar  , 
    artist_id varchar  , 
    year      smallint , 
    duration  decimal  )
""")

## ARTISTS TABLE

## DISTSTYLE : all
## REASON : Table size is small.

artist_table_create = ("""
SET search_path TO dist;

CREATE TABLE artists(
    artist_id varchar PRIMARY KEY SORTKEY, 
    name      varchar, 
    location  varchar, 
    latitude  decimal, 
    longitude decimal)
    DISTSTYLE all;
""")

## TIME TABLE

## DISTSTYLE : all
## REASON : Table size is small.

time_table_create = ("""
SET search_path TO dist;

CREATE TABLE time(
    start_time timestamp PRIMARY KEY SORTKEY, 
    hour       smallint, 
    day        smallint, 
    weekofyear smallint, 
    month      smallint, 
    year       smallint, 
    weekday    smallint)
    DISTSTYLE all;
""")

# STAGING TABLES

staging_events_copy = ("""
SET search_path TO dist;

COPY staging_events FROM {}
iam_role '{}'
JSON {}
region 'us-west-2'
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
SET search_path TO dist;

COPY staging_songs FROM {}
iam_role '{}'
json 'auto'
region 'us-west-2'
""").format(SONG_DATA, IAM_ROLE)

# INSERT QUERIES

## Converts 'ts' column's data type bigint into timestamp.
## 'song_id' and 'artist_id' are coming from songs table
## matching by artist name, song title and song length.
songplay_table_insert = ("""
SET search_path TO dist;

INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
 
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' start_time,
    se.userId, se.level, s.song_id, s.artist_id, 
    se.sessionId, se.location, se.userAgent
FROM staging_events se
JOIN artists a ON a.name = se.artist
JOIN songs s ON s.title = se.song and s.duration = se.length
WHERE se.page='NextSong'
""")

## Inserts data into users with user's most recent level
## where page is 'NextSong'.
user_table_insert = ("""
SET search_path TO dist;

INSERT INTO users
SELECT DISTINCT t.userId, t.firstName, t.lastName, t.gender, se.level
FROM staging_events se
JOIN
    (SELECT userId, firstName, lastName, gender, MAX(ts) max_ts
    FROM staging_events
    WHERE page='NextSong'
    GROUP BY userId, firstName, lastName, gender) t
ON se.userId = t.userId and se.firstName = t.firstName and se.lastName = t.lastName 
AND se.gender = t.gender and se.ts = t.max_Ts;
""")

## Inserts data into songs with distinct values.
song_table_insert = ("""
SET search_path TO dist;

INSERT INTO songs
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs;
""")

## Inserts data into artists with their original name.
artist_table_insert = ("""
SET search_path TO dist;

INSERT INTO artists
SELECT DISTINCT t.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude
FROM staging_songs ss
JOIN
    (SELECT artist_id, MIN(song_id) song_id
    FROM staging_songs
    GROUP BY artist_id) t
ON ss.artist_id = t.artist_id
AND ss.song_id = t.song_id;
""")

# Converts 'ts' column's data type bigint into timestamp
# and insert data into time where page is 'NextSong'.
time_table_insert = ("""
SET search_path TO dist;

INSERT INTO time
SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' start_time,
    EXTRACT(hour from start_time),
    EXTRACT(day from start_time),
    EXTRACT(w from start_time),
    EXTRACT(month from start_time),
    EXTRACT(year from start_time),
    EXTRACT(dow from start_time)
FROM staging_events
WHERE page='NextSong';
""")

# QUERY LISTS

create_table_queries = [schema_create, staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]

drop_table_queries = [nodist_schema_table_drop, dist_schema_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [artist_table_insert, song_table_insert, user_table_insert, time_table_insert, songplay_table_insert]
