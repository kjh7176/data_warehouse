import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar(1),
    itemInSession int,
    lastName varchar,
    length decimal,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration decimal,
    sessionId int,
    song varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userId int)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    artist_id varchar, 
    artist_latitude decimal, 
    artist_location varchar, 
    artist_longitude decimal, 
    artist_name varchar, 
    duration decimal, 
    num_songs int, 
    song_id varchar, 
    title varchar, 
    year smallint)
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id int IDENTITY(0,1) PRIMARY KEY, 
    start_time timestamp REFERENCES time SORTKEY, 
    user_id varchar REFERENCES users DISTKEY, 
    level varchar NOT NULL, 
    song_id varchar REFERENCES songs, 
    artist_id varchar REFERENCES artists, 
    session_id int NOT NULL, 
    location varchar, 
    user_agent varchar)
""")

user_table_create = ("""
CREATE TABLE users(
    user_id varchar PRIMARY KEY DISTKEY SORTKEY, 
    first_name varchar NOT NULL, 
    last_name varchar NOT NULL, 
    gender varchar(1) NOT NULL, 
    level varchar NOT NULL)
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id varchar PRIMARY KEY SORTKEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL REFERENCES artists, 
    year smallint NOT NULL, 
    duration decimal NOT NULL)
    DISTSTYLE all;
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id varchar PRIMARY KEY SORTKEY, 
    name varchar NOT NULL, 
    location varchar, 
    latitude decimal, 
    longitude decimal)
    DISTSTYLE all;
""")

time_table_create = ("""
CREATE TABLE time(
    start_time timestamp PRIMARY KEY SORTKEY, 
    hour smallint, 
    day smallint, 
    weekofyear smallint, 
    month smallint, 
    year smallint, 
    weekday smallint)
    DISTSTYLE all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
iam_role '{}'
JSON {}
region 'us-west-2'
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
iam_role '{}'
json 'auto'
region 'us-west-2'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' start_time,
    se.userId, se.level, s.song_id, s.artist_id, se.sessionId, se.location, se.userAgent
FROM staging_events se
JOIN artists a ON a.name = se.artist
JOIN songs s ON s.title = se.song and s.duration = se.length
WHERE se.page='NextSong'
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE page='NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
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

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [artist_table_insert, song_table_insert, user_table_insert, time_table_insert, songplay_table_insert]
