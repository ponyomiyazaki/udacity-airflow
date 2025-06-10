import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
(artist varchar(200),
auth varchar(50),
firstName varchar(50),
gender varchar(1),
itemInSession smallint,
lastName varchar(50),
length float,
level varchar(5),
location varchar(100),
method varchar(5),
page varchar(100),
registration float,
sessionId smallint,
song varchar(500),
status smallint,
ts timestamp,
userAgent varchar(500),
userId smallint);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(num_songs int,
artist_id varchar(100),
artist_latitude float,
artist_longitude float,
artist_location varchar(500),
artist_name varchar(200),
song_id varchar(100),
title varchar(500),
duration float,
year int);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
(songplay_id int PRIMARY KEY, 
start_time timestamp NOT NULL, 
user_id smallint NOT NULL, 
level varchar(5) NOT NULL,
song_id varchar(100) NOT NULL,
artist_id varchar(100) NOT NULL,
session_id smallint NOT NULL,
location varchar(100) NOT NULL,
user_agent varchar(500) NOT NULL);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
(user_id smallint PRIMARY KEY, 
first_name varchar(50) NOT NULL,
last_name varchar(50) NOT NULL, 
gender varchar(1) NOT NULL, 
level varchar(5) NOT NULL);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song
(song_id varchar(100) PRIMARY KEY, 
title varchar(500) NOT NULL, 
artist_id varchar(100) NOT NULL, 
year int NOT NULL, 
duration float NOT NULL);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist
(artist_id varchar(100) PRIMARY KEY, 
name varchar(200) NOT NULL, 
location varchar(100) NOT NULL, 
latitude float,
longitude float);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
(start_time timestamp PRIMARY KEY, 
hour smallint NOT NULL,
day smallint NOT NULL,
week smallint NOT NULL, 
month smallint NOT NULL, 
year smallint NOT NULL, 
weekday varchar(10) NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    iam_role '{}'
    region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    JSON {};
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role '{}'
    region 'us-west-2'
    JSON 'auto';
""").format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays
(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT e.sessionId as songplay_id,
       e.ts as start_time,
       e.userId as user_id,
       e.level,
       s.songId as song_id,
       s.artistId as artist_id,
       e.sessionId as session_id,
       e.location,
       e.userAgent as user_agent
FROM staging_events e
JOIN staging_songs s
ON e.artist = s.name;
""")

user_table_insert = ("""INSERT INTO users
(user_id, first_name, last_name, gender, level)
SELECT userID as user_id,
       firstName as first_name,
       lastName as last_name,
       gender,
       level,
FROM staging_events;
""")

song_table_insert = ("""INSERT INTO song
(song_id, title, artist_id, year, duration)
SELECT song_id,
       title,
       artist_id,
       year,
       duration
FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artist
(artist_id, name, location, latitude, longitude)
SELECT artist_id,
       artist_name as name,
       artist_location as location,
       artist_latitude as latitude,
       artist_longitude as longitude
FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time
(start_time, hour, day, week, month, year, weekday)
SELECT ts as start_time,
       EXTRACT(HOUR FROM start_time) as hour,
       EXTRACT(DAY FROM start_time) as day,
       EXTRACT(WEEK FROM start_time) as week,
       EXTRACT(MONTH FROM start_time) as month,
       EXTRACT(YEAR FROM start_time) as year,
       EXTRACT(DOW FROM start_time) as weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]