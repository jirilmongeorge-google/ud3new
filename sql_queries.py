import configparser


# CONFIG
config = configparser.ConfigParser()
#config.read('dwh.cfg')
config.read_file(open('dwh.cfg'))
KEY=config.get('AWS','key')
SECRET= config.get('AWS','secret')
IAM_ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events "
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs "
songplay_table_drop = "DROP TABLE IF EXISTS songplays "
user_table_drop = "DROP TABLE IF EXISTS users "
song_table_drop = "DROP TABLE IF EXISTS songs "
artist_table_drop = "DROP TABLE IF EXISTS artists "
time_table_drop = "DROP TABLE IF EXISTS time "

# CREATE TABLES
#startTime, userId, level, songId, artistId, sessionId, location, userAgent
staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist text,
    auth varchar(25),
    firstName varchar(25),
    gender varchar(5),
    itemInSession int,
    lastName varchar(25),
    length float,
    level varchar(25),
    location text,
    method varchar(25),
    page varchar(25),
    registration text,
    sessionId int,
    song text,
    status int,
    ts bigint,
    userAgent text,
    userId int
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs int,
    artist_id varchar(100),
    artist_latitude float,
    artist_longitude float,
    artist_location text,
    artist_name text,
    song_id varchar(100),
    title text,
    duration float,
    year int
)
""")

songplay_table_create = ("""
CREATE TABLE songplays(songplay_id INT IDENTITY(0,1), start_time timestamp, 
user_id int NOT NULL, level varchar, song_id varchar NOT NULL, artist_id varchar NOT NULL, session_id int, location varchar, user_agent varchar, PRIMARY KEY(songplay_id))
""")

user_table_create = ("""
CREATE TABLE users(user_id int NOT NULL, first_name varchar, last_name varchar, gender varchar, level varchar, PRIMARY KEY(user_id))
""")

song_table_create = ("""
CREATE TABLE songs(song_id varchar NOT NULL, title varchar, artist_id varchar, year int, duration float, PRIMARY KEY(song_id))
""")

artist_table_create = ("""
CREATE TABLE artists(artist_id varchar  NOT NULL, name varchar, location varchar, latitude float, longitude  float, PRIMARY KEY(artist_id))
""")

time_table_create = ("""
CREATE TABLE time (start_time timestamp NOT NULL, hour varchar, day varchar, week varchar, month varchar, year varchar, weekday varchar, PRIMARY KEY(start_time))
""")

# STAGING TABLES

staging_events_copy = ("""
copy {} from 's3://udacity-dend/log_data/'  
credentials 'aws_iam_role={}'
JSON 's3://udacity-dend/log_json_path.json' 
region 'us-west-2' 

""").format('staging_events', IAM_ROLE_ARN)

staging_songs_copy = ("""
copy {} from 's3://udacity-dend/song_data/'  
credentials 'aws_iam_role={}'
format as json 'auto'  
region 'us-west-2' 
""").format('staging_songs', IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)  
SELECT DISTINCT (timestamp 'epoch' + e.ts/1000 * interval '1 second'), e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent  
FROM staging_events e JOIN staging_songs s ON e.song=s.title  AND e.artist=s.artist_name  
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level )  
SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration) 
SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude) 
SELECT DISTINCT s.artist_id,s.artist_name,e.location,s.artist_latitude,s.artist_longitude 
FROM staging_events e JOIN staging_songs s ON e.song=s.title  AND e.artist=s.artist_name  
""")

time_table_insert = ("""
INSERT INTO time(start_time , hour , day , week , month, year, weekday)  
SELECT DISTINCT (timestamp 'epoch' + ts/1000 * interval '1 second'), extract(hour from (timestamp 'epoch' + ts/1000 * interval '1 second')), extract(day from (timestamp 'epoch' + ts/1000 * interval '1 second')), extract(week from (timestamp 'epoch' + ts/1000 * interval '1 second')), extract(month from (timestamp 'epoch' + ts/1000 * interval '1 second')), extract(year from (timestamp 'epoch' + ts/1000 * interval '1 second')), extract(dow from (timestamp 'epoch' + ts/1000 * interval '1 second')) FROM staging_events  
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
