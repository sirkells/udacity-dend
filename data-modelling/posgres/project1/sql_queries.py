# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
create table if not exists songplays \
    (songplay_id serial primary key, start_time timestamp not null, user_id int not null, level varchar, song_id varchar, \
    artist_id varchar, session_id int, location varchar, user_agent text)
""")

user_table_create = ("""
create table if not exists users \
    (user_id int primary key, first_name varchar, last_name varchar, \
    gender varchar, level varchar)
""")

song_table_create = ("""
create table if not exists songs \
    (song_id varchar primary key, title varchar, artist_id varchar, \
    year int, duration float8)
""")

artist_table_create = ("""
create table if not exists artists \
    (artist_id varchar primary key, name varchar, location varchar, \
    latitude float8, longitude float8)
""")

time_table_create = ("""
create table if not exists time \
    (start_time timestamp primary key, hour int, day int, week int, month int, year int, weekday varchar)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, \
    artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) \
    ON CONFLICT (songplay_id) DO NOTHING;
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) \
ON CONFLICT (user_id) do update set level=excluded.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) \
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) \
ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) \
ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT song_id, artists.artist_id from \
    (songs join artists on songs.artist_id = artists.artist_id) \
    WHERE (songs.title = %s AND artists.name = %s AND songs.duration = %s)
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]