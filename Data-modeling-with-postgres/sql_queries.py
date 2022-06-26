# DROP TABLES

songplay_table_drop = """DROP TABLE IF EXISTS songplays;"""
user_table_drop = """DROP TABLE IF EXISTS users;"""
song_table_drop = """DROP TABLE IF EXISTS songs;"""
artist_table_drop = """DROP TABLE IF EXISTS artists;"""
time_table_drop = """DROP TABLE IF EXISTS time;"""

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays \
(songplay_id INT PRIMARY KEY,\
 start_time DATE NOT NULL,\
 user_id INT NOT NULL,\
 level INT,\
 song_id VARCHAR NOT NULL,\
 artist_id VARCHAR NOT NULL,\
 session_id INT NOT NULL,\
 location VARCHAR(50),\
 user_agent VARCHAR(50),\
 FOREIGN KEY (start_time) REFERENCES time(start_time),\
 FOREIGN KEY (user_id) REFERENCES users(user_id),\
 FOREIGN KEY (song_id) REFERENCES songs(song_id),\
 FOREIGN KEY (artist_id) REFERENCES artists(artist_id));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users \
(user_id INT PRIMARY KEY,
 first_name VARCHAR(50),\
 last_name VARCHAR(50),\
 gender VARCHAR(20),\
 level INT);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs \
(song_id VARCHAR PRIMARY KEY,\
 title VARCHAR(50),\
 artist_id VARCHAR NOT NULL,\
 year INT,\
 duration REAL);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists \
(artist_id VARCHAR PRIMARY KEY,\
 name VARCHAR(50),\
 location VARCHAR(50),\
 latitude REAL,\
 longitude REAL);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time \
(start_time DATE PRIMARY KEY,\
 hour INT,\
 day INT,\
 week INT,\
 month INT,\
 year INT,\
 weekday INT);
""")

# INSERT RECORDS
songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s);""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s);""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s);""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s);""")

# FIND SONGS

song_select = ("""

""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]