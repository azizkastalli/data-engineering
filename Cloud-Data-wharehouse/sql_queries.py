import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = """DROP TABLE IF EXISTS users """
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR,
        iteminSession INTEGER,
        lastName VARCHAR,
        length DOUBLE PRECISION,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts  BIGINT,
        userAgent VARCHAR,
        userId INTEGER distkey
    );
""")

staging_songs_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INTEGER,
        artist_id VARCHAR, 
        artist_latitude DOUBLE PRECISION, 
        artist_longitude DOUBLE PRECISION, 
        artist_location VARCHAR , 
        artist_name VARCHAR, 
        song_id VARCHAR distkey,
        title VARCHAR, 
        duration DOUBLE PRECISION, 
        year INTEGER
    );
""")


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
    songplay_id INTEGER IDENTITY(0, 1) PRIMARY KEY,
    start_time  BIGINT NOT NULL sortkey NOT NULL, 
    user_id INTEGER NOT NULL,
    level VARCHAR , 
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL, 
    session_id VARCHAR,
    location VARCHAR ,
    user_agent VARCHAR,
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR ,
    level VARCHAR 
    )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR  PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INTEGER,
    duration DOUBLE PRECISION
    )
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR  PRIMARY KEY,
    name VARCHAR,
    location VARCHAR ,
    lattitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
    start_time  BIGINT PRIMARY KEY distkey sortkey,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
    )
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {}
    iam_role {}
    region 'us-west-2'
    json {};
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH']
)

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    region 'us-west-2'
    format as json 'auto';
""").format(
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN']
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT ev.ts, ev.userId, ev.level, s.song_id, s.artist_id, ev.sessionId, s.artist_location, ev.userAgent
        FROM staging_songs s
        RIGHT JOIN  staging_events ev ON 
        (s.title = ev.song) and 
        (s.artist_name = ev.artist)
        
        WHERE
            (ev.userId is not null) and 
            (s.song_id is not null) and
            (s.artist_id is not null) and
            ev.page = 'NextSong'
            
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)

    WITH uniq_staging_events AS (
        SELECT userid, firstname, lastname, gender, level, 
               ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) AS rank
        FROM staging_events
        WHERE
        (userid is not null) AND (firstname is not null) AND (lastname is not null)
         )
         
     SELECT userid, firstname, lastname, gender, level FROM uniq_staging_events
     WHERE rank = 1;
""")



song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
        SELECT DISTINCT s.song_id, s.title, s.artist_id, s.year, s.duration
        FROM staging_songs s
        WHERE
            (s.song_id is not null) and (s.title is not null) and (s.artist_id is not null);
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, lattitude, longitude)
        SELECT DISTINCT s.artist_id, s.artist_name, s.artist_location, s.artist_latitude, s.artist_longitude
        FROM staging_songs s
        WHERE
            (s.artist_id is not null) and (s.artist_name is not null);
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)

    WITH uniq_staging_events AS (
        SELECT TRUNC(TIMESTAMP 'epoch' + ev.ts / 1000 * INTERVAL '1 second') as ts, ts as start_time, 
               ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) AS rank
        FROM staging_events ev
        WHERE  
        (ev.ts is not null) AND (ev.userid is not null)
    )
        SELECT  start_time, 
                extract(hour from ts) as hour,
                extract(day from ts) as day, 
                extract(week from ts) as week, 
                extract(month from ts) as month, 
                extract(year from ts) as year, 
                extract(weekday from ts) as weekday
        FROM uniq_staging_events
        WHERE rank = 1;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]