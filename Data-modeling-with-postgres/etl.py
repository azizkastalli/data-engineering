import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from io import StringIO

def process_song_file(cur, filepath):
    """
    - Reads the song meta-data json file
    - Inserts a record from json file into the songs table
    - Inserts a record from json file into the artists table
    """
    
    # open song file
    df = pd.read_json(filepath,  lines=True)

    # insert song record
    song_data = df[['song_id', 'title' , 'artist_id', 'year', 'duration']].values[0]
    print(song_data)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[[
    'artist_id', 'artist_name',
    'artist_location', 'artist_latitude',
    'artist_longitude'
    ]].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads the log meta-data json file
    - Inserts a record from json file into the time and users tables
    - Inserts songplay records into the songplay table
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    df.ts = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    time_data = (
    df.ts.values,
    df.ts.dt.hour.values,
    df.ts.dt.day.values,
    df.ts.dt.dayofyear.values,
    df.ts.dt.month.values,
    df.ts.dt.year.values,
    df.ts.dt.weekday.values
    )
    column_labels = (
        'timestamp', 'hour', 'day', 'week of year',
        'month', 'year', 'weekday'
    )
    time_df = pd.DataFrame(
    dict((key, value) for key, value in zip(column_labels, time_data))
    )

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, row)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts, row.userId, row.level,
            songid, artistid,
            row.sessionId, row.location,
            row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Extracts all json files paths into a list
    - Prints the total number of files found
    - Interates over all json files and process them 
    by calling the "func" function to process log files
    or song files.
    """
        
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Establishes connection with the sparkify database and gets
    cursor to it.
    - Calls the "process_data" function by setting "func" parameter
    to "process_song_file" function to process the song meta-sata
    - Calls the "process_data" function by setting "func" parameter
    to "process_log_file" function to process the log meta-sata
    - Closes connection with the sparkify database.
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()