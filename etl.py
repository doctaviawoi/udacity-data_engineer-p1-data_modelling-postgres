import os
import glob
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    ########################################### artists table
    # insert artist record
    artist_data = df.loc[:, ('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)
    
    ########################################### songs table
    # insert song record
    song_data = df.loc[:, ('song_id', 'title', 'artist_id', 'year', 'duration')].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page=='NextSong']
    
    ########################################### time table
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms').astype('datetime64[s]')
    
    # batch insert time data records into time table
    time_data = (t, t.dt.time, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('timestamp', 'time', 'hour', 'day', 'week of year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    execute_batch(cur, time_table_insert, time_df.values.tolist())
    
    ########################################### users table
    # load user table
    user_df = df.loc[:, ('userId', 'firstName', 'lastName', 'gender', 'level')]

    # batch insert user records into users table
    execute_batch(cur, user_table_insert, user_df.values.tolist())
    
    
    ########################################### songplays table
    # insert songplay records
    songid_songselect_results = []
    artistid_songselect_results = []

    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, round(row.length,5)))
        results = list(cur.fetchone())
        songid, artistid = results #if results else None, None
        songid_songselect_results.append(songid)
        artistid_songselect_results.append(artistid)
        
    # assign the appended songid and artistid from song_select query as new columns in df dataframe
    df = df.assign(songselect_songid=songid_songselect_results, songselect_artistid=artistid_songselect_results)
    
    # convert ts in epoch ms to datetime
    df['timestamp'] = pd.to_datetime(df['ts'], unit='ms').astype('datetime64[s]')
    
    # insert songplay record
    songplay_df = df.loc[:, ['timestamp', 'userId', 'level', 'songselect_songid', 'songselect_artistid', 'sessionId', 'location', 'userAgent']]
    songplay_data = songplay_df.values.tolist()
    
    execute_batch(cur, songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()