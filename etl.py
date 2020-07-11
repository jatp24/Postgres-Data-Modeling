import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath): 
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    
    
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the user time information, transforms it in order to sore it into the times table.
    Then it extracts the user log information in order to store it into the users table.
    Finally, it extracts information from the songs and artist table to store it into the songplays table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page'] =='NextSong']

    # convert timestamp column to datetime
    df['start_time'] = df['ts'].apply(lambda row: pd.datetime.fromtimestamp(row/1000))
    df['hour'] = df['start_time'].apply(lambda row: row.hour)
    df['day'] = df['start_time'].apply(lambda row: row.day)
    df['week'] = df['start_time'].apply(lambda row: row.week)
    df['month'] = df['start_time'].apply(lambda row: row.month)
    df['year'] = df['start_time'].apply(lambda row: row.year)
    df['weekday'] = df['start_time'].apply(lambda row: row.strftime('%A'))
    df['start_time'] = (df['ts']).astype(str)

    
    # insert time data records
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'] 
    time_df = df[column_labels].copy()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()

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
            
            # insert songplay record

            songplay_data = (row.start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)
            
            # insert songplay record

        else:
            songid, artistid = None, None

        
        
        
        
        


def process_data(cur, conn, filepath, func):
    """
    This procedure stores all the file names in the directory with ending in *.json in the filepath.
    This procedure then loops over the list calling a function to execute over each file.

    
    INPUTS: 
    * cur the cursor variable
    *conn the connection to database.
    * filepath the file path to either the song files or log files.
    * function the function to execute over file.
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
    This procedure creates a connection to the postgres database.
    Then it creates a cur for the database.
    Finally it calls the functions above to execute.

    
    INPUTS: 
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()