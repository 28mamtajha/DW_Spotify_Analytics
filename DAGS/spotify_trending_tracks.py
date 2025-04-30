from airflow import DAG # type: ignore
from airflow.decorators import task # type: ignore
from airflow.models import Variable # type: ignore
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook # type: ignore
from datetime import datetime, timedelta # type: ignore
import spotipy # type: ignore
from spotipy.oauth2 import SpotifyClientCredentials # type: ignore
import pandas as pd # type: ignore

# Function to return Snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()

# Function to get Spotify client
def get_spotify_client():
    client_id = Variable.get("Spotify_Client_ID")
    client_secret = Variable.get("Spotify_Client_Secret")
    
    if not client_id or not client_secret:
        raise ValueError("Spotify credentials not configured in Airflow Variables")
        
    return spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    ))

#Helper function to process track data with proper escaping
def process_track(track, album=None):
    return {
        'track_id': track.get('id', ''),
        'track_name': track.get('name', '').replace("'", "''"),
        'artists': ', '.join([artist.get('name', '').replace("'", "''") 
                             for artist in track.get('artists', [])]),
        'album_name': (album.get('name', '') if album 
                      else track.get('album', {}).get('name', '')).replace("'", "''"),
        'release_date': album.get('release_date', '') if album 
                       else track.get('album', {}).get('release_date', ''),
        'popularity': track.get('popularity', 0),
        'duration_ms': track.get('duration_ms', 0),
        'explicit': 1 if track.get('explicit', False) else 0
    }

@task
def extract_spotify_tracks():
    sp = get_spotify_client()
    tracks = []
    
    #new releases 
    try:
        new_releases = sp.new_releases(limit=50, country='US')
        for album in new_releases['albums']['items']:
            album_tracks = sp.album_tracks(album['id'], limit=50)
            for track in album_tracks['items']:
                tracks.append(process_track(track, album))
    except Exception as e:
        print(f"New releases has failed: {str(e)}")
        
        #featured playlists
        try:
            featured = sp.featured_playlists(country='US', limit=10)
            for playlist in featured['playlists']['items']:
                playlist_tracks = sp.playlist_tracks(playlist['id'], limit=50)
                for item in playlist_tracks['items']:
                    if item and 'track' in item:
                        tracks.append(process_track(item['track']))
        except Exception as e:
            print(f"Featured playlists has failed: {str(e)}")
            raise Exception("All data fetching methods have failed")
    
    if not tracks:
        raise Exception("No tracks to be found")
    
    return tracks

#task to transform raw tracks into required format
@task
def transform_tracks(raw_tracks):
    df = pd.DataFrame(raw_tracks)
    df = df.drop_duplicates('track_id')
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce').dt.date
    return df.to_dict('records')

#task to load data to snowflake table
@task
def load_to_snowflake(con, tracks, target_table):
    try:
        con.execute("BEGIN")

        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                track_id STRING,
                track_name STRING,
                artists STRING,
                album_name STRING,
                release_date DATE,
                popularity INTEGER,
                duration_ms INTEGER,
                explicit BOOLEAN,
                PRIMARY KEY (track_id)
            )
        """)

        con.execute(f"DELETE FROM {target_table};")

        for track in tracks:
            sql = f"""
                INSERT INTO {target_table} (
                    track_id, track_name, artists, album_name,
                    release_date, popularity, duration_ms, explicit
                )
                SELECT
                    '{track['track_id']}',
                    '{track['track_name']}',
                    '{track['artists']}',
                    '{track['album_name']}',
                    '{track['release_date']}',
                    {track['popularity']},
                    {track['duration_ms']},
                    {track['explicit']}
                WHERE NOT EXISTS (
                    SELECT 1 FROM {target_table} 
                    WHERE track_id = '{track['track_id']}'
                );
            """
            con.execute(sql)

        con.execute("COMMIT")
        print(f"Loaded {len(tracks)} records into {target_table}")

    except Exception as e:
        con.execute("ROLLBACK")
        print(f"Load failed: {str(e)}")
        raise


with DAG(
    dag_id='Spotify_Trending_Tracks_Realtime',
    start_date=datetime(2025, 4, 14),
    schedule_interval='15 21 * * *',
    catchup=False,
    tags=["ETL", "Spotify"]
) as dag:
    
    target_table = "USER_DB_CAMEL.RAW.REALTIME_TRENDING_TRACKS"
    conn = return_snowflake_conn()
    raw_tracks = extract_spotify_tracks()
    transformed_tracks = transform_tracks(raw_tracks)
    load_to_snowflake(conn, transformed_tracks, target_table)