from airflow import DAG # type: ignore
from airflow.decorators import task # type: ignore
from airflow.models import Variable # type: ignore
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook # type: ignore
from datetime import datetime # type: ignore
import spotipy # type: ignore
from spotipy.oauth2 import SpotifyOAuth # type: ignore
import pandas as pd # type: ignore

#function for snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()

# function for spotify client
def get_spotify_client():
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=Variable.get("Spotify_Client_ID"),
        client_secret=Variable.get("Spotify_Client_Secret"),
        redirect_uri=Variable.get("Spotify_Redirect_URI"),
        scope="user-read-recently-played",
        cache_path="/opt/airflow/dags/.spotify_token_cache"
    ))

# task to extract new releases and their tracks
@task
def extract_new_releases():
    sp = get_spotify_client()
    new_releases = sp.new_releases(limit=50)
    tracks = []

    for album in new_releases['albums']['items']:
        album_id = album['id']
        album_name = album['name']
        release_date = album['release_date']
        tracks_in_album = sp.album_tracks(album_id)['items']

        for track in tracks_in_album:
            tracks.append({
                "track_id": track['id'],
                "track_name": track['name'],
                "artists": ", ".join([artist['name'] for artist in track['artists']]),
                "album_name": album_name,
                "release_date": release_date,
                "duration_ms": track['duration_ms'],
                "explicit": track['explicit'],
                "popularity": None
            })

    return tracks

# task for loading to snowflake table
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
                duration_ms INTEGER,
                explicit BOOLEAN,
                popularity INTEGER,
                PRIMARY KEY (track_id, release_date)
            );
        """)

        con.execute(f"DELETE FROM {target_table};")

        for track in tracks:
            sql = f"""
                INSERT INTO {target_table} (
                    track_id, track_name, artists, album_name,
                    release_date, duration_ms, explicit, popularity
                )
                SELECT
                    '{track['track_id'].replace("'", "''")}',
                    '{track['track_name'].replace("'", "''")}',
                    '{track['artists'].replace("'", "''")}',
                    '{track['album_name'].replace("'", "''")}',
                    '{track['release_date']}',
                    {track['duration_ms']},
                    {track['explicit']},
                    NULL
                WHERE NOT EXISTS (
                    SELECT 1 FROM {target_table} 
                    WHERE track_id = '{track['track_id'].replace("'", "''")}'
                    AND release_date = '{track['release_date']}'
                );
            """
            con.execute(sql)

        con.execute("COMMIT")
        print(f"Inserted {len(tracks)} records into {target_table}")

    except Exception as e:
        con.execute("ROLLBACK")
        print(f"Load failed: {str(e)}")
        raise

with DAG(
    dag_id="Spotify_New_Releases_Realtime",
    start_date=datetime(2025, 4, 14),
    schedule_interval='15 21 * * *',
    catchup=False,
    tags=["ETL", "Spotify"]
) as dag:

    target_table = "USER_DB_CAMEL.RAW.REALTIME_NEW_RELEASES"

    conn = return_snowflake_conn()
    tracks_data = extract_new_releases()
    load_to_snowflake(conn, tracks_data, target_table)