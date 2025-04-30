from airflow import DAG # type: ignore
from airflow.decorators import task # type: ignore
from airflow.models import Variable # type: ignore
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook # type: ignore
from datetime import datetime # type: ignore
import spotipy # type: ignore
from spotipy.oauth2 import SpotifyClientCredentials # type: ignore

#function for snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()

# function for spotify client
@task
def extract_spotify_albums():
    client_id = Variable.get("Spotify_Client_ID")
    client_secret = Variable.get("Spotify_Client_Secret")
    
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    ))

    results = sp.new_releases(limit=20)
    return results['albums']['items']

@task
def transform_albums(raw_albums):
    data = []
    for album in raw_albums:
        data.append({
            'album_id': album['id'],
            'album_name': album['name'],
            'artist_id': album['artists'][0]['id'],
            'artist_name': album['artists'][0]['name'],
            'release_date': album['release_date'],
            'album_type': album['album_type'],
            'popularity': None,
            'num_tracks': album['total_tracks'],
            'external_url': album['external_urls']['spotify'],
            'image_url': album['images'][0]['url'] if album['images'] else None
        })
    return data

@task
def load_to_snowflake(con, albums, target_table):
    try:
        con.execute("BEGIN")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                album_id STRING,
                album_name STRING,
                artist_id STRING,
                artist_name STRING,
                release_date DATE,
                album_type STRING,
                popularity INTEGER,
                num_tracks INTEGER,
                external_url STRING,
                image_url STRING,
                PRIMARY KEY (album_id, release_date)
            )
        """)
        con.execute(f"DELETE FROM {target_table};")

        for album in albums:
            image_url_value = f"'{album['image_url'].replace("'", "''")}'" if album['image_url'] else 'NULL'
            sql = f"""
                INSERT INTO {target_table} (
                    album_id, album_name, artist_id, artist_name,
                    release_date, album_type, popularity, num_tracks,
                    external_url, image_url
                )
                SELECT
                    '{album['album_id'].replace("'", "''")}',
                    '{album['album_name'].replace("'", "''")}',
                    '{album['artist_id'].replace("'", "''")}',
                    '{album['artist_name'].replace("'", "''")}',
                    '{album['release_date']}',
                    '{album['album_type']}',
                    NULL,
                    {album['num_tracks']},
                    '{album['external_url']}',
                    {image_url_value}
                WHERE NOT EXISTS (
                    SELECT 1 FROM {target_table} 
                    WHERE album_id = '{album['album_id'].replace("'", "''")}'
                    AND release_date = '{album['release_date']}'
                );
            """
            con.execute(sql)

        con.execute("COMMIT")
        print(f"Inserted {len(albums)} records into {target_table}")

    except Exception as e:
        con.execute("ROLLBACK")
        print(f"Load failed: {str(e)}")
        raise


with DAG(
    dag_id='Spotify_Top_Albums_Realtime',
    start_date=datetime(2025, 4, 1),
    catchup=False,
    schedule_interval='15 21 * * *',
    tags=["ETL", "Spotify"]
) as dag:

    target_table = "USER_DB_CAMEL.RAW.REALTIME_TOP_ALBUMS"
    
    conn = return_snowflake_conn()

    raw = extract_spotify_albums()
    transformed = transform_albums(raw)
    load_to_snowflake(conn, transformed, target_table)
