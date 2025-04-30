from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_db_structure(con):
    try:
        con.execute("CREATE DATABASE IF NOT EXISTS user_db_camel;")
        con.execute("CREATE SCHEMA IF NOT EXISTS user_db_camel.raw;")
        
        # Internal stage creation
        con.execute("""
            CREATE STAGE IF NOT EXISTS user_db_camel.raw.spotify_hist_stage
            DIRECTORY = ( ENABLE = true );
        """)

        con.execute("""
            CREATE FILE FORMAT IF NOT EXISTS user_db_camel.raw.spotify_file_format
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            FIELD_DELIMITER = ","
            DATE_FORMAT = "AUTO"
            TIMESTAMP_FORMAT = "AUTO"
            NULL_IF = ('');
        """)

    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e

@task
def create_table(con, table):
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table} (
        track_id STRING,
        artists STRING,
        album_name STRING,
        track_name STRING,
        popularity STRING,
        duration_ms STRING,
        explicit STRING,
        danceability STRING,
        energy STRING,
        key STRING,
        loudness STRING,
        mode STRING,
        speechiness STRING,
        acousticness STRING,
        instrumentalness STRING,
        liveness STRING,
        valence STRING,
        tempo STRING,
        time_signature STRING,
        track_genre STRING,
        PRIMARY KEY (track_id)
    );
    """)

@task
def load_records(con, database, schema, table):
    try:
        con.execute("BEGIN;")
        con.execute(f"DELETE FROM {table};")

        con.execute(f"""
            COPY INTO {table}
            FROM @{database}.{schema}.spotify_hist_stage
            FILE_FORMAT = (FORMAT_NAME = {database}.{schema}.spotify_file_format);
        """)

        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id='Spotify_Hist_Load_ETL',
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=["ETL", "Spotify"],
    schedule='30 20 * * *'
) as dag:

    hist_table = "user_db_camel.raw.music_data_hist"
    database = "user_db_camel"
    schema = "raw"

    cur = return_snowflake_conn()

    create_db_structure(cur) >> create_table(cur, hist_table) >> load_records(cur, database, schema, hist_table)
