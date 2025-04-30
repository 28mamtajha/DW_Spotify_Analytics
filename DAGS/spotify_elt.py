from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook


DBT_PROJECT_DIR = "/opt/airflow/SPOTIFY_ELT_PROJECT"
DBT_BIN = "/home/airflow/.local/bin/dbt"

def check_dbt_results(context):
    task_instance = context['task_instance']
    result = task_instance.xcom_pull(task_ids='dbt_test')
    if 'Failed' in str(result):
        raise Exception('DBT tests failed!')


conn = BaseHook.get_connection('snowflake_conn')

with DAG(
    dag_id='Spotify_ELT',
    start_date=datetime(2025, 4, 1),
    is_paused_upon_creation=False,
    description='DAG to invoke dbt runs using a BashOperator',
    schedule='15 22 * * *',
    catchup=False,
    tags=["ETL", "Spotify"],
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_SCHEMA": conn.schema,
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }
    },
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env={"HOME": "/home/airflow",
             **dag.default_args["env"]
            }
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env={"HOME": "/home/airflow",
             **dag.default_args["env"]
            }
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env={"HOME": "/home/airflow",
             **dag.default_args["env"]
            }
    )


    dbt_run >> dbt_test >> dbt_snapshot