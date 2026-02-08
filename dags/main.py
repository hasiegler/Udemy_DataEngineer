from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

# Define the local timezone
local_tz = pendulum.timezone("America/Los_Angeles")

# Default arguments

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    #"end_date": datetime(2030, 12, 31, tzinfo=local_tz),
}

# Variables
staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='A DAG to extract YouTube video stats and save to JSON',
    schedule='0 14 * * *',  # Daily at 14:00 UTC,
    catchup=False
) as dag_produce:
    
    # Define tasks
    playlistId = get_playlist_id()
    video_ids = get_video_ids(playlistId)
    video_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(video_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id='trigger_update_db',
        trigger_dag_id='update_db'
    )

    # Define task dependencies

    playlistId >> video_ids >> video_data >> save_to_json_task >> trigger_update_db

with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='A DAG to process JSON file and insert data into both staging and core schemas',
    schedule=None,
    catchup=False
) as dag_update:
    
    # Define tasks

    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id='trigger_data_quality',
        trigger_dag_id='data_quality'
    )

    # Define task dependencies

    update_staging >> update_core >> trigger_data_quality

with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description='A DAG to check the data quality of staging and core schemas in the db',
    schedule=None,
    catchup=False
) as dag_quality:
    
    # Define tasks

    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    # Define task dependencies

    soda_validate_staging >> soda_validate_core