from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

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

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='A DAG to extract YouTube video stats and save to JSON',
    schedule_interval='0 14 * * *',  # Daily at 14:00 UTC,
    catchup=False
) as dag:
    
    # Define tasks
    playlistId = get_playlist_id()
    video_ids = get_video_ids(playlistId)
    video_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(video_data)

    # Define task dependencies

    playlistId >> video_ids >> video_data >> save_to_json_task
