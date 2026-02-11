from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import upsert_rows, delete_rows
from datawarehouse.data_transformation import transform_data
from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = 'yt_api'

@task
def staging_table():

    schema = 'staging'

    conn, cur = None, None

    try:    
        
        conn, cur = get_conn_cursor()

        YT_data = load_data()

        create_schema(schema)

        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        upsert_rows(cur, conn, schema, YT_data)

        ids_in_json = {row['video_id'] for row in YT_data}

        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table updated successfully.")
        
    except Exception as e:
        logger.error(f"An error occured during the update of {schema}.{table}: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)

@task
def core_table():

    schema = 'core'

    conn, cur = None, None

    try:    
        
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = set(get_video_ids(cur, schema))

        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        transformed_rows = []
        for row in rows:
            transformed_row = transform_data(dict(row))
            transformed_rows.append(transformed_row)

        upsert_rows(cur, conn, schema, transformed_rows)

        current_video_ids = {row["Video_ID"] for row in transformed_rows}
        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table updated successfully.")
        
    except Exception as e:
        logger.error(f"An error occured during the update of {schema}.{table}: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)