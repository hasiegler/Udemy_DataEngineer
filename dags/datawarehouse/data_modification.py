import logging

from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)
table = "yt_api"


def _staging_row_to_tuple(row):
    """Map staging API row to table column order."""
    return (
        row["video_id"],
        row["title"],
        row["publishedAt"],
        row["duration"],
        row["viewCount"],
        row["likeCount"],
        row["commentCount"],
    )


def _core_row_to_tuple(row):
    """Map core (transformed) row to table column order."""
    return (
        row["Video_ID"],
        row["Video_Title"],
        row["Upload_Date"],
        row["Duration"],
        row["Video_Type"],
        row["Video_Views"],
        row["Likes_Count"],
        row["Comments_Count"],
    )


def upsert_rows(cur, conn, schema, rows):
    """
    Bulk upsert: insert all rows, updating on conflict (Video_ID).
    Preserves same logic as previous insert_rows/update_rows (staging vs core column mapping).
    """
    if not rows:
        return
    try:
        if schema == "staging":
            columns = (
                '"Video_ID", "Video_Title", "Upload_Date", "Duration", '
                '"Video_Views", "Likes_Count", "Comments_Count"'
            )
            tuples = [_staging_row_to_tuple(row) for row in rows]
            sql = (
                f'INSERT INTO {schema}.{table} ({columns}) VALUES %s '
                f'''ON CONFLICT ("Video_ID") DO UPDATE SET
                "Video_Title" = EXCLUDED."Video_Title",
                "Video_Views" = EXCLUDED."Video_Views",
                "Likes_Count" = EXCLUDED."Likes_Count",
                "Comments_Count" = EXCLUDED."Comments_Count"
                '''
            )
        else:
            columns = (
                '"Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", '
                '"Video_Views", "Likes_Count", "Comments_Count"'
            )
            tuples = [_core_row_to_tuple(row) for row in rows]
            sql = (
                f'INSERT INTO {schema}.{table} ({columns}) VALUES %s '
                f'''ON CONFLICT ("Video_ID") DO UPDATE SET
                "Video_Title" = EXCLUDED."Video_Title",
                "Video_Views" = EXCLUDED."Video_Views",
                "Likes_Count" = EXCLUDED."Likes_Count",
                "Comments_Count" = EXCLUDED."Comments_Count"
                '''
            )
        execute_values(cur, sql, tuples, page_size=500)
        conn.commit()
        logger.info(f"Upserted {len(rows)} rows into {schema}.{table}")
    except Exception as e:
        logger.error(f"Error bulk upserting into {schema}.{table}: {e}")
        raise e
    
def delete_rows(cur, conn, schema, ids_to_delete):
    
    try:
        
        ids_formatted = f"""({', '.join(f"'{id}'" for id in ids_to_delete)})"""

        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN {ids_formatted};
            """
        )

        conn.commit()

        logger.info(f"Deleted rows with Video_IDs: {ids_to_delete}")

    except Exception as e:
        logger.error(f"Error deleting rows with Video_IDs: {ids_to_delete} - {e}")
        raise e