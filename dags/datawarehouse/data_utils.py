import os
import socket

from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from psycopg2.extras import RealDictCursor

table = "yt_api"


def _resolve_ipv4(host):
    """Resolve host to IPv4 address. Avoids 'Network is unreachable' when Docker resolves to IPv6."""
    try:
        results = socket.getaddrinfo(host, None, socket.AF_INET)
        if results:
            return results[0][4][0]
    except (socket.gaierror, OSError):
        pass
    return host


def get_conn_cursor():
    # When running in Docker (e.g. WSL2), Supabase hostname can resolve to IPv6 which is unreachable.
    # Connect via IPv4 explicitly when SUPABASE_HOST is set.
    supabase_host = os.getenv("SUPABASE_HOST")
    if supabase_host:
        host = _resolve_ipv4(supabase_host)
        port = int(os.getenv("SUPABASE_PORT", "5432"))
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=os.getenv("SUPABASE_DB", "postgres"),
            user=os.getenv("SUPABASE_USER"),
            password=os.getenv("SUPABASE_PASSWORD"),
            sslmode="require",
            cursor_factory=RealDictCursor,
        )
        cur = conn.cursor()
        return conn, cur
    hook = PostgresHook(postgres_conn_id="POSTGRES_DB_YT_ELT", database="postgres")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()

def create_schema(schema):

    conn, cur = get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)

    conn.commit()

    close_conn_cursor(conn, cur)


def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" VARCHAR(20) NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comments_Count" INT
        );
        """
    else:
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" TIME NOT NULL,
            "Video_Type" VARCHAR(10) NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comments_Count" INT
        );
        """

    cur.execute(table_sql)

    conn.commit()

    close_conn_cursor(conn, cur)

def get_video_ids(cur, schema):

    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()

    video_ids = [row['Video_ID'] for row in ids]

    return video_ids

