import psycopg2.extras
import settings

conn = None


def get_conn():
    global conn
    if conn is None:
        conn = psycopg2.connect(settings.DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn
