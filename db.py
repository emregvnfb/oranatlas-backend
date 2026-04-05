from contextlib import contextmanager
import psycopg
from psycopg.rows import dict_row
from settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def get_connection():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        row_factory=dict_row,
    )

@contextmanager
def get_cursor(commit=False):
    conn = get_connection()
    cur = conn.cursor()
    try:
        yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def fetch_one(query, params=None):
    with get_cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchone()

def fetch_all(query, params=None):
    with get_cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchall()

def execute(query, params=None):
    with get_cursor(commit=True) as cur:
        cur.execute(query, params or ())

def execute_many(query, params_seq):
    with get_cursor(commit=True) as cur:
        for params in params_seq:
            cur.execute(query, params)
