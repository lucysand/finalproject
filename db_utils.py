import sqlite3

DB_NAME = "trees.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    # Trees table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trees (
            tree_id INTEGER PRIMARY KEY,
            borough TEXT
        )
    """)

    # Demographics NTA table (correct table name + correct columns)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS demographics_nta (
            nta_code TEXT PRIMARY KEY,
            neighborhood TEXT,
            borough TEXT,
            median_income REAL,
            poverty_rate REAL,
            pct_white REAL,
            pct_black REAL,
            pct_latino REAL,
            pct_asian REAL
        )
    """)

    conn.commit()
    conn.close()


def row_exists(table, key_column, key_value):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        f"SELECT 1 FROM {table} WHERE {key_column} = ? LIMIT 1",
        (key_value,)
    )

    result = cur.fetchone()
    conn.close()
    return result is not None
