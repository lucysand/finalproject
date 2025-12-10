import sqlite3

DB_NAME = "nyc_data.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def row_exists(table, key_col, key_val):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM {table} WHERE {key_col} = ?", (key_val,))
    exists = cur.fetchone() is not None
    conn.close()
    return exists

def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    # Trees table (nta_name placeholder for now)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trees (
            tree_id INTEGER PRIMARY KEY,
            nta_name TEXT
        )
    """)

    # Demographics table (nta_name only)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS demographics (
            nta_name TEXT PRIMARY KEY,
            median_income REAL,
            poverty_rate REAL,
            pct_white REAL,
            pct_black REAL,
            pct_latino REAL,
            pct_asian REAL
        )
    """)

    # Health wide-format table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS health_demographics_wide (
            nta_name TEXT PRIMARY KEY,
            child_asthma REAL,
            physical_activity REAL
        )
    """)

    conn.commit()
    conn.close()


