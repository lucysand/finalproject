# db_utils.py
import sqlite3
from pathlib import Path

# Centralized database path
DB_PATH = Path(__file__).parent / "nyc_data.db"

def get_connection():
    """Return a connection to the SQLite database."""
    return sqlite3.connect(DB_PATH)

def create_tables():
    """Create all necessary tables if they don't exist."""
    conn = get_connection()
    cur = conn.cursor()

    # Boroughs table (universal numbering system)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS boroughs (
            borough_id INTEGER PRIMARY KEY,
            borough_name TEXT UNIQUE
        );
    """)
    boroughs = [
        (1, "Manhattan"),
        (2, "Brooklyn"),
        (3, "Queens"),
        (4, "Bronx"),
        (5, "Staten Island")
    ]
    cur.executemany("""
        INSERT OR IGNORE INTO boroughs (borough_id, borough_name) VALUES (?, ?)
    """, boroughs)

    # Demographics table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS demographics (
            borough_id INTEGER PRIMARY KEY,
            black_percent REAL
        );
    """)

    # Collisions main table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS collisions (
            collision_id TEXT PRIMARY KEY,
            borough_id INTEGER
        );
    """)

    # Collisions secondary table: injuries
    cur.execute("""
        CREATE TABLE IF NOT EXISTS collision_injuries (
            collision_id TEXT PRIMARY KEY,
            persons_injured INTEGER
        );
    """)

    # Weather table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            weather_id INTEGER PRIMARY KEY,
            borough_id INTEGER,
            temp REAL,
            humidity REAL,
            wind_speed REAL
        );
    """)

    # Yelp table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS yelp (
            yelp_id TEXT PRIMARY KEY,
            borough_id INTEGER,
            rating REAL
        );
    """)

    # Offsets table (track last inserted row per table)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS offsets (
            name TEXT PRIMARY KEY,
            offset INTEGER
        );
    """)

    conn.commit()
    conn.close()
    print(f"Database created at: {DB_PATH}")

def borough_to_id(name):
    """Return the integer ID of a borough name."""
    mapping = {
        "Manhattan": 1,
        "Brooklyn": 2,
        "Queens": 3,
        "Bronx": 4,
        "Staten Island": 5
    }
    return mapping.get(name)

# ----- Offset utilities -----
def get_offset(table_name):
    """Return current offset for a table, default 0 if not exists."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT offset FROM offsets WHERE name=?", (table_name,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 0

def update_offset(name, new_offset, conn=None):
    """Update the offset for a table after inserting rows."""
    own_conn = False
    if conn is None:
        conn = get_connection()
        own_conn = True
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO offsets (name, offset)
        VALUES (?, ?)
        ON CONFLICT(name) DO UPDATE SET offset = excluded.offset
    """, (name, new_offset))
    if own_conn:
        conn.commit()
        conn.close()




if __name__ == "__main__":
    create_tables()
