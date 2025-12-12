# db_utils.py
import sqlite3
from pathlib import Path

# Always save the DB in the finalproject folder
DB_PATH = Path(__file__).parent / "nyc_data.db"

def get_connection():
    return sqlite3.connect(DB_PATH)

def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS trees (
            tree_id TEXT PRIMARY KEY,
            borough TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS demographics (
            borough TEXT PRIMARY KEY,
            black_percent REAL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS crime (
            cmplnt_num TEXT PRIMARY KEY,
            borough TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS collisions (
            collision_id TEXT PRIMARY KEY,
            borough TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS collision_injuries (
            collision_id TEXT PRIMARY KEY,
            persons_injured TEXT
        );
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_tables()
    print(f"Tables created in {DB_PATH}")
