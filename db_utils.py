import sqlite3

DB_PATH = "nyc_data.db"

def get_connection():
    return sqlite3.connect(DB_PATH)

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

    # Demographics table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS demographics (
            borough TEXT PRIMARY KEY,
            black_percent REAL
        )
    """)

    # Crime table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crime (
            complaint_num INTEGER PRIMARY KEY,
            borough TEXT,
            offense TEXT
        )
    """)

    # Collisions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS collisions (
            collision_id INTEGER PRIMARY KEY,
            borough TEXT,
            date TEXT
        )
    """)

    # Collision injuries table (linked via integer key)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS collision_injuries (
            collision_id INTEGER PRIMARY KEY,
            persons_injured INTEGER,
            FOREIGN KEY(collision_id) REFERENCES collisions(collision_id)
        )
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_tables()
    print("Tables created (or already exist)")
