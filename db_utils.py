import sqlite3

DB_PATH = "nyc_data.db"

def get_connection():
    """Return a new SQLite connection."""
    return sqlite3.connect(DB_PATH)

# -------------------------------------------------------------------
# TABLE CREATION
# -------------------------------------------------------------------

def create_all_tables():
    """Creates all tables if they do not already exist."""
    conn = get_connection()
    cur = conn.cursor()

    # ----- TREES TABLE -----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trees (
            tree_id INTEGER PRIMARY KEY,
            status TEXT,
            health TEXT,
            borough TEXT,
            spc_common TEXT
        )
    """)

    # ----- DEMOGRAPHICS TABLE -----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS demographics (
            borough TEXT PRIMARY KEY,
            black_percent REAL
        )
    """)

    # ----- COLLISIONS TABLE -----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS collisions (
            collision_id INTEGER PRIMARY KEY,
            borough TEXT,
            persons_injured INTEGER
        )
    """)

    # ----- CRIME TABLE -----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crime (
            complaint_num INTEGER PRIMARY KEY,
            borough TEXT,
            offense TEXT
        )
    """)

    conn.commit()
    conn.close()


# -------------------------------------------------------------------
# INSERT FUNCTIONS
# -------------------------------------------------------------------

def insert_tree(tree_id, status, health, borough, spc):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO trees (tree_id, status, health, borough, spc_common)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(tree_id) DO UPDATE SET
            status=excluded.status,
            health=excluded.health,
            borough=excluded.borough,
            spc_common=excluded.spc_common
    """, (tree_id, status, health, borough, spc))
    conn.commit()
    conn.close()


def insert_demo(borough, black_percent):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO demographics (borough, black_percent)
        VALUES (?, ?)
        ON CONFLICT(borough) DO UPDATE SET
            black_percent=excluded.black_percent
    """, (borough, black_percent))
    conn.commit()
    conn.close()


def insert_collision(collision_id, borough, injured):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO collisions (collision_id, borough, persons_injured)
        VALUES (?, ?, ?)
        ON CONFLICT(collision_id) DO UPDATE SET
            borough=excluded.borough,
            persons_injured=excluded.persons_injured
    """, (collision_id, borough, injured))
    conn.commit()
    conn.close()


def insert_crime(complaint_num, borough, offense):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO crime (complaint_num, borough, offense)
        VALUES (?, ?, ?)
        ON CONFLICT(complaint_num) DO UPDATE SET
            borough=excluded.borough,
            offense=excluded.offense
    """, (complaint_num, borough, offense))
    conn.commit()
    conn.close()



