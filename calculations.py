import sqlite3

DB_PATH = "nyc_data.db"

def get_connection():
    return sqlite3.connect(DB_PATH)

# -------------------------
# 1. Trees per borough
# -------------------------
def calculate_trees_per_borough():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT borough, COUNT(*) AS tree_count
        FROM trees
        GROUP BY borough;
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

# -------------------------
# 2. Black population percent by borough
# -------------------------
def calculate_black_percent():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT borough, black_percent
        FROM demographics;
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

# -------------------------
# 3. Total crime per borough
# -------------------------
def calculate_crime_counts():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT borough, COUNT(*) AS crime_count
        FROM crime
        GROUP BY borough;
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

# -------------------------
# 4. Total collisions per borough
# -------------------------
def calculate_collision_counts():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT borough, COUNT(*) AS collision_count
        FROM collisions
        GROUP BY borough;
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

# -------------------------
# 5. Tree count vs Black population percent (JOIN)
# -------------------------
def calculate_trees_vs_black_percent():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT t.borough, COUNT(t.tree_id) AS tree_count, d.black_percent
        FROM trees t
        JOIN demographics d ON t.borough = d.borough
        GROUP BY t.borough;
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

# -------------------------
# Optional: print summaries for testing
# -------------------------
if __name__ == "__main__":
    print("Trees per borough:", calculate_trees_per_borough())
    print("Black population percent:", calculate_black_percent())
    print("Crime counts per borough:", calculate_crime_counts())
    print("Collision counts per borough:", calculate_collision_counts())
    print("Trees vs Black percent:", calculate_trees_vs_black_percent())
