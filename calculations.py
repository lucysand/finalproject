# calculations.py
from db_utils import get_connection  # <-- use the correct DB path

# -----------------------
# Trees
# -----------------------
def calc_trees_per_borough():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT borough, COUNT(tree_id) 
            FROM trees 
            WHERE borough IS NOT NULL AND borough != '' 
            GROUP BY borough
        """)
        return cur.fetchall()

# -----------------------
# Demographics
# -----------------------
def calc_black_percent():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT borough, black_percent 
            FROM demographics 
            WHERE borough IS NOT NULL AND borough != ''
            ORDER BY borough
        """)
        return cur.fetchall()

# -----------------------
# Crime
# -----------------------
def calc_crime_counts():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT borough, COUNT(cmplnt_num) 
            FROM crime 
            WHERE borough IS NOT NULL AND borough != '' 
            GROUP BY borough
        """)
        return cur.fetchall()

# -----------------------
# Collisions (main table)
# -----------------------
def calc_collision_counts():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT borough, COUNT(collision_id) 
            FROM collisions 
            WHERE borough IS NOT NULL AND borough != '' 
            GROUP BY borough
        """)
        return cur.fetchall()

# -----------------------
# Collisions: persons injured
# -----------------------
def calc_collision_injuries():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT c.borough, SUM(ci.persons_injured) 
            FROM collisions c
            JOIN collision_injuries ci ON c.collision_id = ci.collision_id
            WHERE c.borough IS NOT NULL AND c.borough != ''
            GROUP BY c.borough
        """)
        return cur.fetchall()

# -----------------------
# Trees vs Black %
# -----------------------
def calc_trees_vs_black():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT t.borough, COUNT(t.tree_id) AS tree_count, d.black_percent
            FROM trees t
            JOIN demographics d ON t.borough = d.borough
            WHERE t.borough IS NOT NULL AND t.borough != ''
            GROUP BY t.borough
        """)
        return cur.fetchall()
