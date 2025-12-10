import sqlite3

DB_PATH = "nyc_data.db"

def get_connection():
    return sqlite3.connect(DB_PATH)

# ---------------------------------------
# 1. Trees per capita
# ---------------------------------------
def calculate_trees_per_capita():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT t.borough, t.tree_count, d.population,
               CAST(t.tree_count AS FLOAT) / d.population AS trees_per_capita
        FROM trees t
        JOIN demographics d ON t.borough = d.borough;
    """)

    rows = cur.fetchall()
    conn.close()
    return rows

# ---------------------------------------
# 2. Collisions per 1,000 residents
# ---------------------------------------
def calculate_collisions_rate():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT c.borough,
               c.number_of_persons_injured,
               c.number_of_persons_killed,
               d.population,
               (CAST(c.number_of_persons_injured AS FLOAT) / d.population) * 1000
                    AS injured_per_1000,
               (CAST(c.number_of_persons_killed AS FLOAT) / d.population) * 1000
                    AS killed_per_1000
        FROM collisions c
        JOIN demographics d ON c.borough = d.borough;
    """)

    rows = cur.fetchall()
    conn.close()
    return rows

# ---------------------------------------
# 3. Crime per 1,000 residents
# ---------------------------------------
def calculate_crime_rate():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT c.borough,
               c.felony, c.misdemeanor, c.violation,
               d.population,
               (CAST(c.felony AS FLOAT) / d.population) * 1000 AS felony_rate,
               (CAST(c.misdemeanor AS FLOAT) / d.population) * 1000 AS misdemeanor_rate,
               (CAST(c.violation AS FLOAT) / d.population) * 1000 AS violation_rate
        FROM crime c
        JOIN demographics d ON c.borough = d.borough;
    """)

    rows = cur.fetchall()
    conn.close()
    return rows

# ---------------------------------------
# 4. Trees vs income correlation dataset
# ---------------------------------------
def get_tree_income_data():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT t.borough, t.tree_count, d.median_income
        FROM trees t
        JOIN demographics d
        ON t.borough = d.borough;
    """)

    rows = cur.fetchall()
    conn.close()
    return rows

# ---------------------------------------
# 5. Crime severity index (weighted)
# ---------------------------------------
def calculate_crime_severity_index():
    """
    Weighting example:
    felony = 3
    misdemeanor = 2
    violation = 1
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT borough,
               felony,
               misdemeanor,
               violation,
               (felony * 3 + misdemeanor * 2 + violation * 1) AS severity_score
        FROM crime;
    """)

    rows = cur.fetchall()
    conn.close()
    return rows
