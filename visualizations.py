import sqlite3
import matplotlib.pyplot as plt

DB_PATH = "nyc_data.db"

def get_connection():
    return sqlite3.connect(DB_PATH)

# -------------------------
# 1. Bar chart: Trees per borough
# -------------------------
def plot_trees_per_borough():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT borough, COUNT(*) FROM trees GROUP BY borough;")
    rows = cur.fetchall()
    conn.close()

    boroughs = [r[0] for r in rows]
    tree_counts = [r[1] for r in rows]

    plt.figure(figsize=(8,6))
    plt.bar(boroughs, tree_counts, color="#2ca02c")  # green
    plt.xlabel("Borough")
    plt.ylabel("Tree Count")
    plt.title("Tree Count by Borough")
    plt.savefig("trees_per_borough.png")
    plt.close()

# -------------------------
# 2. Bar chart: Black population percent
# -------------------------
def plot_black_percent():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT borough, black_percent FROM demographics;")
    rows = cur.fetchall()
    conn.close()

    boroughs = [r[0] for r in rows]
    black_percent = [r[1] for r in rows]

    plt.figure(figsize=(8,6))
    plt.bar(boroughs, black_percent, color="#ff7f0e")  # orange
    plt.xlabel("Borough")
    plt.ylabel("Black Population (%)")
    plt.title("Black Population Percent by Borough")
    plt.savefig("black_percent.png")
    plt.close()

# -------------------------
# 3. Bar chart: Total crime per borough
# -------------------------
def plot_crime_counts():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT borough, COUNT(*) FROM crime GROUP BY borough;")
    rows = cur.fetchall()
    conn.close()

    boroughs = [r[0] for r in rows]
    crime_counts = [r[1] for r in rows]

    plt.figure(figsize=(8,6))
    plt.bar(boroughs, crime_counts, color="#d62728")  # red
    plt.xlabel("Borough")
    plt.ylabel("Total Crime Reports")
    plt.title("Crime Reports by Borough")
    plt.savefig("crime_counts.png")
    plt.close()

# -------------------------
# 4. Bar chart: Total collisions per borough
# -------------------------
def plot_collision_counts():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT borough, COUNT(*) FROM collisions GROUP BY borough;")
    rows = cur.fetchall()
    conn.close()

    boroughs = [r[0] for r in rows]
    collision_counts = [r[1] for r in rows]

    plt.figure(figsize=(8,6))
    plt.bar(boroughs, collision_counts, color="#1f77b4")  # blue
    plt.xlabel("Borough")
    plt.ylabel("Total Collisions")
    plt.title("Collisions by Borough")
    plt.savefig("collision_counts.png")
    plt.close()

# -------------------------
# 5. Scatter plot: Tree count vs Black population percent (JOIN)
# -------------------------
def plot_trees_vs_black_percent():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT t.borough, COUNT(t.tree_id) AS tree_count, d.black_percent
        FROM trees t
        JOIN demographics d ON t.borough = d.borough
        GROUP BY t.borough
    """)
    rows = cur.fetchall()
    conn.close()

    tree_counts = [r[1] for r in rows]
    black_percent = [r[2] for r in rows]
    boroughs = [r[0] for r in rows]

    plt.figure(figsize=(8,6))
    plt.scatter(tree_counts, black_percent, color="#9467bd", s=100)  # purple
    for i, b in enumerate(boroughs):
        plt.text(tree_counts[i], black_percent[i]+0.5, b, ha='center')
    plt.xlabel("Tree Count")
    plt.ylabel("Black Population (%)")
    plt.title("Tree Count vs Black Population Percent by Borough")
    plt.savefig("trees_vs_black_percent.png")
    plt.close()

# -------------------------
# Main
# -------------------------
def main():
    plot_trees_per_borough()
    plot_black_percent()
    plot_crime_counts()
    plot_collision_counts()
    plot_trees_vs_black_percent()

if __name__ == "__main__":
    main()
