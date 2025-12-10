import sqlite3
import matplotlib.pyplot as plt

DB_PATH = "nyc_data.db"

def get_connection():
    return sqlite3.connect(DB_PATH)

# ---------------------------------------------------
# 1. Bar chart: Trees per capita by borough
# ---------------------------------------------------
def plot_trees_per_capita():
    conn = sqlite3.connect("nyc_data.db")
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            t.borough,
            COUNT(*) AS tree_count,
            d.black_percent
        FROM trees t
        JOIN demographics d ON t.borough = d.borough
        GROUP BY t.borough;
    """)

    rows = cur.fetchall()
    conn.close()

    boroughs = [r[0] for r in rows]
    tree_counts = [r[1] for r in rows]

    plt.figure(figsize=(10, 6))
    plt.bar(boroughs, tree_counts)
    plt.xlabel("Borough")
    plt.ylabel("Tree Count")
    plt.title("Tree Count by Borough")
    plt.savefig("trees_per_capita.png")
    plt.close()

# ---------------------------------------------------
# 2. Bar chart: Collisions injured per 1,000 residents
# ---------------------------------------------------
def plot_collisions_per_1000():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT c.borough,
               (CAST(c.number_of_persons_injured AS FLOAT) / d.population) * 1000
        FROM collisions c
        JOIN demographics d ON c.borough = d.borough;
    """)

    data = cur.fetchall()
    conn.close()

    boroughs = [row[0] for row in data]
    injured_rate = [row[1] for row in data]

    plt.figure()
    plt.bar(boroughs, injured_rate)
    plt.xlabel("Borough")
    plt.ylabel("Injuries per 1,000 residents")
    plt.title("Traffic Injuries per 1,000 Residents")
    plt.savefig("collisions_injured_per_1000.png")
    plt.close()

# ---------------------------------------------------
# 3. Bar chart: Crime severity index
# ---------------------------------------------------
def plot_crime_severity():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT borough, (felony * 3 + misdemeanor * 2 + violation) AS score
        FROM crime;
    """)

    data = cur.fetchall()
    conn.close()

    boroughs = [row[0] for row in data]
    scores = [row[1] for row in data]

    plt.figure()
    plt.bar(boroughs, scores)
    plt.xlabel("Borough")
    plt.ylabel("Severity score")
    plt.title("Crime Severity Index by Borough")
    plt.savefig("crime_severity_index.png")
    plt.close()

# ---------------------------------------------------
# 4. Scatter plot: Tree count vs median income
# ---------------------------------------------------
def plot_trees_vs_income():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT t.tree_count, d.median_income
        FROM trees t
        JOIN demographics d ON t.borough = d.borough;
    """)

    data = cur.fetchall()
    conn.close()

    trees = [row[0] for row in data]
    income = [row[1] for row in data]

    plt.figure()
    plt.scatter(trees, income)
    plt.xlabel("Tree count")
    plt.ylabel("Median income")
    plt.title("Relationship Between Tree Count and Median Income")
    plt.savefig("trees_vs_income.png")
    plt.close()

# ---------------------------------------------------
# 5. Stacked bar: Crime breakdown per borough
# ---------------------------------------------------
def plot_crime_breakdown():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT borough, felony, misdemeanor, violation
        FROM crime;
    """)

    data = cur.fetchall()
    conn.close()

    boroughs = [row[0] for row in data]
    felony = [row[1] for row in data]
    misdemeanor = [row[2] for row in data]
    violation = [row[3] for row in data]

    plt.figure()
    plt.bar(boroughs, felony, label="Felony")
    plt.bar(boroughs, misdemeanor, bottom=felony, label="Misdemeanor")
    plt.bar(boroughs, violation,
            bottom=[felony[i] + misdemeanor[i] for i in range(len(felony))],
            label="Violation")

    plt.xlabel("Borough")
    plt.ylabel("Total incidents")
    plt.title("Crime Breakdown by Borough")
    plt.legend()
    plt.savefig("crime_breakdown.png")
    plt.close()

def main():
    plot_trees_per_capita()
    plot_collisions_per_1000()
    plot_crime_severity()
    plot_trees_vs_income()
    plot_crime_breakdown()


if __name__ == "__main__":
    main()