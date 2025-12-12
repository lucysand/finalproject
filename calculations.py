# calculations.py
from db_utils import get_connection
from pathlib import Path

OUTPUT_FILE = Path(__file__).parent / "calculations.txt"

def run_calculations():
    conn = get_connection()
    cur = conn.cursor()

    # 1. Total collisions per borough
    cur.execute("""
        SELECT b.borough_name, COUNT(c.collision_id) 
        FROM collisions c
        JOIN boroughs b ON c.borough_id = b.borough_id
        GROUP BY b.borough_name
    """)
    collisions_per_borough = cur.fetchall()

    # 2. Average persons injured per borough
    cur.execute("""
        SELECT b.borough_name, AVG(ci.persons_injured)
        FROM collision_injuries ci
        JOIN collisions c ON ci.collision_id = c.collision_id
        JOIN boroughs b ON c.borough_id = b.borough_id
        GROUP BY b.borough_name
    """)
    avg_injuries_per_borough = cur.fetchall()

    # 3. Average black population percentage by borough
    cur.execute("""
        SELECT b.borough_name, d.black_percent
        FROM demographics d
        JOIN boroughs b ON d.borough_id = b.borough_id
    """)
    black_percent = cur.fetchall()

    # 4. Average Yelp rating by borough
    cur.execute("""
        SELECT b.borough_name, AVG(y.rating)
        FROM yelp y
        JOIN boroughs b ON y.borough_id = b.borough_id
        GROUP BY b.borough_name
    """)
    avg_yelp = cur.fetchall()

    conn.close()

    # Write results to txt in working folder
    with open(OUTPUT_FILE, "w") as f:
        f.write("=== Collisions per Borough ===\n")
        for borough, count in collisions_per_borough:
            f.write(f"{borough}: {count}\n")

        f.write("\n=== Average Persons Injured per Borough ===\n")
        for borough, avg in avg_injuries_per_borough:
            f.write(f"{borough}: {avg:.2f}\n")

        f.write("\n=== Black Population Percent by Borough ===\n")
        for borough, percent in black_percent:
            f.write(f"{borough}: {percent:.2f}%\n")

        f.write("\n=== Average Yelp Rating per Borough ===\n")
        for borough, rating in avg_yelp:
            f.write(f"{borough}: {rating:.2f}\n")

    print(f"Calculations saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    run_calculations()
