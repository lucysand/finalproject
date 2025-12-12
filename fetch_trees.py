import requests
from db_utils import get_connection

URL = "https://data.cityofnewyork.us/resource/uvpi-gqnh.json"

def fetch_trees():
    limit = 25
    offset = 0
    batches = 5

    for i in range(batches):
        print(f"[trees] fetching batch {i+1} offset {offset}")
        params = {"$limit": limit, "$offset": offset}

        r = requests.get(URL, params=params)
        r.raise_for_status()
        data = r.json()

        conn = get_connection()
        cur = conn.cursor()

        for row in data:
            tree_id = row.get("tree_id")
            borough = row.get("boroname")

            if tree_id:
                cur.execute("""
                    INSERT OR IGNORE INTO trees (tree_id, borough)
                    VALUES (?, ?)
                """, (tree_id, borough))

        conn.commit()
        conn.close()
        offset += limit

if __name__ == "__main__":
    fetch_trees()
