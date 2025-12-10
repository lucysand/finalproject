# fetch_trees.py
import requests
from db_utils import get_connection, row_exists, create_tables

API_URL = "https://data.cityofnewyork.us/resource/uvpi-gqnh.json"
LIMIT_PER_RUN = 25

def fetch_trees(offset=0):
    create_tables()

    params = {
        "$limit": LIMIT_PER_RUN,
        "$offset": offset
    }

    headers = {"X-App-Token": "eGeOtd7LZadVKnHmylpPErmPn"}
    resp = requests.get(API_URL, params=params, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    conn = get_connection()
    cur = conn.cursor()

    inserted = 0

    for row in data:
        # Basic ID extraction
        try:
            tree_id = int(row["tree_id"])
        except:
            continue

        # The field you're seeing in the dataset:
        nta_name = row.get("nta_name")

        # Skip if NTA is missing
        if not nta_name:
            continue

        # Avoid duplicates
        if not row_exists("trees", "tree_id", tree_id):
            cur.execute("""
                INSERT INTO trees (tree_id, nta_name)
                VALUES (?, ?)
            """, (tree_id, nta_name))

            inserted += 1

    conn.commit()
    conn.close()
    print(f"Inserted {inserted} trees. (Max {LIMIT_PER_RUN})")

if __name__ == "__main__":
    fetch_trees(offset=0)

