import requests
from db_utils import get_connection, row_exists, create_tables

API_URL = "https://data.cityofnewyork.us/resource/uvpi-gqnh.json"
LIMIT_PER_RUN = 25

def fetch_tree_batch(offset=0):
    create_tables()  # ensures table exists

    params = {"$limit": LIMIT_PER_RUN, "$offset": offset}
    headers = {"X-App-Token": "m2Zj3gBpqhFVI4W9XcJoz91ZE"}
    resp = requests.get(API_URL, params=params, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    print(f"Fetched {len(data)} records from API")
    return data

def insert_trees(records):
    conn = get_connection()
    cur = conn.cursor()
    inserted = 0

    for row in records:
        tree_id = int(row.get("tree_id", -1))
        borough = row.get("boroname")  # <- updated key
        if tree_id == -1 or borough is None:
            print(f"Skipping row: missing tree_id or borough -> {row}")
            continue

        if not row_exists("trees", "tree_id", tree_id):
            cur.execute(
                "INSERT INTO trees (tree_id, borough) VALUES (?, ?)",
                (tree_id, borough)
            )
            inserted += 1
            print(f"Inserted tree_id {tree_id}, borough {borough}")
        else:
            print(f"Skipping tree_id {tree_id}: already exists")

    conn.commit()
    conn.close()
    print(f"Inserted {inserted} new tree rows (max {LIMIT_PER_RUN})")

if __name__ == "__main__":
    for offset in range(0, 400, 25):
        records = fetch_tree_batch(offset=offset)
        insert_trees(records)

