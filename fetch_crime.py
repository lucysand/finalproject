import requests
import sqlite3
import time

DB_PATH = "nyc_data.db"     # change if needed
TABLE_NAME = "crime"

API_URL = "https://data.cityofnewyork.us/resource/5uac-w243.json"
BATCH_SIZE = 25
MAX_BATCHES = 5   # 20 * 500 = 10,000 rows

def create_table():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            complaint_num INTEGER PRIMARY KEY,
            borough TEXT,
            offense TEXT
        )
    """)
    conn.commit()
    conn.close()

def fetch_crime(offset):
    params = {
        "$limit": BATCH_SIZE,
        "$offset": offset,
        "$select": "cmplnt_num, boro_nm, ofns_desc",
        "$where": "boro_nm IS NOT NULL"
    }
    resp = requests.get(API_URL, params=params)
    resp.raise_for_status()
    return resp.json()

def insert_rows(rows):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    inserted = 0

    for r in rows:
        num = r.get("cmplnt_num")
        borough = r.get("boro_nm")
        offense = r.get("ofns_desc")

        if not num or not borough:
            continue

        cur.execute(f"""
            INSERT INTO {TABLE_NAME}(complaint_num, borough, offense)
            VALUES (?, ?, ?)
            ON CONFLICT(complaint_num)
            DO UPDATE SET 
                borough=excluded.borough,
                offense=excluded.offense
        """, (num, borough, offense))

        inserted += 1

    conn.commit()
    conn.close()
    return inserted

if __name__ == "__main__":
    create_table()

    offset = 0
    for i in range(MAX_BATCHES):
        print(f"Fetching batch {i+1}/{MAX_BATCHES} (offset {offset})...")
        rows = fetch_crime(offset)
        if not rows:
            print("No more rows.")
            break

        count = insert_rows(rows)
        print(f"Inserted/updated {count} rows.")

        offset += BATCH_SIZE
        time.sleep(0.2)

    print("Crime import complete.")












