# fetch_demographics.py

# fetch_demographics.py
# fetch_demographics.py

import requests
import sqlite3
from pathlib import Path

# --- CONFIG ---
DB_PATH = "/Users/lucysanders/Desktop/SI201/finalproject/nyc_data.db"  # absolute path
TABLE_NAME = "demographics"
API_URL = "https://data.cityofnewyork.us/api/v3/views/uh2w-zjsn/query.json"
LIMIT_PER_RUN = 25
APP_TOKEN = "m2Zj3gBpqhFVI4W9XcJoz91ZE"  # replace with your Socrata token

# --- DATABASE UTILITIES ---
def get_connection():
    conn = sqlite3.connect(DB_PATH)
    return conn

def create_table():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            borough TEXT PRIMARY KEY,
            black_percent REAL
        )
    """)
    conn.commit()
    conn.close()

# --- FETCH DATA ---
def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def fetch_demo_batch(offset=0):
    headers = {"X-App-Token": APP_TOKEN}
    params = {"$limit": LIMIT_PER_RUN, "$offset": offset}
    resp = requests.get(API_URL, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()

# --- INSERT / UPDATE ---
def insert_data(records):
    conn = get_connection()
    cur = conn.cursor()
    inserted = 0

    for row in records:
        borough = row.get("borough")
        black_percent = safe_float(row.get("black_1"))  # column in API

        if not borough or black_percent is None:
            continue

        cur.execute(f"""
            INSERT INTO {TABLE_NAME} (borough, black_percent)
            VALUES (?, ?)
            ON CONFLICT(borough) DO UPDATE SET black_percent=excluded.black_percent
        """, (borough, black_percent))
        inserted += 1

    conn.commit()

    # Print all rows to confirm
    cur.execute(f"SELECT * FROM {TABLE_NAME}")
    all_rows = cur.fetchall()
    print(f"\nDatabase path: {Path(DB_PATH).resolve()}")
    print(f"Inserted/updated {inserted} rows. Current table contents:")
    for r in all_rows:
        print(r)

    conn.close()

# --- MAIN ---
if __name__ == "__main__":
    create_table()
    for offset in range(0, 125, LIMIT_PER_RUN):
        records = fetch_demo_batch(offset=offset)
        print(f"Fetched {len(records)} records from API (offset {offset})")
        insert_data(records)







