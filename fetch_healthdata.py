# fetch_health.py
import requests
from db_utils import get_connection, row_exists, create_tables

API_URL = "https://data.cityofnewyork.us/resource/f7b6-v6v3.json"
LIMIT_PER_RUN = 25


def safe_float(val):
    try:
        return float(val)
    except:
        return None


def fetch_health(offset=0):
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
        nta_name = row.get("ntaname")     # <- use NAME instead of code
        indicator = row.get("indicator")
        value = safe_float(row.get("value"))

        if not nta_name or value is None:
            continue

        if indicator not in ["Child asthma", "Physical activity"]:
            continue

        # If this NTA isn't in the table yet, create it
        if not row_exists("health_demographics_wide", "nta_name", nta_name):
            cur.execute("""
                INSERT INTO health_demographics_wide (nta_name)
                VALUES (?)
            """, (nta_name,))

        # Update correct column
        if indicator == "Child asthma":
            cur.execute("""
                UPDATE health_demographics_wide
                SET child_asthma = ?
                WHERE nta_name = ?
            """, (value, nta_name))

        elif indicator == "Physical activity":
            cur.execute("""
                UPDATE health_demographics_wide
                SET physical_activity = ?
                WHERE nta_name = ?
            """, (value, nta_name))

        inserted += 1

    conn.commit()
    conn.close()
    print(f"Inserted/updated {inserted} health rows. (Max {LIMIT_PER_RUN})")


if __name__ == "__main__":
    fetch_health(offset=0)

