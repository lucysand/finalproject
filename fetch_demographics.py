# fetch_demographics.py
import requests
from db_utils import get_connection, row_exists, create_tables

API_URL = "https://data.cityofnewyork.us/resource/hyuz-tij8.json"
LIMIT_PER_RUN = 25

def safe_float(val):
    try:
        return float(val)
    except:
        return None

def fetch_demographics(offset=0):
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
        nta_name = row.get("nta_name")
        if not nta_name:
            continue

        if row_exists("demographics", "nta_name", nta_name):
            continue

        median_income = safe_float(row.get("median_household_income"))
        poverty_rate = safe_float(row.get("poverty_rate"))

        pct_white = safe_float(row.get("percent_white_nonhispanic"))
        pct_black = safe_float(row.get("percent_black_nonhispanic"))
        pct_latino = safe_float(row.get("percent_hispanic"))
        pct_asian = safe_float(row.get("percent_asian_nonhispanic"))

        cur.execute("""
            INSERT INTO demographics (
                nta_name, median_income, poverty_rate,
                pct_white, pct_black, pct_latino, pct_asian
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            nta_name, median_income, poverty_rate,
            pct_white, pct_black, pct_latino, pct_asian
        ))

        inserted += 1

    conn.commit()
    conn.close()
    print(f"Inserted {inserted} new demographic rows.")

if __name__ == "__main__":
    fetch_demographics(offset=0)

