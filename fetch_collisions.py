import requests
import time
from db_utils import get_connection

URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

def safe_request(url, params, retries=3):
    """Retry wrapper to avoid crashing on 500 errors."""
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"  !! Request failed (attempt {attempt+1}/{retries}): {e}")
            time.sleep(1)
    print("  !! Skipping this batch after repeated failures.")
    return None

def fetch_collisions():
    limit = 25
    offset = 0
    batches = 5  # 5 batches × 25 = 125 rows

    conn = get_connection()
    cur = conn.cursor()

    for i in range(batches):
        print(f"[collisions] fetching batch {i+1} offset {offset}")

        params = {"$limit": limit, "$offset": offset}
        data = safe_request(URL, params)

        if not data:
            offset += limit
            continue

        for row in data:
            collision_id = row.get("collision_id")
            borough = row.get("borough")

            if collision_id:
                cur.execute("""
                    INSERT OR IGNORE INTO collisions (collision_id, borough)
                    VALUES (?, ?)
                """, (collision_id, borough))

                # ---- VEHICLE TABLE (2nd table) ----
                vehicles = row.get("vehicle_type_code1")
                if vehicles:
                    cur.execute("""
                        INSERT OR IGNORE INTO collision_injuries (collision_id, persons_injured)
                        VALUES (?, ?)
                    """, (collision_id, vehicles))

        conn.commit()
        offset += limit

    conn.close()
    print("done fetching collisions.")

if __name__ == "__main__":
    fetch_collisions()
