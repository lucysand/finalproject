import requests
from db_utils import get_connection, borough_to_id, get_offset, update_offset

URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
BATCH_SIZE = 25

def safe_request(url, params):
    """Send GET request and return JSON, or empty list on failure."""
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Request failed: {e} -- params={params}")
        return []

def _normalize_borough(raw):
    """Map raw borough string to canonical borough name or None if unknown."""
    if not raw:
        return None
    s_upper = raw.strip().upper()
    if "STATEN" in s_upper:
        return "Staten Island"
    if "MANHATTAN" in s_upper:
        return "Manhattan"
    if "BROOKLYN" in s_upper:
        return "Brooklyn"
    if "QUEENS" in s_upper:
        return "Queens"
    if "BRONX" in s_upper:
        return "Bronx"
    return None  # any other unknown value

def fetch_collisions():
    conn = get_connection()
    cur = conn.cursor()

    offset = get_offset("collisions")
    if offset is None:
        offset = 0

    params = {"$limit": BATCH_SIZE, "$offset": offset}
    data = safe_request(URL, params)
    if not data:
        print("No data returned from API")
        conn.close()
        return

    print(f"Fetched {len(data)} rows (offset={offset})")

    inserted = 0
    for i, row in enumerate(data):
        collision_id = row.get("collision_id") or row.get("unique_key")
        raw_borough = row.get("borough")
        borough_name = _normalize_borough(raw_borough)

        if not collision_id:
            print(f"[row {i}] skipping: no collision id")
            continue

        if not borough_name:
            print(f"[{collision_id}] skipping: unknown borough raw='{raw_borough}'")
            continue

        borough_id = borough_to_id(borough_name)
        if borough_id is None:
            print(f"[{collision_id}] skipping: unknown borough '{borough_name}'")
            continue

        try:
            # Skip if collision already exists
            cur.execute("SELECT 1 FROM collisions WHERE collision_id = ?", (collision_id,))
            if cur.fetchone():
                continue

            # Insert into collisions
            cur.execute(
                "INSERT INTO collisions (collision_id, borough_id) VALUES (?, ?)",
                (collision_id, borough_id)
            )

            # Insert into collision_injuries
            raw_inj = row.get("number_of_persons_injured", 0) or 0
            try:
                persons_injured = int(float(raw_inj))
            except Exception:
                persons_injured = 0

            cur.execute(
                "INSERT OR IGNORE INTO collision_injuries (collision_id, persons_injured) VALUES (?, ?)",
                (collision_id, persons_injured)
            )

            inserted += 1
        except Exception as e:
            print(f"DB insert failed for {collision_id}: {e}")

    # Update offset using same connection to avoid 'database is locked'
    new_offset = offset + BATCH_SIZE
    update_offset("collisions", new_offset, conn=conn)

    conn.commit()
    conn.close()
    print(f"Inserted {inserted} new rows. Next offset = {new_offset}")


if __name__ == "__main__":
    fetch_collisions()
