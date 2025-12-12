import requests
from db_utils import get_connection

URL = "https://data.cityofnewyork.us/resource/uh2w-zjsn.json"
BATCH_SIZE = 25

def fetch_demographics():
    conn = get_connection()
    cur = conn.cursor()

    # Track offset
    cur.execute("INSERT OR IGNORE INTO offsets (name, offset) VALUES ('demographics', 0)")
    cur.execute("SELECT offset FROM offsets WHERE name='demographics'")
    offset = cur.fetchone()[0]

    params = {"$limit": BATCH_SIZE, "$offset": offset}
    r = requests.get(URL, params=params)
    r.raise_for_status()
    data = r.json()
    if not data:
        print("No data from demographics API")
        return

    for row in data:
        borough_name = row.get("borough")
        black_percent_str = row.get("black_1")
        if not borough_name or not black_percent_str:
            continue

        cur.execute("SELECT borough_id FROM boroughs WHERE borough_name=?", (borough_name,))
        res = cur.fetchone()
        if not res:
            continue
        borough_id = res[0]

        try:
            black_percent = float(black_percent_str)
        except:
            continue

        cur.execute("INSERT OR IGNORE INTO demographics (borough_id, black_percent) VALUES (?, ?)", (borough_id, black_percent))

    cur.execute("UPDATE offsets SET offset = ? WHERE name='demographics'", (offset + BATCH_SIZE,))
    conn.commit()
    conn.close()
    print(f"Fetched {len(data)} demographics rows. Next offset = {offset + BATCH_SIZE}")

if __name__ == "__main__":
    fetch_demographics()
