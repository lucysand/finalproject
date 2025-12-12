import requests
from db_utils import get_connection

URL = "https://data.cityofnewyork.us/resource/uh2w-zjsn.json"

def fetch_demo():
    limit = 25
    offset = 0
    batches = 5

    for i in range(batches):
        print(f"[demo] fetching batch {i+1} offset {offset}")

        params = {"$limit": limit, "$offset": offset}
        r = requests.get(URL, params=params)
        r.raise_for_status()
        data = r.json()

        conn = get_connection()
        cur = conn.cursor()

        for row in data:
            borough = row.get("borough")  # <-- correct key
            black_percent = row.get("black_1")
            if borough and black_percent is not None:
                try:
                    black_percent = float(black_percent)
                except ValueError:
                    continue  # skip rows with invalid data
                cur.execute("""
                    INSERT OR IGNORE INTO demographics (borough, black_percent)
                    VALUES (?, ?)
                """, (borough, black_percent))

        conn.commit()
        conn.close()
        offset += limit

if __name__ == "__main__":
    fetch_demo()
