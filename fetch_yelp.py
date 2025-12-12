import requests
from db_utils import get_connection

API_KEY = "xc63QgBY7Uy9c2ZxcA7fEovMqNPHpSJCw9PEbN77vOqTsXeCOUvoX0T_fOz74QJ7pWoAqDSkriO4TG5qQ8mqq37uxovdvu9a6KkPl56M9K4eUWaX6CZKRs4PxJ48aXYx"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}
BASE_URL = "https://api.yelp.com/v3/businesses/search"
BATCH_SIZE = 25

BORO_COORDS = {
    1: "Manhattan",
    2: "Bronx",
    3: "Brooklyn",
    4: "Queens",
    5: "Staten Island"
}

def fetch_yelp():
    conn = get_connection()
    cur = conn.cursor()

    for borough_id, borough_name in BORO_COORDS.items():
        offset_name = f"yelp_{borough_id}"
        cur.execute(f"INSERT OR IGNORE INTO offsets (name, offset) VALUES (?, 0)", (offset_name,))
        cur.execute(f"SELECT offset FROM offsets WHERE name=?", (offset_name,))
        offset = cur.fetchone()[0]

        params = {
            "term": "restaurants",
            "location": f"{borough_name}, NY",
            "limit": BATCH_SIZE,
            "offset": offset
        }
        r = requests.get(BASE_URL, headers=HEADERS, params=params)
        r.raise_for_status()
        data = r.json()

        for biz in data.get("businesses", []):
            yelp_id = biz["id"]
            rating = biz.get("rating", 0)
            cur.execute("INSERT OR IGNORE INTO yelp (yelp_id, borough_id, rating) VALUES (?, ?, ?)", (yelp_id, borough_id, rating))

        cur.execute("UPDATE offsets SET offset = ? WHERE name=?", (offset + BATCH_SIZE, offset_name))

    conn.commit()
    conn.close()
    print("Fetched Yelp businesses (25 per borough)")

if __name__ == "__main__":
    fetch_yelp()
