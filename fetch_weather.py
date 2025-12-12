# ...existing code...
import requests
import time
from db_utils import get_connection, borough_to_id

API_KEY = "d5c2b9bd7c91587480ccf27303841bd6"  # replace with your active key
LAT_LON = {
    "Manhattan": (40.7831, -73.9712),
    "Brooklyn": (40.6782, -73.9442),
    "Queens": (40.7282, -73.7949),
    "Bronx": (40.8448, -73.8648),
    "Staten Island": (40.5795, -74.1502)
}
BATCH_SIZE = 25

def safe_request(url, params, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"!! Request failed (attempt {attempt+1}/{retries}): {e}")
            time.sleep(1)
    print("!! Skipping this batch after repeated failures.")
    return None

def fetch_weather():
    conn = get_connection()
    cur = conn.cursor()

    # Use offsets table like other fetchers
    cur.execute("INSERT OR IGNORE INTO offsets (name, offset) VALUES ('weather', 0)")
    cur.execute("SELECT offset FROM offsets WHERE name='weather'")
    row = cur.fetchone()
    offset = row[0] if row and row[0] is not None else 0

    boroughs = list(LAT_LON.keys())

    # Reset offset if it goes past available boroughs
    if offset >= len(boroughs):
        print("[weather] offset beyond borough list, resetting to 0")
        offset = 0
        cur.execute("UPDATE offsets SET offset = ? WHERE name = 'weather'", (0,))
        conn.commit()

    batch = boroughs[offset:offset + BATCH_SIZE]

    inserted = 0
    for borough in batch:
        b_id = borough_to_id(borough)
        if b_id is None:
            print(f"[weather] unknown borough id for '{borough}', skipping")
            continue

        lat, lon = LAT_LON[borough]

        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {"lat": lat, "lon": lon, "appid": API_KEY, "units": "imperial"}
        data = safe_request(url, params)
        if not data:
            continue

        weather_id = data.get("id")
        temp = data.get("main", {}).get("temp")
        humidity = data.get("main", {}).get("humidity")
        wind_speed = data.get("wind", {}).get("speed")

        if weather_id:
            cur.execute("""
                INSERT OR IGNORE INTO weather (weather_id, borough_id, temp, humidity, wind_speed)
                VALUES (?, ?, ?, ?, ?)
            """, (weather_id, b_id, temp, humidity, wind_speed))
            inserted += 1

    # advance offset by number of boroughs processed
    new_offset = offset + len(batch)
    cur.execute("UPDATE offsets SET offset = ? WHERE name = 'weather'", (new_offset,))
    conn.commit()
    conn.close()
    print(f"[weather] inserted {inserted} rows (offset {offset} -> {new_offset})")

if __name__ == "__main__":
    fetch_weather()