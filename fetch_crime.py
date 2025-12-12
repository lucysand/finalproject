import requests
from db_utils import get_connection

URL = "https://data.cityofnewyork.us/resource/5uac-w243.json"

def fetch_crime():
    limit = 25
    offset = 0
    batches = 5

    for i in range(batches):
        print(f"[crime] fetching batch {i+1} offset {offset}")
        params = {"$limit": limit, "$offset": offset}

        r = requests.get(URL, params=params)
        r.raise_for_status()
        data = r.json()

        conn = get_connection()
        cur = conn.cursor()

        for row in data:
            cmplnt_num = row.get("cmplnt_num")
            borough = row.get("boro_nm")

            if cmplnt_num:
                cur.execute("""
                    INSERT OR IGNORE INTO crime (cmplnt_num, borough)
                    VALUES (?, ?)
                """, (cmplnt_num, borough))

        conn.commit()
        conn.close()
        offset += limit

if __name__ == "__main__":
    fetch_crime()
