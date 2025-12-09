#Names: Anna DeWitt, Lucy Sanders, Aamir Kapasi


#import pyplot


#INITAL SETUP 

def call_api_one(key):
    pass
#def call_api_two(key):

#def call_api_three(key):
    
# fetch_nyc_trees.py

# db_utils.py
import sqlite3


# fetch_trees.py
import requests
from db_utils import get_connection, row_exists, create_tables

API_URL = "https://data.cityofnewyork.us/resource/uvpi-gqnh.json"
LIMIT_PER_RUN = 25

def fetch_trees(offset=0):
    create_tables()

    params = {
        "$limit": LIMIT_PER_RUN,
        "$offset": offset
    }
    headers = {"X-App-Token": "eGeOtd7LZadVKnHmylpPErmPn"}
    resp = requests.get(API_URL, params=params, headers= headers)
    resp.raise_for_status()
    data = resp.json()

    conn = get_connection()
    cur = conn.cursor()

    inserted = 0
    for row in data:
        try:
            tree_id = int(row["tree_id"])
            borough = row.get("borough", None)
            if borough is None:
                continue
        except:
            continue

        if not row_exists("trees", "tree_id", tree_id):
            cur.execute("""
                INSERT INTO trees (tree_id, borough)
                VALUES (?, ?)
            """, (tree_id, borough))
            inserted += 1

    conn.commit()
    conn.close()
    print(f"Inserted {inserted} new tree rows. (Max {LIMIT_PER_RUN})")

if __name__ == "__main__":
    fetch_trees(offset=0)

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
        
        nta_code = row.get("nta_code")
        if not nta_code:
            continue

        if row_exists("demographics_nta", "nta_code", nta_code):
            continue

        neighborhood = row.get("nta_name")
        borough = row.get("borough")

        # Dataset columns (change if needed)
        median_income = safe_float(row.get("median_household_income"))
        poverty_rate = safe_float(row.get("poverty_rate"))
        
        pct_white = safe_float(row.get("percent_white_nonhispanic"))
        pct_black = safe_float(row.get("percent_black_nonhispanic"))
        pct_latino = safe_float(row.get("percent_hispanic"))
        pct_asian = safe_float(row.get("percent_asian_nonhispanic"))

        cur.execute("""
            INSERT INTO demographics_nta (
                nta_code, neighborhood, borough,
                median_income, poverty_rate,
                pct_white, pct_black, pct_latino, pct_asian
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            nta_code, neighborhood, borough,
            median_income, poverty_rate,
            pct_white, pct_black, pct_latino, pct_asian
        ))

        inserted += 1
    
    conn.commit()
    conn.close()
    print(f"Inserted {inserted} new demographic rows. (Max {LIMIT_PER_RUN})")
    
if __name__ == "__main__":
    fetch_demographics(offset=0)


#def create_tables():
   # """Create database tables."""
   # pass  # Implementation goes here



#DATA FETCHING
def fetch_health_batch(limit):
    """Fetch a batch of health data."""
    pass  # Implementation goes here

def fetch_tree_batch(limit):
    """Fetch a batch of tree data."""
    pass  # Implementation goes here

def fetch_demo_batch(limit):
    """Fetch a batch of demographic data."""
    pass  # Implementation goes here



#INSERT DATA
def insert_health(records):
    """Insert health records into the database."""
    pass  # Implementation goes here

def insert_tree(records):
    """Insert tree records into the database."""
    pass  # Implementation goes here    

def insert_demo(records):
    """Insert demographic records into the database."""
    pass  # Implementation goes here



# ?
def select_joined_data():
    """Select joined data from health, tree, and demographic tables."""
    pass  # Implementation goes here


#CALCULATIONS (may need to create more functions (???) tbd i guess)
def compute_calculations():
    """Compute necessary calculations on the data."""
    pass  # Implementation goes here


#VISUALIZATIONS
def plot_income_race():
    pass

def plot_tree_income():
    pass

def plot_health_tree():
    pass

def plot_tree_health_distr():
    pass
 
def plot_income_vs_asthma():
    pass 



