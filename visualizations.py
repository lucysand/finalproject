# visualizations.py
from db_utils import get_connection
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

sns.set_theme(style="whitegrid")
OUTPUT_DIR = Path(__file__).parent

def run_visualizations():
    conn = get_connection()

    # --- 1. Total Collisions per Borough ---
    df_collisions = pd.read_sql("""
        SELECT b.borough_name, COUNT(c.collision_id) as collisions
        FROM collisions c
        JOIN boroughs b ON c.borough_id = b.borough_id
        GROUP BY b.borough_name
    """, conn)

    plt.figure(figsize=(8,5))
    sns.barplot(x='borough_name', y='collisions', data=df_collisions, palette='Set2')
    plt.title("Total Collisions per Borough")
    plt.xlabel("Borough")
    plt.ylabel("Number of Collisions")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "collisions_per_borough.png")
    plt.close()

    # --- 2. Average Persons Injured per Collision by Borough ---
    df_injuries = pd.read_sql("""
        SELECT b.borough_name, AVG(ci.persons_injured) as avg_injured
        FROM collision_injuries ci
        JOIN collisions c ON ci.collision_id = c.collision_id
        JOIN boroughs b ON c.borough_id = b.borough_id
        GROUP BY b.borough_name
    """, conn)

    plt.figure(figsize=(8,5))
    sns.barplot(x='borough_name', y='avg_injured', data=df_injuries, palette='coolwarm')
    plt.title("Average Persons Injured per Collision by Borough")
    plt.xlabel("Borough")
    plt.ylabel("Average Injuries")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "avg_injuries_per_borough.png")
    plt.close()

    # --- 3. Average Yelp Rating per Borough ---
    df_yelp = pd.read_sql("""
        SELECT b.borough_name, AVG(y.rating) as avg_rating
        FROM yelp y
        JOIN boroughs b ON y.borough_id = b.borough_id
        GROUP BY b.borough_name
    """, conn)

    plt.figure(figsize=(8,5))
    sns.barplot(x='borough_name', y='avg_rating', data=df_yelp, palette='magma')
    plt.title("Average Yelp Rating per Borough")
    plt.xlabel("Borough")
    plt.ylabel("Average Rating")
    plt.ylim(0, 5)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "avg_yelp_rating_per_borough.png")
    plt.close()

    # --- 4. Collisions vs Black Population Percent ---
    df_coll_black = pd.read_sql("""
        SELECT b.borough_name, COUNT(c.collision_id) as collisions, d.black_percent
        FROM collisions c
        JOIN boroughs b ON c.borough_id = b.borough_id
        JOIN demographics d ON b.borough_id = d.borough_id
        GROUP BY b.borough_name
    """, conn)

    plt.figure(figsize=(8,5))
    sns.scatterplot(x='black_percent', y='collisions', data=df_coll_black, s=150, color='teal')
    for i, row in df_coll_black.iterrows():
        plt.text(row.black_percent + 0.1, row.collisions, row.borough_name)
    plt.title("Collisions vs Black Population Percent by Borough")
    plt.xlabel("Black Population Percent")
    plt.ylabel("Number of Collisions")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "collisions_vs_black_percent.png")
    plt.close()

    # --- 5. Black Population Percent vs Average Yelp Rating ---
    df_black_yelp = pd.read_sql("""
        SELECT b.borough_name, d.black_percent, AVG(y.rating) as avg_rating
        FROM yelp y
        JOIN boroughs b ON y.borough_id = b.borough_id
        JOIN demographics d ON b.borough_id = d.borough_id
        GROUP BY b.borough_name
    """, conn)

    plt.figure(figsize=(8,5))
    sns.scatterplot(x='black_percent', y='avg_rating', data=df_black_yelp, s=150, color='purple')
    for i, row in df_black_yelp.iterrows():
        plt.text(row.black_percent + 0.1, row.avg_rating, row.borough_name)
    plt.title("Black Population Percent vs Average Yelp Rating")
    plt.xlabel("Black Population Percent")
    plt.ylabel("Average Yelp Rating")
    plt.ylim(0, 5)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "black_percent_vs_avg_yelp.png")
    plt.close()

    conn.close()
    print(f"All 5 visualizations saved to {OUTPUT_DIR}")

if __name__ == "__main__":
    run_visualizations()
