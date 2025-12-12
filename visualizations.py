# visualizations.py
import os
import matplotlib.pyplot as plt
from calculations import (
    calc_trees_per_borough,
    calc_black_percent,
    calc_crime_counts,
    calc_collision_counts,
    calc_trees_vs_black
)

COLORS = {
    "trees": "#2ecc71",
    "black": "#3498db",
    "crime": "#e74c3c",
    "collisions": "#9b59b6",
}

# Save plots directly in the folder where this script lives
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

def plot_bar(data, xlabel, ylabel, title, filename, color):
    if not data:
        print(f"[visual] skip {filename}: no data")
        return

    labels = [row[0] for row in data]
    values = [row[1] for row in data]

    plt.figure(figsize=(8,5))
    plt.bar(labels, values, color=color)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    save_path = os.path.join(PROJECT_DIR, filename)
    plt.savefig(save_path, bbox_inches='tight')  # ensures labels fit
    plt.close()
    print(f"Saved {filename} in {PROJECT_DIR}")

def plot_scatter(data, xlabel, ylabel, title, filename):
    if not data:
        print(f"[visual] skip {filename}: no data")
        return

    boroughs = [row[0] for row in data]
    trees = [row[1] for row in data]
    black_pct = [row[2] for row in data]

    plt.figure(figsize=(8,5))
    plt.scatter(trees, black_pct, color="#1abc9c", s=100)
    for i, b in enumerate(boroughs):
        plt.text(trees[i]+0.2, black_pct[i]+0.002, b)  # adjust label slightly

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    save_path = os.path.join(PROJECT_DIR, filename)
    plt.savefig(save_path, bbox_inches='tight')
    plt.close()
    print(f"Saved {filename} in {PROJECT_DIR}")

def run_all_visualizations():
    # Trees
    trees_data = calc_trees_per_borough()
    plot_bar(trees_data, "Borough", "Tree Count",
             "Trees by Borough", "trees_by_borough.png", COLORS["trees"])

    # Black Population %
    black_data = calc_black_percent()
    plot_bar(black_data, "Borough", "% Black",
             "Black Population % by Borough", "black_percent_by_borough.png", COLORS["black"])

    # Crime
    crime_data = calc_crime_counts()
    plot_bar(crime_data, "Borough", "Crime Count",
             "Crime by Borough", "crime_by_borough.png", COLORS["crime"])

    # Collisions
    collision_data = calc_collision_counts()
    plot_bar(collision_data, "Borough", "Collision Count",
             "Collisions by Borough", "collisions_by_borough.png", COLORS["collisions"])

    # Trees vs Black %
    trees_vs_black_data = calc_trees_vs_black()
    plot_scatter(trees_vs_black_data,
                 "Tree Count", "% Black",
                 "Tree Count vs Black Population %", "trees_vs_black_percent.png")

if __name__ == "__main__":
    run_all_visualizations()
