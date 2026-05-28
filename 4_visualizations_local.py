"""
4_visualizations_local.py
MACS 30113 Final Project — Anyi Li

Note:
Run this script on locally.
Before running, download the CSV files from S3 using the instructions below.

DOWNLOAD THE DATA FROM S3:
  Open a terminal and run:
    aws s3 cp s3://30113-final-project/results/plot_data/monthly_stats_csv/ ~/30113/final/plot_data/monthly_stats/ --recursive
    aws s3 cp s3://30113-final-project/results/plot_data/period_comparison_csv/ ~/30113/final/plot_data/period_comparison/ --recursive
    aws s3 cp s3://30113-final-project/results/plot_data/ttest_results_csv/ ~/30113/final/plot_data/ttest_results/ --recursive
    Feel free to change the file path - I am only showing my own (Anyi's) local path. 

  Then put this script in the same folder as created ./30113/final/plot_data/

RUN:
  pip install matplotlib pandas
  cd ~/30113/final
  python 4_visualizations_local.py

OUTPUT:
  Four PNG image files saved in the same folder as this script:
    plot1_monthly_sentiment.png
    plot2_monthly_volume.png
    plot3_sentiment_volatility.png
    plot4_pre_post_comparison.png
"""

import os
import glob
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# STEP 1: LOAD THE CSV FILES
# Glob all part files saved from spark and read them together

def load_csv_folder(folder_path):
    """Load all CSV part files from a folder into one DataFrame."""
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in: {folder_path}\n"
            "Check if data already download from S3. See the instructions at the top of this file."
        )
    df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
    print(f"Loaded {len(df)} rows from {folder_path}")
    return df

print("Loading data...")
monthly_pd = load_csv_folder("./plot_data/monthly_stats/")
period_pd  = load_csv_folder("./plot_data/period_comparison/")
ttest_pd   = load_csv_folder("./plot_data/ttest_results/")

# Create a proper date column from year + month
monthly_pd["date"] = pd.to_datetime(
    monthly_pd["year"].astype(str) + "-" + monthly_pd["month"].astype(str).str.zfill(2)
)
monthly_pd = monthly_pd.sort_values("date")

print("\nMonthly data preview:")
print(monthly_pd.head())
print("\nPeriod comparison:")
print(period_pd)
print("\nt-test results:")
print(ttest_pd)

COVID_DATE = pd.Timestamp("2020-03-11")
SUBREDDITS = monthly_pd["subreddit"].unique()
COLORS = {"depression": "#e05c5c", "anxiety": "#f0a500", "mentalhealth": "#4a90d9"}

# PLOT 1: Monthly Average Sentiment Over Time
print("\nGenerating Plot 1: Monthly Average Sentiment...")

fig, ax = plt.subplots(figsize=(14, 5))

for sub in SUBREDDITS:
    group = monthly_pd[monthly_pd["subreddit"] == sub]
    color = COLORS.get(sub, None)
    ax.plot(group["date"], group["avg_sentiment"],
            label=f"r/{sub}", marker='o', markersize=3, linewidth=1.5, color=color)

ax.axvline(COVID_DATE, color='black', linestyle='--', linewidth=1.5, label="COVID-19 onset (Mar 11, 2020)")
ax.axhline(0, color='gray', linestyle=':', linewidth=1)
ax.set_title("Monthly Average Sentiment Score by Subreddit", fontsize=14, fontweight='bold')
ax.set_xlabel("Date")
ax.set_ylabel("Average VADER Sentiment Score\n(−1 = most negative, +1 = most positive)")
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("plot1_monthly_sentiment.png", dpi=150)
plt.close()
print("  Saved: plot1_monthly_sentiment.png")

# PLOT 2: Monthly Post Volume Over Time
print("Generating Plot 2: Monthly Post Volume...")

fig, ax = plt.subplots(figsize=(14, 5))

for sub in SUBREDDITS:
    group = monthly_pd[monthly_pd["subreddit"] == sub]
    color = COLORS.get(sub, None)
    ax.plot(group["date"], group["post_volume"],
            label=f"r/{sub}", marker='o', markersize=3, linewidth=1.5, color=color)

ax.axvline(COVID_DATE, color='black', linestyle='--', linewidth=1.5, label="COVID-19 onset (Mar 11, 2020)")
ax.set_title("Monthly Post Volume by Subreddit", fontsize=14, fontweight='bold')
ax.set_xlabel("Date")
ax.set_ylabel("Number of Posts / Comments")
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("plot2_monthly_volume.png", dpi=150)
plt.close()
print("  Saved: plot2_monthly_volume.png")

# PLOT 3: Sentiment Volatility Over Time
print("Generating Plot 3: Sentiment Volatility...")

fig, ax = plt.subplots(figsize=(14, 5))

for sub in SUBREDDITS:
    group = monthly_pd[monthly_pd["subreddit"] == sub]
    color = COLORS.get(sub, None)
    ax.plot(group["date"], group["sentiment_volatility"],
            label=f"r/{sub}", marker='o', markersize=3, linewidth=1.5, color=color)

ax.axvline(COVID_DATE, color='black', linestyle='--', linewidth=1.5, label="COVID-19 onset (Mar 11, 2020)")
ax.set_title("Monthly Sentiment Volatility (Std Dev) by Subreddit", fontsize=14, fontweight='bold')
ax.set_xlabel("Date")
ax.set_ylabel("Sentiment Standard Deviation\n(higher = more emotionally unstable)")
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("plot3_sentiment_volatility.png", dpi=150)
plt.close()
print("  Saved: plot3_sentiment_volatility.png")

# PLOT 4: Pre vs Post COVID Bar Chart
print("Generating Plot 4: Pre vs Post COVID comparison...")

subreddits = period_pd["subreddit"].unique()
x = range(len(subreddits))
width = 0.35

def get_val(df, sub, period, col):
    rows = df[(df["subreddit"] == sub) & (df["period"] == period)]
    if len(rows) > 0:
        return float(rows[col].values[0])
    return 0.0  # returns 0 if pre-COVID data is missing

pre_vals  = [get_val(period_pd, s, "pre_covid",  "avg_sentiment") for s in subreddits]
post_vals = [get_val(period_pd, s, "post_covid", "avg_sentiment") for s in subreddits]

fig, ax = plt.subplots(figsize=(10, 5))
bars_pre  = ax.bar([i - width/2 for i in x], pre_vals,  width, label='Pre-COVID',  color='steelblue', alpha=0.85)
bars_post = ax.bar([i + width/2 for i in x], post_vals, width, label='Post-COVID', color='tomato',    alpha=0.85)

# Add value labels on bars
for bar in bars_pre + bars_post:
    h = bar.get_height()
    if h != 0:
        ax.text(bar.get_x() + bar.get_width()/2., h + 0.002,
                f'{h:.3f}', ha='center', va='bottom', fontsize=9)

ax.set_title("Average Sentiment: Pre vs Post COVID-19", fontsize=14, fontweight='bold')
ax.set_xlabel("Subreddit")
ax.set_ylabel("Average VADER Sentiment Score")
ax.set_xticks(list(x))
ax.set_xticklabels([f"r/{s}" for s in subreddits])
ax.axhline(0, color='gray', linestyle=':', linewidth=1)
ax.legend()
ax.grid(True, alpha=0.3, axis='y')

# Add note if pre-COVID data is missing
if all(v == 0 for v in pre_vals):
    ax.text(0.5, 0.95, "Note: Pre-COVID bars are 0 because only post-COVID test batch is loaded.",
            transform=ax.transAxes, ha='center', va='top', fontsize=9,
            color='gray', style='italic')

plt.tight_layout()
plt.savefig("plot4_pre_post_comparison.png", dpi=150)
plt.close()
print("  Saved: plot4_pre_post_comparison.png")

# DONE
print("\n✓ All 4 plots saved successfully!")
print("Files created:")
for f in ["plot1_monthly_sentiment.png", "plot2_monthly_volume.png",
          "plot3_sentiment_volatility.png", "plot4_pre_post_comparison.png"]:
    print(f"  {f}")
