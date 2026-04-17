"""
Generate static PNG charts for GitHub README.
Outputs: docs/images/eval_metrics.png, lane_risk.png, anomaly_breakdown.png
Usage: python scripts/export_charts.py
"""
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from evaluate_anomaly import compute_zscore_flags, compute_iqr_flags

OUT_DIR = Path("docs/images")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BG = "#1e1e2e"
FG = "#cdd6f4"
AMBER = "#f9a825"
STEEL = "#4fc3f7"
GREEN = "#a6e3a1"
RED   = "#f38ba8"
PINK  = "#cba6f7"


def _style(fig, ax):
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.tick_params(colors=FG, labelsize=10)
    ax.xaxis.label.set_color(FG)
    ax.yaxis.label.set_color(FG)
    ax.title.set_color(FG)
    for spine in ax.spines.values():
        spine.set_edgecolor("#45475a")


# ── Chart 1: Evaluation metrics ─────────────────────────────
def chart_eval_metrics():
    methods = ["Z-Score", "IQR", "Combined"]
    precision = [0.999, 0.861, 0.861]
    recall    = [0.729, 0.998, 0.998]
    f1        = [0.843, 0.924, 0.924]

    x = np.arange(len(methods))
    w = 0.25

    fig, ax = plt.subplots(figsize=(10, 6))
    _style(fig, ax)

    b1 = ax.bar(x - w, precision, w, label="Precision", color=AMBER,  alpha=0.9)
    b2 = ax.bar(x,     recall,    w, label="Recall",    color=STEEL,  alpha=0.9)
    b3 = ax.bar(x + w, f1,        w, label="F1",        color=GREEN,  alpha=0.9)

    for bars in (b1, b2, b3):
        for bar in bars:
            h = bar.get_height()
            ax.annotate(f"{h:.3f}",
                        xy=(bar.get_x() + bar.get_width() / 2, h),
                        xytext=(0, 4), textcoords="offset points",
                        ha="center", va="bottom", color=FG, fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(methods, fontsize=12)
    ax.set_ylim(0, 1.15)
    ax.set_ylabel("Score", fontsize=12)
    ax.set_title("Anomaly Detection Performance  (FAF5-seeded · 75k shipments · 5,231 injected anomalies)", fontsize=12, pad=14)
    ax.legend(facecolor="#313244", edgecolor="#45475a", labelcolor=FG, fontsize=10)
    ax.yaxis.grid(True, color="#45475a", linestyle="--", alpha=0.5)
    ax.set_axisbelow(True)

    fig.tight_layout()
    fig.savefig(OUT_DIR / "eval_metrics.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  eval_metrics.png")


# ── Chart 2: Lane risk ───────────────────────────────────────
def chart_lane_risk():
    lanes   = ["CO-PA", "IN-AZ", "AZ-KY"]
    anom    = [15.4, 15.4, 15.6]
    late    = [10.8, 11.5, 15.6]
    overrun = [979,  524,  261]

    # Normalise overrun to 0-1 for same axis
    overrun_norm = [v / max(overrun) for v in overrun]

    x = np.arange(len(lanes))
    w = 0.25

    fig, ax = plt.subplots(figsize=(10, 6))
    _style(fig, ax)

    b1 = ax.bar(x - w, anom,         w, label="Anomaly Rate %",        color=RED,  alpha=0.9)
    b2 = ax.bar(x,     late,         w, label="Late Delivery Rate %",   color=AMBER, alpha=0.9)
    b3 = ax.bar(x + w, overrun_norm, w, label="Cost Overrun (norm 0-1)", color=PINK,  alpha=0.9)

    for bars, vals, fmt in [(b1, anom, "{:.1f}%"), (b2, late, "{:.1f}%"), (b3, overrun, "+{:.0f}%")]:
        for bar, v in zip(bars, vals):
            h = bar.get_height()
            ax.annotate(fmt.format(v),
                        xy=(bar.get_x() + bar.get_width() / 2, h),
                        xytext=(0, 4), textcoords="offset points",
                        ha="center", va="bottom", color=FG, fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(lanes, fontsize=13, fontweight="bold")
    ax.set_ylabel("Rate / Normalised Score", fontsize=12)
    ax.set_title("Top 3 Underperforming Lanes  (≥50 shipments · composite risk score)", fontsize=12, pad=14)
    ax.legend(facecolor="#313244", edgecolor="#45475a", labelcolor=FG, fontsize=10)
    ax.yaxis.grid(True, color="#45475a", linestyle="--", alpha=0.5)
    ax.set_axisbelow(True)

    fig.tight_layout()
    fig.savefig(OUT_DIR / "lane_risk.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  lane_risk.png")


# ── Chart 3: Anomaly breakdown by lane (top 10) ──────────────
def chart_anomaly_breakdown():
    ships = pd.read_csv("data/processed/shipments.csv")
    gt    = pd.read_parquet("data/processed/anomaly_ground_truth.parquet")

    zscore_ids = compute_zscore_flags(ships)
    iqr_ids    = compute_iqr_flags(ships)

    ships["zscore_flag"] = ships["shipment_id"].isin(zscore_ids).astype(int)
    ships["iqr_flag"]    = ships["shipment_id"].isin(iqr_ids).astype(int)
    ships["iqr_only"]    = ((ships["iqr_flag"] == 1) & (ships["zscore_flag"] == 0)).astype(int)
    ships["both"]        = ((ships["iqr_flag"] == 1) & (ships["zscore_flag"] == 1)).astype(int)
    ships["zscore_only"] = ((ships["zscore_flag"] == 1) & (ships["iqr_flag"] == 0)).astype(int)

    by_lane = ships.groupby("lane_id")[["zscore_only","both","iqr_only"]].sum()
    by_lane["total_flags"] = by_lane.sum(axis=1)
    top10 = by_lane.nlargest(10, "total_flags")

    fig, ax = plt.subplots(figsize=(12, 6))
    _style(fig, ax)

    x = np.arange(len(top10))
    w = 0.6
    b1 = ax.bar(x, top10["zscore_only"], w, label="Z-Score only", color=AMBER, alpha=0.9)
    b2 = ax.bar(x, top10["both"],        w, bottom=top10["zscore_only"], label="Both methods", color=GREEN, alpha=0.9)
    b3 = ax.bar(x, top10["iqr_only"],    w,
                bottom=top10["zscore_only"] + top10["both"],
                label="IQR only", color=STEEL, alpha=0.9)

    ax.set_xticks(x)
    ax.set_xticklabels(top10.index, rotation=30, ha="right", fontsize=11)
    ax.set_ylabel("Flagged Shipments", fontsize=12)
    ax.set_title("Anomaly Flag Breakdown — Top 10 Lanes by Flag Volume", fontsize=12, pad=14)
    ax.legend(facecolor="#313244", edgecolor="#45475a", labelcolor=FG, fontsize=10)
    ax.yaxis.grid(True, color="#45475a", linestyle="--", alpha=0.5)
    ax.set_axisbelow(True)

    fig.tight_layout()
    fig.savefig(OUT_DIR / "anomaly_breakdown.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  anomaly_breakdown.png")


if __name__ == "__main__":
    print("Generating charts...")
    chart_eval_metrics()
    chart_lane_risk()
    chart_anomaly_breakdown()
    print(f"Done — {OUT_DIR}/")
