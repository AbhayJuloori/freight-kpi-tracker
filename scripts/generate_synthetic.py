"""
Generate 75k synthetic shipment records using FAF5 regional data as distribution seed.
Outputs: data/processed/shipments.csv, carrier_rates.csv, fuel_surcharges.csv
Also outputs: data/processed/anomaly_ground_truth.parquet (local validation only, not loaded to Snowflake)
Usage: python scripts/generate_synthetic.py
"""
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
N_SHIPMENTS = 75_000
PROCESSED_DIR = Path("data/processed")

MODES = ["PARCEL", "LTL", "FTL"]
MODE_PROBS = [0.40, 0.35, 0.25]

FAF5_MODE_MAP = {1: "PARCEL", 2: "LTL", 3: "FTL", 4: "FTL", 5: "LTL", 6: "FTL", 7: "LTL"}

CITY_POOL = [
    "Chicago,IL", "Los Angeles,CA", "New York,NY", "Dallas,TX",
    "Atlanta,GA", "Seattle,WA", "Denver,CO", "Memphis,TN",
    "Houston,TX", "Detroit,MI", "Philadelphia,PA", "Phoenix,AZ",
    "Minneapolis,MN", "Kansas City,MO", "Charlotte,NC", "Portland,OR",
    "Cincinnati,OH", "Nashville,TN", "Salt Lake City,UT", "Miami,FL",
    "Boston,MA", "St. Louis,MO", "Louisville,KY", "Columbus,OH",
    "Indianapolis,IN", "San Antonio,TX", "San Jose,CA", "Baltimore,MD",
    "Pittsburgh,PA", "Cleveland,OH",
]

CARRIER_POOL = [f"CARRIER_{i:03d}" for i in range(1, 26)]

RATE_SCHEDULE = {
    "PARCEL": (12.0, 4.0),
    "LTL": (18.5, 5.5),
    "FTL": (8.0, 2.0),
}


def generate_fuel_surcharges() -> pd.DataFrame:
    weeks = pd.date_range("2022-01-03", "2024-06-24", freq="W-MON")
    n = len(weeks)
    diesel = np.concatenate([
        np.linspace(3.50, 5.80, 26),
        np.linspace(5.80, 4.20, 26),
        np.linspace(4.20, 4.00, 26),
        np.linspace(4.00, 3.80, 26),
        np.linspace(3.80, 3.90, 26),
    ])[:n]
    surcharge_pct = ((diesel - 2.50) / 2.50 * 0.30).round(4)
    return pd.DataFrame({
        "week_start": weeks.strftime("%Y-%m-%d"),
        "fuel_price_per_gallon": diesel.round(4),
        "surcharge_pct": surcharge_pct,
    })


def generate_carrier_rates() -> pd.DataFrame:
    rng = np.random.default_rng(SEED)
    rows = []
    for carrier in CARRIER_POOL:
        for mode in MODES:
            mean, std = RATE_SCHEDULE[mode]
            # Each carrier has a slight premium/discount factor
            carrier_factor = rng.uniform(0.85, 1.15)
            for state_pair in _sample_lane_ids(rng, 30):
                lane_factor = rng.uniform(0.90, 1.10)
                rate = max(2.0, rng.normal(mean * carrier_factor * lane_factor, std * 0.3))
                rows.append({
                    "carrier_id": carrier,
                    "mode": mode,
                    "lane_id": state_pair,
                    "base_rate_per_cwt": round(rate, 4),
                    "effective_date": "2023-01-01",
                })
    return pd.DataFrame(rows)


def _sample_lane_ids(rng: np.random.Generator, n: int) -> list[str]:
    states = list({c.split(",")[1] for c in CITY_POOL})
    origins = rng.choice(states, n)
    dests = rng.choice(states, n)
    return [f"{o}-{d}" for o, d in zip(origins, dests)]


def generate_shipments(fuel_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(SEED)

    start = pd.Timestamp("2023-01-01")
    end = pd.Timestamp("2024-06-30")
    dates = pd.to_datetime(rng.integers(start.value, end.value, N_SHIPMENTS))

    modes = rng.choice(MODES, N_SHIPMENTS, p=MODE_PROBS)
    origins = rng.choice(CITY_POOL, N_SHIPMENTS)
    dests = rng.choice(CITY_POOL, N_SHIPMENTS)
    carriers = rng.choice(CARRIER_POOL, N_SHIPMENTS)

    weight_lbs = rng.lognormal(mean=6.5, sigma=1.2, size=N_SHIPMENTS).clip(1, 40_000)
    weight_cwt = weight_lbs / 100

    base_rates = np.array([
        max(2.0, rng.normal(*RATE_SCHEDULE[m])) for m in modes
    ])
    base_costs = (base_rates * weight_cwt).clip(1.0)

    # Fuel surcharge lookup by week
    fuel_df["week_start"] = pd.to_datetime(fuel_df["week_start"])
    fuel_lookup = fuel_df.set_index("week_start")["surcharge_pct"].to_dict()
    week_starts = dates.to_series().dt.to_period("W").dt.start_time.values
    fuel_pcts = np.array([
        fuel_lookup.get(pd.Timestamp(w), 0.15) for w in week_starts
    ])

    total_costs = base_costs * (1 + fuel_pcts)

    # Inject 7% anomalies
    anomaly_mask = rng.random(N_SHIPMENTS) < 0.07
    total_costs[anomaly_mask] *= rng.uniform(2.5, 5.0, int(anomaly_mask.sum()))

    # On-time flags
    on_time_base = {"PARCEL": 0.92, "LTL": 0.87, "FTL": 0.94}
    on_time = np.array([
        int(rng.random() < (on_time_base[m] * (0.6 if anomaly_mask[i] else 1.0)))
        for i, m in enumerate(modes)
    ])

    lane_ids = [
        f"{o.split(',')[1]}-{d.split(',')[1]}"
        for o, d in zip(origins, dests)
    ]

    shipments = pd.DataFrame({
        "shipment_id": [f"SHP{i:07d}" for i in range(N_SHIPMENTS)],
        "ship_date": dates.strftime("%Y-%m-%d"),
        "origin_city": origins,
        "dest_city": dests,
        "mode": modes,
        "carrier_id": carriers,
        "weight_lbs": weight_lbs.round(1),
        "base_rate": base_costs.round(2),
        "fuel_surcharge_pct": fuel_pcts.round(4),
        "total_cost": total_costs.round(2),
        "on_time_flag": on_time,
        "lane_id": lane_ids,
    })

    ground_truth = pd.DataFrame({
        "shipment_id": shipments["shipment_id"],
        "is_anomaly": anomaly_mask.astype(int),
    })

    return shipments, ground_truth


def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("Generating fuel surcharges...")
    fuel_df = generate_fuel_surcharges()
    fuel_df.to_csv(PROCESSED_DIR / "fuel_surcharges.csv", index=False)
    print(f"  {len(fuel_df)} weeks")

    print("Generating carrier rates...")
    rates_df = generate_carrier_rates()
    rates_df.to_csv(PROCESSED_DIR / "carrier_rates.csv", index=False)
    print(f"  {len(rates_df):,} rate records")

    print(f"Generating {N_SHIPMENTS:,} shipments...")
    shipments_df, ground_truth_df = generate_shipments(fuel_df)
    shipments_df.to_csv(PROCESSED_DIR / "shipments.csv", index=False)
    ground_truth_df.to_parquet(PROCESSED_DIR / "anomaly_ground_truth.parquet", index=False)

    n_anomalies = ground_truth_df["is_anomaly"].sum()
    print(f"  {len(shipments_df):,} shipments | {n_anomalies:,} injected anomalies ({n_anomalies/len(shipments_df):.1%})")
    print(f"  Mode distribution:\n{shipments_df['mode'].value_counts()}")
    print(f"  Date range: {shipments_df['ship_date'].min()} → {shipments_df['ship_date'].max()}")
    print(f"\nOutputs in {PROCESSED_DIR}/")


if __name__ == "__main__":
    main()
