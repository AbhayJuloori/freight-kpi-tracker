"""
Generate synthetic shipment records using FAF5 regional data as distribution seed.

Usage:
    python scripts/generate_synthetic.py                        # requires data/raw/faf5_*.csv
    python scripts/generate_synthetic.py --use-priors           # loud warning, no FAF5 needed
    python scripts/generate_synthetic.py --faf5-path /path/to/faf5.csv
    python scripts/generate_synthetic.py --use-priors --n 500   # small fixture for tests

Outputs (data/processed/):
    shipments.csv, carrier_rates.csv, fuel_surcharges.csv,
    anomaly_ground_truth.parquet, generation_metadata.json
"""
import argparse
import json
import sys
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
DEFAULT_N_SHIPMENTS = 75_000
PROCESSED_DIR = Path('data/processed')
RAW_DIR = Path('data/raw')

MODES = ['PARCEL', 'LTL', 'FTL']
MODE_PROBS_PRIOR = [0.40, 0.35, 0.25]

FAF5_MODE_MAP = {1: 'PARCEL', 2: 'LTL', 3: 'FTL', 4: 'FTL', 5: 'LTL', 6: 'FTL', 7: 'LTL'}

FIPS_TO_STATE = {
    4: 'AZ', 6: 'CA', 8: 'CO', 12: 'FL', 13: 'GA',
    17: 'IL', 18: 'IN', 21: 'KY', 24: 'MD', 25: 'MA',
    26: 'MI', 27: 'MN', 29: 'MO', 36: 'NY', 37: 'NC',
    39: 'OH', 41: 'OR', 42: 'PA', 47: 'TN', 48: 'TX',
    49: 'UT', 53: 'WA',
}

CITY_POOL = [
    'Chicago,IL', 'Los Angeles,CA', 'New York,NY', 'Dallas,TX',
    'Atlanta,GA', 'Seattle,WA', 'Denver,CO', 'Memphis,TN',
    'Houston,TX', 'Detroit,MI', 'Philadelphia,PA', 'Phoenix,AZ',
    'Minneapolis,MN', 'Kansas City,MO', 'Charlotte,NC', 'Portland,OR',
    'Cincinnati,OH', 'Nashville,TN', 'Salt Lake City,UT', 'Miami,FL',
    'Boston,MA', 'St. Louis,MO', 'Louisville,KY', 'Columbus,OH',
    'Indianapolis,IN', 'San Antonio,TX', 'San Jose,CA', 'Baltimore,MD',
    'Pittsburgh,PA', 'Cleveland,OH',
]

STATE_TO_CITIES: dict = {}
for _city in CITY_POOL:
    _state = _city.split(',')[1].strip()
    STATE_TO_CITIES.setdefault(_state, []).append(_city)

CARRIER_POOL = [f'CARRIER_{i:03d}' for i in range(1, 26)]

RATE_SCHEDULE = {
    'PARCEL': (12.0, 4.0),
    'LTL': (18.5, 5.5),
    'FTL': (8.0, 2.0),
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Generate synthetic freight data')
    p.add_argument('--use-priors', action='store_true',
                   help='Use hardcoded priors instead of FAF5 (emits loud warning)')
    p.add_argument('--faf5-path', type=Path, default=None,
                   help='Explicit path to FAF5 CSV; overrides auto-detection')
    p.add_argument('--n', type=int, default=DEFAULT_N_SHIPMENTS,
                   help='Number of shipments to generate (default: 75000)')
    return p.parse_args()


def resolve_faf5_path(explicit):
    if explicit is not None:
        return explicit
    candidates = sorted(RAW_DIR.glob('faf5_*.csv'), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def load_faf5_distributions(faf5_path):
    df = pd.read_csv(faf5_path, usecols=['fr_orig', 'fr_dest', 'dms_mode'], low_memory=False)

    def zone_to_state(zone):
        try:
            fips = int(str(int(zone)).zfill(3)[:2])
            return FIPS_TO_STATE.get(fips)
        except (ValueError, TypeError):
            return None

    df['orig_state'] = df['fr_orig'].apply(zone_to_state)
    df['dest_state'] = df['fr_dest'].apply(zone_to_state)
    df = df.dropna(subset=['orig_state', 'dest_state'])

    lane_counts = df.groupby(['orig_state', 'dest_state']).size()
    lane_weights = {f'{o}-{d}': int(cnt) for (o, d), cnt in lane_counts.items()}

    mode_series = df['dms_mode'].map(FAF5_MODE_MAP).dropna()
    mode_counts = mode_series.value_counts()
    mode_probs = np.array([mode_counts.get(m, 0) for m in MODES], dtype=float)
    if mode_probs.sum() == 0:
        raise ValueError('FAF5 mode column produced no usable mode values after mapping')
    mode_probs /= mode_probs.sum()

    return lane_weights, mode_probs


def _all_lane_ids():
    states = sorted(STATE_TO_CITIES.keys())
    return [f'{o}-{d}' for o in states for d in states if o != d]


def generate_fuel_surcharges():
    weeks = pd.date_range('2022-01-03', '2024-06-24', freq='W-MON')
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
        'week_start': weeks.strftime('%Y-%m-%d'),
        'fuel_price_per_gallon': diesel.round(4),
        'surcharge_pct': surcharge_pct,
    })


def generate_carrier_rates(rng, lane_weights=None):
    all_lanes = _all_lane_ids()
    n_lanes_per_group = min(30, len(all_lanes))

    if lane_weights is not None:
        weights = np.array([lane_weights.get(l, 0) for l in all_lanes], dtype=float)
        weights += 1.0
        weights /= weights.sum()
    else:
        weights = None

    rows = []
    for carrier in CARRIER_POOL:
        for mode in MODES:
            mean, std = RATE_SCHEDULE[mode]
            carrier_factor = rng.uniform(0.85, 1.15)
            chosen_lanes = rng.choice(all_lanes, n_lanes_per_group, replace=False, p=weights)
            for lane in chosen_lanes:
                lane_factor = rng.uniform(0.90, 1.10)
                rate = max(2.0, rng.normal(mean * carrier_factor * lane_factor, std * 0.3))
                rows.append({
                    'carrier_id': carrier,
                    'mode': mode,
                    'lane_id': lane,
                    'base_rate_per_cwt': round(rate, 4),
                    'effective_date': '2023-01-01',
                })
    return pd.DataFrame(rows)


def generate_shipments(rates_df, fuel_df, n, rng, mode_probs, run_id):
    start = pd.Timestamp('2023-01-01')
    end = pd.Timestamp('2024-06-30')
    dates = pd.to_datetime(rng.integers(start.value, end.value, n))

    rate_idx = rng.integers(0, len(rates_df), n)
    sampled = rates_df.iloc[rate_idx].reset_index(drop=True)

    origins = []
    dests = []
    for lane_id in sampled['lane_id']:
        orig_state, dest_state = lane_id.split('-')
        origins.append(rng.choice(STATE_TO_CITIES[orig_state]))
        dests.append(rng.choice(STATE_TO_CITIES[dest_state]))

    weight_lbs = rng.lognormal(mean=6.5, sigma=1.2, size=n).clip(1, 40_000)
    weight_cwt = weight_lbs / 100

    base_rate_per_cwt = sampled['base_rate_per_cwt'].to_numpy()
    base_costs = (base_rate_per_cwt * weight_cwt).clip(1.0)

    fuel_df = fuel_df.copy()
    fuel_df['week_start'] = pd.to_datetime(fuel_df['week_start'])
    fuel_lookup = fuel_df.set_index('week_start')['surcharge_pct'].to_dict()
    week_starts = dates.to_series().dt.to_period('W').dt.start_time.values
    fuel_pcts = np.array([fuel_lookup.get(pd.Timestamp(w), 0.15) for w in week_starts])

    total_costs = base_costs * (1 + fuel_pcts)

    anomaly_mask = rng.random(n) < 0.07
    total_costs[anomaly_mask] *= rng.uniform(2.5, 5.0, int(anomaly_mask.sum()))

    on_time_base = {'PARCEL': 0.92, 'LTL': 0.87, 'FTL': 0.94}
    on_time = np.array([
        int(rng.random() < (on_time_base[m] * (0.6 if anomaly_mask[i] else 1.0)))
        for i, m in enumerate(sampled['mode'])
    ])

    shipments = pd.DataFrame({
        'shipment_id': [f'SHP{i:07d}' for i in range(n)],
        'ship_date': dates.strftime('%Y-%m-%d'),
        'origin_city': origins,
        'dest_city': dests,
        'mode': sampled['mode'].to_numpy(),
        'carrier_id': sampled['carrier_id'].to_numpy(),
        'weight_lbs': weight_lbs.round(1),
        'base_rate_per_cwt': base_rate_per_cwt.round(4),
        'base_cost': base_costs.round(2),
        'fuel_surcharge_pct': fuel_pcts.round(4),
        'total_cost': total_costs.round(2),
        'on_time_flag': on_time,
        'lane_id': sampled['lane_id'].to_numpy(),
        'run_id': run_id,
    })

    ground_truth = pd.DataFrame({
        'shipment_id': shipments['shipment_id'],
        'is_anomaly': anomaly_mask.astype(int),
        'run_id': run_id,
    })

    return shipments, ground_truth


def main():
    args = parse_args()
    rng = np.random.default_rng(SEED)
    run_id = str(uuid.uuid4())

    lane_weights = None
    mode_probs = np.array(MODE_PROBS_PRIOR)
    faf5_file_used = None

    if args.use_priors:
        print(
            'WARNING: --use-priors active; using hardcoded priors, NOT FAF5 data. '
            'Any claims about FAF5-seeded generation are invalid for this run.',
            file=sys.stderr,
        )
        seed_source = 'PRIORS'
    else:
        faf5_path = resolve_faf5_path(args.faf5_path)
        if faf5_path is None:
            raise FileNotFoundError(
                'No FAF5 file found in data/raw/. '
                'Run make download to fetch faf5_2022_2024.csv, '
                'or pass --use-priors to use hardcoded distributions.'
            )
        print(f'Loading FAF5 distributions from {faf5_path}...')
        lane_weights, mode_probs = load_faf5_distributions(faf5_path)
        faf5_file_used = str(faf5_path)
        seed_source = 'FAF5'
        print(f'  FAF5 mode distribution: {dict(zip(MODES, mode_probs.round(3)))}')
        print(f'  FAF5 lane pool: {len(lane_weights):,} OD pairs')

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print('Generating fuel surcharges...')
    fuel_df = generate_fuel_surcharges()
    fuel_df.to_csv(PROCESSED_DIR / 'fuel_surcharges.csv', index=False)

    print('Generating carrier rates...')
    rates_df = generate_carrier_rates(rng, lane_weights=lane_weights)
    rates_df.to_csv(PROCESSED_DIR / 'carrier_rates.csv', index=False)
    dup_pks = rates_df.duplicated(subset=['carrier_id', 'mode', 'lane_id']).sum()
    if dup_pks > 0:
        raise RuntimeError(f'CARRIER_RATES has {dup_pks} duplicate PKs — this is a bug')
    print(f'  {len(rates_df):,} rate records | 0 duplicate PKs')

    print(f'Generating {args.n:,} shipments...')
    shipments_df, ground_truth_df = generate_shipments(
        rates_df, fuel_df, args.n, rng, mode_probs, run_id
    )
    shipments_df.to_csv(PROCESSED_DIR / 'shipments.csv', index=False)
    ground_truth_df.to_parquet(PROCESSED_DIR / 'anomaly_ground_truth.parquet', index=False)

    n_anomalies = int(ground_truth_df['is_anomaly'].sum())

    metadata = {
        'run_id': run_id,
        'seed_source': seed_source,
        'faf5_file': faf5_file_used,
        'generated_at': pd.Timestamp.now().isoformat(),
        'n_shipments': args.n,
        'n_anomalies': n_anomalies,
        'anomaly_rate': round(n_anomalies / args.n, 4),
    }
    (PROCESSED_DIR / 'generation_metadata.json').write_text(json.dumps(metadata, indent=2))

    print(f'  {args.n:,} shipments | {n_anomalies:,} anomalies ({n_anomalies/args.n:.1%})')
    print(f'  run_id: {run_id}')
    print(f'  seed_source: {seed_source}')
    print(f'Outputs in {PROCESSED_DIR}/')


if __name__ == '__main__':
    main()
