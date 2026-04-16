"""
Evaluate anomaly detection methods against ground truth.

Usage:
    python scripts/evaluate_anomaly.py --local
    python scripts/evaluate_anomaly.py --flags data/processed/anomaly_flags_export.csv

Output:
    Prints per-method and distinct-shipment metrics tagged with seed_source.
    Writes data/processed/evaluation_report.json.
"""
import argparse
import json
from pathlib import Path

import pandas as pd

PROCESSED_DIR = Path('data/processed')


def compute_zscore_flags(
    df: pd.DataFrame, threshold: float = 2.5, min_count: int = 10
) -> set:
    df = df.copy()
    df['cpl'] = df['total_cost'] / df['weight_lbs'].clip(lower=1e-6)
    stats = (
        df.groupby(['lane_id', 'mode'])['cpl']
        .agg(mean='mean', std='std', count='count')
        .reset_index()
        .query(f'count >= {min_count}')
    )
    merged = df.merge(stats, on=['lane_id', 'mode'], how='inner')
    merged['z'] = (merged['cpl'] - merged['mean']) / merged['std'].clip(lower=1e-6)
    return set(merged.loc[merged['z'].abs() > threshold, 'shipment_id'])


def compute_iqr_flags(df: pd.DataFrame, min_count: int = 10) -> set:
    df = df.copy()
    df['cpl'] = df['total_cost'] / df['weight_lbs'].clip(lower=1e-6)
    stats = (
        df.groupby(['lane_id', 'mode'])['cpl']
        .agg(
            q1=lambda x: x.quantile(0.25),
            q3=lambda x: x.quantile(0.75),
            count='count',
        )
        .reset_index()
        .query(f'count >= {min_count}')
    )
    stats['lower'] = stats['q1'] - 1.5 * (stats['q3'] - stats['q1'])
    stats['upper'] = stats['q3'] + 1.5 * (stats['q3'] - stats['q1'])
    merged = df.merge(stats, on=['lane_id', 'mode'], how='inner')
    flagged = merged.loc[
        (merged['cpl'] > merged['upper']) | (merged['cpl'] < merged['lower']),
        'shipment_id',
    ]
    return set(flagged)


def compute_metrics(flagged: set, truth: pd.DataFrame) -> dict:
    n_total = len(truth)
    n_positive = int(truth['is_anomaly'].sum())
    n_negative = n_total - n_positive

    tp = int(truth.loc[truth['shipment_id'].isin(flagged), 'is_anomaly'].sum())
    fp = len(flagged) - tp
    fn = n_positive - tp
    tn = n_negative - fp

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0

    return {
        'tp': tp, 'fp': fp, 'fn': fn, 'tn': tn,
        'precision': round(precision, 4),
        'recall': round(recall, 4),
        'f1': round(f1, 4),
        'fpr': round(fpr, 4),
        'n_flagged': len(flagged),
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--local', action='store_true',
                       help='Recompute flags from local shipments.csv')
    group.add_argument('--flags', type=Path,
                       help='Path to anomaly_flags_export.csv from Snowflake')
    return p.parse_args()


def main() -> None:
    args = parse_args()

    truth = pd.read_parquet(PROCESSED_DIR / 'anomaly_ground_truth.parquet')
    meta = json.loads((PROCESSED_DIR / 'generation_metadata.json').read_text())
    seed_source = meta['seed_source']
    run_id = meta['run_id']

    if args.local:
        ships = pd.read_csv(PROCESSED_DIR / 'shipments.csv')
        zscore_ids = compute_zscore_flags(ships)
        iqr_ids = compute_iqr_flags(ships)
        method_flags = {'ZSCORE': zscore_ids, 'IQR': iqr_ids}
    else:
        flags_df = pd.read_csv(args.flags)
        method_flags = {
            method: set(grp['shipment_id'])
            for method, grp in flags_df.groupby('flag_type')
        }

    print(f'=== Anomaly Evaluation | seed_source={seed_source} | run_id={run_id} ===')

    per_method = {}
    for method, flagged in method_flags.items():
        m = compute_metrics(flagged, truth)
        per_method[method] = m
        print(f'[{method}] precision={m["precision"]:.3f}  recall={m["recall"]:.3f}  '
              f'f1={m["f1"]:.3f}  fpr={m["fpr"]:.3f}  flagged={m["n_flagged"]:,}')

    all_flagged = set().union(*method_flags.values())
    distinct = compute_metrics(all_flagged, truth)
    print(f'[DISTINCT-SHIPMENT] precision={distinct["precision"]:.3f}  recall={distinct["recall"]:.3f}  '
          f'f1={distinct["f1"]:.3f}  fpr={distinct["fpr"]:.3f}  flagged={distinct["n_flagged"]:,}')

    report = {
        'run_id': run_id,
        'seed_source': seed_source,
        'per_method': per_method,
        'distinct_shipment': distinct,
    }
    out_path = PROCESSED_DIR / 'evaluation_report.json'
    out_path.write_text(json.dumps(report, indent=2))
    print(f'Report written to {out_path}')


if __name__ == '__main__':
    main()
