"""
Integration test: generate (--use-priors, n=5000) -> local anomaly detection -> metrics.
No Snowflake connection required. Fully deterministic (seeded RNG).
"""
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))
from evaluate_anomaly import compute_iqr_flags, compute_metrics, compute_zscore_flags
from generate_synthetic import generate_carrier_rates, generate_fuel_surcharges, generate_shipments

SEED = 42
N_FIXTURE = 5000
PROJECT_ROOT = Path(__file__).parent.parent


@pytest.fixture(scope='module')
def generated():
    rng = np.random.default_rng(SEED)
    run_id = 'integration-test-fixture'
    fuel_df = generate_fuel_surcharges()
    rates_df = generate_carrier_rates(rng)
    ships_df, gt_df = generate_shipments(
        rates_df, fuel_df, N_FIXTURE, rng, np.array([0.4, 0.35, 0.25]), run_id
    )
    return {'ships': ships_df, 'rates': rates_df, 'gt': gt_df, 'run_id': run_id}


def test_use_priors_warning_emitted():
    result = subprocess.run(
        [sys.executable, 'scripts/generate_synthetic.py', '--use-priors', '--n', '50'],
        capture_output=True, text=True,
        cwd=str(PROJECT_ROOT),
    )
    assert result.returncode == 0, f'Generator failed: {result.stderr}'
    assert 'priors' in result.stderr.lower(), (
        f'Expected priors warning in stderr; got: {result.stderr!r}'
    )


def test_generation_metadata_seed_source():
    result = subprocess.run(
        [sys.executable, 'scripts/generate_synthetic.py', '--use-priors', '--n', '50'],
        capture_output=True, text=True,
        cwd=str(PROJECT_ROOT),
    )
    assert result.returncode == 0
    meta_path = PROJECT_ROOT / 'data' / 'processed' / 'generation_metadata.json'
    meta = json.loads(meta_path.read_text())
    assert meta['seed_source'] == 'PRIORS'


def test_carrier_rates_no_duplicate_pks(generated):
    rates = generated['rates']
    dupes = rates.duplicated(subset=['carrier_id', 'mode', 'lane_id']).sum()
    assert dupes == 0, f'CARRIER_RATES has {dupes} duplicate PKs'


def test_join_coverage_100_percent(generated):
    ships = generated['ships']
    rates = generated['rates']
    rate_keys = set(zip(rates['carrier_id'], rates['mode'], rates['lane_id']))
    ship_keys = set(zip(ships['carrier_id'], ships['mode'], ships['lane_id']))
    missing = ship_keys - rate_keys
    assert len(missing) == 0, f'{len(missing)} shipment keys not in CARRIER_RATES'


def test_base_cost_semantics(generated):
    ships = generated['ships']
    computed = (ships['base_rate_per_cwt'] * ships['weight_lbs'] / 100).clip(lower=1.0).round(2)
    assert (ships['base_cost'] - computed).abs().max() < 0.02


def test_zscore_precision_recall(generated):
    ships = generated['ships']
    gt = generated['gt']
    flagged = compute_zscore_flags(ships, min_count=5)
    m = compute_metrics(flagged, gt)
    assert m['precision'] > 0.30, f'Z-score precision {m["precision"]:.3f} below threshold'
    assert m['recall'] > 0.10, f'Z-score recall {m["recall"]:.3f} below threshold'


def test_iqr_precision_recall(generated):
    ships = generated['ships']
    gt = generated['gt']
    flagged = compute_iqr_flags(ships, min_count=5)
    m = compute_metrics(flagged, gt)
    assert m['precision'] > 0.25, f'IQR precision {m["precision"]:.3f} below threshold'
    assert m['recall'] > 0.25, f'IQR recall {m["recall"]:.3f} below threshold'
