"""Stable configuration values for Freight v2 artifacts."""

from __future__ import annotations

import hashlib
import uuid
from enum import StrEnum

SCHEMA_VERSION = "2.0.0"


class SeedSource(StrEnum):
    """Supported inputs for the shipment-distribution seed."""

    FAF5 = "FAF5"
    PRIORS = "PRIORS"
    TEST = "TEST"


def create_run_id(
    seed_source: SeedSource,
    *,
    random_seed: int,
    fixture_name: str | None = None,
    rows: int | None = None,
    anomaly_seed: int | None = None,
) -> str:
    """Create repeatable fixture IDs and opaque UUIDs for normal runs."""
    if seed_source is SeedSource.TEST:
        if not fixture_name:
            raise ValueError("TEST runs require a non-empty fixture_name")
        payload = ":".join(
            (
                SCHEMA_VERSION,
                seed_source.value,
                str(random_seed),
                fixture_name,
                str(rows) if rows is not None else "unspecified",
                str(anomaly_seed) if anomaly_seed is not None else "unspecified",
            )
        )
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
        return f"test-{digest}"
    return str(uuid.uuid4())
