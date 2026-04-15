-- ============================================================
-- FREIGHT_DB schema bootstrap
-- Run once after Snowflake account setup
-- ============================================================

CREATE DATABASE IF NOT EXISTS FREIGHT_DB;
CREATE SCHEMA IF NOT EXISTS FREIGHT_DB.LOGISTICS;

USE SCHEMA FREIGHT_DB.LOGISTICS;

-- ── Core shipment fact table ─────────────────────────────────
CREATE OR REPLACE TABLE SHIPMENTS (
    shipment_id         VARCHAR(12)   NOT NULL PRIMARY KEY,
    ship_date           DATE          NOT NULL,
    origin_city         VARCHAR(50)   NOT NULL,
    dest_city           VARCHAR(50)   NOT NULL,
    mode                VARCHAR(10)   NOT NULL,   -- PARCEL | LTL | FTL
    carrier_id          VARCHAR(12)   NOT NULL,
    weight_lbs          FLOAT         NOT NULL,
    base_rate           FLOAT         NOT NULL,
    fuel_surcharge_pct  FLOAT         NOT NULL,
    total_cost          FLOAT         NOT NULL,
    on_time_flag        SMALLINT      NOT NULL,   -- 1=on-time, 0=late
    lane_id             VARCHAR(10)   NOT NULL    -- e.g. IL-CA
)
CLUSTER BY (ship_date, mode, lane_id);

-- ── Carrier rate dimension ───────────────────────────────────
CREATE OR REPLACE TABLE CARRIER_RATES (
    carrier_id          VARCHAR(12)   NOT NULL,
    mode                VARCHAR(10)   NOT NULL,
    lane_id             VARCHAR(10)   NOT NULL,
    base_rate_per_cwt   FLOAT         NOT NULL,
    effective_date      DATE          NOT NULL,
    PRIMARY KEY (carrier_id, mode, lane_id, effective_date)
);

-- ── Weekly fuel surcharge table ──────────────────────────────
CREATE OR REPLACE TABLE FUEL_SURCHARGES (
    week_start              DATE    NOT NULL PRIMARY KEY,
    fuel_price_per_gallon   FLOAT   NOT NULL,
    surcharge_pct           FLOAT   NOT NULL
);

-- ── Anomaly flags (populated by anomaly_detection.sql) ───────
CREATE OR REPLACE TABLE ANOMALY_FLAGS (
    flag_id             VARCHAR(20)   NOT NULL PRIMARY KEY,
    shipment_id         VARCHAR(12)   NOT NULL REFERENCES SHIPMENTS(shipment_id),
    flag_type           VARCHAR(30)   NOT NULL,   -- ZSCORE | IQR | MOVING_AVG
    lane_id             VARCHAR(10),
    mode                VARCHAR(10),
    region              VARCHAR(50),
    cost_value          FLOAT,
    z_score             FLOAT,
    flag_date           DATE          NOT NULL,
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
