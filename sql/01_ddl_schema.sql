CREATE DATABASE IF NOT EXISTS FREIGHT_DB;
CREATE SCHEMA IF NOT EXISTS FREIGHT_DB.LOGISTICS;
USE SCHEMA FREIGHT_DB.LOGISTICS;

CREATE OR REPLACE TABLE GENERATION_RUNS (
    run_id          VARCHAR(36)   NOT NULL PRIMARY KEY,
    generated_at    TIMESTAMP_NTZ NOT NULL,
    seed_source     VARCHAR(20)   NOT NULL,
    faf5_file       VARCHAR(500),
    n_shipments     INT           NOT NULL,
    n_anomalies     INT           NOT NULL,
    anomaly_rate    FLOAT         NOT NULL
);

CREATE OR REPLACE TABLE SHIPMENTS (
    shipment_id         VARCHAR(12)   NOT NULL PRIMARY KEY,
    ship_date           DATE          NOT NULL,
    origin_city         VARCHAR(50)   NOT NULL,
    dest_city           VARCHAR(50)   NOT NULL,
    mode                VARCHAR(10)   NOT NULL,
    carrier_id          VARCHAR(12)   NOT NULL,
    weight_lbs          FLOAT         NOT NULL,
    base_rate_per_cwt   FLOAT         NOT NULL,
    base_cost           FLOAT         NOT NULL,
    fuel_surcharge_pct  FLOAT         NOT NULL,
    total_cost          FLOAT         NOT NULL,
    on_time_flag        SMALLINT      NOT NULL,
    lane_id             VARCHAR(10)   NOT NULL,
    run_id              VARCHAR(36)   REFERENCES GENERATION_RUNS(run_id)
) CLUSTER BY (ship_date, mode, lane_id);

CREATE OR REPLACE TABLE CARRIER_RATES (
    carrier_id          VARCHAR(12)   NOT NULL,
    mode                VARCHAR(10)   NOT NULL,
    lane_id             VARCHAR(10)   NOT NULL,
    base_rate_per_cwt   FLOAT         NOT NULL,
    effective_date      DATE          NOT NULL,
    PRIMARY KEY (carrier_id, mode, lane_id, effective_date)
);

CREATE OR REPLACE TABLE FUEL_SURCHARGES (
    week_start              DATE    NOT NULL PRIMARY KEY,
    fuel_price_per_gallon   FLOAT   NOT NULL,
    surcharge_pct           FLOAT   NOT NULL
);

CREATE OR REPLACE TABLE ANOMALY_FLAGS (
    flag_id             VARCHAR(20)   NOT NULL PRIMARY KEY,
    shipment_id         VARCHAR(12)   NOT NULL REFERENCES SHIPMENTS(shipment_id),
    flag_type           VARCHAR(30)   NOT NULL,
    lane_id             VARCHAR(10),
    mode                VARCHAR(10),
    region              VARCHAR(50),
    cost_value          FLOAT,
    z_score             FLOAT,
    flag_date           DATE          NOT NULL,
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE LANE_WEEK_TRENDS (
    week_start        DATE         NOT NULL,
    lane_id           VARCHAR(10)  NOT NULL,
    mode              VARCHAR(10)  NOT NULL,
    shipment_count    INT          NOT NULL,
    avg_cost_per_lb   FLOAT        NOT NULL,
    rolling_4wk_avg   FLOAT,
    pct_deviation     FLOAT,
    is_anomalous      SMALLINT     NOT NULL DEFAULT 0,
    PRIMARY KEY (week_start, lane_id, mode)
);
