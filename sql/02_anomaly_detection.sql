-- ============================================================
-- ANOMALY DETECTION — populates ANOMALY_FLAGS
-- Run after each data load. Truncate first to avoid dupes.
-- ============================================================
USE SCHEMA FREIGHT_DB.LOGISTICS;

TRUNCATE TABLE ANOMALY_FLAGS;

-- ── Method 1: Z-Score by lane × mode ────────────────────────
-- Flags shipments where |z-score of cost_per_lb| > 2.5
INSERT INTO ANOMALY_FLAGS (
    flag_id, shipment_id, flag_type, lane_id, mode,
    region, cost_value, z_score, flag_date
)
WITH lane_stats AS (
    SELECT
        lane_id,
        mode,
        AVG(total_cost / NULLIF(weight_lbs, 0))   AS mean_cpl,
        STDDEV(total_cost / NULLIF(weight_lbs, 0)) AS std_cpl
    FROM SHIPMENTS
    GROUP BY lane_id, mode
    HAVING COUNT(*) >= 10
),
scored AS (
    SELECT
        s.shipment_id,
        s.lane_id,
        s.mode,
        s.origin_city                                          AS region,
        s.total_cost / NULLIF(s.weight_lbs, 0)               AS cpl,
        s.total_cost                                          AS cost_value,
        s.ship_date,
        (s.total_cost / NULLIF(s.weight_lbs, 0) - ls.mean_cpl)
            / NULLIF(ls.std_cpl, 0)                           AS z_score
    FROM SHIPMENTS s
    JOIN lane_stats ls ON s.lane_id = ls.lane_id AND s.mode = ls.mode
)
SELECT
    'ZSCORE_' || shipment_id,
    shipment_id,
    'ZSCORE',
    lane_id,
    mode,
    region,
    cost_value,
    z_score,
    ship_date
FROM scored
WHERE ABS(z_score) > 2.5;


-- ── Method 2: IQR on cost_per_lb ────────────────────────────
INSERT INTO ANOMALY_FLAGS (
    flag_id, shipment_id, flag_type, lane_id, mode,
    region, cost_value, z_score, flag_date
)
WITH iqr_bounds AS (
    SELECT
        lane_id,
        mode,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_cost / NULLIF(weight_lbs, 0)) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_cost / NULLIF(weight_lbs, 0)) AS q3
    FROM SHIPMENTS
    GROUP BY lane_id, mode
    HAVING COUNT(*) >= 10
),
bounds AS (
    SELECT
        lane_id,
        mode,
        q1 - 1.5 * (q3 - q1) AS lower_fence,
        q3 + 1.5 * (q3 - q1) AS upper_fence
    FROM iqr_bounds
)
SELECT
    'IQR_' || s.shipment_id,
    s.shipment_id,
    'IQR',
    s.lane_id,
    s.mode,
    s.origin_city,
    s.total_cost,
    NULL,
    s.ship_date
FROM SHIPMENTS s
JOIN bounds b ON s.lane_id = b.lane_id AND s.mode = b.mode
WHERE (s.total_cost / NULLIF(s.weight_lbs, 0)) > b.upper_fence
   OR (s.total_cost / NULLIF(s.weight_lbs, 0)) < b.lower_fence;


-- ── Method 3: 4-Week Moving Average Deviation > 20% ─────────
INSERT INTO ANOMALY_FLAGS (
    flag_id, shipment_id, flag_type, lane_id, mode,
    region, cost_value, z_score, flag_date
)
WITH weekly_avg AS (
    SELECT
        DATE_TRUNC('WEEK', ship_date)              AS week_start,
        lane_id,
        mode,
        AVG(total_cost / NULLIF(weight_lbs, 0))   AS avg_cpl
    FROM SHIPMENTS
    GROUP BY 1, 2, 3
),
rolling AS (
    SELECT
        week_start,
        lane_id,
        mode,
        avg_cpl,
        AVG(avg_cpl) OVER (
            PARTITION BY lane_id, mode
            ORDER BY week_start
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS rolling_4wk_avg
    FROM weekly_avg
),
flagged_weeks AS (
    SELECT week_start, lane_id, mode
    FROM rolling
    WHERE rolling_4wk_avg IS NOT NULL
      AND ABS(avg_cpl - rolling_4wk_avg) / NULLIF(rolling_4wk_avg, 0) > 0.20
)
SELECT
    'MAVG_' || s.shipment_id,
    s.shipment_id,
    'MOVING_AVG',
    s.lane_id,
    s.mode,
    s.origin_city,
    s.total_cost,
    NULL,
    s.ship_date
FROM SHIPMENTS s
JOIN flagged_weeks fw
    ON DATE_TRUNC('WEEK', s.ship_date) = fw.week_start
   AND s.lane_id = fw.lane_id
   AND s.mode = fw.mode;


-- Verify
SELECT flag_type, COUNT(*) AS flags FROM ANOMALY_FLAGS GROUP BY flag_type ORDER BY 1;
SELECT COUNT(DISTINCT shipment_id) AS unique_flagged_shipments FROM ANOMALY_FLAGS;
