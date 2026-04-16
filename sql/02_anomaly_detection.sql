-- ============================================================
-- ANOMALY DETECTION — populates ANOMALY_FLAGS and LANE_WEEK_TRENDS
-- Run after each data load. Truncate first to avoid dupes.
-- ============================================================
USE SCHEMA FREIGHT_DB.LOGISTICS;

TRUNCATE TABLE ANOMALY_FLAGS;
TRUNCATE TABLE LANE_WEEK_TRENDS;

-- ── Method 1: Z-Score by lane × mode ────────────────────────
INSERT INTO ANOMALY_FLAGS (
    flag_id, shipment_id, flag_type, lane_id, mode,
    region, cost_value, z_score, flag_date
)
WITH lane_stats AS (
    SELECT
        lane_id,
        mode,
        AVG(total_cost / NULLIF(weight_lbs, 0))    AS mean_cpl,
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
        s.origin_city                                           AS region,
        s.total_cost                                           AS cost_value,
        s.ship_date,
        (s.total_cost / NULLIF(s.weight_lbs, 0) - ls.mean_cpl)
            / NULLIF(ls.std_cpl, 0)                            AS z_score
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


-- ── Lane-week trend detection → LANE_WEEK_TRENDS ────────────
-- Rolling signal trusted only after >= 4 prior observed weeks.
INSERT INTO LANE_WEEK_TRENDS (
    week_start, lane_id, mode, shipment_count,
    avg_cost_per_lb, rolling_4wk_avg, pct_deviation, is_anomalous
)
WITH weekly_avg AS (
    SELECT
        DATE_TRUNC('WEEK', ship_date)              AS week_start,
        lane_id,
        mode,
        COUNT(*)                                   AS shipment_count,
        AVG(total_cost / NULLIF(weight_lbs, 0))   AS avg_cpl
    FROM SHIPMENTS
    GROUP BY 1, 2, 3
),
prior_week_counts AS (
    SELECT
        week_start,
        lane_id,
        mode,
        COUNT(*) OVER (
            PARTITION BY lane_id, mode
            ORDER BY week_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prior_weeks
    FROM weekly_avg
),
rolling AS (
    SELECT
        w.week_start,
        w.lane_id,
        w.mode,
        w.shipment_count,
        w.avg_cpl,
        AVG(w.avg_cpl) OVER (
            PARTITION BY w.lane_id, w.mode
            ORDER BY w.week_start
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS rolling_4wk_avg,
        p.prior_weeks
    FROM weekly_avg w
    JOIN prior_week_counts p
        ON w.week_start = p.week_start
        AND w.lane_id = p.lane_id
        AND w.mode = p.mode
)
SELECT
    week_start,
    lane_id,
    mode,
    shipment_count,
    avg_cpl                                           AS avg_cost_per_lb,
    rolling_4wk_avg,
    (avg_cpl - rolling_4wk_avg)
        / NULLIF(rolling_4wk_avg, 0)                  AS pct_deviation,
    CASE
        WHEN prior_weeks >= 4
         AND rolling_4wk_avg IS NOT NULL
         AND ABS((avg_cpl - rolling_4wk_avg) / NULLIF(rolling_4wk_avg, 0)) > 0.20
        THEN 1
        ELSE 0
    END                                               AS is_anomalous
FROM rolling;


-- Verify
SELECT flag_type, COUNT(*) AS flags FROM ANOMALY_FLAGS GROUP BY flag_type ORDER BY 1;
