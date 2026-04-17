-- ============================================================
-- POWER BI VIEWS — optimized for Import mode
-- ============================================================
USE SCHEMA FREIGHT_DB.LOGISTICS;

-- KPI 1: On-time delivery rate by carrier and mode (monthly)
CREATE OR REPLACE VIEW VW_CARRIER_ONTIME AS
SELECT
    carrier_id,
    mode,
    DATE_TRUNC('MONTH', ship_date)                      AS month,
    COUNT(*)                                            AS total_shipments,
    SUM(on_time_flag)                                   AS on_time_shipments,
    SUM(on_time_flag)::FLOAT / NULLIF(COUNT(*), 0)      AS on_time_rate
FROM SHIPMENTS
GROUP BY 1, 2, 3;


-- KPI 2: Cost per shipment by mode and origin state (monthly)
CREATE OR REPLACE VIEW VW_COST_BY_MODE_REGION AS
SELECT
    mode,
    SPLIT_PART(origin_city, ',', 2)                     AS origin_state,
    DATE_TRUNC('MONTH', ship_date)                      AS month,
    COUNT(*)                                            AS shipment_count,
    AVG(total_cost)                                     AS avg_total_cost,
    AVG(total_cost / NULLIF(weight_lbs, 0))             AS avg_cost_per_lb,
    MEDIAN(total_cost)                                  AS median_total_cost,
    SUM(total_cost)                                     AS total_spend
FROM SHIPMENTS
GROUP BY 1, 2, 3;


-- KPI 3: Anomaly rate by region and mode (monthly)
CREATE OR REPLACE VIEW VW_ANOMALY_RATE_BY_REGION AS
SELECT
    SPLIT_PART(s.origin_city, ',', 2)                   AS origin_state,
    s.mode,
    DATE_TRUNC('MONTH', s.ship_date)                    AS month,
    COUNT(DISTINCT s.shipment_id)                       AS total_shipments,
    COUNT(DISTINCT af.shipment_id)                      AS flagged_shipments,
    COUNT(DISTINCT af.shipment_id)::FLOAT
        / NULLIF(COUNT(DISTINCT s.shipment_id), 0)      AS anomaly_rate
FROM SHIPMENTS s
LEFT JOIN ANOMALY_FLAGS af ON s.shipment_id = af.shipment_id
GROUP BY 1, 2, 3;


-- KPI 4: Carrier performance scorecard (all-time composite)
CREATE OR REPLACE VIEW VW_CARRIER_SCORECARD AS
SELECT
    s.carrier_id,
    s.mode,
    COUNT(*)                                            AS total_shipments,
    SUM(s.on_time_flag)::FLOAT / NULLIF(COUNT(*), 0)   AS on_time_rate,
    AVG(s.total_cost)                                  AS avg_cost,
    AVG(s.total_cost / NULLIF(s.weight_lbs, 0))        AS avg_cost_per_lb,
    COUNT(DISTINCT af.shipment_id)::FLOAT
        / NULLIF(COUNT(*), 0)                           AS anomaly_rate,
    SUM(s.total_cost)                                  AS total_spend
FROM SHIPMENTS s
LEFT JOIN ANOMALY_FLAGS af ON s.shipment_id = af.shipment_id
GROUP BY 1, 2;


-- KPI 5: Executive summary (single-row KPI card)
CREATE OR REPLACE VIEW VW_EXECUTIVE_SUMMARY AS
SELECT
    COUNT(*)                                            AS total_shipments,
    SUM(total_cost)                                     AS total_freight_spend,
    AVG(total_cost)                                     AS avg_cost_per_shipment,
    SUM(on_time_flag)::FLOAT / NULLIF(COUNT(*), 0)     AS overall_on_time_rate,
    (SELECT COUNT(DISTINCT shipment_id) FROM ANOMALY_FLAGS) AS total_anomalies_flagged,
    (SELECT COUNT(DISTINCT shipment_id) FROM ANOMALY_FLAGS)::FLOAT
        / NULLIF(COUNT(*), 0)                           AS overall_anomaly_rate,
    MIN(ship_date)                                      AS data_start_date,
    MAX(ship_date)                                      AS data_end_date
FROM SHIPMENTS;

-- KPI 6: Underperforming lanes — disproportionate cost overruns + late deliveries
CREATE OR REPLACE VIEW VW_LANE_RISK AS
WITH lane_stats AS (
    SELECT
        s.lane_id,
        COUNT(*)                                                AS total_shipments,
        SUM(s.is_anomaly_flag)                                  AS anomaly_count,
        SUM(s.is_anomaly_flag)::FLOAT / NULLIF(COUNT(*), 0)    AS anomaly_rate,
        SUM(CASE WHEN s.on_time_flag = 0 THEN 1 ELSE 0 END)    AS late_count,
        SUM(CASE WHEN s.on_time_flag = 0 THEN 1 ELSE 0 END)::FLOAT
            / NULLIF(COUNT(*), 0)                               AS late_rate,
        AVG(CASE WHEN s.is_anomaly_flag = 0 THEN s.total_cost END) AS avg_normal_cost,
        AVG(CASE WHEN s.is_anomaly_flag = 1 THEN s.total_cost END) AS avg_anomalous_cost
    FROM (
        SELECT s.*, CASE WHEN af.shipment_id IS NOT NULL THEN 1 ELSE 0 END AS is_anomaly_flag
        FROM SHIPMENTS s
        LEFT JOIN (SELECT DISTINCT shipment_id FROM ANOMALY_FLAGS) af
            ON s.shipment_id = af.shipment_id
    ) s
    GROUP BY lane_id
    HAVING COUNT(*) >= 50
)
SELECT
    lane_id,
    total_shipments,
    anomaly_count,
    ROUND(anomaly_rate * 100, 2)            AS anomaly_rate_pct,
    late_count,
    ROUND(late_rate * 100, 2)               AS late_rate_pct,
    ROUND(avg_normal_cost, 2)               AS avg_normal_cost,
    ROUND(avg_anomalous_cost, 2)            AS avg_anomalous_cost,
    ROUND((avg_anomalous_cost - avg_normal_cost)
        / NULLIF(avg_normal_cost, 0) * 100, 1) AS cost_overrun_pct,
    ROUND(
        0.4 * anomaly_rate + 0.3 * late_rate +
        0.3 * LEAST((avg_anomalous_cost - avg_normal_cost) / NULLIF(avg_normal_cost, 0), 10) / 10
    , 4)                                    AS risk_score
FROM lane_stats
ORDER BY risk_score DESC;
