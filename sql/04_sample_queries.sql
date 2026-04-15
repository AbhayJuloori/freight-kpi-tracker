-- ============================================================
-- SAMPLE / AD-HOC VALIDATION QUERIES
-- ============================================================
USE SCHEMA FREIGHT_DB.LOGISTICS;

-- Top 10 most expensive lanes (avg cost/lb)
SELECT
    lane_id,
    mode,
    COUNT(*) AS shipments,
    ROUND(AVG(total_cost / weight_lbs), 4) AS avg_cost_per_lb,
    ROUND(AVG(total_cost), 2) AS avg_total_cost
FROM SHIPMENTS
GROUP BY 1, 2
HAVING COUNT(*) >= 20
ORDER BY avg_cost_per_lb DESC
LIMIT 10;


-- Carriers with highest anomaly rate
SELECT
    s.carrier_id,
    s.mode,
    COUNT(*) AS total_shipments,
    COUNT(DISTINCT af.shipment_id) AS flagged,
    ROUND(COUNT(DISTINCT af.shipment_id)::FLOAT / COUNT(*), 4) AS anomaly_rate,
    ROUND(AVG(s.total_cost), 2) AS avg_cost
FROM SHIPMENTS s
LEFT JOIN ANOMALY_FLAGS af ON s.shipment_id = af.shipment_id
GROUP BY 1, 2
HAVING COUNT(*) >= 100
ORDER BY anomaly_rate DESC
LIMIT 10;


-- Monthly cost trend by mode
SELECT
    DATE_TRUNC('MONTH', ship_date) AS month,
    mode,
    COUNT(*) AS shipments,
    ROUND(AVG(total_cost), 2) AS avg_cost,
    ROUND(AVG(total_cost / weight_lbs), 4) AS avg_cpl
FROM SHIPMENTS
GROUP BY 1, 2
ORDER BY 1, 2;


-- Fuel surcharge vs average cost correlation
SELECT
    f.week_start,
    f.fuel_price_per_gallon,
    f.surcharge_pct,
    COUNT(s.shipment_id) AS shipments,
    ROUND(AVG(s.total_cost), 2) AS avg_cost
FROM FUEL_SURCHARGES f
LEFT JOIN SHIPMENTS s ON DATE_TRUNC('WEEK', s.ship_date) = f.week_start
GROUP BY 1, 2, 3
ORDER BY 1;


-- Sample anomaly flags with shipment detail
SELECT
    af.flag_type,
    af.lane_id,
    af.mode,
    af.z_score,
    s.ship_date,
    s.carrier_id,
    s.total_cost,
    s.weight_lbs,
    ROUND(s.total_cost / s.weight_lbs, 4) AS cost_per_lb
FROM ANOMALY_FLAGS af
JOIN SHIPMENTS s ON af.shipment_id = s.shipment_id
ORDER BY af.flag_type, af.z_score DESC NULLS LAST
LIMIT 50;
