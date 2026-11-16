
-- One row per account summarising its alert history for risk ranking.
-- Counts incidents (not raw alerts) so overlapping windows do not inflate risk.
-- Depends on V_ALERT_INCIDENTS (01_v_alert_incidents.sql).

CREATE OR REPLACE VIEW FRAUD_DETECTION.MONITORING.V_ACCOUNT_RISK AS
SELECT
    name_orig,
    COUNT(*) AS incident_count,
    SUM(alert_count) AS raw_alert_count,
    ARRAY_AGG(DISTINCT rule_name) AS rules_fired,
    COUNT(DISTINCT rule_name) AS distinct_rule_count,
    MAX(fraud_score) AS max_fraud_score,
    MAX(peak_total_amount) AS max_window_amount,
    MIN(first_alert_time) AS first_alert_time,
    MAX(last_alert_time) AS last_alert_time
FROM FRAUD_DETECTION.MONITORING.V_ALERT_INCIDENTS
GROUP BY name_orig;