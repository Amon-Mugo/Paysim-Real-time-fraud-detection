
-- Incident counts per rule per hou for dashboard charts.
CREATE OR REPLACE VIEW FRAUD_DETECTION.MONITORING.V_ALERT_SUMMARY_HOURLY AS
SELECT
    DATE_TRUNC('hour', first_alert_time) AS alert_hour,
    rule_name,
    COUNT(*) AS incident_count,
    SUM(alert_count) AS raw_alert_count,
    COUNT(DISTINCT name_orig) AS accounts_affected
FROM FRAUD_DETECTION.MONITORING.V_ALERT_INCIDENTS
GROUP BY DATE_TRUNC('hour', first_alert_time), rule_name;