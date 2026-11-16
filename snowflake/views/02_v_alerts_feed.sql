-- V_ALERTS_FEED
-- Investigator feed: one row per incident or single alert, newest first.
-- Adds a human-readable summary so the dashboard needs no JSON parsing.
-- Depends on V_ALERT_INCIDENTS (01_v_alert_incidents.sql).

CREATE OR REPLACE VIEW FRAUD_DETECTION.MONITORING.V_ALERTS_FEED AS
SELECT
    incident_id,
    rule_name,
    name_orig,
    incident_start,
    incident_end,
    last_alert_time,
    alert_count,
    transaction_count,
    peak_total_amount,
    peak_transaction_count,
    amount,
    fraud_score,
    model_version,
    CASE
        WHEN rule_name ILIKE ANY ('structuring%', 'velocity%') THEN
            rule_name || ': up to ' || peak_transaction_count || ' transactions totalling '
            || TO_VARCHAR(peak_total_amount, 'FM999,999,999,990.00')
            || ' across ' || alert_count || ' overlapping window(s)'
        WHEN rule_name ILIKE 'ml_model%' THEN
            'ML score ' || TO_VARCHAR(fraud_score, 'FM0.00')
            || ' on amount ' || TO_VARCHAR(amount, 'FM999,999,999,990.00')
            || ' (' || model_version || ')'
        ELSE rule_name || ' alert'
    END AS summary,
    transaction_ids
FROM FRAUD_DETECTION.MONITORING.V_ALERT_INCIDENTS
ORDER BY last_alert_time DESC, incident_id;