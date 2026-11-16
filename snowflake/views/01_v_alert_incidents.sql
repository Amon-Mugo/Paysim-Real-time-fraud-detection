
-- Collapses overlapping sliding-window alerts (structuring, velocity) into one

CREATE OR REPLACE VIEW FRAUD_DETECTION.MONITORING.V_ALERT_INCIDENTS AS
WITH windowed_alerts AS (
    SELECT
        alert_id,
        rule_name,
        name_orig,
        alert_time,
        transaction_ids,
        ingested_at,
        COALESCE(TRY_TO_TIMESTAMP_NTZ(details:window_start::STRING), alert_time) AS window_start,
        COALESCE(TRY_TO_TIMESTAMP_NTZ(details:window_end::STRING), alert_time) AS window_end,
        details:total_amount::FLOAT AS total_amount,
        details:transaction_count::NUMBER AS window_transaction_count
    FROM FRAUD_DETECTION.MONITORING.ALERTS
    WHERE rule_name ILIKE ANY ('structuring%', 'velocity%')
),

previous_window_end AS (
    SELECT
        *,
        MAX(window_end) OVER (
            PARTITION BY rule_name, name_orig
            ORDER BY window_start, window_end
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_max_end
    FROM windowed_alerts
),

incident_boundaries AS (
    SELECT
        *,
        IFF(prev_max_end IS NULL OR window_start >= prev_max_end, 1, 0) AS is_new_incident
    FROM previous_window_end
),

incident_sequence AS (
    SELECT
        *,
        SUM(is_new_incident) OVER (
            PARTITION BY rule_name, name_orig
            ORDER BY window_start, window_end
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS incident_seq
    FROM incident_boundaries
),

collapsed_incidents AS (
    SELECT
        MD5(CONCAT_WS('|', rule_name, name_orig, TO_VARCHAR(MIN(window_start)))) AS incident_id,
        rule_name,
        name_orig,
        MIN(window_start) AS incident_start,
        MAX(window_end) AS incident_end,
        MIN(alert_time) AS first_alert_time,
        MAX(alert_time) AS last_alert_time,
        COUNT(*) AS alert_count,
        ARRAY_UNION_AGG(transaction_ids) AS transaction_ids,
        MAX(ingested_at) AS last_ingested_at,
        MAX(total_amount) AS peak_total_amount,
        MAX(window_transaction_count) AS peak_transaction_count,
        NULL::FLOAT AS amount,
        NULL::FLOAT AS fraud_score,
        NULL::STRING AS model_version
    FROM incident_sequence
    GROUP BY rule_name, name_orig, incident_seq
),

passthrough_incidents AS (
    SELECT
        alert_id AS incident_id,
        rule_name,
        name_orig,
        alert_time AS incident_start,
        alert_time AS incident_end,
        alert_time AS first_alert_time,
        alert_time AS last_alert_time,
        1 AS alert_count,
        transaction_ids,
        ingested_at AS last_ingested_at,
        NULL::FLOAT AS peak_total_amount,
        NULL::NUMBER AS peak_transaction_count,
        details:amount::FLOAT AS amount,
        details:fraud_score::FLOAT AS fraud_score,
        details:model_version::STRING AS model_version
    FROM FRAUD_DETECTION.MONITORING.ALERTS
    WHERE NOT (rule_name ILIKE ANY ('structuring%', 'velocity%'))
),

all_incidents AS (
    SELECT * FROM collapsed_incidents
    UNION ALL
    SELECT * FROM passthrough_incidents
)

SELECT
    incident_id,
    rule_name,
    name_orig,
    incident_start,
    incident_end,
    first_alert_time,
    last_alert_time,
    alert_count,
    transaction_ids,
    ARRAY_SIZE(transaction_ids) AS transaction_count,
    last_ingested_at,
    peak_total_amount,
    peak_transaction_count,
    amount,
    fraud_score,
    model_version
FROM all_incidents;