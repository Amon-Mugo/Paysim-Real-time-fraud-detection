-- V_TRANSACTIONS_SCORED
-- Recent transactions with ML score and model version, plus a flag and the
-- list of alert rules that reference each transaction. Kafka metadata is
-- excluded. Sorting is left to the dashboard query.

CREATE OR REPLACE VIEW FRAUD_DETECTION.MONITORING.V_TRANSACTIONS_SCORED AS
WITH alerted_transactions AS (
    SELECT
        flattened.value::STRING AS transaction_id,
        ARRAY_AGG(DISTINCT alerts.rule_name) AS alert_rules
    FROM FRAUD_DETECTION.MONITORING.ALERTS AS alerts,
        LATERAL FLATTEN(input => alerts.transaction_ids) AS flattened
    GROUP BY flattened.value::STRING
)

SELECT
    transactions.transaction_id,
    transactions.event_time,
    transactions.transaction_type,
    transactions.amount,
    transactions.name_orig,
    transactions.old_balance_orig,
    transactions.new_balance_orig,
    transactions.name_dest,
    transactions.old_balance_dest,
    transactions.new_balance_dest,
    transactions.latitude,
    transactions.longitude,
    transactions.fraud_score,
    transactions.model_version,
    alerted_transactions.transaction_id IS NOT NULL AS is_alerted,
    alerted_transactions.alert_rules,
    transactions.ingested_at
FROM FRAUD_DETECTION.MONITORING.TRANSACTIONS_RECENT AS transactions
LEFT JOIN alerted_transactions
    ON transactions.transaction_id = alerted_transactions.transaction_id;