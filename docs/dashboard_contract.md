# Dashboard Query Contract

The dashboard reads only the five views below. It must not query the base
tables (`ALERTS`, `TRANSACTIONS_RECENT`); the dashboard role has no access to them.

## Connection

| Setting   | Value                          |
|-----------|--------------------------------|
| Database  | `FRAUD_DETECTION`              |
| Schema    | `MONITORING`                   |
| Warehouse | `FRAUD_WH`                     |
| Role      | `FRAUD_DASHBOARD_READER`       |

The role has `SELECT` on the five views only. Timestamps stored as
`TIMESTAMP_NTZ` are UTC.

## Views

### `V_ALERTS_FEED`: investigator list
One row per incident or single alert, newest first (`last_alert_time` descending).

Columns: `incident_id`, `rule_name`, `name_orig`, `incident_start`, `incident_end`,
`last_alert_time`, `alert_count`, `transaction_count`, `peak_total_amount`,
`peak_transaction_count`, `amount`, `fraud_score`, `model_version`, `summary`,
`transaction_ids`.

- `summary` is a ready-to-display sentence.
- Use this view for the main alert table.

### `V_ALERT_INCIDENTS`: incidents with full detail
Same incidents as the feed, plus `first_alert_time` and `last_ingested_at`, with no
`summary` column.

- Structuring and velocity alerts with overlapping windows are merged into one
  incident per rule and account.
- Geo and ML alerts are one row each.
- `alert_count` is the number of raw alerts merged into the incident.
- `peak_total_amount` and `peak_transaction_count` are the maximum across the
  merged windows. They are `NULL` for ML alerts.
- `amount`, `fraud_score` and `model_version` are filled for ML alerts only.

### `V_TRANSACTIONS_SCORED`: recent transactions
Columns: `transaction_id`, `event_time`, `transaction_type`, `amount`, `name_orig`,
`old_balance_orig`, `new_balance_orig`, `name_dest`, `old_balance_dest`,
`new_balance_dest`, `latitude`, `longitude`, `fraud_score`, `model_version`,
`is_alerted`, `alert_rules`, `ingested_at`.

- `is_alerted` is true when any alert references the transaction.
- `alert_rules` lists the rules that fired on it.
- The view has no `ORDER BY`; sort and filter in the dashboard query.

### `V_ALERT_SUMMARY_HOURLY`: chart data
Columns: `alert_hour`, `rule_name`, `incident_count`, `raw_alert_count`,
`accounts_affected`. One row per rule per hour (by first alert time).

### `V_ACCOUNT_RISK`: account ranking
Columns: `name_orig`, `incident_count`, `raw_alert_count`, `rules_fired`,
`distinct_rule_count`, `max_fraud_score`, `max_window_amount`, `first_alert_time`,
`last_alert_time`. One row per account.

## Behaviour to know

- Rows are purged after 7 days (by `INGESTED_AT`), so the views show recent data only.
- `fraud_score` and `model_version` come from the ML model. Until the real model
  is loaded, values come from the stub model (`stub-v1`).
- Rule names in `rule_name`: `structuring`, `velocity`, `ml_model`, `geo`.
- Views are refreshed as the streaming job lands data; there is no batch refresh.

## Example

```sql
SELECT summary, last_alert_time
FROM FRAUD_DETECTION.MONITORING.V_ALERTS_FEED
LIMIT 20;
```