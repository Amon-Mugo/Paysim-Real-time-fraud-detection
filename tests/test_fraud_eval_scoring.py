"""Unit tests for airflow/dags/fraud_eval_scoring.py."""

import json

import pytest

from airflow.dags.fraud_eval_scoring import evaluate, format_report, parse_ground_truth

GROUND_TRUTH = {"T1": "paysim_fraud", "T2": "structuring", "T3": "velocity"}


def _alert(rule_name, transaction_ids):
    return {"rule_name": rule_name, "transaction_ids": transaction_ids}


def _mixed_alerts():
    return [
        _alert("structuring", ["T2", "N1"]),
        _alert("ml_model", ["T1"]),
        _alert("ml_model", ["N2"]),
    ]


def test_parse_ground_truth_maps_ids_to_patterns_and_skips_blank_lines():
    lines = [
        json.dumps(
            {"transaction_id": "T1", "pattern": "paysim_fraud", "scenario_id": None}
        ),
        "",
        "   ",
        json.dumps(
            {"transaction_id": "T2", "pattern": "structuring", "scenario_id": "STR-1"}
        ),
    ]

    assert parse_ground_truth(lines) == {"T1": "paysim_fraud", "T2": "structuring"}


def test_parse_ground_truth_reports_missing_field_with_line_number():
    lines = [
        json.dumps({"transaction_id": "T1", "pattern": "paysim_fraud"}),
        json.dumps({"transaction_id": "T2"}),
    ]

    with pytest.raises(ValueError, match="line 2"):
        parse_ground_truth(lines)


def test_evaluate_scores_a_mixed_run():
    result = evaluate(GROUND_TRUTH, _mixed_alerts())

    assert result.labelled_fraud == 3
    assert result.alert_count == 3
    assert result.flagged_transactions == 4
    assert result.true_positives == 2
    assert result.false_positives == 2
    assert result.false_negatives == 1
    assert result.precision == pytest.approx(0.5)
    assert result.recall == pytest.approx(2 / 3)
    assert result.f1 == pytest.approx(4 / 7)


def test_evaluate_reports_recall_per_pattern():
    result = evaluate(GROUND_TRUTH, _mixed_alerts())

    recalls = {
        item.pattern: (item.labelled, item.detected, item.recall)
        for item in result.per_pattern
    }
    assert recalls == {
        "paysim_fraud": (1, 1, 1.0),
        "structuring": (1, 1, 1.0),
        "velocity": (1, 0, 0.0),
    }


def test_evaluate_reports_alert_precision_per_rule():
    result = evaluate(GROUND_TRUTH, _mixed_alerts())

    scores = {
        rule.rule_name: (rule.alerts, rule.true_alerts, rule.precision)
        for rule in result.per_rule
    }
    assert scores == {"ml_model": (2, 1, 0.5), "structuring": (1, 1, 1.0)}


def test_overlapping_alerts_do_not_inflate_transaction_counts():
    alerts = [
        _alert("velocity", ["T3", "N1"]),
        _alert("velocity", ["T3", "N1"]),
        _alert("velocity", ["T3", "N2"]),
    ]

    result = evaluate(GROUND_TRUTH, alerts)

    assert result.alert_count == 3
    assert result.true_positives == 1
    assert result.false_positives == 2


def test_evaluate_accepts_snowflake_style_rows():
    alerts = [{"RULE_NAME": "structuring", "TRANSACTION_IDS": json.dumps(["T2", "N1"])}]

    result = evaluate(GROUND_TRUTH, alerts)

    assert result.true_positives == 1
    assert result.false_positives == 1


def test_evaluate_handles_alerts_without_transaction_ids():
    result = evaluate(GROUND_TRUTH, [_alert("geo", None)])

    assert result.alert_count == 1
    assert result.flagged_transactions == 0
    assert result.per_rule[0].true_alerts == 0


def test_evaluate_with_no_alerts_returns_zeroes():
    result = evaluate(GROUND_TRUTH, [])

    assert result.true_positives == 0
    assert result.false_negatives == 3
    assert result.precision == 0.0
    assert result.recall == 0.0
    assert result.f1 == 0.0


def test_false_positive_rate_uses_the_non_fraud_population():
    result = evaluate(GROUND_TRUTH, _mixed_alerts(), total_transactions=102)

    assert result.false_positive_rate == pytest.approx(2 / 99)


def test_false_positive_rate_is_none_without_a_transaction_total():
    assert evaluate(GROUND_TRUTH, _mixed_alerts()).false_positive_rate is None


def test_format_report_lists_headline_metrics_patterns_and_rules():
    report = format_report(
        evaluate(GROUND_TRUTH, _mixed_alerts(), total_transactions=102)
    )

    assert "Precision: 0.5000" in report
    assert "Recall: 0.6667" in report
    assert "False positive rate:" in report
    assert "velocity: 0/1 (0.0000)" in report
    assert "ml_model: 1/2 (0.5000)" in report
