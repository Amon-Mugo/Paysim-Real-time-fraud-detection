"""Score fraud alerts against the generator's ground-truth labels.

The module is pure Python (standard library only) so that unit tests and the
Airflow ``evaluate`` task can both import it. Fetching alerts from Snowflake
and reading the ground-truth file from S3 happen elsewhere and feed this module
plain dictionaries.

Scoring is done at transaction level: a transaction counts as flagged when at
least one alert lists it in ``transaction_ids``. Overlapping sliding-window
alerts therefore never inflate the counts.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Sequence


@dataclass(frozen=True)
class PatternRecall:
    """Recall for one labelled fraud pattern."""

    pattern: str
    labelled: int
    detected: int
    recall: float


@dataclass(frozen=True)
class RuleScore:
    """Alert-level precision for one rule."""

    rule_name: str
    alerts: int
    true_alerts: int
    precision: float


@dataclass(frozen=True)
class EvaluationResult:
    """Metrics for one evaluation run."""

    labelled_fraud: int
    flagged_transactions: int
    true_positives: int
    false_positives: int
    false_negatives: int
    precision: float
    recall: float
    f1: float
    false_positive_rate: Optional[float]
    alert_count: int
    per_pattern: tuple[PatternRecall, ...]
    per_rule: tuple[RuleScore, ...]


def _safe_divide(numerator: float, denominator: float) -> float:
    """Return numerator / denominator, or 0.0 when the denominator is zero."""
    return numerator / denominator if denominator else 0.0


def _parse_transaction_ids(raw: Any) -> list[str]:
    """Normalise the ``transaction_ids`` value of an alert row into a list.

    Snowflake returns ARRAY columns as JSON strings, while tests and other
    callers pass real lists.
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        parsed = json.loads(raw)
    else:
        parsed = raw
    if not isinstance(parsed, (list, tuple)):
        raise ValueError(f"transaction_ids must be a list, got {type(parsed).__name__}")
    return [str(transaction_id) for transaction_id in parsed]


def _normalise_alert(row: Mapping[str, Any]) -> dict[str, Any]:
    """Lower-case the keys of an alert row (Snowflake returns upper-case names)."""
    return {str(key).lower(): value for key, value in row.items()}


def parse_ground_truth(lines: Iterable[str]) -> dict[str, str]:
    """Parse ``ground_truth.jsonl`` lines into a transaction_id -> pattern mapping."""
    labels: dict[str, str] = {}
    for line_number, line in enumerate(lines, start=1):
        text = line.strip()
        if not text:
            continue
        record = json.loads(text)
        try:
            labels[record["transaction_id"]] = record["pattern"]
        except KeyError as error:
            raise ValueError(
                f"ground truth line {line_number} is missing {error}"
            ) from error
    return labels


def evaluate(
    ground_truth: Mapping[str, str],
    alerts: Sequence[Mapping[str, Any]],
    total_transactions: Optional[int] = None,
) -> EvaluationResult:
    """Score alerts against ground-truth labels.

    Args:
        ground_truth: transaction_id -> pattern for every labelled fraud transaction.
        alerts: alert rows with ``rule_name`` and ``transaction_ids`` keys.
        total_transactions: transactions processed in the run; enables the
            false positive rate when given.
    """
    fraud_ids = set(ground_truth)
    flagged_ids: set[str] = set()
    alerts_per_rule: dict[str, int] = defaultdict(int)
    true_alerts_per_rule: dict[str, int] = defaultdict(int)

    for row in alerts:
        alert = _normalise_alert(row)
        transaction_ids = _parse_transaction_ids(alert.get("transaction_ids"))
        rule_name = str(alert["rule_name"])
        alerts_per_rule[rule_name] += 1
        if any(transaction_id in fraud_ids for transaction_id in transaction_ids):
            true_alerts_per_rule[rule_name] += 1
        flagged_ids.update(transaction_ids)

    true_positives = len(flagged_ids & fraud_ids)
    false_positives = len(flagged_ids - fraud_ids)
    false_negatives = len(fraud_ids - flagged_ids)
    precision = _safe_divide(true_positives, true_positives + false_positives)
    recall = _safe_divide(true_positives, true_positives + false_negatives)

    ids_per_pattern: dict[str, set[str]] = defaultdict(set)
    for transaction_id, pattern in ground_truth.items():
        ids_per_pattern[pattern].add(transaction_id)
    per_pattern = tuple(
        PatternRecall(
            pattern=pattern,
            labelled=len(ids),
            detected=len(ids & flagged_ids),
            recall=_safe_divide(len(ids & flagged_ids), len(ids)),
        )
        for pattern, ids in sorted(ids_per_pattern.items())
    )
    per_rule = tuple(
        RuleScore(
            rule_name=rule_name,
            alerts=count,
            true_alerts=true_alerts_per_rule[rule_name],
            precision=_safe_divide(true_alerts_per_rule[rule_name], count),
        )
        for rule_name, count in sorted(alerts_per_rule.items())
    )

    false_positive_rate = None
    if total_transactions is not None:
        false_positive_rate = _safe_divide(
            false_positives, total_transactions - len(fraud_ids)
        )

    return EvaluationResult(
        labelled_fraud=len(fraud_ids),
        flagged_transactions=len(flagged_ids),
        true_positives=true_positives,
        false_positives=false_positives,
        false_negatives=false_negatives,
        precision=precision,
        recall=recall,
        f1=_safe_divide(2 * precision * recall, precision + recall),
        false_positive_rate=false_positive_rate,
        alert_count=len(alerts),
        per_pattern=per_pattern,
        per_rule=per_rule,
    )


def format_report(result: EvaluationResult) -> str:
    """Render an evaluation result as a plain-text report for task logs."""
    lines = [
        f"Labelled fraud transactions: {result.labelled_fraud}",
        f"Alerts: {result.alert_count}",
        f"Flagged transactions: {result.flagged_transactions}",
        f"True positives: {result.true_positives}",
        f"False positives: {result.false_positives}",
        f"False negatives: {result.false_negatives}",
        f"Precision: {result.precision:.4f}",
        f"Recall: {result.recall:.4f}",
        f"F1: {result.f1:.4f}",
    ]
    if result.false_positive_rate is not None:
        lines.append(f"False positive rate: {result.false_positive_rate:.6f}")
    lines.append("Recall per pattern:")
    for item in result.per_pattern:
        lines.append(
            f"  {item.pattern}: {item.detected}/{item.labelled} ({item.recall:.4f})"
        )
    lines.append("Alert precision per rule:")
    for rule in result.per_rule:
        lines.append(
            f"  {rule.rule_name}: {rule.true_alerts}/{rule.alerts} ({rule.precision:.4f})"
        )
    return "\n".join(lines)
