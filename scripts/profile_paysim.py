"""Profile the PaySim dataset: shape, schema, nulls, type mix and fraud rate."""

import argparse
from pathlib import Path

import pandas as pd

DEFAULT_PATH = Path("data/paysim dataset.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile the PaySim CSV.")
    parser.add_argument(
        "path",
        nargs="?",
        type=Path,
        default=DEFAULT_PATH,
        help="Path to the PaySim CSV (default: %(default)s).",
    )
    return parser.parse_args()


def print_section(title: str) -> None:
    print(f"\n=== {title} ===")


def profile(df: pd.DataFrame) -> None:
    print_section("Shape")
    print(f"rows: {len(df):,}")
    print(f"columns: {df.shape[1]}")

    print_section("Schema")
    print(df.dtypes.to_string())

    print_section("Null counts")
    print(df.isna().sum().to_string())

    print_section("Transaction types")
    type_counts = df["type"].value_counts()
    type_share = (type_counts / len(df)).round(4)
    print(pd.DataFrame({"count": type_counts, "share": type_share}).to_string())

    print_section("Fraud rate")
    fraud_count = int(df["isFraud"].sum())
    print(f"fraud rows: {fraud_count:,} ({fraud_count / len(df):.4%})")
    print(f"flagged by source system (isFlaggedFraud): {int(df['isFlaggedFraud'].sum()):,}")

    print_section("Fraud by transaction type")
    by_type = df.groupby("type")["isFraud"].agg(fraud_count="sum", total="count")
    by_type["fraud_rate"] = (by_type["fraud_count"] / by_type["total"]).round(6)
    print(by_type.to_string())

    print_section("Step (hourly) range")
    print(f"min: {df['step'].min()}  max: {df['step'].max()}")
    print(f"distinct steps: {df['step'].nunique()}")

    print_section("Amount statistics")
    print(df["amount"].describe().round(2).to_string())


def main() -> None:
    args = parse_args()
    df = pd.read_csv(args.path)
    profile(df)


if __name__ == "__main__":
    main()