"""
scripts/profile_data.py

One-off data profiling step. Run this LOCALLY, BEFORE designing the
Snowflake bronze tables in dags/dbt/dbtproject/models/bronze/ — not as
part of the Airflow DAG. It profiles the raw source CSVs (the files
you're about to upload to S3) and writes a markdown report so schema
and null-handling decisions are based on what's actually in the data.

Usage:
    python scripts/profile_data.py \
        --listings /path/to/Listings.csv \
        --reviews  /path/to/Reviews.csv \
        --out      docs/data_profile_report.md
"""

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

# Columns worth checking cardinality on -- extend as the schema grows.
CATEGORICAL_COLUMNS = ["city", "host_is_superhost", "room_type", "property_type", "instant_bookable"]

# Numeric columns worth range-checking.
NUMERIC_COLUMNS = ["price", "accommodates", "bedrooms", "minimum_nights", "maximum_nights"]

# Values this project's silver_listings.sql actually branches on for currency —
# kept here so the coverage check below always reflects the real DBT logic.
CURRENCY_HANDLED_CITIES = {
    "New York", "Paris", "Rome", "Sydney", "Bangkok",
    "Cape Town", "Mexico City", "Istanbul", "Hong Kong", "Rio de Janeiro",
}


def profile_dataframe(df: pd.DataFrame, name: str) -> list[str]:
    lines = [f"## {name}", ""]
    lines.append(f"- Rows: **{len(df):,}**")
    lines.append(f"- Columns: **{len(df.columns)}**")
    lines.append(f"- Duplicate rows: **{df.duplicated().sum():,}**")
    lines.append("")

    lines.append("### Null counts (columns with any nulls)")
    lines.append("")
    null_counts = df.isnull().sum()
    null_counts = null_counts[null_counts > 0].sort_values(ascending=False)
    if null_counts.empty:
        lines.append("No nulls found in any column.")
    else:
        lines.append("| Column | Nulls | % of rows |")
        lines.append("|---|---|---|")
        for col, cnt in null_counts.items():
            pct = cnt / len(df) * 100
            lines.append(f"| {col} | {cnt:,} | {pct:.2f}% |")
    lines.append("")

    lines.append("### Categorical column values")
    lines.append("")
    for col in CATEGORICAL_COLUMNS:
        if col not in df.columns:
            continue
        vc = df[col].value_counts(dropna=False)
        lines.append(f"**{col}** — {df[col].nunique(dropna=True)} distinct non-null values")
        lines.append("")
        lines.append("| Value | Count |")
        lines.append("|---|---|")
        for val, cnt in vc.items():
            lines.append(f"| {val} | {cnt:,} |")
        lines.append("")

    numeric_present = [c for c in NUMERIC_COLUMNS if c in df.columns]
    if numeric_present:
        lines.append("### Numeric ranges")
        lines.append("")
        lines.append("| Column | Min | Max | Mean | Nulls |")
        lines.append("|---|---|---|---|---|")
        for col in numeric_present:
            series = pd.to_numeric(df[col].astype(str).str.replace(r"[$,]", "", regex=True), errors="coerce")
            lines.append(
                f"| {col} | {series.min():.2f} | {series.max():.2f} | "
                f"{series.mean():.2f} | {series.isnull().sum():,} |"
            )
        lines.append("")

    lines.append("### Type consistency check")
    lines.append("")
    mixed = [col for col in df.columns if df[col].dropna().map(type).nunique() > 1]
    if mixed:
        lines.append(f"Columns with mixed Python types after load: {mixed}")
    else:
        lines.append("No mixed-type columns detected — every column holds a single consistent type.")
    lines.append("")

    return lines


def main():
    parser = argparse.ArgumentParser(description="Profile raw Airbnb CSVs before schema design.")
    parser.add_argument("--listings", required=True, help="Path to raw Listings.csv")
    parser.add_argument("--reviews", required=True, help="Path to raw Reviews.csv")
    parser.add_argument("--listings-encoding", default="latin-1", help="Encoding of the listings file")
    parser.add_argument("--out", default="docs/data_profile_report.md", help="Output markdown report path")
    args = parser.parse_args()

    report = [f"# Data Profiling Report", f"Generated: {datetime.now().isoformat(timespec='seconds')}", ""]

    listings = pd.read_csv(args.listings, encoding=args.listings_encoding, low_memory=False)
    report += profile_dataframe(listings, "Listings.csv")

    reviews = pd.read_csv(args.reviews, low_memory=False)
    report += profile_dataframe(reviews, "Reviews.csv")

    # Cross-check: does every categorical value that silver_listings.sql
    # branches on for currency actually get covered? Kept as a generic
    # regression check even though the current logic already covers all
    # 10 cities with an ELSE fallback.
    report.append("## Downstream coverage check (silver_listings.sql currency logic)")
    report.append("")
    actual_cities = set(listings["city"].dropna().unique())
    missing = actual_cities - CURRENCY_HANDLED_CITIES
    if missing:
        report.append(f"- ⚠️ Cities present in data but not explicitly handled: **{sorted(missing)}**")
        for c in sorted(missing):
            n = (listings["city"] == c).sum()
            report.append(f"  - {c}: {n:,} rows — would fall through to the ELSE branch")
    else:
        report.append("- All city values are explicitly covered by the currency CASE statement.")
    report.append("")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(report), encoding="utf-8")
    print(f"Report written to {out_path}")


if __name__ == "__main__":
    main()