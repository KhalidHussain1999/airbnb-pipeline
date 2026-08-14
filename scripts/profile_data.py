"""
scripts/profile_data.py

One-off data profiling step. Run this BEFORE designing the Snowflake
bronze tables in dags/dbt/dbtproject/models/bronze/ — not as part of
the Airflow DAG. It profiles the raw source CSVs and writes a markdown
report so schema and null-handling decisions are based on what's
actually in the data.

Accepts either a LOCAL path or an S3 URI for --listings / --reviews:

    # Local file (already UTF-8, e.g. Listings_utf8.csv)
    python scripts/profile_data.py \
        --listings /path/to/Listings_utf8.csv \
        --reviews  /path/to/Reviews_utf8.csv \
        --out      docs/data_profile_report.md

    # Directly from S3 (no manual download needed)
    python scripts/profile_data.py \
        --listings s3://airbnb-pipeline-raw/raw/Listings_utf8.csv \
        --reviews  s3://airbnb-pipeline-raw/raw/Reviews_utf8.csv \
        --out      docs/data_profile_report.md

    # Profiling the ORIGINAL pre-conversion raw file instead (Latin-1)
    python scripts/profile_data.py \
        --listings /path/to/Listings.csv \
        --reviews  /path/to/Reviews.csv \
        --listings-encoding latin-1

S3 access here uses your own local AWS credentials (configured via
`aws configure`), separate from Snowflake's Storage Integration —
this project's Airflow DAG never touches AWS credentials directly,
since Snowflake handles S3 access internally through its own IAM
role trust relationship.
"""

import sys
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


def load_csv(path: str, **read_csv_kwargs) -> pd.DataFrame:
    """Load a CSV from either a local path or an s3:// URI.

    pandas + s3fs handle s3:// paths transparently — no branching needed
    for the happy path. This wrapper just gives a clear, actionable error
    if S3 credentials or the s3fs package aren't set up yet.
    """
    is_s3 = path.startswith("s3://")
    try:
        return pd.read_csv(path, **read_csv_kwargs)
    except ImportError:
        print(
            "\nERROR: reading from S3 requires the 's3fs' package.\n"
            "Install it with:\n    pip install -r scripts/requirements-profiling.txt\n",
            file=sys.stderr,
        )
        sys.exit(1)
    except Exception as e:
        if is_s3:
            print(
                f"\nERROR: could not read '{path}' from S3.\n"
                f"Details: {e}\n\n"
                "Check that:\n"
                "  1. The bucket/key path is correct\n"
                "  2. Your AWS credentials are configured (same ones your Airflow\n"
                "     S3 operator uses — environment variables, ~/.aws/credentials,\n"
                "     or an IAM role)\n"
                "  3. Your IAM user/role has s3:GetObject permission on this bucket\n",
                file=sys.stderr,
            )
        else:
            print(f"\nERROR: could not read local file '{path}': {e}\n", file=sys.stderr)
        sys.exit(1)


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
    parser.add_argument("--listings", required=True, help="Path to raw Listings.csv (local path or s3://bucket/key.csv)")
    parser.add_argument("--reviews", required=True, help="Path to raw Reviews.csv (local path or s3://bucket/key.csv)")
    parser.add_argument("--listings-encoding", default="utf-8",
                         help="Encoding of the listings file. Default is utf-8, matching this "
                              "project's Listings_utf8.csv. Pass --listings-encoding latin-1 "
                              "only if profiling the original pre-conversion raw file.")
    parser.add_argument("--out", default="docs/data_profile_report.md", help="Output markdown report path")
    args = parser.parse_args()

    report = [f"# Data Profiling Report", f"Generated: {datetime.now().isoformat(timespec='seconds')}", ""]

    print(f"Loading listings from: {args.listings}")
    listings = load_csv(args.listings, encoding=args.listings_encoding, low_memory=False)
    report += profile_dataframe(listings, "Listings.csv")

    print(f"Loading reviews from: {args.reviews}")
    reviews = load_csv(args.reviews, low_memory=False)
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