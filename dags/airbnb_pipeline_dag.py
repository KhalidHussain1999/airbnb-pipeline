"""
FILE: dags/airbnb_pipeline_dag.py
PURPOSE: Orchestrates the complete Airbnb ELT pipeline
         S3 → Snowflake Bronze → DBT Silver/Gold
TOOL: Apache Airflow — runs on schedule daily
"""

import logging
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# scripts/ is mounted into the container at /opt/airflow/scripts (see docker-compose.yaml)
sys.path.insert(0, "/opt/airflow/scripts")

# ── CONFIG ────────────────────────────────────────────────────────────────────
SNOWFLAKE_CONN_ID  = "snowflake_conn"
SNOWFLAKE_DATABASE = "AIRBNB_DB"
SNOWFLAKE_SCHEMA   = "BRONZE"
SNOWFLAKE_WH       = "COMPUTE_WH"
DBT_PROJECT_PATH   = "/opt/airflow/dags/dbt/dbtproject"
DBT_PROFILES_PATH  = "/home/airflow/.dbt"
DBT_EXECUTABLE     = "/home/airflow/.local/bin/dbt"
S3_BUCKET          = "airbnb-pipeline-raw"
S3_LISTINGS_KEY    = f"s3://{S3_BUCKET}/raw/Listings_utf8.csv"
S3_REVIEWS_KEY     = f"s3://{S3_BUCKET}/raw/Reviews_utf8.csv"

logger = logging.getLogger(__name__)

def on_failure_alert(context):
    """
    Simple failure callback — logs a clear message with task/dag context.
    Swap the logger.error call for a Slack/email/PagerDuty call as needed.
    """
    task_instance = context.get("task_instance")
    dag_id  = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id = task_instance.task_id if task_instance else "unknown_task"
    exception = context.get("exception")

    logger.error(
        "Airflow task failed | dag=%s task=%s exception=%s",
        dag_id, task_id, exception
    )
# ─────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner":              "khalid",
    "retries":            2,
    "retry_delay":        timedelta(minutes=5),
    "email_on_failure":   False,
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id            = "airbnb_pipeline",
    description       = "Airbnb ELT Pipeline: S3 → Snowflake → DBT",
    default_args      = default_args,
    start_date        = datetime(2024, 1, 1),
    schedule          = "@daily",
    catchup           = False,
    tags              = ["airbnb", "snowflake", "dbt", "s3"],
)
def airbnb_pipeline():

    # ── TASK 1: Check S3 files exist ─────────────────────────────────────────
    @task()
    def check_s3_files():
        """
        Verify both CSV files exist in S3 stage
        before attempting any load operation.
        Uses Snowflake LIST command on the external stage.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute("LIST @AIRBNB_DB.BRONZE.s3_airbnb_stage;")
                files = cursor.fetchall()

                if not files:
                    raise ValueError("No files found in S3 stage. Upload CSVs first.")

                file_names = [f[0] for f in files]
                print(f"Files found in S3 stage: {file_names}")

                required = ["Listings_utf8.csv", "Reviews_utf8.csv"]
                for req in required:
                    if not any(req in f for f in file_names):
                        raise ValueError(f"Required file missing from S3: {req}")

                print("S3 file check passed.")
            finally:
                cursor.close()
        finally:
            conn.close()

    # ── TASK 1.5: Profile source data before loading ─────────────────────────
    @task()
    def profile_source_data():
        """
        Reads the raw CSVs directly from S3 and runs the same checks as
        scripts/profile_data.py: null-rate summary (logged, informational)
        and currency-CASE coverage in silver_listings.sql (hard fail if any
        city in the data isn't handled — this is a real schema-drift guard,
        not just documentation).

        Requires AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION
        to be set in .env (separate from the Snowflake Storage Integration —
        this task reads S3 directly via pandas/s3fs, not through Snowflake).
        """
        from profile_data import load_csv, get_null_summary, check_currency_coverage

        listings = load_csv(S3_LISTINGS_KEY, encoding="utf-8", low_memory=False)

        null_summary = get_null_summary(listings, threshold_pct=5.0)
        if null_summary:
            print("Null rates above 5%% (informational, not blocking):")
            for col, pct in sorted(null_summary.items(), key=lambda x: -x[1]):
                print(f"  {col}: {pct}%")

        missing_cities = check_currency_coverage(listings)
        if missing_cities:
            raise ValueError(
                f"Schema drift detected: {missing_cities} present in source data "
                f"but not handled by silver_listings.sql's currency CASE statement. "
                f"Update the model before this data can load correctly."
            )

        print("Profiling passed: all cities covered by currency logic.")

    # ── TASK 2: Load Listings from S3 → Bronze ───────────────────────────────
    @task()
    def load_listings_to_bronze():
        """
        Run COPY INTO to load Listings CSV from S3 stage
        into AIRBNB_DB.BRONZE.raw_listings table.
        Truncates first to avoid duplicates on re-run.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute("TRUNCATE TABLE AIRBNB_DB.BRONZE.raw_listings;")
                print("Truncated raw_listings table.")

                cursor.execute("""
                    COPY INTO AIRBNB_DB.BRONZE.raw_listings (
                        listing_id, name, host_id, host_since, host_location,
                        host_response_time, host_response_rate, host_acceptance_rate,
                        host_is_superhost, host_listings_count, host_identity_verified,
                        host_has_profile_pic, neighbourhood, district, city,
                        latitude, longitude, property_type, room_type,
                        accommodates, bedrooms, amenities, price,
                        minimum_nights, maximum_nights,
                        review_scores_rating, review_scores_accuracy,
                        review_scores_cleanliness, review_scores_checkin,
                        review_scores_communication, review_scores_location,
                        review_scores_value, instant_bookable
                    )
                    FROM @AIRBNB_DB.BRONZE.s3_airbnb_stage/Listings_utf8.csv
                    FILE_FORMAT = (
                        TYPE                         = CSV
                        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                        SKIP_HEADER                  = 1
                        NULL_IF                      = ('', 'NULL', 'null', 'NA')
                        EMPTY_FIELD_AS_NULL          = TRUE
                        ENCODING                     = 'UTF8'
                    )
                    ON_ERROR = 'CONTINUE';
                """)

                result = cursor.fetchone()
                print(f"Listings load result: {result}")
            finally:
                cursor.close()
        finally:
            conn.close()

    # ── TASK 3: Load Reviews from S3 → Bronze ────────────────────────────────
    @task()
    def load_reviews_to_bronze():
        """
        Run COPY INTO to load Reviews CSV from S3 stage
        into AIRBNB_DB.BRONZE.raw_reviews table.
        Truncates first to avoid duplicates on re-run.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute("TRUNCATE TABLE AIRBNB_DB.BRONZE.raw_reviews;")
                print("Truncated raw_reviews table.")

                cursor.execute("""
                    COPY INTO AIRBNB_DB.BRONZE.raw_reviews (
                        listing_id, review_id, date, reviewer_id
                    )
                    FROM @AIRBNB_DB.BRONZE.s3_airbnb_stage/Reviews_utf8.csv
                    FILE_FORMAT = (
                        TYPE                         = CSV
                        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                        SKIP_HEADER                  = 1
                        NULL_IF                      = ('', 'NULL', 'null', 'NA')
                        EMPTY_FIELD_AS_NULL          = TRUE
                        ENCODING                     = 'UTF8'
                    )
                    ON_ERROR = 'CONTINUE';
                """)

                result = cursor.fetchone()
                print(f"Reviews load result: {result}")
            finally:
                cursor.close()
        finally:
            conn.close()

    # ── TASK 4: Validate Bronze Load ─────────────────────────────────────────
    @task()
    def validate_bronze_load():
        """
        Verify Bronze tables loaded successfully.
        Uses zero check instead of hardcoded threshold.
        Catches complete COPY INTO failures while allowing
        for natural data volume changes in future loads.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM AIRBNB_DB.BRONZE.raw_listings;")
                listings_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM AIRBNB_DB.BRONZE.raw_reviews;")
                reviews_count = cursor.fetchone()[0]

                print(f"raw_listings row count : {listings_count:,}")
                print(f"raw_reviews row count  : {reviews_count:,}")

                if listings_count == 0:
                    raise ValueError(
                        "raw_listings is empty — COPY INTO failed completely. "
                        "Check S3 stage and file format settings."
                    )
                if reviews_count == 0:
                    raise ValueError(
                        "raw_reviews is empty — COPY INTO failed completely. "
                        "Check S3 stage and file format settings."
                    )

                print("Bronze validation passed.")
                print(f"Listings: {listings_count:,} rows loaded successfully.")
                print(f"Reviews:  {reviews_count:,} rows loaded successfully.")
            finally:
                cursor.close()
        finally:
            conn.close()

    # ── TASK 5: DBT Transformations ───────────────────────────────────────────
    # BashOperator instead of subprocess: the task IS the shell command,
    # no extra Python logic needed before/after. Airflow captures
    # stdout/stderr into the task log and fails the task natively on a
    # non-zero exit code — no manual returncode check required.
    run_dbt_transformations = BashOperator(
        task_id      = "run_dbt_transformations",
        bash_command = (
            f"{DBT_EXECUTABLE} run "
            f"--project-dir {DBT_PROJECT_PATH} "
            f"--profiles-dir {DBT_PROFILES_PATH}"
        ),
    )

    # ── TASK 6: Run DBT Tests ─────────────────────────────────────────────────
    run_dbt_tests = BashOperator(
        task_id      = "run_dbt_tests",
        bash_command = (
            f"{DBT_EXECUTABLE} test "
            f"--project-dir {DBT_PROJECT_PATH} "
            f"--profiles-dir {DBT_PROFILES_PATH}"
        ),
    )

    # ── TASK 7: Validate Gold Layer ───────────────────────────────────────────
    @task()
    def validate_gold_layer():
        """
        Final check — verify all Gold tables have data.
        Confirms entire pipeline completed successfully.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            try:
                gold_tables = {
                    "AIRBNB_DB.GOLD.fct_revenue_by_city":       10,
                    "AIRBNB_DB.GOLD.fct_superhost_performance": 20,
                    "AIRBNB_DB.GOLD.fct_seasonal_trends":       100,
                    "AIRBNB_DB.GOLD.fct_price_vs_rating":       100,
                }

                for table, min_rows in gold_tables.items():
                    cursor.execute(f"SELECT COUNT(*) FROM {table};")
                    count = cursor.fetchone()[0]
                    print(f"{table}: {count:,} rows")

                    if count < min_rows:
                        raise ValueError(
                            f"{table} has only {count} rows. "
                            f"Expected at least {min_rows}."
                        )

                print("Gold layer validation passed. Pipeline complete.")
            finally:
                cursor.close()
        finally:
            conn.close()

    # ── DAG DEPENDENCY CHAIN ──────────────────────────────────────────────────
    # Note: run_dbt_transformations / run_dbt_tests are BashOperator instances
    # (defined above), not @task functions — so they're referenced directly,
    # not called with (). Everything else is a TaskFlow task and gets called.
    s3_check      = check_s3_files()
    profile_check = profile_source_data()
    load_listings = load_listings_to_bronze()
    load_reviews  = load_reviews_to_bronze()
    validate_b    = validate_bronze_load()
    gold_check    = validate_gold_layer()

    s3_check >> profile_check >> [load_listings, load_reviews] >> validate_b \
        >> run_dbt_transformations >> run_dbt_tests >> gold_check


airbnb_pipeline()