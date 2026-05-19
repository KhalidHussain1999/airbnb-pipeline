"""
FILE: dags/airbnb_pipeline_dag.py
PURPOSE: Orchestrates the complete Airbnb ELT pipeline
         S3 → Snowflake Bronze → DBT Silver/Gold
TOOL: Apache Airflow — runs on schedule daily
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ── CONFIG ────────────────────────────────────────────────────────────────────
SNOWFLAKE_CONN_ID  = "snowflake_conn"
SNOWFLAKE_DATABASE = "AIRBNB_DB"
SNOWFLAKE_SCHEMA   = "BRONZE"
SNOWFLAKE_WH       = "COMPUTE_WH"
DBT_PROJECT_PATH   = "/opt/airflow/dags/dbt/dbtproject"
DBT_PROFILES_PATH  = "/home/airflow/.dbt"
DBT_EXECUTABLE     = "/home/airflow/.local/bin/dbt"
# ─────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner":            "khalid",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
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
        hook   = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()

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
        cursor.close()

    # ── TASK 2: Load Listings from S3 → Bronze ───────────────────────────────
    @task()
    def load_listings_to_bronze():
        """
        Run COPY INTO to load Listings CSV from S3 stage
        into AIRBNB_DB.BRONZE.raw_listings table.
        Truncates first to avoid duplicates on re-run.
        """
        hook   = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()

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
        cursor.close()

    # ── TASK 3: Load Reviews from S3 → Bronze ────────────────────────────────
    @task()
    def load_reviews_to_bronze():
        """
        Run COPY INTO to load Reviews CSV from S3 stage
        into AIRBNB_DB.BRONZE.raw_reviews table.
        Truncates first to avoid duplicates on re-run.
        """
        hook   = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()

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
        cursor.close()

    # ── TASK 4: Validate Bronze Load ─────────────────────────────────────────
    @task()
    def validate_bronze_load():
        """
        Verify Bronze tables loaded successfully.
        Uses zero check instead of hardcoded threshold.
        Catches complete COPY INTO failures while allowing
        for natural data volume changes in future loads.
        """
        hook   = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()

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

        print(f"Bronze validation passed.")
        print(f"Listings: {listings_count:,} rows loaded successfully.")
        print(f"Reviews:  {reviews_count:,} rows loaded successfully.")
        cursor.close()

    # ── TASK 5: DBT Transformations ───────────────────────────────────────────
    @task()
    def run_dbt_transformations():
        """
        Run all 8 DBT models in correct dependency order:
        bronze → silver → gold
        Calls dbt executable directly using confirmed path.
        """
        import subprocess

        dbt_cmd = [
            DBT_EXECUTABLE, "run",
            "--project-dir", DBT_PROJECT_PATH,
            "--profiles-dir", DBT_PROFILES_PATH
        ]

        print(f"Starting DBT run...")
        print(f"Command: {' '.join(dbt_cmd)}")

        result = subprocess.run(
            dbt_cmd,
            capture_output = True,
            text           = True
        )

        print("DBT STDOUT:", result.stdout)
        print("DBT STDERR:", result.stderr)

        if result.returncode != 0:
            raise ValueError(
                f"DBT run failed with return code {result.returncode}. "
                f"Check logs above for details."
            )
        print("DBT run completed successfully.")

    # ── TASK 6: Run DBT Tests ─────────────────────────────────────────────────
    @task()
    def run_dbt_tests():
        """
        Run all 40 DBT schema tests after transformations.
        Catches data quality issues before they reach Power BI.
        """
        import subprocess

        dbt_cmd = [
            DBT_EXECUTABLE, "test",
            "--project-dir", DBT_PROJECT_PATH,
            "--profiles-dir", DBT_PROFILES_PATH
        ]

        print(f"Starting DBT tests...")
        print(f"Command: {' '.join(dbt_cmd)}")

        result = subprocess.run(
            dbt_cmd,
            capture_output = True,
            text           = True
        )

        print("DBT TEST STDOUT:", result.stdout)
        print("DBT TEST STDERR:", result.stderr)

        if result.returncode != 0:
            raise ValueError(
                f"DBT tests failed. "
                f"Check logs above for data quality issues."
            )
        print("All DBT tests passed.")

    # ── TASK 7: Validate Gold Layer ───────────────────────────────────────────
    @task()
    def validate_gold_layer():
        """
        Final check — verify all Gold tables have data.
        Confirms entire pipeline completed successfully.
        """
        hook   = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()

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
        cursor.close()

    # ── DAG DEPENDENCY CHAIN ──────────────────────────────────────────────────
    s3_check      = check_s3_files()
    load_listings = load_listings_to_bronze()
    load_reviews  = load_reviews_to_bronze()
    validate_b    = validate_bronze_load()
    dbt_run       = run_dbt_transformations()
    dbt_test      = run_dbt_tests()
    gold_check     = validate_gold_layer()

    # DBT tasks defined but not wired yet — testing Snowflake tasks first
    # Uncomment below line after confirming tasks 1-4 work correctly

    # Current chain — Snowflake tasks only
    #s3_check >> [load_listings, load_reviews] >> validate_b >> dbt_run >> dbt_test

    # Full chain — uncomment when ready for DBT
    s3_check >> [load_listings, load_reviews] >> validate_b >> dbt_run >> dbt_test >> gold_check



airbnb_pipeline()