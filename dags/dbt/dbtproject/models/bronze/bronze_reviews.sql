{{ config(
    materialized = 'table',
    schema       = 'bronze'
) }}

WITH raw AS (
    SELECT
        listing_id::INTEGER                     AS listing_id,
        review_id::INTEGER                      AS review_id,
        TRY_TO_DATE(date, 'YYYY-MM-DD')         AS review_date,
        reviewer_id::INTEGER                    AS reviewer_id,
        loaded_at
    FROM AIRBNB_DB.BRONZE.raw_reviews
    WHERE listing_id IS NOT NULL
      AND review_id  IS NOT NULL
),

-- Remove duplicate review_ids from source data
-- 160 duplicates found in Maven Analytics dataset
-- Keep only the first occurrence of each review_id
deduped AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY review_id
                ORDER BY loaded_at
            ) AS row_num
        FROM raw
    )
    WHERE row_num = 1
)

SELECT
    listing_id,
    review_id,
    review_date,
    reviewer_id,
    loaded_at
FROM deduped