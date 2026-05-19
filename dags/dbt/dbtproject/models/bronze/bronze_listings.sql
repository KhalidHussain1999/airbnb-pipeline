{{ config(
    materialized = 'table',
    schema       = 'bronze'
) }}

SELECT
    -- Identifiers
    TRY_TO_NUMBER(listing_id)                                   AS listing_id,
    TRY_TO_NUMBER(host_id)                                      AS host_id,

    -- Listing info
    name::VARCHAR                                               AS listing_name,

    -- Host information
    host_since::VARCHAR                                         AS host_since_raw,
    TRY_TO_DATE(host_since, 'YYYY-MM-DD')                       AS host_since_date,
    host_location::VARCHAR                                      AS host_location,
    TRY_TO_NUMBER(host_listings_count)                          AS host_listings_count,

    -- Boolean casting: 't' → TRUE, 'f' → FALSE
    CASE WHEN host_is_superhost      = 't' THEN TRUE
         ELSE FALSE END                                         AS host_is_superhost,
    CASE WHEN host_identity_verified = 't' THEN TRUE
         ELSE FALSE END                                         AS host_identity_verified,
    CASE WHEN host_has_profile_pic   = 't' THEN TRUE
         ELSE FALSE END                                         AS host_has_profile_pic,
    CASE WHEN instant_bookable       = 't' THEN TRUE
         ELSE FALSE END                                         AS instant_bookable,

    -- Host performance
    host_response_time::VARCHAR                                 AS host_response_time,
    TRY_TO_DOUBLE(REPLACE(host_response_rate, '%', ''))         AS host_response_rate_pct,
    TRY_TO_DOUBLE(REPLACE(host_acceptance_rate, '%', ''))       AS host_acceptance_rate_pct,

    -- Location
    city::VARCHAR                                               AS city,
    neighbourhood::VARCHAR                                      AS neighbourhood,
    district::VARCHAR                                           AS district,
    TRY_TO_DOUBLE(latitude)                                     AS latitude,
    TRY_TO_DOUBLE(longitude)                                    AS longitude,

    -- Property details
    property_type::VARCHAR                                      AS property_type,
    room_type::VARCHAR                                          AS room_type,
    TRY_TO_NUMBER(accommodates)                                 AS accommodates,
    TRY_TO_NUMBER(bedrooms)                                     AS bedrooms,
    amenities::VARCHAR                                          AS amenities,

    -- Price: already clean number in CSV, no $ sign
    TRY_TO_DOUBLE(price)                                        AS price_raw,

    -- Stay rules
    TRY_TO_NUMBER(minimum_nights)                               AS minimum_nights,
    TRY_TO_NUMBER(maximum_nights)                               AS maximum_nights,

    -- Review scores (out of 100)
    TRY_TO_DOUBLE(review_scores_rating)                         AS review_scores_rating,
    TRY_TO_DOUBLE(review_scores_accuracy)                       AS review_scores_accuracy,
    TRY_TO_DOUBLE(review_scores_cleanliness)                    AS review_scores_cleanliness,
    TRY_TO_DOUBLE(review_scores_checkin)                        AS review_scores_checkin,
    TRY_TO_DOUBLE(review_scores_communication)                  AS review_scores_communication,
    TRY_TO_DOUBLE(review_scores_location)                       AS review_scores_location,
    TRY_TO_DOUBLE(review_scores_value)                          AS review_scores_value,

    -- Audit column
    loaded_at

FROM AIRBNB_DB.BRONZE.raw_listings
WHERE listing_id IS NOT NULL