# Data Profiling Report
Generated: 2026-08-13T20:24:04

## Listings.csv

- Rows: **279,712**
- Columns: **33**
- Duplicate rows: **0**

### Null counts (columns with any nulls)

| Column | Nulls | % of rows |
|---|---|---|
| district | 242,700 | 86.77% |
| host_response_time | 128,782 | 46.04% |
| host_response_rate | 128,782 | 46.04% |
| host_acceptance_rate | 113,087 | 40.43% |
| review_scores_value | 91,785 | 32.81% |
| review_scores_location | 91,775 | 32.81% |
| review_scores_checkin | 91,771 | 32.81% |
| review_scores_accuracy | 91,713 | 32.79% |
| review_scores_communication | 91,687 | 32.78% |
| review_scores_cleanliness | 91,665 | 32.77% |
| review_scores_rating | 91,405 | 32.68% |
| bedrooms | 29,435 | 10.52% |
| host_location | 840 | 0.30% |
| name | 175 | 0.06% |
| host_since | 165 | 0.06% |
| host_identity_verified | 165 | 0.06% |
| host_has_profile_pic | 165 | 0.06% |
| host_total_listings_count | 165 | 0.06% |
| host_is_superhost | 165 | 0.06% |

### Categorical column values

**city** — 10 distinct non-null values

| Value | Count |
|---|---|
| Paris | 64,690 |
| New York | 37,012 |
| Sydney | 33,630 |
| Rome | 27,647 |
| Rio de Janeiro | 26,615 |
| Istanbul | 24,519 |
| Mexico City | 20,065 |
| Bangkok | 19,361 |
| Cape Town | 19,086 |
| Hong Kong | 7,087 |

**host_is_superhost** — 2 distinct non-null values

| Value | Count |
|---|---|
| f | 229,294 |
| t | 50,253 |
| nan | 165 |

**room_type** — 4 distinct non-null values

| Value | Count |
|---|---|
| Entire place | 182,005 |
| Private room | 86,988 |
| Hotel room | 5,857 |
| Shared room | 4,862 |

**property_type** — 144 distinct non-null values

| Value | Count |
|---|---|
| Entire apartment | 138,989 |
| Private room in apartment | 47,322 |
| Private room in house | 13,292 |
| Entire house | 13,273 |
| Entire condominium | 11,250 |
| Room in boutique hotel | 5,771 |
| Entire loft | 4,587 |
| Private room in condominium | 4,462 |
| Private room in bed and breakfast | 4,238 |
| Entire serviced apartment | 3,973 |
| Room in hotel | 3,205 |
| Private room in townhouse | 2,959 |
| Shared room in apartment | 2,420 |
| Entire townhouse | 2,331 |
| Entire guest suite | 2,273 |
| Private room in serviced apartment | 1,542 |
| Entire villa | 1,508 |
| Room in aparthotel | 1,495 |
| Private room in guest suite | 1,370 |
| Private room in guesthouse | 1,337 |
| Private room in hostel | 1,232 |
| Entire guesthouse | 1,084 |
| Room in bed and breakfast | 1,045 |
| Private room in loft | 922 |
| Room in serviced apartment | 860 |
| Shared room in hostel | 824 |
| Shared room in house | 637 |
| Room in hostel | 575 |
| Private room in villa | 563 |
| Entire cottage | 562 |
| Private room | 452 |
| Tiny house | 287 |
| Entire bungalow | 245 |
| Entire place | 231 |
| Shared room in condominium | 199 |
| Shared room in bed and breakfast | 167 |
| Private room in tiny house | 154 |
| Private room in bungalow | 120 |
| Shared room in townhouse | 107 |
| Entire cabin | 91 |
| Shared room in loft | 89 |
| Shared room in guesthouse | 83 |
| Farm stay | 80 |
| Private room in casa particular | 77 |
| Entire home/apt | 70 |
| Boat | 69 |
| Private room in farm stay | 69 |
| Private room in cottage | 62 |
| Shared room in serviced apartment | 58 |
| Camper/RV | 55 |
| Private room in cabin | 53 |
| Private room in nature lodge | 50 |
| Entire chalet | 50 |
| Shared room in guest suite | 50 |
| Private room in earth house | 48 |
| Entire bed and breakfast | 46 |
| Private room in resort | 39 |
| Shared room in boutique hotel | 37 |
| Casa particular | 33 |
| Earth house | 31 |
| Shared room in yurt | 30 |
| Shared room in villa | 29 |
| Private room in chalet | 22 |
| Shared room in tiny house | 22 |
| Private room in boat | 20 |
| Entire floor | 20 |
| Dome house | 20 |
| Room in nature lodge | 20 |
| Houseboat | 18 |
| Shared room | 18 |
| Private room in kezhan | 18 |
| Private room in dome house | 17 |
| Campsite | 14 |
| Private room in barn | 14 |
| Private room in hut | 12 |
| Castle | 12 |
| Island | 12 |
| Private room in camper/rv | 11 |
| Entire resort | 11 |
| Tent | 11 |
| Shared room in earth house | 11 |
| Barn | 10 |
| Shared room in hotel | 10 |
| Private room in minsu | 9 |
| Private room in floor | 9 |
| Shared room in casa particular | 9 |
| Room in apartment | 8 |
| Private room in yurt | 8 |
| Private room in castle | 8 |
| Treehouse | 8 |
| Shared room in cabin | 8 |
| Shared room in dorm | 8 |
| Entire hostel | 7 |
| Private room in tent | 7 |
| Private room in treehouse | 7 |
| Private room in houseboat | 7 |
| Private room in dorm | 6 |
| Private room in island | 6 |
| Room in casa particular | 6 |
| Shared room in dome house | 6 |
| Cave | 5 |
| Shared room in chalet | 5 |
| Shared room in aparthotel | 5 |
| Yurt | 4 |
| Lighthouse | 4 |
| Room in resort | 4 |
| Shared room in bungalow | 4 |
| Room in pension | 4 |
| Shared room in cave | 4 |
| Entire dorm | 3 |
| Pension | 3 |
| Hut | 3 |
| Shared room in cottage | 3 |
| Room in guesthouse | 3 |
| Private room in pension | 2 |
| Private room in tipi | 2 |
| Entire vacation home | 2 |
| Private room in pousada | 2 |
| Private room in in-law | 2 |
| Private room in lighthouse | 2 |
| Shared room in farm stay | 2 |
| Shared room in pension | 2 |
| Private room in train | 2 |
| Shared room in kezhan | 2 |
| Shared room in boat | 2 |
| Shared room in castle | 2 |
| Bus | 2 |
| Shared room in island | 2 |
| Shared room in nature lodge | 2 |
| Private room in bus | 2 |
| Entire in-law | 1 |
| Windmill | 1 |
| Shared room in floor | 1 |
| Shared room in hut | 1 |
| Train | 1 |
| Shared room in tent | 1 |
| Shared room in parking space | 1 |
| Shared room in igloo | 1 |
| Room in heritage hotel | 1 |
| Igloo | 1 |
| Private room in cave | 1 |
| Holiday park | 1 |
| Private room in holiday park | 1 |
| Tipi | 1 |

**instant_bookable** — 2 distinct non-null values

| Value | Count |
|---|---|
| f | 164,105 |
| t | 115,607 |

### Numeric ranges

| Column | Min | Max | Mean | Nulls |
|---|---|---|---|---|
| price | 0.00 | 625216.00 | 608.79 | 0 |
| accommodates | 0.00 | 16.00 | 3.29 | 0 |
| bedrooms | 1.00 | 50.00 | 1.52 | 29,435 |
| minimum_nights | 1.00 | 9999.00 | 8.05 | 0 |
| maximum_nights | 1.00 | 2147483647.00 | 27558.60 | 0 |

### Type consistency check

No mixed-type columns detected — every column holds a single consistent type.

## Reviews.csv

- Rows: **5,373,143**
- Columns: **4**
- Duplicate rows: **0**

### Null counts (columns with any nulls)

No nulls found in any column.

### Categorical column values

### Type consistency check

No mixed-type columns detected — every column holds a single consistent type.

## Downstream coverage check (silver_listings.sql currency logic)

- All city values are explicitly covered by the currency CASE statement.
