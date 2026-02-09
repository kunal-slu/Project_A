# Transformation Showcase (PySpark)

This report shows realistic transformations and PySpark optimizations on Project A data.

## Data realism summary

- Orders date range: 2024-02-09 → 2026-02-08
- Approx distinct customers: 25,484
- Approx distinct products: 10,430

## Bronze sample (orders)

| order_id | customer_id | product_id | order_date | order_timestamp | quantity | unit_price | total_amount | currency | payment_method | shipping_method | status | op | is_deleted | shipping_address | billing_address | discount_percent | tax_amount | shipping_cost | promo_code | sales_rep_id | channel | created_at | updated_at | source_system | ingestion_timestamp | order_month | order_quarter | order_year | order_segment | customer_order_count | fulfillment_days |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ORD-00000001 | CUST-000221 | PROD-002275 | 2025-11-28 | 2025-11-28 01:14:50 | 7 | 880.59 | 6678.96 | EUR | PAYPAL | 2_DAY | COMPLETED | INSERT | false | 6912 Order St | 9381 Billing St | 0.0 | 493.13 | 21.7 | PROMO406 | REP-0095 | CALL_CENTER | 2025-11-28 01:14:50 | 2026-01-21 21:47:15 | snowflake | 2026-02-08 20:40:34.931109 | 11 | 4 | 2025 | high_value | 1 | 4 |
| ORD-00000002 | CUST-022755 | PROD-006524 | 2024-09-24 | 2024-09-24 05:57:27 | 10 | 1344.49 | 12347.57 | GBP | BANK_TRANSFER | 2_DAY | COMPLETED | INSERT | false | 2693 Order St | 1641 Billing St | 15.0 | 914.25 | 5.16 | PROMO114 | REP-0082 | PARTNER | 2024-09-24 05:57:27 | 2025-07-25 16:45:01 | snowflake | 2026-02-08 20:40:34.931109 | 9 | 3 | 2024 | high_value | 1 | 12 |
| ORD-00000003 | CUST-001815 | PROD-000129 | 2025-04-01 | 2025-04-01 07:46:49 | 4 | 782.1 | 3399.72 | GBP | PAYPAL | 2_DAY | CANCELLED | INSERT | false | 9664 Order St | 4906 Billing St | 0.0 | 250.27 | 21.05 | PROMO218 | REP-0036 | CALL_CENTER | 2025-04-01 07:46:49 | 2025-10-01 00:03:41 | snowflake | 2026-02-08 20:40:34.931109 | 4 | 2 | 2025 | high_value | 1 |  |
| ORD-00000004 | CUST-000265 | PROD-000334 | 2025-09-28 | 2025-09-28 10:29:28 | 5 | 257.16 | 1339.44 | AUD | BANK_TRANSFER | GROUND | COMPLETED | INSERT | false | 7308 Order St | 4794 Billing St | 5.0 | 97.72 | 20.21 | PROMO427 | REP-0055 | CALL_CENTER | 2025-09-28 10:29:28 | 2025-10-06 03:06:05 | snowflake | 2026-02-08 20:40:34.931109 | 9 | 3 | 2025 | high_value | 1 | 10 |
| ORD-00000005 | CUST-001548 | PROD-007584 | 2025-05-06 | 2025-05-06 08:11:31 | 1 | 362.29 | 391.91 | AUD | CREDIT_CARD | OVERNIGHT | COMPLETED | INSERT | false | 1699 Order St | 6235 Billing St | 5.0 | 27.53 | 20.2 | PROMO315 | REP-0160 | WEB | 2025-05-06 08:11:31 | 2025-05-12 10:21:49 | snowflake | 2026-02-08 20:40:34.931109 | 5 | 2 | 2025 | mid_value | 1 | 2 |
| ORD-00000006 | CUST-017839 | PROD-000086 | 2025-01-26 | 2025-01-26 01:46:22 | 5 | 523.74 | 2278.17 | EUR | DEBIT_CARD | 2_DAY | PENDING | INSERT | false | 3761 Order St | 4653 Billing St | 20.0 | 167.6 | 15.61 | PROMO353 | REP-0165 | CALL_CENTER | 2025-01-26 01:46:22 | 2026-01-09 06:34:04 | snowflake | 2026-02-08 20:40:34.931109 | 1 | 1 | 2025 | high_value | 1 |  |
| ORD-00000007 | CUST-010675 | PROD-000034 | 2026-01-09 | 2026-01-09 09:04:20 | 8 | 406.11 | 2999.65 | CAD | DEBIT_CARD | OVERNIGHT | SHIPPED | INSERT | false | 3744 Order St | 3374 Billing St | 15.0 | 220.92 | 17.18 |  | REP-0062 | PARTNER | 2026-01-09 09:04:20 | 2026-01-15 21:40:09 | snowflake | 2026-02-08 20:40:34.931109 | 1 | 1 | 2026 | high_value | 1 | 5 |
| ORD-00000008 | CUST-000497 | PROD-000137 | 2024-12-18 | 2024-12-18 04:47:48 | 10 | 656.34 | 6745.15 | USD | BANK_TRANSFER | OVERNIGHT | COMPLETED | INSERT | false | 3539 Order St | 88 Billing St | 5.0 | 498.82 | 11.1 | PROMO310 | REP-0194 | WEB | 2024-12-18 04:47:48 | 2025-07-08 20:18:04 | snowflake | 2026-02-08 20:40:34.931109 | 12 | 4 | 2024 | high_value | 1 | 11 |

## Silver sample (deduped + cleaned)

| order_id | customer_id | product_id | order_date | total_amount | quantity | status | updated_at |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ORD-00000001 | CUST-000221 | PROD-002275 | 2025-11-28 | 6678.96 | 7 | COMPLETED | 2026-01-21 21:47:15 |
| ORD-00000002 | CUST-022755 | PROD-006524 | 2024-09-24 | 12347.57 | 10 | COMPLETED | 2025-07-25 16:45:01 |
| ORD-00000007 | CUST-010675 | PROD-000034 | 2026-01-09 | 2999.65 | 8 | SHIPPED | 2026-01-15 21:40:09 |
| ORD-00000011 | CUST-001222 | PROD-000143 | 2025-03-03 | 5248.75 | 4 | COMPLETED | 2025-06-20 13:27:11 |
| ORD-00000028 | CUST-003875 | PROD-004494 | 2025-12-09 | 675.6 | 1 | PENDING | 2025-12-23 13:33:57 |
| ORD-00000037 | CUST-038119 | PROD-000161 | 2024-08-29 | 6776.77 | 4 | SHIPPED | 2025-02-16 03:47:29 |
| ORD-00000038 | CUST-001752 | PROD-000433 | 2024-03-29 | 15247.06 | 9 | COMPLETED | 2025-01-01 16:18:00 |
| ORD-00000048 | CUST-001192 | PROD-000497 | 2024-05-25 | 386.6 | 4 | COMPLETED | 2025-01-22 11:16:37 |

## Gold sample (fact_orders)

| customer_id | product_id | order_id | order_date | total_amount | quantity | status | updated_at | product_name | category | price_usd | country | order_month | order_year |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| CUST-000221 | PROD-002275 | ORD-00000001 | 2025-11-28 | 6678.96 | 7 | COMPLETED | 2026-01-21 21:47:15 | Product 275 - Home | Home | 231.2 | AU | 11 | 2025 |
| CUST-022755 | PROD-006524 | ORD-00000002 | 2024-09-24 | 12347.57 | 10 | COMPLETED | 2025-07-25 16:45:01 | Product 524 - Clothing | Clothing | 430.55 | CA | 9 | 2024 |
| CUST-010675 | PROD-000034 | ORD-00000007 | 2026-01-09 | 2999.65 | 8 | SHIPPED | 2026-01-15 21:40:09 | Product 34 - Home | Home | 506.38 | GB | 1 | 2026 |
| CUST-001222 | PROD-000143 | ORD-00000011 | 2025-03-03 | 5248.75 | 4 | COMPLETED | 2025-06-20 13:27:11 | Product 143 - Automotive | Automotive | 873.27 | BR | 3 | 2025 |
| CUST-003875 | PROD-004494 | ORD-00000028 | 2025-12-09 | 675.6 | 1 | PENDING | 2025-12-23 13:33:57 | Product 494 - Sports | Sports | 1275.28 | DE | 12 | 2025 |
| CUST-038119 | PROD-000161 | ORD-00000037 | 2024-08-29 | 6776.77 | 4 | SHIPPED | 2025-02-16 03:47:29 | Product 161 - Books | Books | 934.29 | GB | 8 | 2024 |
| CUST-001752 | PROD-000433 | ORD-00000038 | 2024-03-29 | 15247.06 | 9 | COMPLETED | 2025-01-01 16:18:00 | Product 433 - Clothing | Clothing | 1220.12 | AU | 3 | 2024 |
| CUST-001192 | PROD-000497 | ORD-00000048 | 2024-05-25 | 386.6 | 4 | COMPLETED | 2025-01-22 11:16:37 | Product 497 - Toys | Toys | 504.27 | DE | 5 | 2024 |

## Gold sample (customer metrics)

| customer_id | order_count | total_revenue | avg_order_value | last_order_date |
| --- | --- | --- | --- | --- |
| CUST-002321 | 30 | 339848.29000000004 | 11328.276333333335 | 2025-11-04 |
| CUST-000114 | 39 | 332229.77999999997 | 8518.712307692307 | 2026-01-30 |
| CUST-001838 | 39 | 330179.72 | 8466.146666666666 | 2026-01-16 |
| CUST-001936 | 36 | 325647.25999999995 | 9045.75722222222 | 2026-01-30 |
| CUST-000487 | 34 | 322811.15 | 9494.445588235294 | 2026-01-27 |
| CUST-001344 | 31 | 316116.10000000003 | 10197.293548387099 | 2026-01-29 |
| CUST-002409 | 28 | 311096.35 | 11110.583928571428 | 2026-02-03 |
| CUST-001484 | 39 | 308200.56000000006 | 7902.578461538463 | 2026-01-21 |

## Optimizations used

- Broadcast join for product dimension (`broadcast`)
- Repartition on `order_date` before heavy joins
- Cache to avoid recomputation
- AQE enabled (adaptive query execution)

## Explain plan (join)

See: `artifacts/transform_demo/explain_join_plan.txt`
