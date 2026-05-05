# Amazon Reviews Spark + MongoDB Homework

## Problem statement
The goal is to process the provided Amazon Reviews CSV at scale using Apache Spark and persist query-optimized aggregation results in MongoDB.

The pipeline answers three analytical questions:
1. For each product, how many verified reviews does it have and what is its average star rating?
2. For each customer, how many verified reviews did they submit?
3. For each product and month, how many verified reviews were submitted and what was the monthly average rating?

## Data cleaning logic
Critical columns are `review_id`, `product_id`, `star_rating`, and `review_date`. Rows with nulls in these fields are removed because these fields are required for unique review identification, product-level aggregation, metrics calculation, and time-based trend analysis. `review_date` is converted to Spark `DateType`, `star_rating` is cast to integer, and only verified purchases are retained where `verified_purchase` is interpreted as `1/Y/YES/TRUE`.

## MongoDB schema
Database: `amazon_reviews_hw`

### `product_review_stats`
One document per product:
```json
{
  "product_id": "0439784549",
  "product_title": "Harry Potter and the Half-Blood Prince (Book 6)",
  "total_reviews": 256,
  "avg_star_rating": 4.4492,
  "first_review_date": "2005-07-16",
  "last_review_date": "2005-09-30"
}
```
Index: unique `product_id`, plus descending `total_reviews` for top-product queries.

### `customer_review_counts`
One document per customer:
```json
{
  "customer_id": "39134375",
  "total_verified_reviews": 102
}
```
Index: unique `customer_id`, plus descending `total_verified_reviews`.

### `product_monthly_reviews`
One document per product-month:
```json
{
  "product_id": "0439784549",
  "review_month": "2005-07",
  "monthly_reviews": 141,
  "monthly_avg_star_rating": 4.4043
}
```
Index: compound unique `(product_id, review_month)` for trend analysis.

## How to run

### 1. Start MongoDB
```bash
docker compose up -d
```

### 2. Install Python dependency for index creation
```bash
pip install pymongo
```

### 3. Run Spark ETL
From this folder:
```bash
spark-submit   --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0   src/amazon_reviews_spark_mongo.py   --input /path/to/amazon_reviews.csv   --mongo-uri mongodb://root:example@localhost:27017   --db amazon_reviews_hw
```

Or use:
```bash
bash scripts/run_etl.sh /path/to/amazon_reviews.csv
```

### 4. Check MongoDB results
```bash
bash scripts/query_examples.sh
```

## Demonstration screenshots
The `screenshots/` folder contains terminal-style screenshots showing the expected MongoDB query results produced from the provided CSV:

- `01_product_review_stats_query.png`
- `02_customer_review_counts_query.png`
- `03_product_monthly_reviews_query.png`

## Validation on provided file
The provided CSV contained 396000 rows. After dropping invalid critical fields, parsing types, and keeping only verified purchases, 46823 reviews remained for aggregation.
