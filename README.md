# HW7: Amazon Reviews Analytics with Spark, Cassandra, Redis and REST API

This project is a continuation of the Amazon Reviews data engineering task. It uses Apache Spark for transformations and aggregations, loads query-optimized tables into Cassandra, and exposes the data through REST endpoints with Redis caching.

## Problem statement

Design a scalable analytical system for Amazon Reviews that supports efficient queries by product, customer and review activity over time. Consumers interact only with REST API endpoints, not with Cassandra directly.

## Architecture

```text
Amazon Reviews CSV
        |
        v
Apache Spark ETL
        |
        v
Cassandra query-optimized tables
        |
        v
FastAPI REST API <--> Redis cache, TTL = 5 minutes
```

## Cassandra schema design

The schema follows Cassandra's query-first modeling approach. There are no `ALLOW FILTERING` queries.

| Endpoint need | Cassandra table |
|---|---|
| Reviews by `product_id` | `reviews_by_product` |
| Reviews by `product_id` and `star_rating` | `reviews_by_product_rating` |
| Reviews by `customer_id` | `reviews_by_customer` |
| N most reviewed products by period | `top_products_by_month` |
| N most productive verified customers by period | `top_verified_customers_by_month` |
| N most productive haters by period | `top_haters_by_month` |
| N most productive backers by period | `top_backers_by_month` |

For period-based analytics, Spark pre-aggregates data by monthly buckets (`yyyy-MM`). For a date range covering several months, the API queries each monthly partition and merges the results in memory. This is an intentional trade-off: Cassandra reads remain partition-key based and avoid `ALLOW FILTERING`.

## REST endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/products/{product_id}/reviews?limit=100` | Return all reviews for a product |
| GET | `/products/{product_id}/reviews/{star_rating}?limit=100` | Return reviews for product with selected rating |
| GET | `/customers/{customer_id}/reviews?limit=100` | Return all reviews for customer |
| GET | `/analytics/top-products?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&n=10` | N most reviewed products |
| GET | `/analytics/top-verified-customers?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&n=10` | N most productive verified customers |
| GET | `/analytics/top-haters?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&n=10` | N most productive 1-2 star reviewers |
| GET | `/analytics/top-backers?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&n=10` | N most productive 4-5 star reviewers |

All endpoint responses are cached in Redis for 300 seconds.

## Project structure

```text
.
├── cassandra/schema.cql
├── data/.gitkeep
├── docker-compose.yml
├── Dockerfile.api
├── examples/sample_api_results.json
├── requirements.txt
├── screenshots/
├── scripts/
│   ├── example_queries.sh
│   ├── init_cassandra.sh
│   ├── run_spark_etl.sh
│   └── start_api.sh
└── src/
    ├── api/main.py
    └── spark/load_to_cassandra.py
```

## How to run

### 1. Put dataset into the project

Rename the provided CSV file to:

```bash
data/amazon_reviews.csv
```

### 2. Start Cassandra and Redis, initialize schema

```bash
chmod +x scripts/*.sh
./scripts/init_cassandra.sh
```

### 3. Run Spark ETL

```bash
./scripts/run_spark_etl.sh
```

### 4. Start REST API

```bash
./scripts/start_api.sh
```

API documentation will be available at:

```text
http://localhost:8000/docs
```

### 5. Run example queries

```bash
./scripts/example_queries.sh
```

## Manual examples

```bash
curl "http://localhost:8000/products/0439784549/reviews?limit=5"
curl "http://localhost:8000/products/0439784549/reviews/5?limit=5"
curl "http://localhost:8000/customers/50122160/reviews?limit=5"
curl "http://localhost:8000/analytics/top-products?start_date=2005-01-01&end_date=2005-12-31&n=5"
curl "http://localhost:8000/analytics/top-verified-customers?start_date=2005-01-01&end_date=2005-12-31&n=5"
curl "http://localhost:8000/analytics/top-haters?start_date=2005-01-01&end_date=2005-12-31&n=5"
curl "http://localhost:8000/analytics/top-backers?start_date=2005-01-01&end_date=2005-12-31&n=5"
```

## Screenshots

Screenshots are placed in the `screenshots/` directory and demonstrate expected successful API query results.

## Notes on performance

- No endpoint uses `ALLOW FILTERING`.
- Product/customer review lookups use direct partition key access.
- Ranking endpoints use monthly pre-aggregated tables and merge monthly results inside API.
- Redis cache reduces repeated Cassandra reads for hot API requests.
- TTL is set to 5 minutes according to task requirements.
