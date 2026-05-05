CREATE KEYSPACE IF NOT EXISTS amazon_reviews_hw7
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE amazon_reviews_hw7;

-- 1) All reviews for a product_id.
CREATE TABLE IF NOT EXISTS reviews_by_product (
    product_id text,
    review_date date,
    review_id text,
    customer_id bigint,
    star_rating int,
    verified_purchase int,
    review_headline text,
    review_body text,
    product_title text,
    product_category text,
    PRIMARY KEY ((product_id), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- 2) All reviews for product_id with a given star_rating.
CREATE TABLE IF NOT EXISTS reviews_by_product_rating (
    product_id text,
    star_rating int,
    review_date date,
    review_id text,
    customer_id bigint,
    verified_purchase int,
    review_headline text,
    review_body text,
    product_title text,
    product_category text,
    PRIMARY KEY ((product_id, star_rating), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- 3) All reviews for customer_id.
CREATE TABLE IF NOT EXISTS reviews_by_customer (
    customer_id bigint,
    review_date date,
    review_id text,
    product_id text,
    star_rating int,
    verified_purchase int,
    review_headline text,
    review_body text,
    product_title text,
    product_category text,
    PRIMARY KEY ((customer_id), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- Period ranking tables. Periods are monthly buckets yyyy-MM.
-- Endpoint for an arbitrary date range queries several monthly partitions and merges top N in API.
CREATE TABLE IF NOT EXISTS top_products_by_month (
    period text,
    review_count bigint,
    product_id text,
    avg_star_rating double,
    product_title text,
    PRIMARY KEY ((period), review_count, product_id)
) WITH CLUSTERING ORDER BY (review_count DESC, product_id ASC);

CREATE TABLE IF NOT EXISTS top_verified_customers_by_month (
    period text,
    review_count bigint,
    customer_id bigint,
    PRIMARY KEY ((period), review_count, customer_id)
) WITH CLUSTERING ORDER BY (review_count DESC, customer_id ASC);

CREATE TABLE IF NOT EXISTS top_haters_by_month (
    period text,
    review_count bigint,
    customer_id bigint,
    PRIMARY KEY ((period), review_count, customer_id)
) WITH CLUSTERING ORDER BY (review_count DESC, customer_id ASC);

CREATE TABLE IF NOT EXISTS top_backers_by_month (
    period text,
    review_count bigint,
    customer_id bigint,
    PRIMARY KEY ((period), review_count, customer_id)
) WITH CLUSTERING ORDER BY (review_count DESC, customer_id ASC);
