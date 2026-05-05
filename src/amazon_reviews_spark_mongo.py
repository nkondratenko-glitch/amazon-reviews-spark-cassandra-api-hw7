#!/usr/bin/env python3
"""
Amazon Reviews ETL with Apache Spark and MongoDB.

Pipeline:
1. Load Amazon Reviews CSV into Spark DataFrame.
2. Clean data: drop nulls in critical columns, cast dates/ratings, keep verified purchases only.
3. Aggregate:
   - total number of reviews and average star rating per product;
   - total number of verified reviews by customer;
   - monthly review counts per product.
4. Store results in MongoDB collections optimized with indexes.

Run example:
spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  src/amazon_reviews_spark_mongo.py \
  --input /data/amazon_reviews.csv \
  --mongo-uri mongodb://root:example@localhost:27017 \
  --db amazon_reviews_hw
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType


def parse_args():
    parser = argparse.ArgumentParser(description="Process Amazon Reviews with Spark and save aggregations to MongoDB")
    parser.add_argument("--input", required=True, help="Path to Amazon Reviews CSV file")
    parser.add_argument("--mongo-uri", default="mongodb://root:example@localhost:27017", help="MongoDB connection URI")
    parser.add_argument("--db", default="amazon_reviews_hw", help="MongoDB database name")
    return parser.parse_args()


def normalize_verified_purchase(col):
    """Handle both 0/1 and Y/N-style verified_purchase values."""
    as_str = F.upper(F.trim(col.cast(StringType())))
    return F.when(as_str.isin("1", "Y", "YES", "TRUE"), F.lit(1)).otherwise(F.lit(0))


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("AmazonReviewsSparkMongoETL")
        .config("spark.mongodb.write.connection.uri", args.mongo_uri)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", True)
        .csv(args.input)
    )

    critical_columns = ["review_id", "product_id", "star_rating", "review_date"]

    cleaned_df = (
        raw_df
        .dropna(subset=critical_columns)
        .withColumn("star_rating", F.col("star_rating").cast(IntegerType()))
        .withColumn("review_date", F.to_date(F.col("review_date"), "yyyy-MM-dd"))
        .withColumn("verified_purchase_int", normalize_verified_purchase(F.col("verified_purchase")))
        .dropna(subset=["star_rating", "review_date"])
        .filter(F.col("verified_purchase_int") == 1)
    )

    product_stats = (
        cleaned_df
        .groupBy("product_id")
        .agg(
            F.first("product_title", ignorenulls=True).alias("product_title"),
            F.count("review_id").alias("total_reviews"),
            F.round(F.avg("star_rating"), 4).alias("avg_star_rating"),
            F.min("review_date").alias("first_review_date"),
            F.max("review_date").alias("last_review_date"),
        )
        .orderBy(F.desc("total_reviews"))
    )

    customer_counts = (
        cleaned_df
        .groupBy("customer_id")
        .agg(F.count("review_id").alias("total_verified_reviews"))
        .orderBy(F.desc("total_verified_reviews"))
    )

    monthly_product_reviews = (
        cleaned_df
        .withColumn("review_month", F.date_format(F.col("review_date"), "yyyy-MM"))
        .groupBy("product_id", "review_month")
        .agg(
            F.count("review_id").alias("monthly_reviews"),
            F.round(F.avg("star_rating"), 4).alias("monthly_avg_star_rating")
        )
        .orderBy("product_id", "review_month")
    )

    collections = {
        "product_review_stats": product_stats,
        "customer_review_counts": customer_counts,
        "product_monthly_reviews": monthly_product_reviews,
    }

    for collection_name, df in collections.items():
        (
            df.write
            .format("mongodb")
            .mode("overwrite")
            .option("database", args.db)
            .option("collection", collection_name)
            .save()
        )
        print(f"Saved {df.count()} documents into {args.db}.{collection_name}")

    # Create query-optimized indexes after Spark writes complete.
    try:
        from pymongo import MongoClient, ASCENDING, DESCENDING
        client = MongoClient(args.mongo_uri)
        db = client[args.db]
        db.product_review_stats.create_index([("product_id", ASCENDING)], unique=True, name="idx_product_id_unique")
        db.customer_review_counts.create_index([("customer_id", ASCENDING)], unique=True, name="idx_customer_id_unique")
        db.product_monthly_reviews.create_index(
            [("product_id", ASCENDING), ("review_month", ASCENDING)],
            unique=True,
            name="idx_product_month_unique",
        )
        db.product_review_stats.create_index([("total_reviews", DESCENDING)], name="idx_total_reviews_desc")
        db.customer_review_counts.create_index([("total_verified_reviews", DESCENDING)], name="idx_customer_reviews_desc")
        print("MongoDB indexes created successfully.")
    except Exception as exc:
        print(f"WARNING: could not create indexes via pymongo: {exc}")
        print("Install pymongo or create indexes manually using scripts/query_examples.sh")

    spark.stop()


if __name__ == "__main__":
    main()
