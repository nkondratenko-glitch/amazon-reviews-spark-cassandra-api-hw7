import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType


def get_spark() -> SparkSession:
    cassandra_host = os.getenv("CASSANDRA_HOST", "localhost")
    return (
        SparkSession.builder
        .appName("AmazonReviewsSparkCassandraHW7")
        .config("spark.cassandra.connection.host", cassandra_host)
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )


def write_cassandra(df, keyspace: str, table: str, mode: str = "append") -> None:
    (
        df.write
        .format("org.apache.spark.sql.cassandra")
        .mode(mode)
        .options(table=table, keyspace=keyspace)
        .save()
    )


def main() -> None:
    input_path = os.getenv("INPUT_PATH", "data/amazon_reviews.csv")
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "amazon_reviews_hw7")

    spark = get_spark()

    raw = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("quote", '"')
        .csv(input_path)
    )

    critical_columns = ["review_id", "product_id", "customer_id", "star_rating", "review_date"]
    cleaned = (
        raw.dropna(subset=critical_columns)
        .withColumn("customer_id", F.col("customer_id").cast(LongType()))
        .withColumn("star_rating", F.col("star_rating").cast(IntegerType()))
        .withColumn("verified_purchase", F.col("verified_purchase").cast(IntegerType()))
        .withColumn("review_date", F.to_date("review_date", "yyyy-MM-dd"))
        .dropna(subset=["review_date"])
        .filter(F.col("star_rating").between(1, 5))
        .withColumn("period", F.date_format("review_date", "yyyy-MM"))
        .select(
            "review_id", "product_id", "customer_id", "star_rating", "verified_purchase",
            "review_date", "period", "review_headline", "review_body", "product_title", "product_category"
        )
    )

    reviews_by_product = cleaned.select(
        "product_id", "review_date", "review_id", "customer_id", "star_rating", "verified_purchase",
        "review_headline", "review_body", "product_title", "product_category"
    )
    write_cassandra(reviews_by_product, keyspace, "reviews_by_product")

    reviews_by_product_rating = cleaned.select(
        "product_id", "star_rating", "review_date", "review_id", "customer_id", "verified_purchase",
        "review_headline", "review_body", "product_title", "product_category"
    )
    write_cassandra(reviews_by_product_rating, keyspace, "reviews_by_product_rating")

    reviews_by_customer = cleaned.select(
        "customer_id", "review_date", "review_id", "product_id", "star_rating", "verified_purchase",
        "review_headline", "review_body", "product_title", "product_category"
    )
    write_cassandra(reviews_by_customer, keyspace, "reviews_by_customer")

    top_products = (
        cleaned.groupBy("period", "product_id")
        .agg(
            F.count("review_id").alias("review_count"),
            F.avg("star_rating").alias("avg_star_rating"),
            F.first("product_title", ignorenulls=True).alias("product_title")
        )
        .select("period", "review_count", "product_id", "avg_star_rating", "product_title")
    )
    write_cassandra(top_products, keyspace, "top_products_by_month")

    top_verified_customers = (
        cleaned.filter(F.col("verified_purchase") == 1)
        .groupBy("period", "customer_id")
        .agg(F.count("review_id").alias("review_count"))
        .select("period", "review_count", "customer_id")
    )
    write_cassandra(top_verified_customers, keyspace, "top_verified_customers_by_month")

    top_haters = (
        cleaned.filter(F.col("star_rating").isin(1, 2))
        .groupBy("period", "customer_id")
        .agg(F.count("review_id").alias("review_count"))
        .select("period", "review_count", "customer_id")
    )
    write_cassandra(top_haters, keyspace, "top_haters_by_month")

    top_backers = (
        cleaned.filter(F.col("star_rating").isin(4, 5))
        .groupBy("period", "customer_id")
        .agg(F.count("review_id").alias("review_count"))
        .select("period", "review_count", "customer_id")
    )
    write_cassandra(top_backers, keyspace, "top_backers_by_month")

    print("Spark ETL completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
