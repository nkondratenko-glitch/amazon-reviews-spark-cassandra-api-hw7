import json
import os
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Iterable

import redis
from cassandra.cluster import Cluster
from fastapi import FastAPI, HTTPException, Query

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "amazon_reviews_hw7")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "300"))

app = FastAPI(title="Amazon Reviews Cassandra API", version="1.0.0")
cluster = None
session = None
redis_client = None


def row_to_dict(row: Any) -> dict[str, Any]:
    result = dict(row._asdict())
    for key, value in list(result.items()):
        if isinstance(value, (date, datetime)):
            result[key] = value.isoformat()
    return result


def months_between(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date().replace(day=1)
    end = datetime.strptime(end_date, "%Y-%m-%d").date().replace(day=1)
    if start > end:
        raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")
    months = []
    current = start
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        year = current.year + (current.month // 12)
        month = current.month % 12 + 1
        current = current.replace(year=year, month=month)
    return months


def get_cached_or_query(cache_key: str, producer):
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)
    result = producer()
    redis_client.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(result, ensure_ascii=False, default=str))
    return result


@app.on_event("startup")
def startup_event() -> None:
    global cluster, session, redis_client
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    session.row_factory = None
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


@app.on_event("shutdown")
def shutdown_event() -> None:
    if cluster:
        cluster.shutdown()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/products/{product_id}/reviews")
def get_reviews_by_product(product_id: str, limit: int = Query(100, ge=1, le=1000)):
    cache_key = f"product_reviews:{product_id}:{limit}"

    def query():
        rows = session.execute(
            "SELECT product_id, review_date, review_id, customer_id, star_rating, verified_purchase, "
            "review_headline, review_body, product_title, product_category "
            "FROM reviews_by_product WHERE product_id = %s LIMIT %s",
            (product_id, limit),
        )
        return [row_to_dict(row) for row in rows]

    return get_cached_or_query(cache_key, query)


@app.get("/products/{product_id}/reviews/{star_rating}")
def get_reviews_by_product_and_rating(product_id: str, star_rating: int, limit: int = Query(100, ge=1, le=1000)):
    if star_rating < 1 or star_rating > 5:
        raise HTTPException(status_code=400, detail="star_rating must be between 1 and 5")
    cache_key = f"product_rating_reviews:{product_id}:{star_rating}:{limit}"

    def query():
        rows = session.execute(
            "SELECT product_id, star_rating, review_date, review_id, customer_id, verified_purchase, "
            "review_headline, review_body, product_title, product_category "
            "FROM reviews_by_product_rating WHERE product_id = %s AND star_rating = %s LIMIT %s",
            (product_id, star_rating, limit),
        )
        return [row_to_dict(row) for row in rows]

    return get_cached_or_query(cache_key, query)


@app.get("/customers/{customer_id}/reviews")
def get_reviews_by_customer(customer_id: int, limit: int = Query(100, ge=1, le=1000)):
    cache_key = f"customer_reviews:{customer_id}:{limit}"

    def query():
        rows = session.execute(
            "SELECT customer_id, review_date, review_id, product_id, star_rating, verified_purchase, "
            "review_headline, review_body, product_title, product_category "
            "FROM reviews_by_customer WHERE customer_id = %s LIMIT %s",
            (customer_id, limit),
        )
        return [row_to_dict(row) for row in rows]

    return get_cached_or_query(cache_key, query)


def top_from_monthly_table(table_name: str, start_date: str, end_date: str, n: int, entity_field: str):
    periods = months_between(start_date, end_date)
    cache_key = f"top:{table_name}:{start_date}:{end_date}:{n}"

    def query():
        totals: dict[Any, dict[str, Any]] = defaultdict(lambda: {"review_count": 0})
        for period in periods:
            rows = session.execute(
                f"SELECT * FROM {table_name} WHERE period = %s LIMIT %s",
                (period, max(n * 10, 100)),
            )
            for row in rows:
                d = row_to_dict(row)
                entity_id = d[entity_field]
                totals[entity_id][entity_field] = entity_id
                totals[entity_id]["review_count"] += int(d.get("review_count", 0))
                if "product_title" in d and d.get("product_title"):
                    totals[entity_id]["product_title"] = d["product_title"]
                if "avg_star_rating" in d and d.get("avg_star_rating") is not None:
                    totals[entity_id].setdefault("monthly_avg_star_rating", []).append(float(d["avg_star_rating"]))
        result = sorted(totals.values(), key=lambda x: (-x["review_count"], str(x[entity_field])))[:n]
        for item in result:
            if "monthly_avg_star_rating" in item:
                vals = item.pop("monthly_avg_star_rating")
                item["avg_star_rating_approx"] = round(sum(vals) / len(vals), 4) if vals else None
        return result

    return get_cached_or_query(cache_key, query)


@app.get("/analytics/top-products")
def get_top_products(start_date: str, end_date: str, n: int = Query(10, ge=1, le=100)):
    return top_from_monthly_table("top_products_by_month", start_date, end_date, n, "product_id")


@app.get("/analytics/top-verified-customers")
def get_top_verified_customers(start_date: str, end_date: str, n: int = Query(10, ge=1, le=100)):
    return top_from_monthly_table("top_verified_customers_by_month", start_date, end_date, n, "customer_id")


@app.get("/analytics/top-haters")
def get_top_haters(start_date: str, end_date: str, n: int = Query(10, ge=1, le=100)):
    return top_from_monthly_table("top_haters_by_month", start_date, end_date, n, "customer_id")


@app.get("/analytics/top-backers")
def get_top_backers(start_date: str, end_date: str, n: int = Query(10, ge=1, le=100)):
    return top_from_monthly_table("top_backers_by_month", start_date, end_date, n, "customer_id")
