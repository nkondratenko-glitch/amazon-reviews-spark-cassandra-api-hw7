#!/usr/bin/env bash
set -euo pipefail

MONGO_URI=${MONGO_URI:-mongodb://root:example@localhost:27017/amazon_reviews_hw?authSource=admin}

mongosh "$MONGO_URI" <<'MONGO'
print("\n1) Product review counts and average ratings:")
db.product_review_stats.find(
  {},
  {_id: 0, product_id: 1, product_title: 1, total_reviews: 1, avg_star_rating: 1}
).sort({total_reviews: -1}).limit(5).pretty()

print("\n2) Number of verified reviews by customer:")
db.customer_review_counts.find(
  {},
  {_id: 0, customer_id: 1, total_verified_reviews: 1}
).sort({total_verified_reviews: -1}).limit(5).pretty()

print("\n3) Monthly product trend for one popular product:")
const topProduct = db.product_review_stats.findOne({}, {sort: {total_reviews: -1}}).product_id;
db.product_monthly_reviews.find(
  {product_id: topProduct},
  {_id: 0, product_id: 1, review_month: 1, monthly_reviews: 1, monthly_avg_star_rating: 1}
).sort({review_month: 1}).limit(12).pretty()

print("\n4) Indexes:")
db.product_review_stats.getIndexes()
db.customer_review_counts.getIndexes()
db.product_monthly_reviews.getIndexes()
MONGO
