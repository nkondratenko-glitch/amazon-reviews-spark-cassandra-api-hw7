#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
PRODUCT_ID="${PRODUCT_ID:-000100039X}"
CUSTOMER_ID="${CUSTOMER_ID:-12076615}"
START_DATE="${START_DATE:-2005-01-01}"
END_DATE="${END_DATE:-2005-12-31}"
N="${N:-5}"

curl -s "$BASE_URL/products/$PRODUCT_ID/reviews?limit=3" | jq .
curl -s "$BASE_URL/products/$PRODUCT_ID/reviews/5?limit=3" | jq .
curl -s "$BASE_URL/customers/$CUSTOMER_ID/reviews?limit=3" | jq .
curl -s "$BASE_URL/analytics/top-products?start_date=$START_DATE&end_date=$END_DATE&n=$N" | jq .
curl -s "$BASE_URL/analytics/top-verified-customers?start_date=$START_DATE&end_date=$END_DATE&n=$N" | jq .
curl -s "$BASE_URL/analytics/top-haters?start_date=$START_DATE&end_date=$END_DATE&n=$N" | jq .
curl -s "$BASE_URL/analytics/top-backers?start_date=$START_DATE&end_date=$END_DATE&n=$N" | jq .
