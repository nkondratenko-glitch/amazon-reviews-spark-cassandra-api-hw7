#!/usr/bin/env bash
set -euo pipefail

docker compose up -d api

echo "API docs: http://localhost:8000/docs"
