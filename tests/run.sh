#!/bin/bash

set -e

# Common arguments
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="postgres"
DB_USERNAME="postgres"
DB_PASSWORD="password"

CH_URL="http://localhost:8123"
CH_DATABASE="pgrep"
CH_USERNAME="default"
CH_PASSWORD="password"

MAX_BATCH_SIZE=1000
MAX_BATCH_FILL_DURATION_SECS=10

# Choose mode: "copy" or "cdc"
MODE="$1"

if [ "$MODE" == "copy" ]; then
  SCHEMA="public"
  TABLE="my_table"

  cargo run -p pg_replicate --example clickhouse --features="clickhouse" -- \
    --db-host "$DB_HOST" \
    --db-port "$DB_PORT" \
    --db-name "$DB_NAME" \
    --db-username "$DB_USERNAME" \
    --db-password "$DB_PASSWORD" \
    --ch-url "$CH_URL" \
    --ch-database "$CH_DATABASE" \
    --ch-username "$CH_USERNAME" \
    --ch-password "$CH_PASSWORD" \
    --max-batch-size "$MAX_BATCH_SIZE" \
    --max-batch-fill-duration-secs "$MAX_BATCH_FILL_DURATION_SECS" \
    copy-table \
    --schema "$SCHEMA" \
    --name "$TABLE"

elif [ "$MODE" == "cdc" ]; then
  PUBLICATION="my_publication"
  SLOT_NAME="my_slot"

  cargo run -p pg_replicate --example clickhouse --features="clickhouse" -- \
    --db-host "$DB_HOST" \
    --db-port "$DB_PORT" \
    --db-name "$DB_NAME" \
    --db-username "$DB_USERNAME" \
    --db-password "$DB_PASSWORD" \
    --ch-url "$CH_URL" \
    --ch-database "$CH_DATABASE" \
    --ch-username "$CH_USERNAME" \
    --ch-password "$CH_PASSWORD" \
    --max-batch-size "$MAX_BATCH_SIZE" \
    --max-batch-fill-duration-secs "$MAX_BATCH_FILL_DURATION_SECS" \
    cdc \
    "$PUBLICATION" \
    "$SLOT_NAME"

else
  echo "Usage: $0 [copy|cdc]"
  exit 1
fi
