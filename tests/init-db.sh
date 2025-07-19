#!/bin/bash
set -e

echo "Init script started..."

# Wait until PostgreSQL is ready
until pg_isready -U "$POSTGRES_USER"; do
  echo "Waiting for postgres to be ready..."
  sleep 1
done

psql -U "$POSTGRES_USER" -tAc "CREATE TABLE IF NOT EXISTS table1(
  id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);"

psql -U "$POSTGRES_USER" -tAc "CREATE TABLE IF NOT EXISTS table2(
  id uuid PRIMARY KEY default gen_random_uuid(),
  name VARCHAR(100) NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);"


psql -U "$POSTGRES_USER" -tAc """
create extension if not exists pgcrypto;
create type mood_enum as enum ('happy', 'sad', 'ok');
create table if not exists all_postgres_types
(
    id                   uuid primary key default gen_random_uuid(),
    col_smallint         smallint,
    col_integer          integer,
    col_bigint           bigint,
    col_decimal          decimal(10, 2),
    col_numeric          numeric(10, 2),
    col_real             real,
    col_double_precision double precision,

    col_serial           serial,
    col_bigserial        bigserial,

    col_money            money,

    col_char             char(10),
    col_varchar          varchar(50),
    col_text             text,

    col_bytea            bytea,

    col_boolean          boolean,

    col_date             date,
    col_time             time,
    col_timetz           time with time zone,
    col_timestamp        timestamp,
    col_timestamptz      timestamp with time zone,
    col_interval         interval,

    col_json             json,
    col_jsonb            jsonb,
    col_xml              xml,

    col_uuid             uuid,

    col_inet             inet,
    col_cidr             cidr,
    col_macaddr          macaddr,
    col_macaddr8         macaddr8,

    col_point            point,
    col_line             line,
    col_lseg             lseg,
    col_box              box,
    col_path             path,
    col_polygon          polygon,
    col_circle           circle,

    col_bit              bit(8),
    col_bit_varying      bit varying(16),

    col_tsquery          tsquery,
    col_tsvector         tsvector,

    col_enum             mood_enum,

    col_array_int        integer[],
    col_array_text       text[],
    col_array_timestamp       timestamp[],
    col_array_timestamptz       timestamptz[],

    col_range_int4       int4range,
    col_range_num        numrange,
    col_range_ts         tsrange,
    col_range_tstz       tstzrange,
    col_range_date       daterange
);
"""


echo "Waiting for WAL level to be set to logical..."
until [ "$(psql -U "$POSTGRES_USER" -tAc "SHOW wal_level;")" = "logical" ]; do
  echo "WAL level is not set to logical yet. Waiting..."
  sleep 2
done

psql -U "$POSTGRES_USER" -tAc "create publication bqpub for table table1, table2, all_postgres_types;"

echo "Init script finished."
