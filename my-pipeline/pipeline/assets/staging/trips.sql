/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
    primary_key: true
  - name: pickup_location_id
    type: integer
    description: "TLC Taxi Zone where trip started"
    primary_key: true
  - name: dropoff_location_id
    type: integer
    description: "TLC Taxi Zone where trip ended"
    primary_key: true
  - name: fare_amount
    type: float
    description: "Time and distance fare"
    primary_key: true
  - name: taxi_type
    type: string
    description: "Type of taxi service"
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - yellow
          - green
  - name: total_amount
    type: float
    description: "Total amount charged"
    checks:
      - name: not_null
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
    checks:
      - name: non_negative

custom_checks:
  - name: no_negative_total_amount
    description: "Ensure no trips have negative total amount"
    query: |
      SELECT COUNT(*)
      FROM staging.trips
      WHERE total_amount < 0
        AND pickup_datetime >= '{{ start_datetime }}'
        AND pickup_datetime < '{{ end_datetime }}'
    value: 0

@bruin */

WITH raw_trips AS (
    SELECT *
    FROM ingestion.trips
    WHERE pickup_datetime >= '{{ start_datetime }}'
      AND pickup_datetime < '{{ end_datetime }}'
      AND vendor_id IS NOT NULL
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM raw_trips
),

enriched AS (
    SELECT
        d.taxi_type,
        d.vendor_id,
        d.pickup_datetime,
        d.dropoff_datetime,
        d.passenger_count,
        d.trip_distance,
        d.pickup_location_id,
        d.dropoff_location_id,
        d.rate_code_id,
        d.store_and_fwd_flag,
        CAST(d.payment_type AS INTEGER) AS payment_type,
        COALESCE(p.payment_type_name, 'unknown') AS payment_type_name,
        d.fare_amount,
        d.extra,
        d.mta_tax,
        d.tip_amount,
        d.tolls_amount,
        d.improvement_surcharge,
        d.total_amount,
        d.congestion_surcharge,
        d.airport_fee,
        d.trip_type,
        d.ehail_fee
    FROM deduplicated d
    LEFT JOIN ingestion.payment_lookup p
        ON CAST(d.payment_type AS INTEGER) = p.payment_type_id
    WHERE d.row_num = 1
)

SELECT * FROM enriched
