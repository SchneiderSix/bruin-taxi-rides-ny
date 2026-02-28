/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: "Date of the trips"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi service"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Payment method name"
    primary_key: true
  - name: total_trips
    type: bigint
    description: "Number of trips"
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: "Sum of total amounts charged"
    checks:
      - name: not_null
  - name: avg_trip_distance
    type: float
    description: "Average trip distance in miles"
  - name: avg_fare
    type: float
    description: "Average fare amount"

@bruin */

SELECT
    CAST(pickup_datetime AS DATE) AS pickup_date,
    taxi_type,
    payment_type_name,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_distance,
    SUM(fare_amount) AS total_fare,
    SUM(tip_amount) AS total_tip,
    SUM(total_amount) AS total_amount,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(fare_amount) AS avg_fare
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
    CAST(pickup_datetime AS DATE),
    taxi_type,
    payment_type_name
