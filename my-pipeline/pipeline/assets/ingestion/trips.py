"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: taxi_type
    type: string
    description: "Type of taxi service (yellow or green)"
  - name: vendor_id
    type: integer
    description: "Taxi technology provider ID"
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
  - name: passenger_count
    type: float
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
  - name: pickup_location_id
    type: integer
    description: "TLC Taxi Zone where trip started"
  - name: dropoff_location_id
    type: integer
    description: "TLC Taxi Zone where trip ended"
  - name: rate_code_id
    type: float
    description: "Rate code at end of trip"
  - name: store_and_fwd_flag
    type: string
    description: "Whether trip record was held in vehicle memory"
  - name: payment_type
    type: float
    description: "Payment method code"
  - name: fare_amount
    type: float
    description: "Time and distance fare"
  - name: extra
    type: float
    description: "Miscellaneous extras and surcharges"
  - name: mta_tax
    type: float
    description: "MTA tax"
  - name: tip_amount
    type: float
    description: "Tip amount"
  - name: tolls_amount
    type: float
    description: "Total tolls paid"
  - name: improvement_surcharge
    type: float
    description: "Improvement surcharge"
  - name: total_amount
    type: float
    description: "Total amount charged"
  - name: congestion_surcharge
    type: float
    description: "Congestion surcharge"
  - name: airport_fee
    type: float
    description: "Airport fee"
  - name: trip_type
    type: float
    description: "Trip type (green taxis only)"
  - name: ehail_fee
    type: float
    description: "E-hail fee (green taxis only)"
  - name: extracted_at
    type: timestamp
    description: "Timestamp when data was extracted"

@bruin"""

import os
import json
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime
from dateutil.relativedelta import relativedelta


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2024-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2024-02-01")

    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", '{}'))
    taxi_types = bruin_vars.get("taxi_types", ["yellow", "green"])

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    months = []
    current = start.replace(day=1)
    while current < end:
        months.append(current.strftime("%Y-%m"))
        current += relativedelta(months=1)

    all_dfs = []
    now = datetime.utcnow()

    rename_maps = {
        "yellow": {
            "VendorID": "vendor_id",
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "passenger_count": "passenger_count",
            "trip_distance": "trip_distance",
            "PULocationID": "pickup_location_id",
            "DOLocationID": "dropoff_location_id",
            "RatecodeID": "rate_code_id",
            "store_and_fwd_flag": "store_and_fwd_flag",
            "payment_type": "payment_type",
            "fare_amount": "fare_amount",
            "extra": "extra",
            "mta_tax": "mta_tax",
            "tip_amount": "tip_amount",
            "tolls_amount": "tolls_amount",
            "improvement_surcharge": "improvement_surcharge",
            "total_amount": "total_amount",
            "congestion_surcharge": "congestion_surcharge",
            "Airport_fee": "airport_fee",
            "airport_fee": "airport_fee",
        },
        "green": {
            "VendorID": "vendor_id",
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime",
            "passenger_count": "passenger_count",
            "trip_distance": "trip_distance",
            "PULocationID": "pickup_location_id",
            "DOLocationID": "dropoff_location_id",
            "RatecodeID": "rate_code_id",
            "store_and_fwd_flag": "store_and_fwd_flag",
            "payment_type": "payment_type",
            "fare_amount": "fare_amount",
            "extra": "extra",
            "mta_tax": "mta_tax",
            "tip_amount": "tip_amount",
            "tolls_amount": "tolls_amount",
            "improvement_surcharge": "improvement_surcharge",
            "total_amount": "total_amount",
            "congestion_surcharge": "congestion_surcharge",
            "trip_type": "trip_type",
            "ehail_fee": "ehail_fee",
        },
    }

    standard_columns = [
        "taxi_type", "vendor_id", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance", "pickup_location_id",
        "dropoff_location_id", "rate_code_id", "store_and_fwd_flag",
        "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "total_amount",
        "congestion_surcharge", "airport_fee", "trip_type", "ehail_fee",
        "extracted_at",
    ]

    for taxi_type in taxi_types:
        rmap = rename_maps.get(taxi_type, rename_maps["yellow"])
        for ym in months:
            url = f"{base_url}/{taxi_type}_tripdata_{ym}.parquet"
            print(f"Fetching: {url}")
            try:
                resp = requests.get(url, timeout=120)
                resp.raise_for_status()
                df = pd.read_parquet(BytesIO(resp.content))
                df = df.rename(columns=rmap)
                df["taxi_type"] = taxi_type
                df["extracted_at"] = now

                for col in standard_columns:
                    if col not in df.columns:
                        df[col] = None

                df = df[standard_columns]
                all_dfs.append(df)
                print(f"  -> {len(df)} rows")
            except Exception as e:
                print(f"  -> SKIP ({e})")
                continue

    if not all_dfs:
        return pd.DataFrame(columns=standard_columns)

    result = pd.concat(all_dfs, ignore_index=True)
    print(f"Total rows: {len(result)}")
    return result
