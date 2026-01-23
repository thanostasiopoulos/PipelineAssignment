# Technical assignment

- Part 1: **Data ingestion & transformation**
- Part 2: **Storage & modeling design** 
- Part 3: **Simple data api with FastAPI** 
- Part 4: **Code quality & reliability**


## Repo layout

```
src/
  pipeline/
    run_pipeline.py
    spark_transform.py
    __init__.py
  app/
    main.py
    __init__.py
  tests/
  __init__.py
  test_api.py
input/
output/
  bronze
  silver
  gold
```

## Running the code
a virtual environment has been created in vscode to include all the runtime environments and libraries

## Run the pipeline
C:/Users/thano/Projects/FuturaeAssignment/.venv/Scripts/python.exe -m src.pipeline.run_pipeline

## Run the FastAPI
uvicorn src.app.main:app --reload

## API URL with parameters
http://127.0.0.1:8000/metrics?service=checkout&from=2026-01-13T11%3A32%3A14Z&to=2026-01-23T11%3A32%3A14Z

## Running test
C:/Users/thano/Projects/FuturaeAssignment/.venv/Scripts/python.exe -m pytest src/tests/test_api.py


## Outputs:

- `output/bronze/raw_events.csv` 
- `output/silver/cleaned_events.csv`
- `output/silver/aggregated_metrics.csv`
- `output/silver/error_cases.csv`


## API Endpoints:

- `GET /health`
- `GET /metrics?service=checkout&from=2026-01-13T11%3A32%3A14Z&to=2026-01-23T11%3A32%3A14Z`

## Part 1 – Data ingestion & transformation

## Cleaning decisions (drop vs keep)

The pipeline treats the dataset as **streaming-like operational events** and aims to keep what can still contribute to aggregate metrics.

**Drop an event if any of these are missing/invalid:**
- `event_id`: without it we can't reference or dedupe the record reliably
- `service`: required for the requested aggregations
- `event_type`: could substantiate the event and give more general information than the status code
- `timestamp`: required for time-bucketing and requested aggregations

**Keep the event even if these are missing/invalid:**
- `latency_ms`: request counts can still be computed; latency averages become “best effort”
- `status_code`: request counts still work; error rate is computed only when status code is usable
- `user_id`: not needed for the required metrics, depending on the scenario can be important

**Timestamp normalization**
- Parsing the timestamp using `try_to_timestamp` and creating a new field name event_ts that has normalized the seconds
- Normalized to **UTC**, output as ISO-8601 with `Z` suffix for the event_ts field. kept the original just for reference

**Handle missing or invalid fields**
- String fields are trimed and lowered.
- `status_code`: checked for values <100
- `latency_ms`: checked for negative values
- Rows with missing fields are kept in the error column in the bronze layer

## Aggregations

The pipeline computes **per service per minute**:

- `request_count`: number of cleaned events in the bucket
- `avg_latency_ms`: average over events with non-null latency
- `error_rate`: `errors / status_count` where error = status not in `[200..299]`




## Part 2 – Storage & modeling design

## Raw events best fit for Google Cloud Storage and alternatively BiqQuery
- Cloud Storage: immutable raw landing (partitioned by ingestion date/hour), cheap storage and replay.
- BigQuery: queryable raw table for analytics/debugging and possible backfills.


## Cleaned events → BigQuery and BigTable
- BigQuery: for analytics/BI and ML feature generation at scale
- Bigtable: for ultra-low-latency reads

## Aggregated metrics → BigQuery and BigTable
- BigQuery: for analytics/BI and ML feature generation at scale
- Bigtable: for ultra-low-latency reads



## Schema evolution

- Its best not to change the schema but if needed to add new columns its better to have them nullable.
- Include `schema_version` in the event when feasible
- Maintain a compatibility layer in the pipeline that can interpret old versions
- Validate critical fields (service/timestamp/event_id) continuously

For metrics:
- Add new metric columns as nullable
- Keep the groups stable (`service, minute`) to avoid breaking downstream consumers


## Part 3 – Simple data API (FastAPI)

## This repo exposes:
- `GET /metrics` with optional filters: `service`, `from`, `to`
- `GET /health`

## Implementation details:
- Metrics are loaded from `outputs/metrics_per_minute.csv` at startup into memory.
- For a real system, the API would query BigQuery (batch) or Bigtable (low-latency).



## Part 4 – Code quality & reliability

## What to monitor in production
Pipeline:
- Input volume (events/sec), lag, and backlog
- Drop rate by reason (missing_service, invalid_timestamp, etc.) this is more related with the business more than the engineering part
- Per-service metric anomalies (spikes in error_rate, missing latency)

## API:
- Request latency, error rate (5xx), and timeouts
- Data freshness (latest minute ingested)

## What could break as data volume grows
- There could be delays in the ELT pipeline if the input file was considerably larger
- This pipeline though was created to run as a batch job and not as a streaming job so its easier to manage

## Next improvements (if more time)
- Change the overwrite to append mode in the file
- Create a log to monitor the pipeline running times and results
- Replace CSV with Parquet and add schema enforcement
- Implement more unit tests

