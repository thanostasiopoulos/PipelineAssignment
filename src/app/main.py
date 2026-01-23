from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

metrics_path = Path("output/silver/aggregated_metrics.csv")

# Create FastAPI app
app = FastAPI(title="Metrics API", version="1.0.0")

# Create a local spark session
spark = (
    SparkSession.builder 
        .appName("metrics-api") 
        .master("local[*]") 
        .config("spark.sql.session.timeZone", "UTC") 
        .getOrCreate()
)


# Load metrics from CSV using Spark
def get_metrics_filtered(service: Optional[str] = None, from_ts: Optional[str] = None, to_ts: Optional[str] = None) -> List[dict]:
    
    if not metrics_path.exists():
        return []
    
    df = spark.read.option("header", "true").csv(str(metrics_path))
    if service:
        df = df.filter(F.lower(F.col("service")) == service.lower())
    if from_ts:
        df = df.filter(F.col("event_ts").cast("timestamp") >= F.lit(from_ts).cast("timestamp"))
    if to_ts:
        df = df.filter(F.col("event_ts").cast("timestamp") <= F.lit(to_ts).cast("timestamp"))
    
    
    # Convert filtered Spark DataFrame to list of dicts
    # Convert to pandas and then to list of dicts for easier handling
    return [
        {
            "event_ts": row["event_ts"],
            "service": row["service"],
            "request_count": _safe_int(row["request_count"]),
            "avg_latency_ms": _safe_float(row["avg_latency_ms"]),
            "status_count": _safe_int(row["status_count"]),
            "error_rate": _safe_percentage(row["error_rate"]),
        }
        for row in df.toPandas().to_dict('records')
    ]

# Helper function to parse datetime strings
def _parse_dt(input_date: str) -> datetime:
    input_date = input_date.strip()
    if not input_date:
        raise ValueError("empty datetime")
    if input_date.endswith("Z"):
        input_date = input_date.replace("Z", "+00:00")
    dt = datetime.fromisoformat(input_date)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)



# Health check endpoint
@app.get("/health")
def health() -> dict:
    # Dynamically count rows in the metrics file
    metrics_rows = len(get_metrics_filtered())
    return {"status": "ok", "metrics_rows": metrics_rows}

# Metrics endpoint with optional filters
@app.get("/metrics")
def get_metrics(
    service: Optional[str] = Query(None, description="Filter by service case-insensitive"),
    from_ts: Optional[str] = Query(None, alias="from", description="Start datetime (ISO-8601)"),
    to_ts: Optional[str] = Query(None, alias="to", description="End datetime (ISO-8601)"),
) -> List[dict]:

    # Optionally validate from_ts and to_ts format here if needed
    from_dt = None
    to_dt = None
    try:
        if from_ts:
            from_dt = _parse_dt(from_ts)
        if to_ts:
            to_dt = _parse_dt(to_ts)
        if from_dt and to_dt and to_dt < from_dt:
            raise HTTPException(status_code=400, detail="'to' must be >= 'from'")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid datetime format")
    return get_metrics_filtered(service, from_ts, to_ts)

# Helper functions for safe int conversions
def _safe_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0
# Helper functions for safe float conversions
def _safe_float(val):
    try:
        fval = float(val)
        # check for NaN
        if fval != fval or fval is None: 
            return 0.00
        return round(fval, 2)
    except (TypeError, ValueError):
        return 0.00

# Helper functions for safe percentage conversions
def _safe_percentage(val):
    try:

        fval = float(val)
        # check for NaN
        if fval != fval or fval is None:  # check for NaN
            return "0.0%"
        return f"{round(fval * 100, 2)}%"
    except (TypeError, ValueError):
        return "0.0%"

