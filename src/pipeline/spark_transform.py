from pydoc import resolve
import pyspark
from typing import Dict, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# Schema for raw events
raw_schema = T.StructType(
    [
        T.StructField("event_id", T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("service", T.StringType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("latency_ms", T.IntegerType(), True),
        T.StructField("status_code", T.IntegerType(), True),
        T.StructField("user_id", T.StringType(), True),
        T.StructField("error_records", T.StringType(), True),
    ]
)

# Create a local spark session
def get_spark() -> SparkSession:

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("futurae-assignment")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark

# Read input file with the specified schema
# Add a column to capture ingestion timestamp
# Optional arguments to specify the mode and corrupt record column name
def read_raw_file(spark: SparkSession, path: str, mode: str = "PERMISSIVE", corrupt_record_col: str = "corrupted_record") -> DataFrame:

    df = (
        spark.read
        .option("multiLine", "false")
        .option("mode", mode)
        .option("schema", raw_schema)
        .option("columnNameOfCorruptRecord", "error_records")
        .json(path)
        .withColumn("extracted_ts", F.current_timestamp())
    )
    
    row_count = df.count()
    print(f"Read: {row_count} raw events -> {path}")
    
    return df

# Transform and clean raw events
# DROP if event_id/service/event_type missing OR timestamp cannot be parsed
# KEEP if latency_ms/status_code/user_id missing and replace with NULL
def clean_raw_file(df_raw: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
  
    # Copy input dataframe
    df = df_raw

    # Normalize strings
    df = df.withColumn("event_id", F.lower(F.trim(F.col("event_id"))))
    df = df.withColumn("service", F.lower(F.trim(F.col("service"))))
    df = df.withColumn("event_type", F.lower(F.trim(F.col("event_type"))))

    # Parse timestamp - returns NULL if parsing fails
    df = df.withColumn("event_ts", _parse_timestamp(F.col("timestamp")))
    #df = df.withColumn("event_ts", dt.normalize())

    df = df.withColumn("user_id", F.trim(F.col("user_id")))
    df = df.withColumn("user_id", F.when((F.col("user_id") == "") | F.col("user_id").isNull(), F.lit(None)).otherwise(F.col("user_id")))

    # Capture negative or invalid latency/status_code as NULL
    df = df.withColumn("latency_ms", F.col("latency_ms").cast("int"))
    df = df.withColumn("latency_ms", F.when(F.col("latency_ms") < 0, F.lit(None)).otherwise(F.col("latency_ms")))

    # Capture invalid status_code as NULL
    df = df.withColumn("status_code", F.col("status_code").cast("int"))
    df = df.withColumn("status_code", F.when(F.col("status_code") < 100, F.lit(None)).otherwise(F.col("status_code")))

    missing_event_id = F.col("event_id").isNull() | (F.col("event_id") == "")
    missing_service = F.col("service").isNull() | (F.col("service") == "")
    missing_event_type = F.col("event_type").isNull() | (F.col("event_type") == "")
    invalid_timestamp = F.col("event_ts").isNull() | (F.col("event_type") == "")

    # Count erroneous cases: 
    error_cases: Dict[str, int] = {
        "missing_event_id": 0,
        "missing_service": 0,
        "missing_event_type": 0,
        "invalid_timestamp": 0,
    }

    error_cases["missing_event_id"] = df.filter(missing_event_id).count()
    error_cases["missing_service"] = df.filter(missing_service & ~missing_event_id).count()
    error_cases["missing_event_type"] = df.filter(missing_event_type & ~missing_event_id & ~missing_service).count()
    error_cases["invalid_timestamp"] = df.filter(invalid_timestamp & ~missing_event_id & ~missing_service & ~missing_event_type).count()

    keep_mask = ~(missing_event_id | missing_service | missing_event_type | invalid_timestamp)

    # Filter erroneous records and select relevant columns
    df_clean = (
        df.filter(keep_mask)
        .select(
            "event_id",
            "timestamp",
            "service",
            "event_type",
            "latency_ms",
            "status_code",
            "user_id",
            "event_ts",
        )
        .withColumn("cleaned_at", F.current_timestamp())
    )

    return df_clean, error_cases



# Parse the timestamp column into a proper timestamp type or NULL if parsing fails
# Truncate to minute level (removes seconds and milliseconds)
def _parse_timestamp(col: F.Column) -> F.Column:
    return F.date_trunc("minute", F.try_to_timestamp(col))



# Aggregate metrics per service per minute
def aggregate_metrics(df_clean: DataFrame) -> DataFrame:

    is_error = F.col("status_code").isNotNull() & ~F.col("status_code").between(200, 299)

    df_grouped = (
        df_clean
        .groupBy("event_ts", "service")
        .agg(
            F.count("*").alias("request_count"),
            F.avg(F.col("latency_ms")).alias("avg_latency_ms"),
            F.count(F.col("status_code")).alias("status_count"),
            F.try_divide(F.sum(F.when(is_error, 1).otherwise(0)).cast("double"), F.count(F.col("status_code"))).alias("error_rate"),
        )
        .orderBy("event_ts", "service")
    )

    return df_grouped
