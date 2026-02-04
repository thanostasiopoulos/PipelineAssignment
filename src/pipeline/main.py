import argparse
from pathlib import Path
import pandas as pd


from pipeline.spark_transform import (
    get_spark,
    read_raw_file,
    clean_raw_file,
    aggregate_metrics,
)


def main() -> int:
    input_path = "input/"
    output_bronze_path = "output/bronze/"
    output_silver_path = "output/silver/"

    # Optional arguments to specify input and output file names
    parser = argparse.ArgumentParser(
        description="Data ingestion & transformation with pyspark."
    )
    parser.add_argument(
        "--inputfile", default="events.jsonl", help="Input JSON file name"
    )
    parser.add_argument(
        "--outputfile", default="raw_events.csv", help="Output CSV file name"
    )
    args = parser.parse_args()

    # Input file
    input = Path(str(input_path) + str(args.inputfile))

    # End if file is missing
    if not input.exists():
        raise SystemExit(f"Input file not found at : {input}")

    spark = get_spark()

    # CSV paths - Used for easier verification of results
    raw_csv_path = output_bronze_path + "raw_events.csv"
    cleaned_csv_path = output_silver_path + "cleaned_events.csv"
    error_cases_csv_path = output_silver_path + "error_cases.csv"
    metrics_csv_path = output_silver_path + "aggregated_metrics.csv"

    # Parquet paths
    raw_parquet_path = output_bronze_path + "raw_events.parquet"
    cleaned_parquet_path = output_silver_path + "cleaned_events.parquet"
    metrics_parquet_path = output_silver_path + "aggregated_metrics.parquet"

    try:
        # Extract file
        df_raw = read_raw_file(spark, str(input_path) + str(args.inputfile))

        # Load raw as parquet file in bronze layer
        # df_raw.write.mode("overwrite").parquet(str(raw_parquet_path))

        # Load raw as csv file in bronze layer
        df_raw.toPandas().to_csv(raw_csv_path, index=False)

        # Transform data
        df_cleaned, error_cases = clean_raw_file(df_raw)

        # Load cleaned dataframe in silver layer
        df_cleaned.toPandas().to_csv(cleaned_csv_path, index=False)

        # Load cleaned as parquet file in silver layer
        # df_cleaned.write.mode("overwrite").parquet(cleaned_parquet_path)

        # Load error cases as csv file in silver layer
        pd.DataFrame(list(error_cases.items()), columns=["error_type", "count"]).to_csv(
            error_cases_csv_path, index=False
        )

        # Aggregate metrics per minute
        df_metrics = aggregate_metrics(df_cleaned)

        # Load metrics dataframe in silver layer
        df_metrics.toPandas().to_csv(metrics_csv_path, index=False)

        # Load metrics as parquet file in silver layer
        # df_metrics.write.mode("overwrite").parquet(metrics_parquet_path)

        # Print summary
        raw_count = df_raw.count()
        clean_count = df_cleaned.count()
        metric_count = df_metrics.count()

        print(f"Wrote: {raw_count} raw events -> {raw_csv_path}")
        print(f"Wrote: {clean_count} cleaned events -> {cleaned_csv_path}")
        print(f"Dropped: {error_cases}")
        print(f"Wrote {metric_count} metric rows -> {metrics_csv_path}")
        return 0
    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
