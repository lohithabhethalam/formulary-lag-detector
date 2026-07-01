"""
spark_lag.py

PySpark version of the formulary lag calculation.
Loads the raw batch from data/raw/lag_batch.json,
adds a lag_category column, and writes to Delta Lake
with MERGE for idempotent runs.
"""

import json
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DELTA_PATH = "data/delta/formulary_lag"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("formulary_lag")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def load_lag_batch(spark: SparkSession, path: str = "data/raw/lag_batch.json"):
    """Loads raw lag batch JSON into a Spark DataFrame."""
    with open(path) as f:
        records = json.load(f)

    return spark.createDataFrame(records)


def add_lag_category(df):
    """
    Adds a lag_category column based on weeks to market.
    Thresholds based on PBM formulary review cycles:
    - Fast: under 26 weeks (6 months) — quick adoption
    - Medium: 26-78 weeks (6-18 months) — typical lag
    - Slow: over 78 weeks (18 months+) — delayed adoption
    """
    return df.withColumn(
        "lag_category",
        F.when(F.col("lag_weeks") < 26, "fast")
         .when(F.col("lag_weeks") < 78, "medium")
         .otherwise("slow")
    )


def write_to_delta(df, output_path: str = DELTA_PATH):
    """
    Writes lag results to Delta Lake using MERGE on
    application_number — idempotent, safe to re-run.
    """
    if DeltaTable.isDeltaTable(df.sparkSession, output_path):
        delta_table = DeltaTable.forPath(df.sparkSession, output_path)
        delta_table.alias("existing").merge(
            df.alias("updates"),
            "existing.application_number = updates.application_number"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").save(output_path)


def main():
    spark = build_spark_session()

    df = load_lag_batch(spark)
    print(f"Loaded {df.count()} records")

    df = add_lag_category(df)

    print("\nLag distribution by category:")
    df.groupBy("lag_category").count().orderBy("lag_category").show()

    print("Sample records:")
    df.select("drug_name", "lag_days", "lag_weeks", "lag_category").show(5, truncate=False)

    write_to_delta(df)
    print(f"Wrote {df.count()} records to Delta Lake")

    spark.stop()


if __name__ == "__main__":
    main()