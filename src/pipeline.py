


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, row_number, to_date
from pyspark.sql.window import Window


def create_spark_session():
    """Create or reuse the Spark session provided by the environment."""
    return (
        SparkSession.builder
        .appName("DataQualityGovernancePipeline")
        .getOrCreate()
    )


def validate_customer_data(df):
    """Separate valid customer records from invalid records."""

    checked_df = (
        df.withColumn("customer_name", trim(col("name")))
        .withColumn(
            "rejection_reason",
            when(
                col("customer_id").isNull(),
                "MISSING_CUSTOMER_ID",
            ).when(
                col("customer_name").isNull()
                | (col("customer_name") == ""),
                "MISSING_CUSTOMER_NAME",
            ),
        )
    )

    valid_df = checked_df.filter(col("rejection_reason").isNull())

    quarantine_df = checked_df.filter(
        col("rejection_reason").isNotNull()
    )

    return valid_df, quarantine_df


def clean_customer_data(df):
    """Clean records and keep the latest version of each customer."""

    valid_df, _ = validate_customer_data(df)

    cleaned_df = (
        valid_df
        .drop("name", "rejection_reason")
        .withColumnRenamed("customer_name", "name")
        .withColumn("updated_at", to_date(col("updated_at")))
    )

    window_spec = (
        Window.partitionBy("customer_id")
        .orderBy(col("updated_at").desc_nulls_last())
    )

    return (
        cleaned_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


def run_pipeline():
    spark = create_spark_session()

    data = [
        (1, " Ravi ", "Hyderabad", "2026-10-20"),
        (2, "Priya", "Bangalore", "2026-10-21"),
        (3, None, "Chennai", "2026-10-22"),
        (4, " ", "Mumbai", "2026-10-22"),
        (1, "Ravi Updated", "Hyderabad", "2026-10-23"),
        (None, "Kiran", "Delhi", "2026-10-22"),
    ]

    columns = ["customer_id", "name", "city", "updated_at"]
    bronze_df = spark.createDataFrame(data, columns)

    try:
        print("\n=== BRONZE: Raw Customer Data ===")
        bronze_df.show(truncate=False)

        _, quarantine_df = validate_customer_data(bronze_df)

        print("\n=== QUARANTINE: Invalid Records ===")
        quarantine_df.show(truncate=False)

        silver_df = clean_customer_data(bronze_df)

        print("\n=== SILVER: Clean Customer Data ===")
        silver_df.show(truncate=False)

        print("\n=== GOLD: Customer Count by City ===")
        gold_df = silver_df.groupBy("city").count()
        gold_df.show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
