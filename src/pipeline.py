
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


def create_spark_session():
    return (
        SparkSession.builder
        .appName("DataQualityGovernancePipeline")
        .master("local[*]")
        .getOrCreate()
    )


def clean_customer_data(df):
    # Remove records with missing IDs or blank names
    cleaned_df = df.filter(
        col("customer_id").isNotNull()
        & col("name").isNotNull()
        & (trim(col("name")) != "")
    )

    # Keep one record per customer ID
    return cleaned_df.dropDuplicates(["customer_id"])


def run_pipeline():
    spark = create_spark_session()

    data = [
        (1, " Ravi ", "Hyderabad"),
        (2, "Priya", "Bangalore"),
        (3, None, "Chennai"),
        (4, " ", "Mumbai"),
        (1, "Ravi Duplicate", "Hyderabad"),
        (None, "Kiran", "Delhi"),
    ]

    columns = ["customer_id", "name", "city"]
    bronze_df = spark.createDataFrame(data, columns)

    print("\n=== BRONZE: Raw Customer Data ===")
    bronze_df.show(truncate=False)

    silver_df = clean_customer_data(bronze_df)

    print("\n=== SILVER: Clean Customer Data ===")
    silver_df.show(truncate=False)

    print("\n=== GOLD: Customer Count by City ===")
    gold_df = silver_df.groupBy("city").count()
    gold_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    run_pipeline()
