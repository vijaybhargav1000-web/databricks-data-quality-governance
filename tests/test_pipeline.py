
from pyspark.sql import SparkSession

from src.pipeline import clean_customer_data


def test_clean_customer_data():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("DataQualityTests")
        .getOrCreate()
    )

    try:
        data = [
            (1, "Ravi", "Hyderabad"),
            (2, "Priya", "Bangalore"),
            (3, None, "Chennai"),
            (4, " ", "Mumbai"),
            (None, "Kiran", "Delhi"),
            (1, "Ravi Duplicate", "Hyderabad"),
        ]

        columns = ["customer_id", "name", "city"]
        df = spark.createDataFrame(data, columns)

        result = clean_customer_data(df)

        assert result.count() == 2
        assert result.filter(
            result.customer_id.isNull()
        ).count() == 0
        assert result.filter(
            result.name.isNull()
        ).count() == 0
        assert result.filter(
            result.name == ""
        ).count() == 0

        assert result.select("customer_id").distinct().count() == 2

    finally:
        spark.stop()
