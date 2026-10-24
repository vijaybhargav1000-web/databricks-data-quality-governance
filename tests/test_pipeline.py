

from pyspark.sql import SparkSession

from src.pipeline import clean_customer_data, validate_customer_data


def test_clean_customer_data():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("DataQualityTests")
        .getOrCreate()
    )

    try:
        data = [
            (1, "Ravi", "Hyderabad", "2026-10-20"),
            (2, "Priya", "Bangalore", "2026-10-21"),
            (3, None, "Chennai", "2026-10-22"),
            (4, " ", "Mumbai", "2026-10-22"),
            (None, "Kiran", "Delhi", "2026-10-22"),
            (1, "Ravi Updated", "Hyderabad", "2026-10-23"),
        ]

        columns = ["customer_id", "name", "city", "updated_at"]
        df = spark.createDataFrame(data, columns)

        result = clean_customer_data(df)

        # Only two valid, unique customers should remain.
        assert result.count() == 2

        # Customer IDs must not be null.
        assert result.filter(
            result.customer_id.isNull()
        ).count() == 0

        # Customer names must not be null or blank.
        assert result.filter(
            result.name.isNull()
        ).count() == 0

        assert result.filter(
            result.name == ""
        ).count() == 0

        # Customer IDs must be unique.
        assert result.select("customer_id").distinct().count() == 2

        # The latest record should be retained for customer ID 1.
        latest_name = (
            result.filter(result.customer_id == 1)
            .select("name")
            .first()["name"]
        )

        assert latest_name == "Ravi Updated"

    finally:
        spark.stop()


def test_validate_customer_data_quarantines_invalid_records():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("QuarantineTests")
        .getOrCreate()
    )

    try:
        data = [
            (1, "Ravi", "Hyderabad", "2026-10-20"),
            (2, "Priya", "Bangalore", "2026-10-21"),
            (3, None, "Chennai", "2026-10-22"),
            (4, " ", "Mumbai", "2026-10-22"),
            (None, "Kiran", "Delhi", "2026-10-22"),
        ]

        columns = ["customer_id", "name", "city", "updated_at"]
        df = spark.createDataFrame(data, columns)

        valid_df, quarantine_df = validate_customer_data(df)

        # Three records should be quarantined.
        assert quarantine_df.count() == 3

        # Two records have invalid names.
        assert (
            quarantine_df
            .filter(
                quarantine_df.rejection_reason
                == "MISSING_CUSTOMER_NAME"
            )
            .count()
            == 2
        )

        # One record has a missing customer ID.
        assert (
            quarantine_df
            .filter(
                quarantine_df.rejection_reason
                == "MISSING_CUSTOMER_ID"
            )
            .count()
            == 1
        )

        # Two records pass validation before deduplication.
        assert valid_df.count() == 2

    finally:
        spark.stop()
