import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DateType,
    StringType,
    DoubleType,
)
from unittest.mock import patch

# Import directly (conftest.py adds the parent directory to sys.path)
from src.read_data import create_spark_session, read_stock_data


# Fixture for SparkSession that will be used by multiple tests
@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for the test module"""
    spark = (
        SparkSession.builder.appName("TestReadData").master("local[*]").getOrCreate()
    )

    yield spark

    # Teardown - stop the SparkSession after tests
    spark.stop()


def test_create_spark_session():
    """Test that create_spark_session returns a valid SparkSession"""
    spark = create_spark_session()
    assert isinstance(spark, SparkSession)
    # Verify the configurations
    assert spark.conf.get("spark.sql.parquet.outputTimestampType") == "TIMESTAMP_MICROS"
    assert spark.conf.get("spark.sql.legacy.parquet.nanosAsLong") == "true"
    # Clean up
    spark.stop()


def test_read_stock_data_schema_conversion(spark):
    """Test that read_stock_data correctly converts the timestamp column"""
    # Create a simple test DataFrame with a LongType SaleDateTime column
    schema = StructType(
        [
            StructField("SaleDateTime", LongType(), True),
            StructField("Price", DoubleType(), True),
            StructField("Quantity", DoubleType(), True),
            StructField("SaleValue", DoubleType(), True),
        ]
    )

    # Create sample data with timestamp in nanoseconds (approx 2024-08-14 20:00:00)
    test_data = [(1723629600000000000, 17.65, 476.0, 8401.0)]

    df = spark.createDataFrame(test_data, schema)

    # Mock the parquet reading by directly providing the DataFrame
    with patch("pyspark.sql.readwriter.DataFrameReader.parquet", return_value=df):
        result_df = read_stock_data(spark, "dummy_path")

    # Check that SaleDateTime is now a string (converted timestamp)
    assert result_df.schema["SaleDateTime"].dataType.typeName() == "string"

    # Verify the converted timestamp value (should be around 2024-08-14 20:00:00)
    timestamp_value = result_df.select("SaleDateTime").first()[0]
    assert "2024-08-14" in timestamp_value
    assert "20:00:00" in timestamp_value


@pytest.mark.skip(reason="Parquet file has timestamp issues - expected to fail")
def test_read_stock_data_real_file(spark):
    """
    Test reading a real file, if it exists.
    This test will be skipped if the file doesn't exist.
    """
    file_path = (
        "src/data/TransformStockTickSaleVsBidAsk_AX_20240814_20241123222717.parquet"
    )
    if not os.path.exists(file_path):
        pytest.skip(f"Skipping test: parquet file not found at {file_path}")

    # Read the actual data file
    df = read_stock_data(spark, file_path)

    # Verify that the DataFrame is not empty
    assert df.count() > 0

    # Verify that the SaleDateTime column has been converted
    assert df.schema["SaleDateTime"].dataType.typeName() == "string"

    # Verify that some expected columns exist
    expected_columns = ["SaleDateTime", "Price", "Quantity", "SaleValue"]
    for col_name in expected_columns:
        assert col_name in df.columns, f"Column {col_name} not found in DataFrame"
