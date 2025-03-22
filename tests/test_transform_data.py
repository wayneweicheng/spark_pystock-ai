import os
import sys
import pytest
import shutil
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Import directly (conftest.py adds the parent directory to sys.path)
from src.transform_data import group_by_hour, write_results


# Fixtures for SparkSession and test data
@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for the test module"""
    spark = (
        SparkSession.builder.appName("TestTransformData")
        .master("local[*]")
        .getOrCreate()
    )

    yield spark

    # Teardown - stop the SparkSession after tests
    spark.stop()


@pytest.fixture(scope="module")
def test_df(spark):
    """Create test DataFrame with sample data"""
    schema = StructType(
        [
            StructField("SaleDateTime", StringType(), True),
            StructField("Price", DoubleType(), True),
            StructField("Quantity", DoubleType(), True),
            StructField("SaleValue", DoubleType(), True),
        ]
    )

    # Create sample data with timestamps across different hours
    test_data = [
        ("2024-08-14 09:15:00", 25.50, 100.0, 2550.0),
        ("2024-08-14 09:45:00", 26.00, 200.0, 5200.0),
        ("2024-08-14 10:20:00", 26.50, 150.0, 3975.0),
        ("2024-08-14 10:45:00", 27.00, 300.0, 8100.0),
        ("2024-08-15 09:30:00", 27.50, 250.0, 6875.0),
    ]

    return spark.createDataFrame(test_data, schema)


@pytest.fixture(scope="module")
def temp_dir():
    """Create a temporary directory for test outputs"""
    dir_path = tempfile.mkdtemp()

    yield dir_path

    # Teardown - clean up the temp directory
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)


def test_group_by_hour(test_df):
    """Test that group_by_hour correctly aggregates data by hour"""
    result_df = group_by_hour(test_df)

    # Convert to pandas for easier assertion testing
    result_pd = result_df.toPandas()

    # The data only has 2 distinct hours (9 and 10), regardless of date
    assert len(result_pd) == 2

    # Check for the hour 09
    hour_09_data = result_pd[result_pd["HourOfDay"] == 9]
    assert len(hour_09_data) == 1
    assert hour_09_data.iloc[0]["TotalSaleValue"] == 14625.0  # 2550 + 5200 + 6875
    assert hour_09_data.iloc[0]["TotalQuantity"] == 550.0  # 100 + 200 + 250
    assert hour_09_data.iloc[0]["MaxPrice"] == 27.5  # max of 25.5, 26.0, 27.5
    assert hour_09_data.iloc[0]["MinPrice"] == 25.5  # min of 25.5, 26.0, 27.5

    # Check for the hour 10
    hour_10_data = result_pd[result_pd["HourOfDay"] == 10]
    assert len(hour_10_data) == 1
    assert hour_10_data.iloc[0]["TotalSaleValue"] == 12075.0  # 3975 + 8100
    assert hour_10_data.iloc[0]["TotalQuantity"] == 450.0  # 150 + 300
    assert hour_10_data.iloc[0]["MaxPrice"] == 27.0  # max of 26.5, 27.0
    assert hour_10_data.iloc[0]["MinPrice"] == 26.5  # min of 26.5, 27.0


def test_group_by_hour_with_verbose(test_df):
    """Test the verbose parameter of group_by_hour"""
    # This test just ensures the function doesn't error when verbose=True
    # Actual output verification would require capturing stdout
    result_df = group_by_hour(test_df, verbose=True)
    # The aggregation should result in 2 rows because there are only 2 distinct hours
    assert len(result_df.toPandas()) == 2


def test_write_results(spark, test_df, temp_dir):
    """Test that write_results correctly writes to CSV"""
    # First, aggregate the test data
    aggregated_df = group_by_hour(test_df)

    # Define output path
    output_path = os.path.join(temp_dir, "test_output.csv")

    # Write results to CSV
    write_results(aggregated_df, output_path)

    # Verify that the file exists
    assert os.path.exists(output_path)

    # Read the CSV file back to verify content
    read_df = spark.read.csv(output_path, header=True, inferSchema=True)

    # It should have the same number of rows as the aggregated DataFrame
    assert read_df.count() == aggregated_df.count()

    # Verify that the columns were written correctly - using the actual column names
    expected_columns = [
        "HourOfDay",
        "TotalSaleValue",
        "TotalQuantity",
        "MaxPrice",
        "MinPrice",
    ]
    for col_name in expected_columns:
        assert col_name in read_df.columns, f"Column {col_name} not found in output CSV"


def test_write_results_creates_directory(test_df, temp_dir):
    """Test that write_results creates the output directory if it doesn't exist"""
    # Define a nested path that doesn't exist
    nested_dir = os.path.join(temp_dir, "nested", "dir")
    output_path = os.path.join(nested_dir, "test_output.csv")

    # Verify that the directory doesn't exist yet
    assert not os.path.exists(nested_dir)

    # Aggregate the test data
    aggregated_df = group_by_hour(test_df)

    # Write results to the nested path
    write_results(aggregated_df, output_path, verbose=True)

    # Verify that the file and directory were created
    assert os.path.exists(nested_dir)
    assert os.path.exists(output_path)
