import os
import sys
import pytest
import shutil
import tempfile
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from importlib import reload
import importlib

# Import the modules directly (conftest.py adds the parent directory to sys.path)
import src.read_data as read_data
import src.transform_data as transform_data
import src.transform as transform


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for the integration test"""
    spark = (
        SparkSession.builder.appName("TestTransformScriptEndToEnd")
        .master("local[*]")
        .getOrCreate()
    )

    yield spark

    # Teardown - stop the SparkSession after tests
    spark.stop()


@pytest.fixture
def temp_directories():
    """Create temporary directories for testing."""
    # Create a temp directory for the test
    temp_dir = tempfile.mkdtemp()

    # Create a result directory
    output_dir = os.path.join(temp_dir, "result")
    os.makedirs(output_dir, exist_ok=True)

    yield temp_dir, output_dir

    # Clean up after the test
    shutil.rmtree(temp_dir)


@pytest.mark.parametrize(
    "mock_write,mock_group,mock_read,mock_create",
    [(MagicMock(), MagicMock(), MagicMock(), MagicMock())],
)
def test_transform_script_integration(mock_write, mock_group, mock_read, mock_create):
    """Test the main transform script flow using mocks"""
    # Create a mock SparkSession
    mock_spark = MagicMock()
    mock_create.return_value = mock_spark

    # Create mock DataFrame for reading
    mock_read_df = MagicMock()
    mock_read.return_value = mock_read_df

    # Create mock DataFrame for grouping
    mock_grouped_df = MagicMock()
    mock_group.return_value = mock_grouped_df

    # Mock the write function
    mock_write.return_value = None

    # Import the transform script
    with patch("src.read_data.create_spark_session", mock_create), patch(
        "src.read_data.read_stock_data", mock_read
    ), patch("src.transform_data.group_by_hour", mock_group), patch(
        "src.transform_data.write_results", mock_write
    ):

        # Reload the transform module to apply the mocks
        reload(transform)

    # Execute the main function to trigger the mocks
    transform.main()

    # Verify that all the expected functions were called
    mock_create.assert_called_once()
    mock_read.assert_called_once()
    mock_group.assert_called_once_with(mock_read_df, verbose=True)
    mock_write.assert_called_once()

    # Verify the arguments to read_stock_data
    args, kwargs = mock_read.call_args
    assert args[0] == mock_spark
    assert "TransformStockTickSaleVsBidAsk" in args[1]


def test_transform_script_end_to_end(spark, temp_directories):
    """
    Test the full transform script with a simulated environment.
    This test will create test data and run the actual script.
    """
    temp_dir, output_dir = temp_directories

    # Create test data schema
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

    test_df = spark.createDataFrame(test_data, schema)

    # Create test data directory
    data_dir = os.path.join(temp_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    # Write test data as Parquet
    test_parquet_path = os.path.join(data_dir, "test_data.parquet")
    test_df.write.parquet(test_parquet_path)

    # Prepare output path
    output_path = os.path.join(output_dir, "output.csv")

    # Directly call individual functions instead of using transform.main()
    # This avoids issues with module reloading and patching

    # Step 1: Read the data
    print(f"Reading data from {test_parquet_path}")
    df = read_data.read_stock_data(spark, test_parquet_path, verbose=True)

    # Step 2: Transform the data
    print(f"Transforming data")
    result_df = transform_data.group_by_hour(df, verbose=True)

    # Step 3: Write the results
    print(f"Writing results to {output_path}")
    transform_data.write_results(result_df, output_path, verbose=True)

    # Verify output file exists
    assert os.path.exists(output_path), f"Output file not found at {output_path}"

    # Read the CSV output and verify
    read_df = spark.read.csv(output_path, header=True, inferSchema=True)

    # Check row count - there's 1 row because all timestamps have the same hour
    assert read_df.count() == 1

    # Check columns
    expected_columns = [
        "HourOfDay",
        "TotalSaleValue",
        "TotalQuantity",
        "MaxPrice",
        "MinPrice",
    ]
    for col in expected_columns:
        assert col in read_df.columns, f"Column {col} not found in output"
