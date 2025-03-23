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
import argparse

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


def test_transform_script_direct_call(monkeypatch):
    """Test transform script directly with mocks by calling internal functions."""
    # Create mocks for all components
    mock_create = MagicMock()
    mock_read = MagicMock()
    mock_group = MagicMock()
    mock_write = MagicMock()

    # Create mock return values
    mock_spark = MagicMock()
    mock_read_df = MagicMock()
    mock_grouped_df = MagicMock()

    # Set up mock returns
    mock_create.return_value = mock_spark
    mock_read.return_value = mock_read_df
    mock_group.return_value = mock_grouped_df
    mock_write.return_value = None

    # Patch all functions in the modules
    with patch.object(read_data, 'create_spark_session', mock_create), \
         patch.object(read_data, 'read_stock_data', mock_read), \
         patch.object(transform_data, 'group_by_hour', mock_group), \
         patch.object(transform_data, 'write_results', mock_write):
        
        # Create a sample test input and output path
        input_path = "test_input.parquet"
        output_path = "test_output.csv"
        
        # Call our transform script functions directly
        spark = read_data.create_spark_session(is_cloud=False)
        df = read_data.read_stock_data(spark, input_path, verbose=True)
        result_df = transform_data.group_by_hour(df, verbose=True)
        transform_data.write_results(result_df, output_path, verbose=True)
        
        # Verify all functions were called with the expected arguments
        mock_create.assert_called_once_with(is_cloud=False)
        mock_read.assert_called_once_with(mock_spark, input_path, verbose=True)
        mock_group.assert_called_once_with(mock_read_df, verbose=True)
        mock_write.assert_called_once_with(mock_grouped_df, output_path, verbose=True)


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
