import os
import sys
import argparse

# Add the parent directory to the path if running as a script
if __name__ == "__main__":
    # Add current directory to path (helps in cloud environments)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # If we're in the cloud environment and 'src' isn't in the current path
    if not os.path.exists(os.path.join(current_dir, 'src')) and not current_dir.endswith('src'):
        # Create a symbolic src directory that points to the current directory
        # This allows imports like "from src.xyz import abc" to work
        sys.path.insert(0, os.path.abspath(os.path.dirname(current_dir)))
        
        # This is a workaround for cloud environments where we don't have the src directory
        sys.modules['src'] = type('SrcModule', (), {})
        sys.modules['src.read_data'] = __import__('read_data')
        sys.modules['src.transform_data'] = __import__('transform_data')
    else:
        # Standard local environment setup
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.read_data import create_spark_session, read_stock_data
from src.transform_data import group_by_hour, write_results

# Define constants for default paths (makes it easier to patch for tests)
INPUT_PATH = (
    "src/data/TransformStockTickSaleVsBidAsk_AX_20240814_20241123222717.parquet"
)
OUTPUT_PATH = "src/result/output.csv"

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Process stock data using PySpark.')
    parser.add_argument('--input-path', 
                        default=INPUT_PATH,
                        help='Path to the input Parquet file')
    parser.add_argument('--output-path', 
                        default=OUTPUT_PATH,
                        help='Path where to save the output CSV file')
    parser.add_argument('--verbose', 
                        action='store_true', 
                        default=True,
                        help='Enable verbose output')
    parser.add_argument('--cloud', 
                        action='store_true', 
                        help='Run in cloud mode (GCP/AWS)')
    return parser.parse_args()

def main():
    try:
        # Parse command line arguments
        args = parse_args()
        
        # Get paths from args or use defaults
        input_path = args.input_path
        output_path = args.output_path
        verbose = args.verbose
        is_cloud = args.cloud
        
        # Debug: Print actual paths being used
        if verbose:
            print(f"Input Path: {input_path}")
            print(f"Output Path: {output_path}")
            print(f"Cloud Mode: {is_cloud}")

        # Create a Spark session with cloud flag if needed
        spark = create_spark_session(is_cloud=is_cloud)

        print("Reading parquet file...")
        # Read the parquet file
        df = read_stock_data(spark, input_path, verbose=verbose)

        print("Performing aggregation by hour...")
        # Group by hour and calculate aggregates
        result_df = group_by_hour(df, verbose=verbose)

        # For local paths, ensure the output directory exists
        if not is_cloud and not (output_path.startswith("gs://") or output_path.startswith("s3://")):
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Write results to CSV
        write_results(result_df, output_path, verbose=verbose)
        
        print(f"Successfully wrote results to: {output_path}")

        # Stop the Spark session
        spark.stop()
        
        return 0

    except Exception as e:
        print(f"Error: {e}")
        print("\nUsage:")
        print("Local: poetry run python src/transform.py")
        print("Cloud: spark-submit src/transform.py --input-path gs://path/to/input.parquet --output-path gs://path/to/output --cloud")
        return 1


if __name__ == "__main__":
    sys.exit(main())
