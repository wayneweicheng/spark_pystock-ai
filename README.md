# PyStock-AI

A PySpark application for processing stock data from Parquet files, transforming it by grouping by hour, and calculating various aggregations.

## Project Structure

```
pystock-ai/
├── src/                 # Source code
│   ├── data/            # Input data
│   ├── result/          # Output results
│   ├── read_data.py     # Module for reading Parquet data
│   ├── transform_data.py # Module for transforming data
│   └── transform.py     # Main application
├── tests/               # Unit tests
│   ├── test_read_data.py
│   ├── test_transform_data.py
│   └── test_transform_script.py
└── pyproject.toml       # Poetry configuration
```

## Requirements

- Python 3.11+
- Poetry for dependency management

## Setup

### Installing Poetry

If you don't have Poetry installed, you can install it by following the instructions on the [official Poetry website](https://python-poetry.org/docs/#installation).

### Installing Dependencies

Clone the repository and install dependencies using Poetry:

```bash
# Clone the repository
git clone https://github.com/yourusername/pystock-ai.git
cd pystock-ai

# Install dependencies
poetry install
```

This will create a virtual environment and install all required dependencies defined in `pyproject.toml`.

## Usage

### Running the Application

1. Place your Parquet files in the `src/data/` directory
2. Run the main script:

```bash
poetry run python src/transform.py
```

The application will:
1. Read the Parquet file from `src/data/`
2. Group the data by hour
3. Calculate total sale value, total quantity, max price, and min price per hour
4. Write the results to `src/result/output.csv`

### Customizing Input/Output Paths

You can modify the input and output paths by editing the constants at the top of `src/transform.py`:

```python
INPUT_PATH = "path/to/your/input.parquet"
OUTPUT_PATH = "path/to/your/output.csv"
```

## Testing

Run all tests:

```bash
poetry run pytest
```

Run specific test files:

```bash
poetry run pytest tests/test_read_data.py
poetry run pytest tests/test_transform_data.py
poetry run pytest tests/test_transform_script.py
```

Run tests with verbose output:

```bash
poetry run pytest -v
```

## Code Structure

- **read_data.py**: Contains functions for creating a Spark session and reading Parquet files
- **transform_data.py**: Contains functions for transforming the data (grouping by hour) and writing results
- **transform.py**: Main script that ties everything together

## Expected Input Format

The application expects the input Parquet file to have the following columns:
- `SaleDateTime`: Timestamp of the sale
- `Price`: Price of the stock
- `Quantity`: Quantity of shares sold
- `SaleValue`: Total value of the sale

## Output Format

The application produces a CSV file with the following columns:
- `HourOfDay`: Hour of the day (integer: 0-23)
- `TotalSaleValue`: Sum of all sale values in that hour
- `TotalQuantity`: Sum of all quantities in that hour
- `MaxPrice`: Maximum price in that hour
- `MinPrice`: Minimum price in that hour

## Setup cloud

### To sync local data to gcs
gsutil rsync -x ".DS_Store" src/data gs://data-demo-bucket-wayne/transform-stock-tick-sale-vs-bidask/

### Check to ensure files are uploaded
gsutil ls gs://data-demo-bucket-wayne/transform-stock-tick-sale-vs-bidask/