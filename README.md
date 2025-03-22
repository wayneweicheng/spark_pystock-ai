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
└── tests/               # Unit tests
    ├── test_read_data.py
    ├── test_transform_data.py
    └── test_transform_script.py
```

## Requirements

- Python 3.7+ 
- PySpark 3.1+

Install dependencies:

```bash
pip install pyspark pandas pytest
```

## Usage

### Running the Application

1. Place your Parquet files in the `src/data/` directory
2. Run the main script:

```bash
python src/transform.py
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
pytest tests/
```

Run specific test files:

```bash
pytest tests/test_read_data.py
pytest tests/test_transform_data.py
pytest tests/test_transform_script.py
```

Run tests with verbose output:

```bash
pytest -v tests/
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
- `hour`: Hour timestamp (format: 'YYYY-MM-DD HH:00:00')
- `total_sale_value`: Sum of all sale values in that hour
- `total_quantity`: Sum of all quantities in that hour
- `max_price`: Maximum price in that hour
- `min_price`: Minimum price in that hour
