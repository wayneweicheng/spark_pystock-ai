# PyStock-AI

A demonstration PySpark application showcasing best practices for unit testing Spark applications and deploying them to Google Cloud Dataproc serverless with Terraform.

## Project Overview

This project demonstrates:
1. How to structure a testable PySpark application
2. Writing effective unit tests for PySpark code
3. Setting up local development with Poetry
4. Deploying to Google Dataproc serverless using Terraform IaC

The application itself processes stock data from Parquet files, transforming it by grouping by hour, and calculating various aggregations.

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
│   ├── conftest.py      # Pytest fixtures
│   ├── test_read_data.py
│   ├── test_transform_data.py
│   └── test_transform_script.py
├── terraform/           # Infrastructure as Code
│   └── dataproc/        # Dataproc serverless configuration
└── pyproject.toml       # Poetry configuration
```

## Requirements

- Python 3.11+
- Poetry for dependency management
- Google Cloud SDK for cloud deployment
- Terraform for infrastructure provisioning

## Local Development Setup

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

## Running the Application Locally

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

You can modify the input and output paths by editing the constants at the top of `src/transform.py` or use command-line arguments:

```bash
poetry run python src/transform.py --input_path=path/to/your/input.parquet --output_path=path/to/your/output.csv
```

## Testing PySpark Applications

This project demonstrates comprehensive testing approaches for PySpark applications, including:

- Unit testing individual Spark functions
- Testing Spark DataFrame transformations
- Mocking Spark session and DataFrame operations
- Integration testing complete workflows

### Running Tests

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

## Cloud Deployment

This project includes Terraform configurations to deploy the application to Google Cloud Dataproc serverless.

### Setting Up Google Cloud

1. Install Google Cloud SDK and authenticate:
   ```bash
   gcloud auth login
   gcloud config set project your-project-id
   ```

2. Enable required GCP APIs:
   ```bash
   gcloud services enable dataproc.googleapis.com storage.googleapis.com compute.googleapis.com iam.googleapis.com
   ```

### Deploying with Terraform

1. Navigate to the Terraform directory:
   ```bash
   cd terraform/dataproc
   ```

2. Update `terraform.tfvars` with your GCP project details:
   ```hcl
   project_id  = "your-gcp-project-id"
   region      = "your-preferred-region"
   environment = "dev"
   ```

3. Deploy using the provided script:
   ```bash
   ./deploy.sh
   ```

This will:
- Initialize Terraform
- Create necessary GCP resources
- Upload application code to GCS
- Submit a Dataproc serverless batch job
- Monitor job execution

### Data Synchronization

Sync local data to Google Cloud Storage:

```bash
gsutil rsync -x ".DS_Store" src/data gs://data-demo-bucket-wayne/transform-stock-tick-sale-vs-bidask/
```

Verify uploaded files:

```bash
gsutil ls gs://data-demo-bucket-wayne/transform-stock-tick-sale-vs-bidask/
```

## Expected Input/Output Formats

### Input Format

The application expects the input Parquet file to have the following columns:
- `SaleDateTime`: Timestamp of the sale
- `Price`: Price of the stock
- `Quantity`: Quantity of shares sold
- `SaleValue`: Total value of the sale

### Output Format

The application produces a CSV file with the following columns:
- `HourOfDay`: Hour of the day (integer: 0-23)
- `TotalSaleValue`: Sum of all sale values in that hour
- `TotalQuantity`: Sum of all quantities in that hour
- `MaxPrice`: Maximum price in that hour
- `MinPrice`: Minimum price in that hour