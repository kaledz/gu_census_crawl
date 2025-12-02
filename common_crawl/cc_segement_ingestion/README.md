# cc_segement_ingestion

A Databricks Asset Bundle for ingesting Common Crawl data from AWS S3 into Databricks tables. This package provides utilities and notebooks for batch and incremental ingestion of Common Crawl crawl data, supporting both full historical loads and incremental updates.

## Overview

This package facilitates the ingestion of Common Crawl data from the `commoncrawl` S3 bucket into Databricks using a medallion architecture (Bronze/Silver layers). It provides:

- **Master Index Management**: Lists and manages Common Crawl master indexes
- **Batch Ingestion**: Processes multiple crawls in batch operations
- **Incremental Ingestion**: Updates existing tables with new crawl data
- **S3 Integration**: Uses boto3 to interact with Common Crawl's S3 bucket

## Project Structure

```
cc_segement_ingestion/
├── src/
│   ├── cc_segement_ingestion/
│   │   ├── __init__.py
│   │   └── helpers.py          # Core Common Crawl helper functions (cc class)
│   ├── master_index.py         # Master index listing and bronze/silver layer processing
│   ├── batch_2025_crawls.py    # Batch ingestion for 2025 crawls
│   ├── batch_last_5_years_crawls.py  # Batch ingestion for 2020-2024 crawls
│   └── incremental_ingestion_crawls.py  # Incremental ingestion from most recent crawl
├── tests/
│   ├── conftest.py             # Pytest configuration and fixtures
│   └── sample_taxis_test.py    # Example test file
├── fixtures/                   # Test data fixtures (JSON/CSV)
├── databricks.yml              # Databricks Asset Bundle configuration
├── pyproject.toml              # Python project configuration and dependencies
└── README.md                   # This file
```

## Core Components

### Helper Functions (`src/cc_segement_ingestion/helpers.py`)

The `cc` class provides core functionality for interacting with Common Crawl S3 data:

- **`list_master_indexes()`**: Lists all master indexes from the Common Crawl S3 bucket
- **`list_crawls(prefix, as_json=False, s3_client=None)`**: Lists S3 objects under a given Common Crawl prefix
- **`batch_crawl_list(df, column)`**: Prepares a deduplicated list of crawl prefixes from a DataFrame
- **`batch_ingest_crawls(crawl_list)`**: Ingests multiple crawls and unions them into a single Spark DataFrame

### Notebooks

#### 1. Master Index (`src/master_index.py`)
- Lists all Common Crawl master indexes from S3
- Saves to Bronze layer: `census_bureau_capstone.bronze.raw_master_crawls`
- Filters 2025 crawls and saves to Silver layer: `census_bureau_capstone.silver.cleaned_master_crawls_2025`

#### 2. Batch 2025 Crawls (`src/batch_2025_crawls.py`)
- Reads filtered 2025 master crawls from Silver layer
- Batch ingests all 2025 crawl data
- Saves to Bronze layer: `census_bureau_capstone.bronze.raw_all_crawls`

#### 3. Batch Last 5 Years Crawls (`src/batch_last_5_years_crawls.py`)
- Processes crawls from 2020-2024
- Batch ingests each year's crawls sequentially
- Appends to Bronze layer: `census_bureau_capstone.bronze.raw_all_crawls`

#### 4. Incremental Ingestion (`src/incremental_ingestion_crawls.py`)
- Retrieves the most recent crawl from the Silver layer
- Ingests only new data using left anti-join to avoid duplicates
- Updates Bronze layer: `census_bureau_capstone.bronze.raw_all_crawls`

## Dependencies

### Runtime Dependencies
- Python 3.10-3.13
- boto3 (for S3 access)
- pandas
- pyspark

### Development Dependencies
- pytest
- databricks-dlt
- databricks-connect>=15.4,<15.5

See `pyproject.toml` for complete dependency specifications.

## Configuration

### Databricks Asset Bundle

The package is configured as a Databricks Asset Bundle with two targets:

- **dev**: Development mode with prefixed resources and paused schedules
- **prod**: Production mode with full permissions

Configuration is defined in `databricks.yml`.

### AWS Credentials

The notebooks require AWS credentials stored in Databricks secrets:

- **Scope**: `aws_cc`
- **Keys**: 
  - `aws_access_key_id`
  - `aws_secret_access_key`

Set up these secrets in Databricks before running the notebooks.

### Catalog and Schema

The package uses Unity Catalog with configurable catalog and schema:
- **Catalog**: `workspace` (configurable via bundle variables)
- **Schema**: `dev` (dev) or `prod` (production)
- **Target Catalog**: `census_bureau_capstone` (hardcoded in notebooks)

## Usage

### Prerequisites

1. **AWS Credentials**: Configure Databricks secrets for AWS S3 access
2. **Databricks Workspace**: Access to the configured Databricks workspace
3. **Unity Catalog**: Access to the `census_bureau_capstone` catalog or a duplicated version

### Running Notebooks

#### Initial Setup (Master Index)
```python
# Run master_index.py to:
# 1. List all master indexes from Common Crawl
# 2. Save to bronze.raw_master_crawls
# 3. Filter 2025 crawls and save to silver.cleaned_master_crawls_2025
```

#### Batch Ingestion
```python
# For 2025 crawls:
# Run batch_2025_crawls.py

# For historical data (2020-2024):
# Run batch_last_5_years_crawls.py
```

#### Incremental Updates
```python
# Run incremental_ingestion_crawls.py to:
# 1. Get most recent crawl from silver layer
# 2. Ingest new data only (deduplicated)
# 3. Update bronze.raw_all_crawls
```

### Using Helper Functions

```python
import sys
sys.path.append('/path/to/cc_segement_ingestion/src/cc_segement_ingestion')
from helpers import *

# Initialize S3 client
s3 = boto3.client("s3", region_name="us-east-1", ...)

# List master indexes
df_master = cc.list_master_indexes()

# List crawls for a specific prefix
df_crawls = cc.list_crawls("crawl-data/CC-MAIN-2025-05/")

# Batch process multiple crawls
crawl_list = cc.batch_crawl_list(df_master, "master_index")
df_all = cc.batch_ingest_crawls(crawl_list)
```

## Deployment

### Using Databricks Asset Bundles

1. **Deploy to Development**:
   ```bash
   databricks bundle deploy
   ```

2. **Deploy to Production**:
   ```bash
   databricks bundle deploy -t prod
   ```

3. **Run Deployed Jobs**:
   - Use the Databricks UI deployment panel
   - Or schedule notebooks using the **Schedule** option

### Building the Package

The package builds as a wheel artifact:
```bash
uv build --wheel
```

## Data Architecture

The package follows a medallion architecture:

### Bronze Layer
- **`raw_master_crawls`**: All Common Crawl master indexes
- **`raw_all_crawls`**: All crawl file metadata (Key, LastModified, ETag, Size, StorageClass, ChecksumAlgorithm, crawl_prefix)

### Silver Layer
- **`cleaned_master_crawls_2025`**: Filtered master indexes for 2025 crawls

## Common Crawl Data

This package ingests metadata from Common Crawl's public S3 bucket:
- **Bucket**: `commoncrawl`
- **Region**: `us-east-1`
- **Prefix**: `crawl-data/`

The ingested data includes S3 object metadata for Common Crawl files, which can then be used to access the actual crawl data.

## Development

### Code Style
- Line length: 125 characters (configured in `pyproject.toml`)
- Uses Black for formatting

### Adding New Features
1. Add helper functions to `src/cc_segement_ingestion/helpers.py`
2. Create new notebooks in `src/` for specific workflows
3. Update this README with new functionality

## Documentation

- [Databricks Asset Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- [Databricks Asset Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
- [Common Crawl Documentation](https://commoncrawl.org/)

