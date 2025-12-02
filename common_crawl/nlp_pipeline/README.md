# nlp_pipeline

An end-to-end NLP pipeline for processing WET files from Common Crawl. This Databricks Asset Bundle extracts, transforms, and filters web crawl data to identify census-related product information.

## Overview

This pipeline processes Common Crawl WET files through multiple stages:
1. **Extraction**: Samples and extracts WET crawls from Common Crawl
2. **Transformation**: Parses, filters, and processes text content
3. **Loading**: Saves processed data to bronze, silver, and gold tables in Unity Catalog

## Project Structure

```
nlp_pipeline/
├── databricks.yml          # Databricks Asset Bundle configuration
├── pyproject.toml          # Python project dependencies
├── resources/
│   └── nlp_pipeline.job.yml # Job definition (runs every 3 hours)
├── src/
│   ├── wet_files_pipeline.py          # Main pipeline notebook
│   └── pipeline_logic/
│       ├── helpers.py      # Helper functions for extraction and filtering
│       └── utils.py        # Utility functions for S3 and file operations
└── scratch/                # Prototype and development scripts
```

## Pipeline Steps

The pipeline executes the following steps:

1. **Read and Sample Crawls**: Reads available WET crawls and samples by year (2021-2025)
2. **Save Samples to Bronze**: Stores sampled crawl metadata in `bronze.sample_size_raw`
3. **Copy WET Files**: Copies selected WET files from Common Crawl S3 to destination bucket
4. **Read and Parse WET Files**: Parses WET file format into structured dataframes
5. **Language Detection**: Detects and filters for English content
6. **Domain Filtering**: Filters by top-level domains (.com, .org, .edu, .gov)
7. **Keyword Filtering**: Filters content matching product-related keywords
8. **Save Results**: Writes filtered data to `silver.census_product_cleaned` and summary to `gold.nlp_pipeline_summary`
9. **Cleanup**: Removes temporary files

## Databricks Asset Bundle Configuration

### Targets

- **dev**: Development environment (default)
  - Mode: `development`
  - Resources prefixed with `[dev username]`
  - Schedules paused by default
  
- **prod**: Production environment
  - Mode: `production`
  - Deployed to `/Workspace/Users/kl1147@georgetown.edu/.bundle/nlp_pipeline/prod`
  - Permissions: `CAN_MANAGE` for kl1147@georgetown.edu

### Workspace

- Host: `https://dbc-a1e27fcd-fa88.cloud.databricks.com`

## Job Configuration

The pipeline runs as a scheduled job (`nlp_pipeline_loop`):
- **Schedule**: Every 3 hours (UNPAUSED)
- **Email Notifications**: Sends failure notifications to kl1147@georgetown.edu
- **Task**: Executes `src/wet_files_pipeline.py` notebook

## Getting Started

### 1. Deployment

- Click the **deployment rocket** 🚀 in the left sidebar to open the **Deployments** panel
- Select your target environment (dev or prod)
- Click **Deploy**

### 2. Running Jobs

- To run the deployed job manually, hover over `nlp_pipeline_loop` in the **Deployments** panel and click **Run**
- The job runs automatically every 3 hours when scheduled

### 3. Building the Package

The asset bundle builds a Python wheel package:
```bash
uv build --wheel
```

The built wheel is available at `dist/*.whl` and is automatically included in the job environment.

## Database Schema

The pipeline writes to the following Unity Catalog tables `(you must set up a schema before attempting to execute)`:

- **Bronze Layer**: `census_bureau_capstone.bronze.sample_size_raw`
  - Stores sampled crawl metadata
  
- **Silver Layer**: `census_bureau_capstone.silver.census_product_cleaned`
  - Stores filtered and processed product-related content
  
- **Gold Layer**: `census_bureau_capstone.gold.nlp_pipeline_summary`
  - Stores pipeline execution summaries and statistics

## Dependencies

- Python 3.10-3.13
- PySpark
- Databricks runtime libraries
- FastText (for language detection)

## Documentation

- [Databricks Asset Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- [Databricks Asset Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
