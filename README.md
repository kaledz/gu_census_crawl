# Common Crawl Census Bureau Data Usage Analysis

A comprehensive Databricks-based pipeline for ingesting, processing, and analyzing Common Crawl web data to identify how U.S. Census Bureau data is being used across the internet. This project distinguishes between content that properly cites Census Bureau sources versus content that repackages Census data without proper attribution.

## 🎯 Project Purpose

This project addresses a critical data governance question: **How is U.S. Census Bureau data being used across the web, and is it being properly attributed?**

The pipeline:
- **Ingests** Common Crawl metadata and web content from AWS S3
- **Processes** web text to identify Census Bureau-related content
- **Classifies** content as either properly citing Census sources or repackaging data without attribution
- **Analyzes** usage patterns through n-gram analysis and domain investigation

## 📁 Project Structure

```
common_crawl/
├── cc_segement_ingestion/     # Common Crawl metadata ingestion pipeline
│   ├── src/                    # Ingestion notebooks and helper functions
│   ├── databricks.yml          # Databricks Asset Bundle configuration
│   └── pyproject.toml          # Python dependencies
│
├── nlp_pipeline/               # NLP processing pipeline for WET files
│   ├── src/                    # Pipeline notebooks and logic
│   ├── resources/              # Job configurations
│   ├── databricks.yml          # Databricks Asset Bundle configuration
│   └── pyproject.toml          # Python dependencies
│
└── analysis/                   # Classification and analysis notebooks
    ├── classification_model.py # ML models for cite vs. repackage classification
    ├── dictionary.py           # Census Bureau terminology dictionaries
    ├── ngram_analysis.py       # N-gram pattern analysis
    └── sites_of_interest.py    # Domain-specific investigations
```

## 🏗️ Architecture Overview

The project follows a **medallion architecture** (Bronze → Silver → Gold) in Databricks Unity Catalog:

### Data Flow

1. **Bronze Layer** (Raw Data)
   - `raw_master_crawls`: Common Crawl master index metadata
   - `raw_all_crawls`: All crawl file metadata (S3 object info)
   - `sample_size_raw`: Sampled WET crawl metadata

2. **Silver Layer** (Cleaned Data)
   - `cleaned_master_crawls_2025`: Filtered 2025 master indexes
   - `census_product_cleaned`: Filtered and processed Census-related web content

3. **Gold Layer** (Enriched Analytics)
   - `census_repackaged_enriched`: Classified content (cites vs. repackages)
   - `unigrams_repackaged`: N-gram analysis results
   - `nlp_pipeline_summary`: Pipeline execution summaries

## 🚀 Getting Started

### Prerequisites

- **Databricks Workspace**: Access to `https://dbc-a1e27fcd-fa88.cloud.databricks.com`
- **Unity Catalog**: Access to `census_bureau_capstone` catalog (or create your own)
- **AWS Access**: S3 read access to `commoncrawl` bucket and destination bucket for WET files
- **Databricks CLI**: For deployment (optional - can use UI instead)
- **Python 3.10-3.13**: With `uv` package manager

### Databricks Setup

This project requires several Databricks components to be configured. For detailed setup instructions, refer to the official Databricks documentation:

#### Required Setup Steps

1. **Configure Secrets**: Create a secret scope named `aws_cc` with AWS credentials
   - [Databricks Secrets Documentation](https://docs.databricks.com/security/secrets/index.html)
   - Required keys: `aws_access_key_id`, `aws_secret_access_key`

2. **Set Up Unity Catalog**: Create catalog and schemas (bronze, silver, gold)
   - [Unity Catalog Setup Guide](https://docs.databricks.com/data-governance/unity-catalog/get-started.html)
   - Target catalog: `census_bureau_capstone`

3. **Configure S3 Access**: Set up cluster IAM role or access keys for S3
   - [AWS S3 Access with Databricks](https://docs.databricks.com/storage/aws-storage.html)
   - Source: `s3://commoncrawl/` (public, read-only)
   - Destination: Your S3 bucket for WET file storage

4. **Install Databricks Extension** (for IDE development)
   - [Databricks Extension for VS Code](https://docs.databricks.com/dev-tools/vscode-ext.html)
   - Or use the Databricks UI for deployment

## 📦 Deployment

This project uses **Databricks Asset Bundles** for deployment. Each component (`cc_segement_ingestion` and `nlp_pipeline`) can be deployed independently.

### Deployment Methods

**Option 1: Using Databricks UI (Recommended)**
1. Open the **Deployments** panel (🚀 rocket icon) in Databricks
2. Select your target environment (dev or prod)
3. Click **Deploy**

**Option 2: Using Databricks CLI**
```bash
# Deploy to development (default)
cd cc_segement_ingestion && databricks bundle deploy
cd nlp_pipeline && databricks bundle deploy

# Deploy to production
cd cc_segement_ingestion && databricks bundle deploy -t prod
cd nlp_pipeline && databricks bundle deploy -t prod
```

### Deployment Targets

- **Development (`dev`)**: Resources prefixed with `[dev username]`, schedules paused
- **Production (`prod`)**: Full resource names, schedules active, production permissions

For detailed deployment instructions, see:
- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Deploying Asset Bundles](https://docs.databricks.com/dev-tools/bundles/deploy.html)

## 🔄 Usage

### Pipeline Components

**1. Common Crawl Ingestion** (`cc_segement_ingestion/`)
- Ingests Common Crawl metadata from AWS S3
- Notebooks: `master_index.py`, `batch_2025_crawls.py`, `batch_last_5_years_crawls.py`, `incremental_ingestion_crawls.py`
- See [cc_segement_ingestion README](cc_segement_ingestion/README.md) for detailed usage

**2. NLP Pipeline** (`nlp_pipeline/`)
- Processes WET files to extract and filter Census-related content
- Runs automatically every 3 hours in production
- See [nlp_pipeline README](nlp_pipeline/README.md) for detailed usage

**3. Analysis** (`analysis/`)
- Classification models, n-gram analysis, and domain investigations
- See [analysis README](analysis/README.md) for detailed usage

### Running Jobs

After deployment, jobs can be run via:
- **Databricks UI**: Workflows → Jobs → Run
- **Deployments Panel**: Click job name → Run
- **Scheduled**: Jobs run automatically based on configured schedules

For detailed job management, see:
- [Databricks Jobs Documentation](https://docs.databricks.com/workflows/jobs/index.html)
- [Running and Monitoring Jobs](https://docs.databricks.com/workflows/jobs/run-now.html)

## 🔧 Configuration

### Bundle Configuration

Both pipelines are configured via `databricks.yml` files with two targets:
- **Development (`dev`)**: Development mode with prefixed resources and paused schedules
- **Production (`prod`)**: Production mode with active schedules and full permissions

Workspace: `https://dbc-a1e27fcd-fa88.cloud.databricks.com` workspace used but requires new one to utlize

For configuration details, see:
- [Databricks Asset Bundles Configuration](https://docs.databricks.com/dev-tools/bundles/reference.html)
- [Bundle Variables and Targets](https://docs.databricks.com/dev-tools/bundles/variables.html)

## 📊 Data Sources

### Common Crawl

- **Bucket**: `s3://commoncrawl/`
- **Region**: `us-east-1`
- **Data Types**:
  - **WARC**: Web ARChive format (raw web content)
  - **WAT**: Web Archive Transformation (metadata)
  - **WET**: Web Extract Text (extracted text content)

This project primarily uses:
- Master indexes for crawl discovery
- WET files for text extraction and analysis

### Common Crawl Documentation

- [Common Crawl Main Page](https://commoncrawl.org/)
- [Data Access Guide](https://commoncrawl.org/the-data/get-started/)
- [S3 Bucket Structure](https://commoncrawl.org/the-data/get-started/#s3-bucket-structure)


### Local Development

For local development, use Databricks Connect:
- [Databricks Connect Documentation](https://docs.databricks.com/dev-tools/databricks-connect.html)
- Recommended version: `databricks-connect>=15.4,<15.5` (compatible with cluster runtime)

## 📚 Key Technologies

- **Databricks**: Cloud data platform and compute
- **PySpark**: Distributed data processing
- **Unity Catalog**: Data governance and cataloging
- **Databricks Asset Bundles**: Infrastructure as code for Databricks
- **boto3**: AWS S3 integration
- **FastText**: Language detection
- **Spark MLlib**: Machine learning models

## 🔍 Monitoring and Troubleshooting

### Job Monitoring

Monitor jobs via:
- **Databricks UI**: Workflows → Jobs → View runs
- **Deployments Panel**: Click job name → View run history
- **Email Notifications**: Configured for job failures

### Troubleshooting

For common issues and solutions, refer to:
- [Databricks Troubleshooting Guide](https://docs.databricks.com/troubleshooting/index.html)
- [Job Run Monitoring](https://docs.databricks.com/workflows/jobs/monitor-job-runs.html)
- [Unity Catalog Troubleshooting](https://docs.databricks.com/data-governance/unity-catalog/troubleshooting.html)

## 📖 Additional Documentation

- [cc_segement_ingestion README](cc_segement_ingestion/README.md) - Detailed ingestion pipeline docs
- [nlp_pipeline README](nlp_pipeline/README.md) - Detailed NLP pipeline docs
- [analysis README](analysis/README.md) - Analysis notebook documentation

### Databricks Documentation

- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Asset Bundles in Workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Secrets Management](https://docs.databricks.com/security/secrets/index.html)
- [Jobs and Workflows](https://docs.databricks.com/workflows/jobs/index.html)
- [AWS S3 Integration](https://docs.databricks.com/storage/aws-storage.html)

### External Resources

- [Common Crawl Documentation](https://commoncrawl.org/)

## 👥 Contributing

When adding new features:

1. **Ingestion Pipeline**: Add helper functions to `cc_segement_ingestion/src/cc_segement_ingestion/helpers.py`
2. **NLP Pipeline**: Add logic to `nlp_pipeline/src/pipeline_logic/`
3. **Analysis**: Add notebooks to `analysis/` directory
4. Update relevant README files with new functionality

## 🆘 Support

For issues or questions:
- Check individual component READMEs for specific guidance
- Review Databricks job logs for error details
- Consult Databricks and Common Crawl documentation

---

**Last Updated**: 12/2/2025
**Maintainer**: kl1147@georgetown.edu

