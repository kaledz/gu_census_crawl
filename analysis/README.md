# Census Bureau Data Usage Analysis

This project analyzes web content to identify how U.S. Census Bureau data is being used across the internet, with a focus on distinguishing between content that properly cites Census Bureau sources versus content that repackages Census data without proper attribution.

## Overview

The analysis pipeline processes web content to classify usage patterns of Census Bureau data, identifying:
- **Cites**: Content that properly attributes Census Bureau data with explicit citations or references
- **Repackages**: Content that uses Census data without proper attribution or citation

## Project Structure

### `classification_model.py`
Machine learning pipeline for classifying web content as "cites" or "repackages" Census Bureau data.

**Key Features:**
- Text normalization and preprocessing
- Feature engineering using Census Bureau terminology patterns
- Multiple classification models:
  - Linear SVM (Support Vector Machine)
  - Logistic Regression
  - Cosine Similarity-based classification
- Model evaluation with AUC, F1 score, precision, and recall metrics
- Class weighting to handle imbalanced datasets

**Data Sources:**
- Reads from: `census_bureau_capstone.silver.census_product_cleaned`
- Writes to: `census_bureau_capstone.gold.census_repackaged_enriched`

### `dictionary.py`
Contains dictionaries and regex patterns for identifying Census Bureau products and terminology in text.

**Components:**
- **ACS Terms**: Patterns for identifying American Community Survey (ACS) references
  - Subject table IDs (S0101, S0102, etc.)
  - Detailed table IDs (Bxxxxx, Cxxxxx)
  - Data profiles and comparison profiles
  - ACS-specific tools and products
- **Census Terms**: Comprehensive patterns for various Census Bureau programs:
  - Decennial Census
  - Economic Census
  - Business programs (CBP, BDS, LBD, QWI, LEHD)
  - Household surveys (CPS, SIPP, AHS)
  - Geographic reference files (TIGER/Line, Gazetteer, FIPS codes)

### `ngram_analysis.py`
N-gram analysis to identify distinguishing patterns between cited and repackaged content.

**Features:**
- URL domain analysis (top-level domains, root URLs)
- N-gram frequency analysis:
  - Unigrams (single words)
  - Bigrams (two-word phrases)
  - Trigrams (three-word phrases)
- Comparative visualization of n-grams in cited vs. repackaged content
- Custom stop word filtering for domain-specific terms

**Outputs:**
- Saves top repackaged unigrams to: `census_bureau_capstone.gold.unigrams_repackaged`

### `sites_of_interest.py`
Investigation of specific websites expected to appear in the dataset.

**Sites Analyzed:**
- Urban Institute
- Pew Research Center

Analyzes whether these sites are present in the data and how they use Census Bureau data (cited vs. repackaged).

## Data Pipeline

1. **Silver Layer**: Cleaned Census product data (`census_product_cleaned`)
2. **Gold Layer**: 
   - Enriched classification data (`census_repackaged_enriched`)
   - N-gram analysis results (`unigrams_repackaged`)

## Key Features & Heuristics

### Classification Signals

The model uses multiple signals to classify content:

1. **Explicit Attribution**: 
   - Phrases like "source: U.S. Census Bureau"
   - "According to the U.S. Census Bureau"
   - "From the U.S. Census Bureau"

2. **Census Domain References**:
   - census.gov, data.census.gov, api.census.gov
   - factfinder.census.gov

3. **Numeric Density**:
   - High density of numeric characters (>12%) without attribution suggests repackaging

4. **Census Terminology**:
   - Detection of specific Census Bureau program names
   - Table IDs and product references
   - Geographic reference terms

5. **Text Features**:
   - TF-IDF vectorization of normalized text
   - HashingTF for efficient feature representation

## Technologies Used

- **PySpark**: Distributed data processing and ML
- **Spark MLlib**: Machine learning pipelines and models
- **Pandas**: Data manipulation and analysis
- **Matplotlib/Seaborn**: Data visualization
- **scikit-learn**: Text vectorization (CountVectorizer)

## Model Performance

The classification models are evaluated using:
- **AUC (Area Under ROC Curve)**: Overall model discrimination
- **F1 Score**: Balanced precision and recall
- **Precision**: For "repackages" class (minimizing false positives)
- **Recall**: For "repackages" class (minimizing false negatives)

## Usage Notes

- These notebooks are designed for **Databricks** environment
- Requires access to the `census_bureau_capstone` database
- Some models (SVM) may be resource-intensive for large datasets
- Logistic Regression is recommended as a lighter-weight alternative

## Future Enhancements

- Expand dictionary patterns for additional Census Bureau products
- Incorporate more sophisticated NLP techniques
- Add support for multi-class classification (beyond binary)
- Enhance cosine similarity-based detection methods
