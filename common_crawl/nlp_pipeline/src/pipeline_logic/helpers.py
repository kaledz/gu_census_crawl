from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, sum as spark_sum, collect_list, concat_ws, udf, lower, row_number
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Window
import re


# ============================================================================
# EXTRACTION FUNCTIONS
# ============================================================================

def sample_crawls(df, years, limit=2):
    """Sample crawls by year, return pandas DataFrame."""
    samples = []
    for year in years:
        sample = (
            df.filter(F.col("crawl_start").cast("string").contains(year))
            .orderBy("crawl_start")
            .limit(limit)
        )
        if sample.count() > 0:
            samples.append(sample)
    
    if not samples:
        print("No samples found for the given criteria.")
        return None
    
    df_out = samples[0]
    for sample in samples[1:]:
        df_out = df_out.union(sample)
    
    return df_out.toPandas()


def get_key_list(df_samples):
    """Get list of keys from pandas DataFrame."""
    return df_samples['Key'].tolist()


def copy_wet_files(s3, key_list, source_bucket, dest_bucket, dest_prefix="common_crawl/wet_files/"):
    """Download WET files from source bucket and upload to destination."""
    import os
    
    for source_key in key_list:
        filename = source_key.split("/")[-1]
        dest_key = f"{dest_prefix}{filename}"
        local_file = f"/tmp/{filename}"
        
        s3.download_file(source_bucket, source_key, local_file)
        s3.upload_file(local_file, dest_bucket, dest_key)
        os.remove(local_file)
        
        print(f"Copied s3://{source_bucket}/{source_key} to s3://{dest_bucket}/{dest_key}")


# ============================================================================
# TRANSFORMATION FUNCTIONS
# ============================================================================

def extract_header(text, header_name):
    """Extract a specific header value from WET headers."""
    pattern = rf'{re.escape(header_name)}:\s*"?([^"\n]+?)"?\s*$'
    match = re.search(pattern, text, re.MULTILINE)
    if match:
        value = match.group(1).strip()
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        return value
    return None


def parse_single_wet_record(record_text):
    """Parse a single WET record into structured fields."""
    if not record_text or not isinstance(record_text, str):
        return None
    
    record = {
        'Type': None,
        'Target-URI': None,
        'Date': None,
        'Record-ID': None,
        'WARC-Filename': None,
        'isPartOf': None,
        'Refers-To': None,
        'Block-Digest': None,
        'Identified-Content-Language': None,
        'Content-Type': None,
        'Content-Length': None,
        'Content': None
    }
    
    content_length_match = re.search(r'Content-Length:\s*(\d+)', record_text)
    if not content_length_match:
        return None
    
    record['Content-Length'] = content_length_match.group(1)
    
    parts = re.split(r'\nContent-Length:\s*\d+\s*\n\n', record_text, maxsplit=1)
    if len(parts) < 2:
        return None
    
    headers = parts[0] + '\nContent-Length: ' + record['Content-Length']
    content = parts[1].strip()
    
    record['Type'] = extract_header(headers, 'WARC-Type')
    record['Target-URI'] = extract_header(headers, 'WARC-Target-URI')
    record['Date'] = extract_header(headers, 'WARC-Date')
    record['Record-ID'] = extract_header(headers, 'WARC-Record-ID')
    record['Refers-To'] = extract_header(headers, 'WARC-Refers-To')
    record['WARC-Filename'] = extract_header(headers, 'WARC-Filename')
    record['isPartOf'] = extract_header(headers, 'isPartOf')
    record['Block-Digest'] = extract_header(headers, 'WARC-Block-Digest')
    record['Identified-Content-Language'] = extract_header(headers, 'WARC-Identified-Content-Language')
    record['Content-Type'] = extract_header(headers, 'Content-Type')
    record['Content'] = content
    
    return record


def convert_wet_dataframe(df):
    """Convert raw WET text DataFrame to structured format."""
    from pyspark.sql.functions import monotonically_increasing_id
    
    df_numbered = df.withColumn("row_id", monotonically_increasing_id())
    df_marked = df_numbered.withColumn("is_record_start", when(col("value") == "WARC/1.0", 1).otherwise(0))
    
    window_spec = Window.orderBy("row_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df_grouped = df_marked.withColumn("record_id", spark_sum("is_record_start").over(window_spec))
    
    df_records = df_grouped.groupBy("record_id").agg(
        concat_ws("\n", collect_list("value")).alias("record_text")
    )
    df_records = df_records.filter(col("record_text") != "")
    
    record_schema = StructType([
        StructField('Type', StringType(), True),
        StructField('Target-URI', StringType(), True),
        StructField('Date', StringType(), True),
        StructField('Record-ID', StringType(), True),
        StructField('Refers-To', StringType(), True),
        StructField('WARC-Filename', StringType(), True),
        StructField('isPartOf', StringType(), True),
        StructField('Block-Digest', StringType(), True),
        StructField('Identified-Content-Language', StringType(), True),
        StructField('Content-Type', StringType(), True),
        StructField('Content-Length', StringType(), True),
        StructField('Content', StringType(), True)
    ])
    
    parse_udf = udf(parse_single_wet_record, record_schema)
    df_parsed = df_records.withColumn("parsed", parse_udf(col("record_text")))
    df_final = df_parsed.filter(col("parsed").isNotNull()).select("parsed.*")
    df_final = df_final.filter(col("Type") != "warcinfo")
    
    return df_final


def filter_by_domain(df):
    """Filter records to .com, .org, .edu, .gov domains."""
    return df.filter(col("Target-URI").rlike(r'://[^/]*\.(com|org|edu|gov)(/|$)'))


def keyword_string(keyword_list):
    """Convert keyword list to regex string."""
    return '|'.join(keyword_list)


def filter_by_keywords(df, keywords):
    """Filter records by keywords in Content."""
    return df.filter(lower(col("Content")).rlike(keyword_string(keywords)))


# ============================================================================
# KEYWORD DEFINITIONS
# ============================================================================

KEYWORDS_PRODUCT = [
    "u.s. census bureau",
    "us census bureau", 
    "uscb",
    "census bureau",
    "census.gov",
    "data.census.gov",
    "factfinder.census.gov",
    "american community survey",
]

KEYWORDS_GEOGRAPHIC = [
    "zip code",
    "zip codes",
    "census tract",
    "block group",
    "county",
    "state",
    "metropolitan area",
    "city",
    "radius",
    "geographic area",
    "location"
]

KEYWORDS_SUBJECT = [
    # Social Keywords
    "ancestry",
    "citizen voting-age population",
    "citizenship status",
    "disability status",
    "educational attainment",
    "fertility",
    "grandparets as caregivers",
    "language spoken at home",
    "marital history",
    "marital status",
    "migration residence 1 year ago",
    "place of birth",
    "school enrollment",
    "undergraduate field of degree",
    "veterans status",
    "period of military service",
    "year of entry",
    # Economic keywords
    "class of worker",
    "commuting and place of work",
    "employment status",
    "food stamps",
    "supplemental nutrition assistance program",
    "health insurance coverage",
    "income and earnings",
    "industry",
    "occupation",
    "poverty status",
    "work status last year",
    # Housing keywords
    "bedrooms",
    "computer & internet use",
    "house heating fuel",
    "kitchen facilities",
    "occupancy/vacancy status",
    "occupants per room",
    "rent",
    "rooms",
    "selected monthly owner costs",
    "telephone service available",
    "tenure (owner/renter)",
    "units in structure",
    "value of home",
    "vehicles available",
    "year householder moved in unit",
    "year structure built",
    # Demographic keywords
    "age; sex",
    "group quarters population",
    "hispanic or latino origin",
    "race",
    "relationship to householder",
    "total population"
]
