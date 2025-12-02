# Databricks notebook source
# MAGIC %md
# MAGIC # Wet File Data Extraction
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import libraries 

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lower, when, sum as spark_sum, collect_list, concat_ws, udf, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import re
import boto3
import botocore
import warnings
warnings.filterwarnings('ignore', category=UserWarning, message='.*No Partition Defined for Window operation.*')

import time

time_start = time.time()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Connect to Boto3 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Set Secrets

# COMMAND ----------

aws_access_key_id = dbutils.secrets.get(scope='aws_cc', key='aws_access_key_id')
aws_secret_access_key = dbutils.secrets.get(scope='aws_cc', key='aws_secret_access_key')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Intialize boto3 client

# COMMAND ----------

# Optional: build client once (faster)
s3 = boto3.client(
    's3',
    region_name='us-east-1',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Text transformation & Basic Filtering

# COMMAND ----------

# MAGIC %run "/Workspace/Shared/gu_census_crawl/common_crawl/testing/wet_files_extraction"

# COMMAND ----------

display(df)

df_raw = df

total_records_raw = df_raw.count()

print(f"Total records in raw data: {total_records_raw}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Extract Header Function
# MAGIC - Finds and extracts specific metadata fields from WET record headers
# MAGIC - Example: Extracts the date, URL, or content type from the header section

# COMMAND ----------

# Function to extract header
def extract_header(text, header_name):
    """
    Extract a specific header value from WET headers.
    
    Parameters:
    text (str): The text containing headers
    header_name (str): The name of the header to extract
    
    Returns:
    str: The header value, or None if not found
    """
    # Pattern to match header with optional quotes
    pattern = rf'{re.escape(header_name)}:\s*"?([^"\n]+?)"?\s*$'
    match = re.search(pattern, text, re.MULTILINE)
    if match:
        value = match.group(1).strip()
        # Remove quotes if they're at the very start and end
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        return value
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Parse Single Record
# MAGIC - Takes one complete WET record and breaks it into organized fields
# MAGIC - Extracts: URL, date, language, content type, and the actual web page text
# MAGIC - Returns a structured dictionary with all extracted information

# COMMAND ----------

# Function to parse a single WET record
def parse_single_wet_record(record_text):
    """
    Parse a single WET record into structured fields.
    
    Parameters:
    record_text (str): A complete WET record as text
    
    Returns:
    dict: Parsed record with all fields
    """
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
    
    # Find Content-Length line and extract value
    content_length_match = re.search(r'Content-Length:\s*(\d+)', record_text)
    if not content_length_match:
        return None
    
    record['Content-Length'] = content_length_match.group(1)
    
    # Split into headers and content
    # Content starts after the first blank line following Content-Length
    parts = re.split(r'\nContent-Length:\s*\d+\s*\n\n', record_text, maxsplit=1)
    if len(parts) < 2:
        return None
    
    headers = parts[0] + '\nContent-Length: ' + record['Content-Length']
    content = parts[1].strip()
    
    # Extract headers
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Convert to Dataframe
# MAGIC - MAIN FUNCTION: Converts raw WET file into a clean, structured table
# MAGIC - Process:
# MAGIC   - Identifies where each web page record starts and ends in the file
# MAGIC   - Groups all lines belonging to each record together
# MAGIC   - Parses each record to extract metadata and content
# MAGIC   - Removes system records (warcinfo) that aren't actual web pages
# MAGIC - Input: Raw WET file loaded line-by-line
# MAGIC - Output: Table with one row per web page, columns for URL, date, content, etc.

# COMMAND ----------

# Main function that converts the text to columnar format
def convert_wet_dataframe(df):
    """
    Convert a Spark DataFrame (loaded via spark.read.text) containing WARC data 
    into columnar format.
    
    Parameters:
    df: Spark DataFrame with 'value' column containing lines from WARC file
    
    Returns:
    DataFrame: Parsed DataFrame with columns:
        Type, Target-URI, Date, Record-ID, Refers-To, Block-Digest,
        Identified-Content-Language, Content-Type, Content-Length, Content
    """
    from pyspark.sql.functions import monotonically_increasing_id
    
    # Add row number for ordering
    df_numbered = df.withColumn("row_id", monotonically_increasing_id())
    
    # Identify lines that start a new WARC record
    df_marked = df_numbered.withColumn(
        "is_record_start",
        when(col("value") == "WARC/1.0", 1).otherwise(0)
    )
    
    # Create record_id by cumulative sum
    window_spec = Window.orderBy("row_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df_grouped = df_marked.withColumn(
        "record_id",
        spark_sum("is_record_start").over(window_spec)
    )
    
    # Group all lines belonging to same record and concatenate with newlines
    df_records = df_grouped.groupBy("record_id").agg(
        concat_ws("\n", collect_list("value")).alias("record_text")
    )
    
    # Filter out empty records
    df_records = df_records.filter(col("record_text") != "")
    
    # Define schema for parsed output
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
    
    # UDF to parse each record
    parse_udf = udf(parse_single_wet_record, record_schema)
    
    # Parse records
    df_parsed = df_records.withColumn("parsed", parse_udf(col("record_text")))
    
    # Filter out null parsed records and expand struct
    df_final = df_parsed.filter(col("parsed").isNotNull()).select("parsed.*")

    # Remove warcinfo records
    df_final = df_final.filter(col("Type") != "warcinfo")
    
    return df_final

# COMMAND ----------

df_clean = convert_wet_dataframe(df)

display(df_clean.limit(10))

# COMMAND ----------

total_clean_count = df_clean.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Basic Filtering
# MAGIC - Filters the parsed data to keep only relevant records
# MAGIC - Keeps only:
# MAGIC   * English-only content (no mixed languages)
# MAGIC   * Websites from .com, .org, .edu, or .gov domains
# MAGIC - Input: Parsed WET table
# MAGIC - Output: Filtered table with only English content from specified domains

# COMMAND ----------

!pip install fasttext

# patch: fasttext_improved_with_fallback.py
# NOTE: Ensure fasttext is installed on driver and executors (notebook %pip is not enough for cluster executors).
# You can run: pip install fasttext
from typing import Optional, Tuple
import fasttext
import unicodedata
from collections import Counter
import math
import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, BooleanType

# heuristic pieces (expanded small common set)
_EN_COMMON = {
    "the", "be", "to", "of", "and", "a", "in", "that", "have", "i", "it", "for", "not", "on", "with",
    "he", "as", "you", "do", "at", "this", "but", "his", "by", "from", "they", "we", "say", "her", "she",
    "or", "an", "will", "my", "one", "all", "would", "there", "their", "what", "so", "up", "out", "if",
    "about", "who", "get", "which", "go", "me", "when", "make", "can", "like", "time", "no", "just",
    "him", "know", "take", "people", "into", "year", "your", "good", "some", "could", "them", "see",
    "other", "than", "then", "now", "look", "only", "come", "its", "over", "think", "also", "back",
    "after", "use", "two", "how", "our", "work", "first", "well", "way", "even", "new", "want", "because",
    "any", "these", "give", "day", "most", "us"
}

# Regex to detect non-Latin scripts quickly (Cyrillic, Greek, Arabic, Devanagari, CJK, Hiragana/Katakana, Hangul)
_NON_LATIN_RE = re.compile(
    "[" +
    "\u0400-\u04FF" +  # Cyrillic
    "\u0370-\u03FF" +  # Greek
    "\u0600-\u06FF" +  # Arabic
    "\u0900-\u097F" +  # Devanagari
    "\u4E00-\u9FFF" +  # CJK
    "\u3040-\u30FF" +  # Hiragana/Katakana
    "\uAC00-\uD7AF" +  # Hangul
    "]"
)

_TOKEN_RE = re.compile(r"[a-zA-Z']+")

# fastText model cache per process
_FT_MODEL = None
_FT_MODEL_PATH = None


def _load_fasttext_model_once(path: str):
    global _FT_MODEL, _FT_MODEL_PATH
    if _FT_MODEL is None or _FT_MODEL_PATH != path:
        _FT_MODEL = fasttext.load_model(path)
        _FT_MODEL_PATH = path
    return _FT_MODEL


def _diacritic_ratio(s: str) -> float:
    """
    Compute ratio of combining diacritic marks relative to original string length.
    Normalizes to NFD so combining marks are explicit.
    """
    if not s:
        return 0.0
    n = unicodedata.normalize("NFD", s)
    comb = sum(1 for ch in n if unicodedata.category(ch) == "Mn")
    return comb / max(1, len(s))


def _top_noneng_from_counts(label_counts: Counter) -> Tuple[str, int]:
    """
    Return top non-English label (excluding any synthetic '__diacritic__') and its count.
    """
    noneng = [(k, v) for k, v in label_counts.items() if not k.startswith("en") and k != "__diacritic__"]
    if not noneng:
        return "", 0
    noneng.sort(key=lambda x: -x[1])
    return noneng[0][0], noneng[0][1]


def detect_language_status_fasttext_with_fallback(
    df: DataFrame,
    text_col: str = "Content",
    out_col: str = "lang_status",
    add_bool: bool = True,
    bool_col: Optional[str] = "has_other_lang",
    model_path: Optional[str] = None,
    # fastText / heuristic params
    chunk_size: int = 2000,
    step: Optional[int] = None,
    min_chunk_chars: int = 50,
    conf_threshold: float = 0.70,            # tighten confidence to reduce noise
    en_chunk_ratio_threshold: float = 0.75,  # require stronger english share
    require_min_en_chunks: int = 2,          # require >=2 english chunks for english_only
    # noise suppression for latin-script non-English (e.g., vie)
    min_accepted_chunks: int = 2,            # require >=2 accepted chunks before trusting ft
    require_noneng_votes: int = 2,           # require >=2 non-eng votes to consider non-english/mixed
    min_noneng_share: float = 0.4,           # non-eng share threshold to call non_english
    diacritic_threshold: float = 0.03,       # 3% combining marks -> diacritic evidence
    return_debug: bool = False,              # if True, return debug columns
) -> DataFrame:
    """
    Language detection UDF: fastText with heuristic fallback and diacritic-aware logic.
    Returns DataFrame with out_col and optional bool_col. If return_debug=True,
    additional columns are returned for tuning.
    """

    # basic validation
    if text_col not in df.columns:
        raise ValueError(f"text_col '{text_col}' not found in DataFrame")
    if add_bool and (not bool_col or not bool_col.strip()):
        raise ValueError("add_bool=True requires a non-empty bool_col name")
    if step is None:
        step = chunk_size
    if chunk_size <= 0 or step <= 0:
        raise ValueError("chunk_size and step must be positive")
    if min_chunk_chars < 1:
        raise ValueError("min_chunk_chars must be >= 1")
    if not (0.0 <= conf_threshold <= 1.0):
        raise ValueError("conf_threshold must be between 0 and 1")
    if not (0.0 <= en_chunk_ratio_threshold <= 1.0):
        raise ValueError("en_chunk_ratio_threshold must be between 0 and 1")
    if not (0.0 <= min_noneng_share <= 1.0):
        raise ValueError("min_noneng_share must be between 0 and 1")

    # heuristic fallback classifier
    def _heuristic_classify(text: Optional[str]):
        if text is None or str(text).strip() == "":
            if return_debug:
                return ("english_only", 0, 0, 0, "", 0, 0.0)
            return "english_only"
        s = str(text)
        L = len(s)
        i = 0
        saw_english = False
        saw_non = False
        accepted_chunks = en_votes = non_votes = 0
        top_noneng_label = ""
        top_noneng_votes = 0
        max_conf = 0.0
        while i < L:
            chunk = s[i : i + chunk_size]
            i += step
            if _NON_LATIN_RE.search(chunk):
                saw_non = True
                non_votes += 1
                accepted_chunks += 1
                if non_votes > top_noneng_votes:
                    top_noneng_votes = non_votes
                    top_noneng_label = "non_latin"
                if saw_english and saw_non:
                    break
                continue
            toks = _TOKEN_RE.findall(chunk.lower())
            if toks:
                common = sum(1 for t in toks if t in _EN_COMMON)
                ratio = common / len(toks)
                if common >= 3 or ratio >= 0.3:
                    saw_english = True
                    en_votes += 1
                    accepted_chunks += 1
                else:
                    saw_non = True
                    non_votes += 1
                    accepted_chunks += 1
            if saw_english and saw_non:
                break
        if accepted_chunks == 0:
            if return_debug:
                return ("english_only", accepted_chunks, en_votes, non_votes, top_noneng_label, top_noneng_votes, max_conf)
            return "english_only"
        if saw_english and not saw_non:
            if return_debug:
                return ("english_only", accepted_chunks, en_votes, non_votes, top_noneng_label, top_noneng_votes, max_conf)
            return "english_only"
        if saw_non and not saw_english:
            if return_debug:
                return ("non_english", accepted_chunks, en_votes, non_votes, top_noneng_label, top_noneng_votes, max_conf)
            return "non_english"
        if return_debug:
            return ("mixed", accepted_chunks, en_votes, non_votes, top_noneng_label, top_noneng_votes, max_conf)
        return "mixed"

    # fastText-based classifier with diacritic-aware counting and per-label counts
    def _fasttext_classify(text: Optional[str]):
        """
        Returns either a status string or a tuple of debug values depending on return_debug.
        Debug tuple: (status, accepted_chunks, en_votes, non_votes, top_noneng_label, top_noneng_votes, max_conf)
        """
        if text is None or str(text).strip() == "":
            if return_debug:
                return ("english_only", 0, 0, 0, "", 0, 0.0)
            return "english_only"
        if model_path is None:
            return _heuristic_classify(text)

        # try loading model once per process
        try:
            model = _load_fasttext_model_once(model_path)
        except Exception:
            # fallback to heuristic on load failure
            return _heuristic_classify(text)

        s = str(text)
        L = len(s)
        i = 0
        accepted = 0
        label_counts = Counter()
        max_conf = 0.0
        diacritic_votes = 0

        while i < L:
            chunk = s[i : i + chunk_size].replace("\n", " ").strip()
            i += step
            if len(chunk) < min_chunk_chars:
                continue

            # compute diacritic evidence for Latin-script languages (e.g., Vietnamese)
            dr = _diacritic_ratio(chunk)
            has_diacritics = dr >= diacritic_threshold

            try:
                labels, probs = model.predict(chunk, k=1)
            except Exception:
                # skip chunk if prediction fails
                continue
            if not labels or not probs:
                # still treat diacritics as weak evidence if present
                if has_diacritics:
                    label_counts["__diacritic__"] += 1
                    diacritic_votes += 1
                    accepted += 1
                continue

            # parse confidence and label
            try:
                conf = float(probs[0])
            except Exception:
                continue
            if math.isnan(conf) or conf < conf_threshold:
                # even if low-confidence, count diacritic presence as weak non-eng evidence
                if has_diacritics:
                    label_counts["__diacritic__"] += 1
                    diacritic_votes += 1
                    accepted += 1
                continue

            accepted += 1
            lab = str(labels[0]).replace("__label__", "").lower()
            label_counts[lab] += 1
            if conf > max_conf:
                max_conf = conf
            if has_diacritics:
                # synthetic diacritic vote boosts non-English evidence (useful for Latin-script languages)
                label_counts["__diacritic__"] += 1
                diacritic_votes += 1

            # early mixed detection: if at least two distinct labels seen and enough accepted chunks
            if len(label_counts) >= 2 and accepted >= min_accepted_chunks:
                # if multiple labels and neither dominates, call mixed early
                en_votes_tmp = sum(v for k, v in label_counts.items() if k.startswith("en"))
                non_votes_tmp = sum(v for k, v in label_counts.items() if not k.startswith("en"))
                if non_votes_tmp >= require_noneng_votes and (non_votes_tmp / accepted) >= min_noneng_share:
                    if return_debug:
                        top_label, top_votes = _top_noneng_from_counts(label_counts)
                        return ("non_english", accepted, en_votes_tmp, non_votes_tmp, top_label, top_votes, max_conf)
                    return "non_english"
                # otherwise treat as mixed (borderline)
                if return_debug:
                    top_label, top_votes = _top_noneng_from_counts(label_counts)
                    return ("mixed", accepted, en_votes_tmp, non_votes_tmp, top_label, top_votes, max_conf)
                return "mixed"

        # after scanning chunks, make decision
        if accepted < min_accepted_chunks:
            # Not enough reliable evidence from fastText -> fallback to heuristic
            return _heuristic_classify(text)

        en_votes = sum(v for k, v in label_counts.items() if k.startswith("en"))
        noneng_items = [(k, v) for k, v in label_counts.items() if not k.startswith("en")]
        non_votes = sum(v for _, v in noneng_items)
        # include diacritic_votes if present (already counted into label_counts under "__diacritic__")
        top_noneng_label, top_noneng_votes = _top_noneng_from_counts(label_counts)

        # If non-english evidence exists but is weak, apply noise suppression rules
        if non_votes > 0 and non_votes < require_noneng_votes:
            # strong English majority -> english_only
            if en_votes >= require_min_en_chunks and (en_votes / accepted) >= en_chunk_ratio_threshold:
                if return_debug:
                    return ("english_only", accepted, en_votes, non_votes, top_noneng_label, top_noneng_votes, max_conf)
                return "english_only"
            # multiple different weak non-eng labels -> mixed
            if len(noneng_items) > 1:
                if return_debug:
                    return ("mixed", accepted, en_votes, non_votes, top_noneng_label, top_noneng_votes, max_conf)
                return "mixed"
            # single weak non-eng label -> escalate if diacritics present and share sufficient
            if diacritic_votes > 0 and ((non_votes + diacritic_votes) / accepted) >= min_noneng_share:
                if return_debug:
                    return ("non_english", accepted, en_votes, non_votes + diacritic_votes, top_noneng_label, top_noneng_votes, max_conf)
                return "non_english"
            # otherwise fallback to heuristic
            return _heuristic_classify(text)

        # solid evidence cases
        if en_votes >= require_min_en_chunks and (en_votes / accepted) >= en_chunk_ratio_threshold:
            if return_debug:
                return ("english_only", accepted, en_votes, non_votes, top_noneng_label, top_noneng_votes, max_conf)
            return "english_only"

        if non_votes > 0 and en_votes == 0:
            # require noneng share threshold to avoid labeling due to a couple stray chunks
            if (non_votes / accepted) >= min_noneng_share:
                if return_debug:
                    return ("non_english", accepted, en_votes, non_votes, top_noneng_label, top_noneng_votes, max_conf)
                return "non_english"
            else:
                if return_debug:
                    return ("mixed", accepted, en_votes, non_votes, top_noneng_label, top_noneng_votes, max_conf)
                return "mixed"

        if en_votes > 0 and non_votes > 0:
            # mixed scenario: require noneng_votes >= require_noneng_votes
            if non_votes >= require_noneng_votes:
                if return_debug:
                    return ("mixed", accepted, en_votes, non_votes, top_noneng_label, top_noneng_votes, max_conf)
                return "mixed"
            # otherwise fallback heuristic
            return _heuristic_classify(text)

        # fallback conservative english
        if return_debug:
            return ("english_only", accepted, en_votes, non_votes, top_noneng_label, top_noneng_votes, max_conf)
        return "english_only"

    # UDF wrapper wiring (avoid lambdas for clearer tracebacks)
    def _udf_status_only(text: Optional[str]) -> str:
        res = _fasttext_classify(text)
        if isinstance(res, tuple):
            return res[0]
        return res

    def _udf_with_debug(text: Optional[str]):
        # returns tuple: (status, accepted_chunks, en_votes, non_votes, top_noneng_label, top_noneng_votes, max_conf)
        res = _fasttext_classify(text)
        if isinstance(res, tuple):
            return res
        # if classifier returns string, wrap into tuple
        return (res, 0, 0, 0, "", 0, 0.0)

    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

    if return_debug:
        schema = StructType([
            StructField(out_col, StringType(), True),
            StructField("accepted_chunks", IntegerType(), True),
            StructField("en_votes", IntegerType(), True),
            StructField("non_votes", IntegerType(), True),
            StructField("top_noneng_label", StringType(), True),
            StructField("top_noneng_votes", IntegerType(), True),
            StructField("max_conf", DoubleType(), True),
        ])
        udf_struct = udf(_udf_with_debug, schema)
        df_with_struct = df.withColumn("_lang_struct", udf_struct(col(text_col).cast("string")))
        df_out = df_with_struct \
            .withColumn(out_col, F.col("_lang_struct." + out_col)) \
            .withColumn("accepted_chunks", F.col("_lang_struct.accepted_chunks")) \
            .withColumn("en_votes", F.col("_lang_struct.en_votes")) \
            .withColumn("non_votes", F.col("_lang_struct.non_votes")) \
            .withColumn("top_noneng_label", F.col("_lang_struct.top_noneng_label")) \
            .withColumn("top_noneng_votes", F.col("_lang_struct.top_noneng_votes")) \
            .withColumn("max_conf", F.col("_lang_struct.max_conf")) \
            .drop("_lang_struct")
    else:
        udf_status = udf(_udf_status_only, StringType())
        df_out = df.withColumn(out_col, udf_status(col(text_col).cast("string")))

    if add_bool:
        udf_bool = udf(lambda s: s in ("mixed", "non_english"), BooleanType())
        df_out = df_out.withColumn(bool_col, udf_bool(col(out_col)))

    return df_out

# COMMAND ----------

df_clean = detect_language_status_fasttext_with_fallback(df_clean, text_col="Content", out_col="lang_status", add_bool=False)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = Window.orderBy("Record-ID")
df_clean = (
    df_clean
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("Content-Length") > 30)
)

display(df_clean)

# COMMAND ----------

df_clean = (
    df_clean.filter(
        col("lang_status") == "english_only"
    )
)
total_english_records_found = df_clean.count()
display(df_clean)

# COMMAND ----------

# Function to apply basic filtering
def filter_wet_records(df):
    """
    Filter WET records to only include:
    - URLs with .com, .org, .edu, or .gov domains
    - Records where Identified-Content-Language is 'eng'
    
    Parameters:
    df: Parsed WET DataFrame with columns including 'Target-URI' and 'Identified-Content-Language'
    
    Returns:
    DataFrame: Filtered DataFrame
    """
    from pyspark.sql.functions import lower, trim

    # Filter for specific domains (.com, .org, .edu, .gov)
    # Using regex to match domains properly (not matching .com.au, etc.)
    df_filtered = df.filter(
        col("Target-URI").rlike(r'://[^/]*\.(com|org|edu|gov)(/|$)')
    )
    
    return df_filtered

# COMMAND ----------

df_filter = filter_wet_records(df_clean)

display(df_filter)

# COMMAND ----------

df_filter.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Keyword Filtering

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Keyword Strategy

# COMMAND ----------

# Define product-level keywords
keywords_product = [
        "u.s. census bureau",
        "us census bureau", 
        "uscb",
        "census bureau",
        "census.gov",
        "data.census.gov",
        "factfinder.census.gov",
        "american community survey",
    ]

# Geographic Terms
keywords_geographic = [
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



# COMMAND ----------

# MAGIC %md
# MAGIC **Subject Keywords**:
# MAGIC
# MAGIC The Census Bureau defines all subjects covered in the American Community Survey on their website. https://www.census.gov/programs-surveys/acs/guidance/subjects.html#descriptionaccordion-73370cfb1f-item-264b8c4d39

# COMMAND ----------

# Define subject-level keywords
keywords_subject = [

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
    "tenure (owner/renter)"
    "units in structure",
    "value of home",
    "vehicles available",
    "year householder moved in unit",
    "year structure built"

    # Demographic keywords
    "age; sex",
    "group quarters population",
    "hispanic or latino origin",
    "race",
    "relationship to householder",
    "total population"
]

# COMMAND ----------

# Flattened list of all keywords (useful for regex or filtering)
def keyword_string(keyword_list):
    """Returns a text string for rlike filtering"""    
    return '|'.join(keyword_list)


# COMMAND ----------

keyword_string(keywords_product)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Product Filtering Strategy

# COMMAND ----------

df_top_1 = df_filter.filter(
    lower(col("Content")).rlike(keyword_string(keywords_product)))

total_products_found= df_top_1.count()



# COMMAND ----------

from pyspark.sql import Row


summary_df = spark.createDataFrame([
    Row(
        total_products_found=total_products_found,
        total_clean_count=total_clean_count,
        total_english_records_found=total_english_records_found,
        total_records_processed=total_records_raw
    )
])

display(summary_df)

summary_df.write.mode("append").saveAsTable("census_bureau_capstone.gold.nlp_pipeline_summary")

# COMMAND ----------

#Write to silver layer
df_top_1.write.mode("append").saveAsTable("census_bureau_capstone.silver.census_product_cleaned")

# COMMAND ----------

df_check = spark.read.table("census_bureau_capstone.silver.census_product_cleaned")

total_duplicates = df_check.groupBy("Record-ID").count().filter(col("count") > 1).count()

print(f"Total Duplicates: {total_duplicates}")

# COMMAND ----------

# Drop all files from the directory
for file_path in file_paths:
    dbutils.fs.rm(file_path)

# COMMAND ----------

time_end = time.time()
print(f"Elapsed time: {time_end - time_start:.2f} seconds")