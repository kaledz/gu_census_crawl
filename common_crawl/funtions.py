
%pip install beautifulsoup4
%pip install warcio

# import the necessary packages
import pandas as pd
from bs4 import BeautifulSoup
import urllib.request
from urllib.error import URLError, HTTPError
from urllib.parse import urljoin, quote
import requests
import os
import gzip
import re
import json
from warcio.archiveiterator import ArchiveIterator
import time


#--------------------------------Class and functions for parsing web data-----------------------------
# Define headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

class WebParser():
    # Function to fetch HTML content
    def fetch_html(url):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req) as response:
                content_type = response.getheader('Content-Type')
                content = response.read()

                # Check if the content type is available
                if content_type:
                    # Split the content type if it includes a comma
                    parts = content_type.split(",", 1)
                    mediatype = parts[0]  # Get the primary media type

                    # Check if the content is text/html and decode accordingly
                    if 'text/html' in mediatype:
                        try:
                            return content.decode('utf-8')
                        except UnicodeDecodeError:
                            print(f"Decoding error for {url}, returning raw content")
                            return content  # Return raw binary if decoding fails
                else:
                    print(f"No content type received for {url}, returning raw content")
                    return content  # Return raw content if no content type

        except (HTTPError, URLError) as e:
            print(f"Error fetching {url}: {e}")
            return ''

    # Function to parse HTML and find CSS and JavaScript links
    def get_assets(html, base_url):
        soup = BeautifulSoup(html, 'html.parser')

        # Find all CSS links
        css_links = []
        for link in soup.find_all('link', rel='stylesheet'):
            href = link.get('href')
            if href:
                full_url = urljoin(base_url, href)
                # Encode URL to handle spaces and other special characters
                css_links.append(quote(full_url, safe='/:?&='))

        # Find all JavaScript links
        js_links = []
        for script in soup.find_all('script', src=True):
            src = script.get('src')
            if src:
                full_url = urljoin(base_url, src)
                # Encode URL to handle spaces and other special characters
                js_links.append(quote(full_url, safe='/:?&='))

        return css_links, js_links

    # Function to download resources with handling for binary content
    def download_resource(url):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req) as response:
                content = response.read()

                # Try to decode as text, fallback to binary
                try:
                    return content.decode('utf-8')
                except:
                    return content  # Return raw binary if decoding fails
        except:
            # print(f"Error downloading {url}: {e}")
            return ''
#----------------------------------------Class for data extraction functions of common crawl data--------------------------        
class CrawlExtractor():
    #Function to get WET file paths for US-specific URLs from Common Crawl Index
    def get_us_wet_file_urls(query_url):
        response = requests.get(query_url, stream=True)
        if response.status_code == 200:
            wet_file_urls = set()
            for line in response.iter_lines():
                if line:
                    record = json.loads(line.decode('utf-8'))
                    url = record.get('url', '')
                    filename = record.get('filename', '')

                    if filename.endswith('.warc.gz'):
                        wet_filename = filename.replace('.warc.gz', '.warc.wet.gz').replace("/warc/", "/wet/")
                        wet_file_urls.add(f"https://data.commoncrawl.org/{wet_filename}")
            return list(wet_file_urls)
        else:
            print(f"Failed to fetch data: {response.status_code}")
            return []
        
    # Function to download a file from a URL
    def download_file(url, dest_folder):
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder)
        local_filename = os.path.join(dest_folder, url.split('/')[-1])
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return local_filename

    # Function to extract text from WET file
    def extract_text_from_wet(wet_file_path):
        with gzip.open(wet_file_path, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'conversion':
                    text = record.content_stream().read().decode('utf-8')
                    yield record.rec_headers.get_header('WARC-Target-URI'), text

    # Function to check for US Census Bureau mentions
    def contains_us_census_info(content):
        keywords = ["US Census Bureau", "USCB", "Census Bureau"]
        return any(re.search(keyword, content, re.IGNORECASE) for keyword in keywords)

    # Function to save URLs
    def save_urls(urls, output_file):
        with open(output_file, 'w') as f:
            json.dump(urls, f, indent=4)

    def extract_urls_from_warc(warc_wet_gz_file):
        file = download_file(warc_wet_gz_file, "wet_files")
        urls = []
        with gzip.open(file, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'conversion':  # WET files usually have 'conversion' records
                    url = record.rec_headers.get_header('WARC-Target-URI')
                    if url:
                        urls.append(url)
        os.remove(file)
        return urls
            
