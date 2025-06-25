import fitz  # PyMuPDF
import pandas as pd
import re
import time
from urllib.parse import urlparse, unquote
import os
import ssl 

# Keywords should be written in a .txt file, one per line
# Keywords reader is case insensitive,
KEYWORDS_FILE = "keywords.txt"

# URLs should be written in a .txt file, one per line
INPUT_URL_FILE = "pdf_urls.txt"
# Company names should correspond to each company name. The .txt file should have the same # of lines as the urls file
COMPANY_NAMES_FILE = "company_names.txt" 
OUTPUT_EXCEL_FILE = "WebScraper_PDF.xlsx"
OUTPUT_CSV_FILE = "WebScraper_PDF.csv"
# Test or change later? Outputs are inconsistent
SENTENCES_OUTPUT_FILE = "KeywordSentences_PDF.txt" 
ERROR_LOG_FILE = "error_log_pdf.txt"
MAX_RETRIES = 2
RETRY_DELAY = 5
   
def extract_keywords(keywords_file):
    try:
        with open(keywords_file, "r") as f:
            keywords = [line.strip() for line in f if line.strip()] 
        return keywords
    except Exception as e:
        print(f"Error reading keywords from {keywords_file}: {e}")
        log_error("N/A", f"Error reading keywords: {e}")
        return []

def extract_filename(url):
    try:
        parsed_url = urlparse(url)
        filename = unquote(os.path.basename(parsed_url.path))
        if not filename:
            return "Unknown"
        return filename
    except Exception:
        return "Unknown"

def read_pdf_content(url):
    import urllib.request
    import certifi
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(url, context=ssl_context) as response:
                pdf_content = response.read()
            with fitz.open(stream=pdf_content) as doc:
                text = ""
                for page in doc:
                    text += page.get_text()
                return text
        except Exception as e:
            print(f"Error reading PDF from {url} (Attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                log_error(url, f"Failed to read PDF after {MAX_RETRIES} attempts: {e}")
                return None
    return None


def analyze_text_for_keywords(text, keywords):
    occurrences = {}
    found_sentences_for_pdf = set() 
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=[.!?])\s+', text)

    for keyword in keywords:
        count = 0
        # regex case insensitive
        keyword_pattern = rf"\b{re.escape(keyword)}\b"
        for sentence in sentences:
            if re.search(keyword_pattern, sentence, re.IGNORECASE):
                count += 1
                
                found_sentences_for_pdf.add(sentence.strip())
        if count > 0:
            occurrences[keyword] = count
    
    return occurrences, list(found_sentences_for_pdf)

def log_error(url, error_message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(ERROR_LOG_FILE, "a") as f:
        f.write(f"{timestamp} - URL: {url} - Error: {error_message}\n")

def read_company_names(names_file):
    try:
        with open(names_file, "r", encoding="utf-8") as f:
            names = [line.strip() for line in f if line.strip()]
        return names
    except FileNotFoundError:
        print(f"Warning: Company names file '{names_file}' not found. Using extracted filenames for all URLs.")
        return []
    except Exception as e:
        print(f"Error reading company names from {names_file}: {e}")
        log_error("N/A", f"Error reading company names: {e}")
        return []

def process_filings(url_file, keywords, company_names=None):
    results = []
    all_extracted_sentences = set() 

    with open(url_file, "r") as f:
        urls = [line.strip() for line in f if line.strip()] 

    total_files = len(urls)

    
    if company_names and len(company_names) != total_files:
        print(f"Warning: Mismatch between number of URLs ({total_files}) and company names ({len(company_names)}).")
        print("Falling back to extracted filenames for URLs without a corresponding company name.")

    for index, url in enumerate(urls):
        print(f"Processing file {index + 1} of {total_files}: {url}")

        
        display_name = filename = extract_filename(url)
        if company_names and index < len(company_names):
            display_name = company_names[index] 

        text = read_pdf_content(url)
        if text:
            
            occurrences, pdf_sentences = analyze_text_for_keywords(text, keywords)
            
            if occurrences:
                results.append({
                    "URL": url,
                    "Name": display_name, # Using 'Name' for custom or extracted filename, shouldn't be used 
                    **occurrences
                })
            else:
                results.append({
                    "URL": url,
                    "Name": display_name, 
                    "No Keywords Found": "No keywords found in this file."
                })
            
            all_extracted_sentences.update(pdf_sentences)
        
        time.sleep(1) 
    
    return results, list(all_extracted_sentences)

def save_results(results, excel_file, csv_file):
    if not results:
        print("No results to save for Excel/CSV.")
        return
    try:
        df = pd.DataFrame(results)
        df.to_excel(excel_file, index=False)
        df.to_csv(csv_file, index=False, encoding="utf-8")
        print(f"Results saved to {excel_file} and {csv_file}")
    except Exception as e:
        print(f"Error saving results: {e}")
        log_error("N/A", f"Error saving results: {e}")

def save_sentences_to_file(sentences, output_file):
    if not sentences:
        print("No sentences to save.")
        return
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            for sentence in sentences:
                f.write(sentence + "\n")
        print(f"Extracted sentences saved to {output_file}")
    except Exception as e:
        print(f"Error saving sentences to {output_file}: {e}")
        log_error("N/A", f"Error saving sentences: {e}")


if __name__ == "__main__":
    print("Starting keyword extraction from financial filings...")
    start_time = time.time()
    
    keywords = extract_keywords(KEYWORDS_FILE)
    if not keywords:
        print("No keywords found. Exiting.")
        exit()
    
    
    company_names = read_company_names(COMPANY_NAMES_FILE)

    
    results_for_df, extracted_sentences = process_filings(INPUT_URL_FILE, keywords, company_names)
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"Processing completed in {duration:.2f} seconds.")
    
    
    save_results(results_for_df, OUTPUT_EXCEL_FILE, OUTPUT_CSV_FILE)
    
    
    save_sentences_to_file(extracted_sentences, SENTENCES_OUTPUT_FILE)
    
    print("Done!")