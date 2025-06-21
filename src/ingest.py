# ingestion_script.py
import csv
import os
import json
from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm

# --- Configuration ---
ES_HOST = "http://localhost:9200" # Your Elasticsearch host
ES_INDEX = "events"      # Name of the Elasticsearch index
DATA_FILE_NAME = "dblp-v10.csv"
# Note: The URL above is for TSV.gz. The JSON.gz file is generally available from a different source
# or requires conversion. For simplicity and direct parsing, let's use a common format often
# derived from these datasets, or assume a local JSONL file for this example.
# A more direct JSON.gz example (if available for download):
# DATA_URL = "https://example.com/path/to/your_data.json.gz" # Replace with actual JSON.gz if found

# For this example, let's simulate a JSONL file that would be generated from a TSV or similar,
# or assume you've placed a JSONL.gz file locally.
# If you actually need to download the TSV and convert, that's another step.
# For simplicity, let's use a small, easily downloadable JSONL.gz sample for demonstration
# and indicate how to adapt it for a larger dataset.

# For a large dataset like 2M, downloading and processing can be time-consuming.
# Let's assume you have a JSON Lines (.json) file or JSON Lines (.json.gz) file ready.
# If not, you might need to convert the TSV.gz file first.

# Placeholder for a direct JSONL.gz download or conversion.
# For a real large dataset, you'd likely download a file like
# 'video_games.json.gz' which is already in JSON Lines format.
# As it's hard to find a direct public JSONL.gz for 2M items,
# let's write the script expecting a JSONL.gz file to exist.
# I'll provide a method to generate a dummy one for testing if needed.


# --- Main Ingestion Logic ---
def ingest_data_to_elasticsearch(es_host, es_index, data_file_path):
    """
    Connects to Elasticsearch and ingests data from a gzipped JSON Lines file.
    """
    print(f"Connecting to Elasticsearch at {es_host}...")
    try:
        # Connect to Elasticsearch
        # For ES 8.x with security disabled, no auth is needed.
        # If security is enabled, you'd need `basic_auth=('username', 'password')`
        # or `api_key=('id', 'api_key')`
        es = Elasticsearch(es_host, basic_auth=("elastic", "changeme"))
        if not es.ping():
            raise ValueError(
                "Connection to Elasticsearch failed! Make sure it's running and accessible."
            )
        print("Successfully connected to Elasticsearch.")

        # Create index if it doesn't exist
        if not es.indices.exists(index=es_index):
            print(f"Creating index '{es_index}'...")
            # You can define a mapping here for better search relevance and data types
            # For simplicity, we'll let Elasticsearch infer the mapping.
            es.indices.create(index=es_index)
            print(f"Index '{es_index}' created.")
        else:
            print(f"Index '{es_index}' already exists. Documents will be added/updated.")

        print(f"Reading data from {data_file_path} and ingesting into Elasticsearch...")

        def generate_actions():
            """
            Generator function to yield Elasticsearch bulk actions.
            Reads documents line by line from the gzipped JSONL file.
            """
            with open(data_file_path, 'rt', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for doc in reader:
                    action = {
                        "_index": es_index,
                        "_id": doc.get("id"), # Use review_id if present
                        "_source": doc
                    }
                    yield action

        # Use helpers.bulk for efficient batch ingestion
        # chunk_size defines how many documents are sent in one batch
        # max_retries and initial_backoff are for handling transient errors
        successes, errors = helpers.bulk(
            es,
            generate_actions(),
            index=es_index,
            chunk_size=5000,       # Send 5000 documents per batch
            request_timeout=60,    # Timeout for each bulk request
            max_retries=3,         # Retry failed batches up to 3 times
            initial_backoff=1,     # Initial backoff time for retries
            raise_on_error=False,  # Don't raise an exception on first error
        )

        print("\n--- Ingestion Summary ---")
        print(f"Successfully ingested: {successes} documents.")
        if errors:
            print(f"Documents with errors during ingestion: {len(errors)}")
            # For brevity, not printing all errors. You might want to log them.
            # print("First 5 errors:", errors[:5])
        else:
            print("No errors reported during ingestion.")

    except ValueError as ve:
        print(f"Error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- Script Execution ---
if __name__ == "__main__":
    # --- IMPORTANT: Choose one of the following data preparation methods ---

    # Option 1: Generate dummy data (for quick testing, approx 2M documents)
    # This will create a `video_games_reviews.json.gz` file with dummy content.
    # It might take a few minutes to generate 2M documents.
    # generate_dummy_data(2000000, DATA_FILE_NAME)

    # Option 2: Use an actual downloaded Amazon reviews JSONL.gz file.
    # You would typically download a file like 'video_games.json.gz' from a dataset source
    # and place it in the same directory as this script.
    # For instance, if you manually downloaded `amazon_reviews_us_Video_Games_v1_00.json.gz`
    # and renamed it to `video_games_reviews.json.gz`, uncomment the line below.
    # Make sure this file exists before running the script with this option.
    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/", DATA_FILE_NAME)
    if not os.path.exists(data_path):
        print(f"Data file '{data_path}' not found.")
        print("Please choose an option to prepare data:")
        print("1. Uncomment `generate_dummy_data` line to create a dummy file (takes time for 2M).")
        print("2. Download a real Amazon reviews JSONL.gz file (e.g., from Kaggle or similar) and place it here.")
        print("   A good search term would be 'Amazon reviews video games jsonl.gz'")
        exit() # Exit if no data file found for ingestion


    ingest_data_to_elasticsearch(ES_HOST, ES_INDEX, data_path)

    # Optional: Verify some documents
    # es = Elasticsearch(ES_HOST)
    # count = es.count(index=ES_INDEX)
    # print(f"Total documents in '{ES_INDEX}': {count['count']}")

    # search_results = es.search(index=ES_INDEX, query={"match_all": {}}, size=5)
    # print("\nFirst 5 documents:")
    # for hit in search_results['hits']['hits']:
    #     print(json.dumps(hit['_source'], indent=2))
