import csv
import os
import re

from nltk.corpus import stopwords

DATA_FILE_NAME = "dblp-v10.csv"

def extract_and_clean_words(csv_filepath, text_column='abstract', min_word_count=10000):
    """
    Extracts words from a specified text column in a CSV,
    cleans them (lowercase, removes punctuation), and removes English stopwords.
    Continues extracting until a minimum word count is reached.

    Args:
        csv_filepath (str): The path to your CSV file.
        text_column (str): The name of the column containing the text to extract words from.
                           Defaults to 'abstract'.
        min_word_count (int): The minimum number of words to extract before stopping.

    Returns:
        tuple: A tuple containing:
               - list: A list of cleaned, non-stopwords.
               - int: The total count of extracted words.
    """
    english_stop_words = set(stopwords.words('english'))
    all_extracted_words = []
    current_word_count = 0

    try:
        with open(csv_filepath, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            # Check if the text_column exists in the header
            if text_column not in reader.fieldnames:
                print(f"Error: Column '{text_column}' not found in the CSV header. Available columns: {', '.join(reader.fieldnames)}")
                return [], 0

            for row in reader:
                if current_word_count >= min_word_count:
                    break

                text = row.get(text_column, '') # Get text from the specified column

                if text:
                    # 1. Convert to lowercase
                    text = text.lower()
                    # 2. Remove punctuation and numbers, replace with space
                    text = re.sub(r'[^a-z\s]', '', text)
                    # 3. Tokenize by splitting on whitespace
                    words = text.split()
                    # 4. Remove stop words and filter out empty strings
                    cleaned_words = [word for word in words if word not in english_stop_words and word]

                    all_extracted_words.extend(cleaned_words)
                    current_word_count += len(cleaned_words)

    except FileNotFoundError:
        print(f"Error: The file '{csv_filepath}' was not found.")
        return [], 0
    except Exception as e:
        print(f"An error occurred: {e}")
        return [], 0

    print(f"\n--- Extraction Complete ---")
    print(f"Target word count: {min_word_count}")
    print(f"Actual words extracted: {current_word_count}")

    return all_extracted_words, current_word_count

# --- EXAMPLE USAGE (YOU NEED TO REPLACE THESE WITH YOUR ACTUAL VALUES) ---
# csv_file_path = "your_data.csv"
column_to_extract = "abstract" # Or "title", depending on your choice

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
        raise Exception("No data file found")

    all_extracted_text, total_words = extract_and_clean_words(data_path, column_to_extract)
    print(all_extracted_text)

# # print(f"Extracted {total_words} words.")
# # print(all_extracted_text[:1000]) # Print first 1000 characters to check