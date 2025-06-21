import base64

from locust import HttpUser, task, between
import random

from src.terms import words


class ElasticsearchUser(HttpUser):
    """
    Locust user class to test Elasticsearch search performance.
    """
    wait_time = between(1, 3) # Users wait between 1 and 3 seconds between tasks
    host = "http://localhost:9200" # Your Elasticsearch host
    username = "elastic"
    password = "changeme"

    # A list of predefined search terms.
    def on_start(self):
        # Encode credentials
        credentials = f"{self.username}:{self.password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        # Set the Authorization header for all subsequent requests
        self.client.headers = {"Authorization": f"Basic {encoded_credentials}"}


    @task
    def search_documents(self):
        """
        Performs a search query on the Elasticsearch index.
        """
        # Choose a random search term from our list
        query_term = random.sample(words, 10)
        query_term = " ".join(query_term)

        # Define the Elasticsearch search body
        # We'll search in 'review_body' and 'review_title' fields.
        bool_query_original = {
            "bool": {
                "should": [
                    {"match_phrase": {"content": {"query": query_term, "boost": 30}}},
                    {"match_phrase": {"content": {"query": query_term, "slop": 6, "boost": 10}}},
                    {"match": {"content": {"query": query_term, "minimum_should_match": "80%"}}},
                ],
                "minimum_should_match": 1,
            }
        }

        search_body = {
            "query": bool_query_original,
            "size": 50 # Request 10 hits per search
        }

        # Perform the POST request to the _search endpoint
        # The 'name' parameter helps group requests in Locust statistics
        try:
            with self.client.post(
                "/events/_search",
                json=search_body,
                name="/events/_search [POST]", # Meaningful name for reporting
                catch_response=True # Allows checking response status and content
            ) as response:
                if response.status_code == 200:
                    response_data = response.json()
                    # Check if search hits are present
                    if "hits" in response_data and "total" in response_data["hits"]:
                        # print(f"Search for '{query_term}' successful. Total hits: {response_data['hits']['total']['value']}")
                        response.success() # Mark the request as successful
                    else:
                        response.failure(f"Search response missing 'hits' data: {response.text}")
                else:
                    response.failure(
                        f"Search failed with status code {response.status_code}: {response.text}"
                    )
        except Exception as e:
            self.environment.events.request_failure.fire(
                request_type="POST",
                name="/events/_search [POST]",
                response_time=0,
                exception=e,
                response_length=0,
            )
            print(f"An error occurred during search: {e}")

# If you want to run this script directly (for quick test without full web UI setup)
# This part is mostly for development/debugging, typically you run locust from CLI.
# if __name__ == "__main__":
#     # This part is illustrative; typically Locust is run via the command line.
#     # For actual use, follow the instructions below.
#     pass
