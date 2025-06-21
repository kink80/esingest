# Setup
- Clone https://github.com/deviantony/docker-elk to get ES stack running locally
- Use UV for dependency management, https://docs.astral.sh/uv/
- On Windows, make sure you have admin privileges for running scripts in the Powershell
- create a virtual environment `python -m venv .venv`
- `.venv\Scipts\activate` or `.venv/bin/activate`
- type `pip install -e .` or `uv sync` to get the project dependencies updated
- download the dataset from https://www.kaggle.com/datasets/nechbamohammed/research-papers-dataset?resource=download into data folder within the project, extract the csv file [dblp-v10.csv](data/dblp-v10.csv) 

# Scripts
- [extract_words.py](src/extract_words.py)
  - this will extract words from the dataset, excluding the stopword
- [ingest.py](src/ingest.py)
  - ingest all the data in an elasticsearch cluster/instance
- [locust_es_search_original.py](src/locust_es_search_original.py)
  -  `locust -f src/locust_es_search_original.py` to run performance tests by using the original query
- [locust_es_search_rescore.py](src/locust_es_search_rescore.py)
  - `locust -f src/locust_es_search_rescore.py` to run performance tests by using the rescore query