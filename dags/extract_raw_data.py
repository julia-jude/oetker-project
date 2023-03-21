import urllib.request
import ssl
import logging

def extract_raw_data_ftn():
    """
    Downloads data from the URL and saves it to tthe below json file
    """

    #Define the file path where the downloaded data will be saved.
    file_path = "/opt/airflow/output/raw_data.json"

    #sets the default HTTPS context to a unverified context to disable certificate validation
    ssl._create_default_https_context = ssl._create_unverified_context

    data_url = "https://storage.googleapis.com/datascience-public/data-eng-challenge/MOCK_DATA.json"

    #Download the data from the URL and save it to the file path
    urllib.request.urlretrieve(data_url, file_path)
    logging.info("success....raw data file saved into /opt/airflow/output/ folder")

if __name__== '__main__':
    extract_raw_data_ftn()