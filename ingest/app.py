import requests
import re
from google.cloud import storage
import os

storage_client = storage.Client()

def get_covid19_cases():
    """Download covid-19-world cases data from the aws repository. If fails to download, returns None."""
    url = "https://covid19-lake.s3.us-east-2.amazonaws.com/rearc-covid-19-world-cases-deaths-testing/csv/covid-19-world-cases-deaths-testing.csv"
    resp = requests.get(url)
    if not resp.status_code == 200:
        print(f"failed to download the file with status {resp.status}")
        return
    with open("covid-19-world-cases.csv", "bw") as fileref:
        fileref.write(resp.content)
    return resp.status 


def upload_to_bucket(file_name, bucket_name):
    """Upload a file to desired `bucket_name`"""
    blob_name = file_name
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename("covid-19-world-cases.csv")

    # returns a public url
    return blob.public_url


def main():
    bucket_name = "covid19_cases"
    file_name = "covid-19-world-cases.csv"
    get_covid19_cases()
    print("Downloaded covid cases data")
    upload_to_bucket(file_name, bucket_name)

if __name__ == "__main__":
    main()
