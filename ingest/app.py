import requests
import re
#import google.cloud
#from google.cloud import storage
import os

#storage_client = storage.Client()

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

get_covid19_cases()


def upload_to_bucket(path_to_file, bucket_name):
    """Upload a file from `path_to_file` to desired `bucket_name`.

    :param path_to_file: The path to the file on the host machine (local system or server).
    :param bucket_name: The target storage blob bucket name.
    """
    blob_name = os.path.basename(path_to_file)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)

    # returns a public url
    return blob.public_url


def main():
    dates = get_available_dailies()
    bucket_name = "covid19_twitter"
    for idx, date in enumerate(dates):
        print(f"{idx}: downloading {date}")
        download_path = get_covid19_twitter(date)
        if not download_path:
            continue
        print(f"{idx}: uploading {date}")
        upload_to_bucket(download_path, bucket_name)
        print(f"{idx}: removing {date}")
        os.remove(download_path)


#if __name__ == "__main__":
#    main()
