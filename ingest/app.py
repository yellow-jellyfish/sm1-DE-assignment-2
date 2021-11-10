import requests
import re
from google.cloud import storage
import os

storage_client = storage.Client()


def upload_to_bucket(path_to_file, bucket_name):
    blob_name = os.path.basename(path_to_file)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)

    # returns a public url
    return blob.public_url


def get_covid19_twitter(date):
    url = f"https://github.com/thepanacealab/covid19_twitter/raw/master/dailies/{date}/{date}-dataset.tsv.gz"
    resp = requests.get(url)
    if not resp.status_code == 200:
        print(f"failed to download the file with status {resp.status}")
        return

    path_to_file = f"./{date}-dataset.tsv.gz"
    with open(path_to_file, "bw") as fp:
        fp.write(resp.content)
    return path_to_file


def get_available_dailies():
    url = "https://api.github.com/repos/thepanacealab/covid19_twitter/git/trees/master?recursive=1"
    resp = requests.get(url)
    if not resp.status_code == 200:
        raise Exception(
            f"failed to get available dailies with status {resp.status_code}"
        )

    tree = resp.json()["tree"]
    dates = []
    for item in tree:
        path = item["path"]
        if (
            not path.startswith("dailies")
            or not path.endswith("dataset.tsv.gz")
            or "clean" in path
        ):
            continue
        regex = r"dailies\/([0-9]{4}-[0-9]{2}-[0-9]{2})\/"
        date = re.match(regex, path)
        if not date:
            raise Exception(f"failed to find the date in {path}")
        dates.append(date.group(1))

    return sorted(set(dates))


if __name__ == "__main__":
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
