import requests
import re
from google.cloud import storage
import os

storage_client = storage.Client()


def get_available_dailies():
    """Extract the dates available in `dailies` directory in the root of
    the Covid19 Twitter repository.
    https://github.com/thepanacealab/covid19_twitter/tree/master/dailies

    The result would be a list of dates as string like:
    ['2020-03-22', '2020-03-23', ...]
    """
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


def get_covid19_twitter(date):
    """Download covid19 twitter data from the repository with the given
    `date` and store it in the current path. Return the current path
    as the result like:

    ./2020-03-22-dataset.tsv.gz

    if failed to download, return None
    """
    url = f"https://github.com/thepanacealab/covid19_twitter/raw/master/dailies/{date}/{date}-dataset.tsv.gz"
    resp = requests.get(url)
    if not resp.status_code == 200:
        print(f"failed to download the file with status {resp.status}")
        return

    path_to_file = f"./{date}-dataset.tsv.gz"
    with open(path_to_file, "bw") as fp:
        fp.write(resp.content)
    return path_to_file


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


if __name__ == "__main__":
    main()
