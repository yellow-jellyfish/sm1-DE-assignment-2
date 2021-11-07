import requests


def get_covid19_twitter(date):
    url = f"https://github.com/thepanacealab/covid19_twitter/raw/master/dailies/{date}/{date}_clean-dataset.tsv.gz"
    resp = requests.get(url)
    if not resp.status_code == 200:
        raise Exception(f"failed to download the file with status {resp.status}")
    with open(f"./{date}_clean-dataset.tsv.gz", "bw") as fp:
        fp.write(resp.content)

if __name__ == "__main__":
    get_covid19_twitter("2021-10-31")