import requests
import re

def get_covid19_twitter(date):
    url = f"https://github.com/thepanacealab/covid19_twitter/raw/master/dailies/{date}/{date}_clean-dataset.tsv.gz"
    resp = requests.get(url)
    if not resp.status_code == 200:
        raise Exception(f"failed to download the file with status {resp.status}")
    with open(f"./{date}_clean-dataset.tsv.gz", "bw") as fp:
        fp.write(resp.content)

def get_available_dailies():
    url = "https://api.github.com/repos/thepanacealab/covid19_twitter/git/trees/master?recursive=1"
    resp = requests.get(url)
    if not resp.status_code == 200:
        raise Exception(f"failed to get available dailies with status {resp.status_code}")
    
    tree = resp.json()['tree']
    dates = []
    for item in tree:
        path = item["path"]
        if not path.startswith("dailies") or not path.endswith("clean-dataset.tsv.gz"):
            continue
        regex = r"dailies\/([0-9]{4}-[0-9]{2}-[0-9]{2})\/"
        date = re.match(regex, path)
        if not date:
            raise Exception(f"failed to find the date in {path}")
        dates.append(date.group(1))
        
    return sorted(set(dates))


if __name__ == "__main__":
    # get_covid19_twitter("2021-10-31")
    dates = get_available_dailies()
    for idx, date in enumerate(dates):
        print(f"{idx}: downloading {date}")
        get_covid19_twitter(date)
        if idx >=5:
            break