import requests
from pathlib import Path
import json

BASE_DIR = Path(__file__).parent.parent

with open(f"{BASE_DIR}/config/config.json", "r") as conf:
    config = json.load(conf)


def get_file_status(url):
    """Checks if the files is present in HDFS"""
    response = requests.get(url)
    if response.status_code == 200:
        return True
    else:
        return False


def get_file_from_hdfs(url, filename):
    """Downloads files form hdfs to local file system"""
    try:
        print(f"calling {url} and saving to {filename}")
        r = requests.get(url, stream=True)
        with open(filename, "wb") as fd:
            for chunk in r.iter_content(chunk_size=1048576):
                fd.write(chunk)
        return True
    except Exception as ex:
        print("FAILEDDDDD")
        return False


def put_files_to_hdfs(url, data=None):
    """Pushes files from local files system to hdfs"""
    try:
        r = requests.put(url, data=data)
        if r.status_code == 201:
            print("Successfully loaded file in hdfs.")
        else:
            print("Failed to load file in hdfs.")
    except Exception as e:
        print("Failed..")
        print(e)
