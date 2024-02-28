"""
Uploads files to the Internet Archive based on a CSV containing file URLs and metadata.

The main entry point is process_csv(), which takes a CSV path, IA keys, and number of workers. It
reads the CSV, spawns threads to process each row, downloads the file, uploads it to IA, and logs
the result. Helper functions handle the individual steps.
"""

import argparse
import configparser
import csv
import logging
import os
import random
import re
import string
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from urllib.parse import quote, urlparse
from urllib.request import urlopen

from internetarchive import get_session, upload
from tqdm import tqdm


def configure_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    file_handler = logging.FileHandler("log.txt")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s:%(message)s")
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s:%(message)s")
    )

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def load_session(config_file):
    return get_session(config_file=config_file)


def create_identifier():
    identifier = "".join(random.choices(string.ascii_letters + string.digits, k=30))
    return identifier


def clean_metadata_text(text):
    return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]+", " ", text)


def write_failed_url(url, file_path="failed.txt"):
    with open(file_path, "a") as f:
        f.write(url + "\n")


def download_file_with_progress(url, output_path):
    try:
        response = urlopen(url)
        file_size = int(response.headers["Content-Length"])
        chunk_size = 1024

        with open(output_path, "wb") as f, tqdm(
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            total=file_size,
            desc=f"Downloading: {os.path.basename(output_path)}",
        ) as bar:
            for chunk in iter(lambda: response.read(chunk_size), b""):
                f.write(chunk)
                bar.update(len(chunk))
        return True
    except HTTPError as e:
        logging.error(
            f"HTTP Error when downloading {os.path.basename(url)}: {e.code} {e.reason}"
        )
        return False
    except Exception as e:
        logging.error(f"Failed to download {os.path.basename(url)}: {e}")
        return False


def encode_url(url):
    parsed_url = urlparse(url)
    path = quote(parsed_url.path, safe="/+")
    return f"{parsed_url.scheme}://{parsed_url.netloc}{path}"


def upload_to_internet_archive(file_path, metadata, keys):
    identifier = create_identifier()
    try:
        upload(
            identifier,
            files=[file_path],
            metadata=metadata,
            access_key=keys["access_key"],
            secret_key=keys["secret_key"],
            retries=5,
            retries_sleep=3,
            verbose=True,
            verify=True,
        )
        logging.info(
            f"Successfully uploaded {os.path.basename(file_path)} to {identifier}."
        )
        return True
    except Exception as e:
        logging.error(f"Failed to upload {os.path.basename(file_path)}: {e}")
        return False


def process_row(row, keys):
    file_url = encode_url(row["file"])
    file_name = os.path.basename(file_url)

    logging.info(f"Starting download for {file_name} from {file_url}")

    with tempfile.NamedTemporaryFile(
        delete=False, suffix=os.path.splitext(file_name)[1]
    ) as temp_file:
        local_file_path = temp_file.name

    if download_file_with_progress(file_url, local_file_path):
        logging.info(f"Downloaded {file_name} to {local_file_path}")

        metadata = {"mediatype": "movies"}
        for key, value in row.items():
            if key not in ["identifier", "file"]:
                metadata[key] = clean_metadata_text(value)

        if upload_to_internet_archive(local_file_path, metadata, keys):
            os.remove(local_file_path)
            logging.info(f"Removed local file {local_file_path}")
        else:
            write_failed_url(file_url)

        time.sleep(1)
    else:
        write_failed_url(file_url)
        os.remove(local_file_path)


def process_csv(csv_path, keys, max_workers=3):
    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(lambda row: process_row(row, keys), reader)


if __name__ == "__main__":
    configure_logging()

    # Get credentials
    config = configparser.ConfigParser()
    file_path = "ia.ini"
    config.read(file_path)

    access_key = config["s3"]["access"]
    secret_key = config["s3"]["secret"]
    keys = {"access_key": access_key, "secret_key": secret_key}

    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", help="Path to CSV file")
    parser.add_argument(
        "-w", "--workers", type=int, default=3, help="Number of workers"
    )
    args = parser.parse_args()

    process_csv(args.csv_path, keys, max_workers=args.workers)
