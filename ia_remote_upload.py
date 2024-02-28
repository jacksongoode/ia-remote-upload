"""
Uploads files to the Internet Archive based on a CSV containing file URLs and metadata.

The main entry point is process_csv(), which takes a CSV path, IA keys, and number of workers. It
reads the CSV, spawns threads to process each row, downloads the file, uploads it to IA, and logs
the result. Helper functions handle the individual steps.
"""

import argparse
import configparser
import csv
import hashlib
import logging
import os
import random
import string
import tempfile
import time
from urllib.parse import quote, urlparse

import requests
from internetarchive import delete, get_item, get_session, upload
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map


def configure_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    log_format = logging.Formatter(
        "%(asctime)s %(levelname)s: %(message)s", datefmt="%H:%M:%S"
    )

    file_handler = logging.FileHandler("log.txt")
    file_handler.setFormatter(log_format)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def load_session(config_file):
    return get_session(config_file=config_file)


def create_identifier(id_type, row=None):
    if id_type == "hash":
        row_str = "".join(str(value) for value in row.values())
        hash_object = hashlib.md5(row_str.encode(), usedforsecurity=False)
        identifier = hash_object.hexdigest()
    else:
        # 8 * 10^32 possibilities
        identifier = "".join(random.choices(string.ascii_letters + string.digits, k=8))
    return identifier


def clean_metadata_text(text):
    return text.replace("\x00", "")


def clean_csv_data(csv_data):
    cleaned_data = []
    for row in csv_data:
        cleaned_row = {k: clean_metadata_text(v) for k, v in row.items()}
        cleaned_data.append(cleaned_row)
    return cleaned_data


def write_failed_url(url, file_path="failed.txt"):
    with open(file_path, "a") as f:
        f.write(url + "\n")


def download_file_with_progress(url, output_path):
    response = requests.get(url, stream=True, timeout=60)
    total_size = int(response.headers.get("Content-Length", 0))

    with open(output_path, "wb") as f, tqdm(
        desc=output_path,
        total=total_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
            f.write(data)
            bar.update(len(data))

    return True


def encode_url(url):
    parsed_url = urlparse(url)
    path = quote(parsed_url.path, safe="/+")
    return f"{parsed_url.scheme}://{parsed_url.netloc}{path}"


def upload_to_internet_archive(file_path, metadata, keys, identifier):
    try:
        logging.info(f"Uploading {os.path.basename(file_path)} to {identifier}.")
        upload(
            identifier,
            files=[file_path],
            metadata=metadata,
            access_key=keys["access_key"],
            secret_key=keys["secret_key"],
            retries=3,
            retries_sleep=5,
            verbose=True,
            verify=True,
            delete=True,
        )
        logging.info(
            f"Successfully uploaded {os.path.basename(file_path)} to {identifier}."
        )
        return True
    except Exception as e:
        logging.error(f"Failed to upload {os.path.basename(file_path)}: {e}")
        return False


def delete_item(identifier):
    item = get_item(identifier)
    if item.exists:
        delete(identifier)
    else:
        logging.warning(f"Item {identifier} not found for deletion")


def process_row(row, keys, sleep=3, id_type="hash", skip=True, delete=False):
    file_url = encode_url(row["file"])
    file_name = os.path.basename(file_url)
    identifier = None

    if id_type == "identifier":
        identifier = row["identifier"]
    elif id_type == "hash":
        # Use row to generate hash
        identifier = create_identifier(id_type, row)
    else:
        identifier = create_identifier(id_type)

    # Check if ID already exists
    if get_item(identifier).exists:
        if skip:
            logging.info(f"Skipping existing item: {row['identifier']}")
        else:
            logging.error(f"Item already exists: {row['identifier']}")
        return

    logging.info(f"Starting download for {file_name} from {file_url}")

    with tempfile.NamedTemporaryFile(
        delete=False, suffix=os.path.splitext(file_name)[1]
    ) as temp_file:
        local_file_path = temp_file.name

    if download_file_with_progress(file_url, local_file_path):
        logging.info(f"Downloaded {file_name} to {local_file_path}")

        metadata = {
            key: value
            for key, value in row.items()
            if key not in ["identifier", "file"]
        }

        if delete:
            delete_item(identifier)
            logging.info(f"Deleted item with identifier: {identifier}")
        else:
            if upload_to_internet_archive(local_file_path, metadata, keys, identifier):
                if os.path.exists(local_file_path):
                    os.remove(local_file_path)
                    logging.info(f"Removed local file {local_file_path}")
            else:
                write_failed_url(file_url)
    else:
        write_failed_url(file_url)
        os.remove(local_file_path)

    time.sleep(random.uniform(0, sleep))


def process_csv(csv_path, keys, id_type="hash", skip=True, delete=False, max_workers=3):
    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        csv_data = list(csv.DictReader(csvfile))

    cleaned_data = clean_csv_data(csv_data)

    # Map n workers to each row
    thread_map(
        lambda row: process_row(row, keys, id_type=id_type, skip=skip),
        cleaned_data,
        max_workers=max_workers,
    )


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
        "-w", "--workers", type=int, default=1, help="Number of workers"
    )
    parser.add_argument(
        "--id_type",
        default="hash",
        help="Type of identifier to generate (identifier, hash, or random)",
    )
    parser.add_argument(
        "--skip", action="store_true", help="Skip if item already exists"
    )
    parser.add_argument(
        "--delete", action="store_true", help="Delete items based on identifier"
    )
    args = parser.parse_args()

    # Loop over CSV rows
    process_csv(
        args.csv_path,
        keys,
        id_type=args.id_type,
        skip=args.skip,
        delete=args.delete,
        max_workers=args.workers,
    )
