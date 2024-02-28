# Internet Archive Remote Media Uploader

This project provides a script to bulk upload remote files to the [Internet Archive](archive.org) from a CSV file containing URLs and metadata.

**💡 Recommendation:** Because Google is a monopoly, you can get _far_ better download/upload speeds by using a Colab notebook. However, IA might think you are a spam bot if you upload too quickly 🙃

<a href="https://colab.research.google.com/github/jacksongoode/ia-remote-upload/blob/main/ia_remote_upload_colab.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open in Colab"/></a>

If you would like to run the script yourself, instructions are below.

## Installation

Install packages into a venv. You can use [uv](https://github.com/astral-sh/uv) to do this:

### macOS and Linux

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

### Windows

```ps
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
.venv\Scripts\activate
uv pip install -r requirements.txt
```

Then log in to your Internet Archive account with:

```
ia configure
```

Note the location the credentials (ia.ini) are stored in and copy them to the local directory.

## Usage

Create a CSV file with the following columns:

- file - The full URL to the file to upload
- title - The title for the item on Internet Archive
- creator - The creator/author for the item
- date - The date for the item
- mediatype - The mediatype for the item, e.g. "movies"

Optionally:

- identifier - A unique ID for the item (a hash-based or random ID can also be generated)

As well as any other metadata fields you want to add. Please see the [documentation](https://archive.org/developers/internetarchive/cli.html) on the Internet Archives CLI.

- [Metadata schema](https://archive.org/developers/metadata-schema/)
- [Example CSV](https://archive.org/download/ia-pex/uploading.csv)
- [Credentials](https://archive.org/developers/tutorial-get-ia-credentials.html)

## Run

```sh
python ia_remote_upload.py path/to/csv.csv -w 1
```

This will spawn threads to download each file and upload it to Internet Archive along with the given metadata.

Progress and results will be logged to log.txt. Any failed downloads will be saved in failed.txt
