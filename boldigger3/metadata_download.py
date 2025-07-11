import importlib.util, datetime
from pathlib import Path
import requests_html_playwright
from dateutil.parser import parse
from tqdm import tqdm
import urllib.request
import duckdb, tarfile, json, os
import pandas as pd


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

SELECTED_COLUMNS = [
    "processid","sex","life_stage","inst","country/ocean","identified_by","identification_method",
    "coord", "nuc", "marker_code",
]


def empty_folder(path):
    for file in path.glob("*"):
        if file.is_dir():
            empty_folder(file)
            file.rmdir()
        else:
            file.unlink()


def parse_column_types_from_metadata(metadata):
    fields = metadata["resources"][0]["schema"]["fields"]
    duckdb_type_map = {
        "string": "TEXT", "char": "TEXT", "number": "DOUBLE", "integer": "BIGINT",
        "float": "DOUBLE", "string:date": "DATE", "array": "TEXT"
    }
    column_type_dict = {}
    for field in fields:
        col_name = field["name"]
        raw_type = field.get("type", "string").lower()
        duckdb_type = duckdb_type_map.get(raw_type, "TEXT")
        column_type_dict[col_name] = duckdb_type
    return column_type_dict

def get_version_file_path():
    spec = importlib.util.find_spec("boldigger3").origin
    boldigger3_path = Path(spec).parent
    return boldigger3_path / "database" / "version.txt"

def is_version_fresh():
    version_path = get_version_file_path()
    if not version_path.exists():
        return False
    try:
        with open(version_path) as f:
            last_date = parse(f.read().strip())
            return (datetime.datetime.now() - last_date).days < 7
    except Exception:
        return False

def write_version_file():
    version_path = get_version_file_path()
    with open(version_path, 'w') as f:
        f.write(datetime.datetime.now().isoformat())

def check_database():
    spec = importlib.util.find_spec("boldigger3").origin
    boldigger3_path = Path(spec).parent
    database_path = boldigger3_path.joinpath("database")
    database_path.mkdir(exist_ok=True)

    with requests_html_playwright.HTMLSession() as session:
        r = session.get("https://bench.boldsystems.org/index.php/datapackages/Latest")
        r = r.html.find('[name="download-datapackage-unauthenticated"]', first=True)
        package_id = r.attrs.get("data-package-id")
        package_date = parse(package_id.split(".")[-1])
        database_name = f"database_snapshot_{package_date.strftime('%Y-%m-%d')}.tar.gz"
        output_path = database_path.joinpath(database_name)

        if output_path.is_file() and is_version_fresh():
            print(f"{datetime.datetime.now():%H:%M:%S}: Data package is up to date.")
            return False, "", output_path
        else:
            print(f"{datetime.datetime.now():%H:%M:%S}: Data package is outdated or version expired.")
            print(f"{datetime.datetime.now():%H:%M:%S}: Updating now.")
            print(f"{datetime.datetime.now():%H:%M:%S}: Removing old version.")
            empty_folder(database_path)
            print(f"{datetime.datetime.now():%H:%M:%S}: Starting to download.")
            r = session.get(f"https://bench.boldsystems.org/index.php/API_Datapackage?id={package_id}")
            uid = r.text.replace('"', "")
            download_url = f"https://bench.boldsystems.org/index.php/API_Datapackage?id=BOLD_Public.04-Jul-2025&uid={uid}"
            return True, download_url, output_path

def download_url(url, output_path):
    with DownloadProgressBar(unit="B", unit_scale=True, miniters=1, desc="Downloading BOLD snapshot") as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)

def database_to_duckdb(output_path):
    db_path = str(output_path).replace(".tar.gz", ".duckdb")
    table_name = "bold_public"
    extract_path = Path(output_path).with_suffix("")

    try:
        if not extract_path.exists() or not list(extract_path.glob("*.json")) or not list(extract_path.glob("*.tsv")):
            print("Extracting archive...")
            with tarfile.open(output_path, 'r:gz') as tar:
                tar.extractall(path=extract_path)
        else:
            print("Extracted files already exist, skipping extraction.")

        json_path = next(extract_path.glob("*.json"))
        tsv_path = next(extract_path.glob("*.tsv"))

        with open(json_path) as f:
            metadata = json.load(f)

        full_column_types = parse_column_types_from_metadata(metadata)
        selected_column_types = {col: full_column_types[col] for col in SELECTED_COLUMNS if col in full_column_types}

        con = duckdb.connect(db_path)
        columns_def = ", ".join(f'"{col}" {dtype}' for col, dtype in selected_column_types.items())
        create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_def});'
        print(f"Creating table with schema:\n{create_sql}")
        con.execute(create_sql)

        print("Reading TSV in chunks using pandas...")
        chunksize = 100_000
        total_rows = 0
        for chunk in pd.read_csv(tsv_path, sep='\t', usecols=SELECTED_COLUMNS, chunksize=chunksize, low_memory=False):
            con.register("chunk", chunk)
            con.execute(f"INSERT INTO {table_name} SELECT * FROM chunk")
            con.unregister("chunk")
            total_rows += len(chunk)
            print(f"Inserted {total_rows} rows so far...")

        print("TSV data successfully loaded into DuckDB using pandas chunks.")

        # Clean up extracted files
        for item in extract_path.glob("*"):
            if item.is_file():
                item.unlink()
        extract_path.rmdir()

        write_version_file()

    except Exception as e:
        print("Error occurred during database creation:", e)
        if Path(db_path).exists():
            Path(db_path).unlink()  # remove corrupt db
        if extract_path.exists():
            for item in extract_path.glob("*"):
                if item.is_file():
                    item.unlink()
            extract_path.rmdir()
        raise e

def main():
    print(f"{datetime.datetime.now():%H:%M:%S}: Welcome to BOLDigger3.")
    print(f"{datetime.datetime.now():%H:%M:%S}: Checking data package availability.")
    downloaded_needed, url, output_path = check_database()
    if downloaded_needed:
        download_url(url, output_path)
        print(f"{datetime.datetime.now():%H:%M:%S}: Compiling database stored at {output_path}, this will take a while.")
        database_to_duckdb(output_path)