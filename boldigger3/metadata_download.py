import importlib.util, datetime, sys
from pathlib import Path
import requests_html
from dateutil.parser import parse
from tqdm import tqdm
import urllib.request
import duckdb, tarfile, json, os
import pandas as pd


# class to generate a download progress bar with tqdm
class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def empty_folder(path: str):
    """Function to empty a folder for a given path, including folders and files

    Args:
        path (str): Path to empty
    """
    for file in path.glob("*"):
        if file.is_dir():
            empty_folder(file)
            file.rmdir()
        else:
            file.unlink()


def parse_column_types_from_metadata(metadata: object) -> dict:
    """Function to parse column types from json encoded metadata.

    Args:
        metadata (object): JSON metadata

    Returns:
        dict: Returns a dict with parsed column types
    """
    # extract fields from json
    fields = metadata["resources"][0]["schema"]["fields"]
    # map to duckdb datatypes
    duckdb_type_map = {
        "string": "TEXT",
        "char": "TEXT",
        "number": "DOUBLE",
        "integer": "BIGINT",
        "float": "DOUBLE",
        "string:date": "DATE",
        "array": "TEXT",
    }
    # store the output here
    column_type_dict = {}
    for field in fields:
        col_name = field["name"]
        raw_type = field.get("type", "string").lower()
        duckdb_type = duckdb_type_map.get(raw_type, "TEXT")
        column_type_dict[col_name] = duckdb_type
    return column_type_dict


def get_version_file_path() -> object:
    """Function that returns the versioning file path.

    Returns:
        object: Version file path as pathlib Path object.
    """
    spec = importlib.util.find_spec("boldigger3").origin
    boldigger3_path = Path(spec).parent
    return boldigger3_path.joinpath("database", "version.txt")


def is_version_fresh(package_date: str) -> bool:
    """Function to check if the database is up to date.

    Args:
        package_date (str): Recent data package date from BOLD as string in YYYY-MM-DD

    Returns:
        bool: Returns True if the version is up to date
    """
    version_path = get_version_file_path()
    # when there is no version file --> no db has been created
    if not version_path.exists():
        return False

    with open(version_path) as f:
        last_date = f.read().strip()
        if package_date == last_date:
            return True
        else:
            return False


def write_version_file(package_date: str):
    """Function to write a version file with the current package release date

    Args:
        package_date (str): Recent data package date from BOLD as string in YYYY-MM-DD
    """
    version_path = get_version_file_path()
    with open(version_path, "w") as f:
        f.write(package_date)


def check_database() -> tuple:
    """Function to check if a new release of the BOLD data package is available.

    Returns:
        tuple: Returns a tuple with bool (downloaded needed), str (download url), str (output path to store the download), str (package date as string)
    """
    spec = importlib.util.find_spec("boldigger3").origin
    boldigger3_path = Path(spec).parent
    database_path = boldigger3_path.joinpath("database")
    database_path.mkdir(exist_ok=True)

    with requests_html.HTMLSession() as session:
        # fetch the latest version from the BOLD website
        r = session.get("https://bench.boldsystems.org/index.php/datapackages/Latest")
        r = r.html.find('[name="download-datapackage-unauthenticated"]', first=True)
        # extract the package id
        package_id = r.attrs.get("data-package-id")
        package_date = parse(package_id.split(".")[-1])
        package_date = package_date.strftime("%Y-%m-%d")
        # generate an output path for the download and a duck db path for the duckdb database
        database_name = f"database_snapshot_{package_date}.tar.gz"
        output_path = database_path.joinpath(database_name)
        duckdb_name = f"database_snapshot_{package_date}.duckdb"
        duckdb_path = database_path.joinpath(duckdb_name)

        # if the duck db database exists and the version file can be read and is up to date, nothing is to do
        if duckdb_path.is_file() and is_version_fresh(package_date):
            print(f"{datetime.datetime.now():%H:%M:%S}: Database is up to date.")
            return False, "", output_path, package_date
        # triggers the download
        else:
            print(
                f"{datetime.datetime.now():%H:%M:%S}: Database is outdated or version expired. Updating now."
            )
            print(f"{datetime.datetime.now():%H:%M:%S}: Removing old version.")
            empty_folder(database_path)
            print(f"{datetime.datetime.now():%H:%M:%S}: Starting to download.")
            r = session.get(
                f"https://bench.boldsystems.org/index.php/API_Datapackage?id={package_id}"
            )
            uid = r.text.replace('"', "")
            download_url = f"https://bench.boldsystems.org/index.php/API_Datapackage?id={package_id}&uid={uid}"
            return True, download_url, output_path, package_date


def download_url(url: str, output_path: str):
    """Function to download from a url and display a progressbar

    Args:
        url (str): url to download
        output_path (str): output path to write to
    """
    with DownloadProgressBar(
        unit="B", unit_scale=True, miniters=1, desc="Downloading BOLD snapshot"
    ) as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)


def database_to_duckdb(output_path: str, package_date: str):
    """Function to extract the downloaded tar.gz and stream it into duckdb

    Args:
        output_path (str): Output path of the downloaded file.
        package_date (str): Package date as string YYYY-MM-DD

    Raises:
        e: _description_
    """
    db_path = str(output_path).replace(".tar.gz", ".duckdb")
    table_name = "bold_public"
    extract_path = Path(output_path).with_suffix("")

    # only select these columns from tsv
    SELECTED_COLUMNS = [
        "processid",
        "sex",
        "life_stage",
        "inst",
        "country/ocean",
        "identified_by",
        "identification_method",
        "coord",
        "nuc",
        "marker_code",
    ]
    # @ Manan: I see you catch every exception here. Is there a more elegant solution to this? I do not see any reason
    # that the duckdb stream fails except KeyBoardInterrupt.
    try:
        if (
            not extract_path.exists()
            or not list(extract_path.glob("*.json"))
            or not list(extract_path.glob("*.tsv"))
        ):
            print(f"{datetime.datetime.now():%H:%M:%S}: Extracting downloaded archive.")
            with tarfile.open(output_path, "r:gz") as tar:
                tar.extractall(path=extract_path)
        else:
            # @ Manan:Is there any case (except testing) where this actually happens? I think it could be removed
            print(
                f"{datetime.datetime.now():%H:%M:%S}: Extracted files already exist, skipping extraction."
            )

        json_path = next(extract_path.glob("*.json"))
        tsv_path = next(extract_path.glob("*.tsv"))

        # load and parse the json data
        with open(json_path) as f:
            metadata = json.load(f)

        full_column_types = parse_column_types_from_metadata(metadata)
        selected_column_types = {
            col: full_column_types[col]
            for col in SELECTED_COLUMNS
            if col in full_column_types
        }

        # stream data to duck db with duck db csv reader
        # open the connection
        con = duckdb.connect(db_path)

        # define the columns
        columns_def = ", ".join(
            f'CAST("{col}" AS {dtype}) AS "{col}"'
            for col, dtype in selected_column_types.items()
        )

        # stream the data to duck db
        con.execute(
            f"""
            CREATE TABLE bold_public AS
                    SELECT {columns_def}
                    FROM read_csv_auto('{tsv_path}', delim='\t', header=True)
            """
        )

        # close the connection
        con.close()

        print(
            f"{datetime.datetime.now():%H:%M:%S}: BOLD package successfully streamed into database."
        )

        # Clean up extracted files
        for item in extract_path.glob("*"):
            if item.is_file():
                item.unlink()
        extract_path.rmdir()
        output_path.unlink()

        # write the version file with the current package date.
        write_version_file(package_date)

    # @ Manan Maybe just catch KeyBoardInterrupt and then adjust code for specific errors users might encounter instead just catching everything?
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
    """Main function to trigger the addition data download."""
    # give user output
    print(f"{datetime.datetime.now():%H:%M:%S}: Welcome to BOLDigger3.")
    print(f"{datetime.datetime.now():%H:%M:%S}: Checking data package availability.")

    # check if a new download and duck db parsing is needed
    downloaded_needed, url, output_path, package_date = check_database()

    # if the download is needed, download and stream to duckdb
    if downloaded_needed:
        download_url(url, output_path)
        print(
            f"{datetime.datetime.now():%H:%M:%S}: Compiling downloaded data stored at {output_path}, this will take a while."
        )
        database_to_duckdb(output_path, package_date)
