import datetime, sys, getpass, requests_html, duckdb
from pathlib import Path
from tqdm import tqdm
import urllib.request


class DownloadProgressBar(tqdm):
    """tqdm subclass that integrates with urllib.request.urlretrieve's reporthook."""

    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def login() -> requests_html.HTMLSession:
    """Log into BOLD Systems and return an authenticated session.

    Prompts for username and password, sends a POST request to the BOLD login
    endpoint, and verifies success by checking for a logout link. Exits the
    program if login fails.

    Returns:
        requests_html.HTMLSession: Authenticated session for subsequent requests.
    """
    username = input(f"{datetime.datetime.now():%H:%M:%S}: Enter your BOLD username: ")
    password = getpass.getpass(
        f"{datetime.datetime.now():%H:%M:%S}: Enter your BOLD password: "
    )

    data = {
        "name": username,
        "password": password,
        "destination": "MAS_Management_UserConsole",
        "loginType": "",
    }

    session = requests_html.HTMLSession()
    session.post("https://bench.boldsystems.org/index.php/Login", data=data)

    r = session.get("https://bench.boldsystems.org/index.php/datapackages/Latest")
    log_out_text = r.html.find(".site-navigation > li:nth-child(4) > a:nth-child(1)")[
        0
    ].text

    if log_out_text == "Log out":
        print(
            f"{datetime.datetime.now():%H:%M:%S}: Login successful, checking database status."
        )
        return session
    else:
        print(
            f"{datetime.datetime.now():%H:%M:%S}: Login failed, please check credentials."
        )
        sys.exit()


def check_db_status(
    session: requests_html.HTMLSession, output_dir: Path
) -> tuple[bool, str, str]:
    """Check whether the local database is up to date with the latest BOLD release.

    Fetches the latest package metadata from the BOLD website and compares it
    against an existing local .ddb file.

    Args:
        session: Authenticated requests_html.HTMLSession.
        output_dir: Directory to look for an existing database file.

    Returns:
        Tuple of (up_to_date, data_url, package_id).
    """
    r = session.get("https://bench.boldsystems.org/index.php/datapackages/Latest")
    r = r.html.find(
        "div.row:nth-child(5) > div:nth-child(1) > table:nth-child(3) > tbody:nth-child(1) > tr:nth-child(4) > td:nth-child(2) > button:nth-child(1)"
    )
    package_id = r[0].attrs["data-package-id"]
    data_url = r[0].attrs["data-url"]

    db_path = output_dir.joinpath(f"{package_id}.ddb")

    if db_path.is_file():
        print(f"{datetime.datetime.now():%H:%M:%S}: Database is up to date.")
        return True, data_url, package_id
    else:
        print(
            f"{datetime.datetime.now():%H:%M:%S}: Database does not exist or is outdated."
        )
        return False, data_url, package_id


def download_and_save_database(
    output_dir: Path,
    session: requests_html.HTMLSession,
    data_url: str,
    package_id: str,
) -> None:
    """Download the latest BOLD public database and convert it to DuckDB format.

    Fetches a one-time UID from the BOLD API, downloads the Parquet file with a
    progress bar, builds a DuckDB table from it, and removes the Parquet file
    afterwards. Any outdated .ddb file is deleted before writing the new one.

    Args:
        output_dir: Directory to save the database to.
        session: Authenticated requests_html.HTMLSession.
        data_url: Relative download URL obtained from check_db_status.
        package_id: Package identifier used to name the output files.
    """
    uid = session.get(f"https://bench.boldsystems.org{data_url}")
    uid = uid.text.replace('"', "")
    download_url = f"https://bench.boldsystems.org{data_url}&uid={uid}"

    download_filename = output_dir.joinpath(f"{package_id}.parquet")

    with DownloadProgressBar(
        unit="B", unit_scale=True, miniters=1, desc="Downloading public database"
    ) as t:
        urllib.request.urlretrieve(
            download_url, filename=download_filename, reporthook=t.update_to
        )

    ddb_output_path = output_dir.joinpath(f"{package_id}.ddb")

    if ddb_output_path.is_file():
        ddb_output_path.unlink()
        print(
            f"{datetime.datetime.now():%H:%M:%S}: Outdated database successfully removed."
        )

    print(f"{datetime.datetime.now():%H:%M:%S}: Building new database.")

    with duckdb.connect(ddb_output_path) as con:
        con.execute(f"""
                    CREATE TABLE bold_public
                    AS SELECT
                        processid,
                        sex,
                        life_stage,
                        inst,
                        "country/ocean",
                        identified_by,
                        identification_method,
                        coord,
                        nuc,
                        marker_code
                    FROM read_parquet('{download_filename}')
                    """)

    print(
        f"{datetime.datetime.now():%H:%M:%S}: New database saved at {ddb_output_path}."
    )

    if download_filename.is_file():
        download_filename.unlink()


def main(output_dir: str) -> None:
    """Main function to download the BOLD public database via BOLDigger3."""
    print(f"{datetime.datetime.now():%H:%M:%S}: Welcome to BOLDigger3.")
    print(f"{datetime.datetime.now():%H:%M:%S}: This is the database download module.")
    print(
        f"{datetime.datetime.now():%H:%M:%S}: The database download requires BOLD credentials."
    )

    session = login()
    output_dir = Path(output_dir)

    up_to_date, data_url, package_id = check_db_status(
        session=session, output_dir=output_dir
    )

    if up_to_date:
        return None
    else:
        print(
            f"{datetime.datetime.now():%H:%M:%S}: Do you want to download the latest database release?"
        )
        response = input(f"{datetime.datetime.now():%H:%M:%S}: yes/no: ")
        if response == "yes":
            download_and_save_database(
                output_dir=output_dir,
                session=session,
                data_url=data_url,
                package_id=package_id,
            )
        else:
            return None
