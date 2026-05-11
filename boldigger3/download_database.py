import datetime, sys, getpass, requests_html
from pathlib import Path
from tqdm import tqdm
import urllib.request


# class to generate a download progress bar with tqdm
class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def login():
    """_summary_

    Returns:
        requests_html.HTMLSession : Session where the user is logged in.
    """
    username = input(f"{datetime.datetime.now():%H:%M:%S}: Enter your BOLD username: ")
    password = getpass.getpass(
        f"{datetime.datetime.now():%H:%M:%S}: Enter your BOLD password: "
    )

    # data to attach to the post request to log in
    data = {
        "name": username,
        "password": password,
        "destination": "MAS_Management_UserConsole",
        "loginType": "",
    }

    # create an html session for the database download
    session = requests_html.HTMLSession()

    # send a post request to log in
    session.post("https://bench.boldsystems.org/index.php/Login", data=data)

    # test if the login was successful
    r = session.get("https://bench.boldsystems.org/index.php/datapackages/Latest")

    # look for the log out text
    log_out_text = r.html.find(".site-navigation > li:nth-child(4) > a:nth-child(1)")[
        0
    ].text

    # return session if successfull
    if log_out_text == "Log out":
        print(
            f"{datetime.datetime.now():%H:%M:%S}: Login successful, starting download."
        )
        return session
    else:
        print(
            f"{datetime.datetime.now():%H:%M:%S}: Login failed, please check credentials."
        )
        sys.exit()


def check_db_status(session: object, output_dir: str):
    """Function to check if a database download is needed or if the database is up to date already.

    Args:
        session (object): requests_html.HTMLSession object, already logged in.
        output_dir (str): directory to save the database to. Used to find if a database is existing already.
    """
    # check the latest database from the BOLD website
    r = session.get("https://bench.boldsystems.org/index.php/datapackages/Latest")
    r = r.html.find(
        "div.row:nth-child(5) > div:nth-child(1) > table:nth-child(3) > tbody:nth-child(1) > tr:nth-child(4) > td:nth-child(2) > button:nth-child(1)"
    )
    package_id = r[0].attrs["data-package-id"]
    data_url = r[0].attrs["data-url"]

    # check if such a file exists in the directory
    db_path = output_dir.joinpath(f"{package_id}.ddb")

    if db_path.is_file():
        print(f"{datetime.datetime.now():%H:%M:%S}: Database is up to date.")
        return True, session, data_url, package_id
    else:
        print(
            f"{datetime.datetime.now():%H:%M:%S}: Database does not exist or is outdated."
        )
        return False, session, data_url, package_id


def download_and_save_database(
    output_dir: str, session: object, data_url: str, package_id: str
):
    # this is the base url

    # extract the uid
    uid = session.get(f"https://bench.boldsystems.org{data_url}")
    uid = uid.text.replace('"', "")
    download_url = f"https://bench.boldsystems.org{data_url}&uid={uid}"

    # download the parquet
    download_filename = output_dir.joinpath(f"{package_id}.parquet")

    with DownloadProgressBar(
        unit="B", unit_scale=True, miniters=1, desc="Downloading public database"
    ) as t:
        urllib.request.urlretrieve(
            download_url, filename=download_filename, reporthook=t.update_to
        )


def main(output_dir: str):
    """Main function to download the BOLD public database via BOLDigger3"""
    print(f"{datetime.datetime.now():%H:%M:%S}: Welcome to BOLDigger3.")
    print(f"{datetime.datetime.now():%H:%M:%S}: This is the database download module.")
    print(
        f"{datetime.datetime.now():%H:%M:%S}: The database download requires BOLD credentials."
    )
    # log into boldsystems.org and fine the latest database link
    session = login()

    # download the database to a specific directory and extract to duckdb
    output_dir = Path(output_dir)

    # check database status
    up_to_date, session, data_url, package_id = check_db_status(
        session=session, output_dir=output_dir
    )

    if up_to_date:
        return None
    if not up_to_date:
        print(
            f"{datetime.datetime.now():%H:%M:%S}: Do you want to download the latest database release?."
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
