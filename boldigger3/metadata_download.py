import importlib.util, datetime, getpass
from pathlib import Path
import requests_html_playwright
from dateutil.parser import parse


def check_database():
    # find the current place of the database
    spec = importlib.util.find_spec("boldigger3").origin
    boldigger3_path = Path(spec).parent
    database_path = boldigger3_path.joinpath("database")

    # check if there is already a data folder, else make one
    if database_path.is_dir():
        pass
    else:
        database_path.mkdir()

    # get the latest database info
    with requests_html_playwright.HTMLSession() as session:
        r = session.get("https://bench.boldsystems.org/index.php/datapackages/Latest")
        r = r.html.find('[name="download-datapackage-unauthenticated"]', first=True)
        package_id = r.attrs.get("data-package-id")
        package_date = parse(package_id.split(".")[-1])

        # check if the current database exists
        database_name = f"database_snapshot_{package_date.strftime("%Y-%m-%d")}.tar.gz"
        output_path = database_path.joinpath(database_name)

        # delete old database if neccessary
        if output_path.is_file():
            print(
                "{}: Database is up to date. ".format(
                    datetime.datetime.now().strftime("%H:%M:%S")
                )
            )
            return ""
        else:
            print(
                "{}: Database is outdated. Current release is {}. ".format(
                    datetime.datetime.now().strftime("%H:%M:%S"),
                    package_date.strftime("%Y-%m-%d"),
                )
            )
            print(
                "{}: Starting to download. ".format(
                    datetime.datetime.now().strftime("%H:%M:%S")
                )
            )

            # remove old database files and generate a download link
            for file in database_path.glob("*"):
                file.unlink()

            # generate the new download link
            r = session.get(
                f"https://bench.boldsystems.org/index.php/API_Datapackage?id={package_id}"
            )
            uid = r.text.replace('"', "")
            download_url = f"https://bench.boldsystems.org/index.php/API_Datapackage?id=BOLD_Public.04-Jul-2025&uid={uid}"

            return download_url


def main():
    print(
        "{}: Welcome to BOLDigger3. ".format(
            datetime.datetime.now().strftime("%H:%M:%S")
        )
    )

    # give user output, ask for credentials
    print(
        "{}: Checking database availability.".format(
            datetime.datetime.now().strftime("%H:%M:%S")
        )
    )

    # check if the database already exists. compare dates and update if neccessary.
    # update database if needed, remove older versions
    download_url = check_database()
    print(download_url)
