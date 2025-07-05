import importlib.util, datetime, getpass
from pathlib import Path
from playwright.sync_api import sync_playwright
from dateutil.parser import parse


def check_database():
    # find the current place of the database
    spec = importlib.util.find_spec("boldigger3").origin
    boldigger3_path = Path(spec).parent
    database_path = boldigger3_path.joinpath("data")

    # check if there is already a data folder, else make one
    if database_path.is_dir():
        pass
    else:
        database_path.mkdir()

    # check the latest release of the database
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(
            "https://bench.boldsystems.org/index.php/datapackages/Latest", timeout=30000
        )
        date = page.locator(
            "xpath=/html/body/div[1]/div[3]/div/div[2]/div/h2"
        ).text_content()
        # extract the date of the current release
        date = parse(date.split(" ")[-1])


def main():
    print(
        "{}: Welcome to BOLDigger3. To download additional data BOLD credentials are required.".format(
            datetime.datetime.now().strftime("%H:%M:%S")
        )
    )
    user_name = input("BOLD username:")
    password = getpass.getpass("BOLD password:")

    print(user_name, password)
    # give user output, ask for credentials
    print(
        "{}: Checking database availability.".format(
            datetime.datetime.now().strftime("%H:%M:%S")
        )
    )

    # check if the database already exists. compare dates and update if neccessary.
    check_database()
