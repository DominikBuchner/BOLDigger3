import datetime, sys, time, random, more_itertools, requests_html_playwright, json, gzip
from tqdm import tqdm
from pathlib import Path
from Bio import SeqIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.sync_api import sync_playwright
from playwright._impl._errors import TimeoutError
from io import BytesIO


# function to read the fasta file to identify into a dictionary
def parse_fasta(fasta_path: str) -> tuple:
    """Function to read a fasta file and parse it into a dictionary.

    Args:
        fasta_path (str): Path to the fasta file to be identified.

    Returns:
        tuple: Data of the fasta file in a dict, the full path to the fasta file, the directory where this fasta file is located.
    """
    # extract the directory from the fasta path
    fasta_path = Path(fasta_path)
    fasta_name = fasta_path.stem
    project_directory = fasta_path.parent

    # use SeqIO to read the data into dict- automatically check fir the type of fasta
    fasta_dict = SeqIO.to_dict(SeqIO.parse(fasta_path, "fasta"))

    # trim header to maximum allowed chars of 99. names are preserved in the SeqRecord object
    fasta_dict = {key[:99]: value for key, value in fasta_dict.items()}

    # create a set of all valid DNA characters
    valid_chars = {
        "A",
        "C",
        "G",
        "T",
        "M",
        "R",
        "W",
        "S",
        "Y",
        "K",
        "V",
        "H",
        "D",
        "B",
        "X",
        "N",
    }

    # check all sequences for invalid characters or too short sequences
    raise_invalid_fasta = False

    for key in fasta_dict.keys():
        if len(fasta_dict[key].seq) < 80:
            print(
                "{}: Sequence {} is too short (< 80 bp).".format(
                    datetime.datetime.now().strftime("%H:%M:%S"), key
                )
            )
            raise_invalid_fasta = True

        # check if the sequences contain invalid chars
        elif not set(fasta_dict[key].seq.upper()).issubset(valid_chars):
            print(
                "{}: Sequence {} contains invalid characters.".format(
                    datetime.datetime.now().strftime("%H:%M:%S"), key
                )
            )
            raise_invalid_fasta = True

    if not raise_invalid_fasta:
        return fasta_dict, fasta_name, project_directory
    else:
        sys.exit()


# function to build the base urls and params
def build_url_params(database: int, operating_mode: int) -> tuple:
    """Function that generates a base URL and the params for the POST request to the ID engine.

    Args:
        database (int): Between 1 and 7 referring to the database, see readme for details.
        operating_mode (int): Between 1 and 3 referring to the operating mode, see readme for details

    Returns:
        tuple: Contains the base URL as str and the params as dict
    """

    # the database int is translated here
    idx_to_database = {
        1: "public.bin-tax-derep",
        2: "species",
        3: "all.bin-tax-derep",
        4: "DS-CANREF22",
        5: "public.plants",
        6: "public.fungi",
        7: "all.animal-alt",
    }

    # the operating mode is translated here
    idx_to_operating_mode = {
        1: {"mi": 0.94, "maxh": 25},
        2: {"mi": 0.9, "maxh": 50},
        3: {"mi": 0.85, "maxh": 100},
    }

    # params can be calculated from the database and operating mode
    params = {
        "db": idx_to_database[database],
        "mi": idx_to_operating_mode[operating_mode]["mi"],
        "mo": 100,
        "maxh": idx_to_operating_mode[operating_mode]["maxh"],
        "order": 3,
    }

    # format the base url
    base_url = "https://id.boldsystems.org/submission?db={}&mi={}&mo={}&maxh={}&order={}".format(
        params["db"], params["mi"], params["mo"], params["maxh"], params["order"]
    )

    return base_url, params


# function to send all POST requests to the BOLD id engine API
def build_post_requests(fasta_dict: dict, base_url: str, params: dict) -> list:
    """Function to send the POST request for the dataset to the BOLD id engine.

    Args:
        fasta_dict (dict): Dict that holds the fasta data.
        base_url (str): base_url for the id engine post request
        params (dict): params for the id engine post request

    Returns:
        list: list of result URLs where the results can be fetched later
    """
    # determine the query size from the params
    query_size_dict = {0.94: 1000, 0.9: 200, 0.85: 100}
    query_size = query_size_dict[params["mi"]]

    # split the fasta dict in query sized chunks
    query_data = more_itertools.chunked(fasta_dict.keys(), query_size)

    # produce a generator that holds all sequence and key data to loop over for the post requests
    query_generators = (
        (">{}\n{}\n".format(key, fasta_dict[key].seq) for key in query_subset)
        for query_subset in query_data
    )

    # send the post requests
    with requests_html_playwright.HTMLSession() as session:

        # build a retry strategy for the html session
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36"
            }
        )
        retry_strategy = Retry(total=10, backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)

        # gather result urls here
        results_urls = []

        with tqdm(desc="Queuing sequences in ID engine", total=len(fasta_dict)) as pbar:
            for query_string in query_generators:
                # generate the data string
                data = "".join(query_string)

                # generate the files to send via the id engine
                files = {"file": ("submitted.fas", data, "text/plain")}

                base_url = "https://id.boldsystems.org/submission?db=public.bin-tax-derep&mi=0.94&mo=100&maxh=25&order=3"
                params = {
                    "db": "public.bin-tax-derep",
                    "mi": 0.94,
                    "mo": 100,
                    "maxh": 25,
                    "order": 3,
                }

                # submit the post request
                response = session.post(base_url, params=params, files=files)

                # wait for a moment to not overwhelm the id engine API
                time.sleep(random.uniform(1, 3))

                # fetch the result and build result urls from it
                result = json.loads(response.text)
                result_url = "https://id.boldsystems.org/results/{}".format(
                    result["sub_id"]
                )

                # append the resulting url
                results_urls.append(result_url)
                # update the progress bar
                pbar.update(len(data.split(">")) - 1)

    # return the result urls
    return results_urls


# function to download the results as json
def download_json(results_urls: list) -> list:
    """Function to download the JSON Results from the BOLD id engine download URLs

    Args:
        results_urls (list): List of download urls.

    Returns:
        list: List of download urls for the JSON results.
    """
    # start a headless playwright session to render the javascript
    # no async code needed since waiting for the rendering is required anyways
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        download_urls = []

        # define variables for user output
        total_downloads = len(results_urls)
        counter = 1

        # loop over all results urls
        with tqdm(
            total=len(results_urls), desc="Collecting JSON download links"
        ) as pbar:
            while results_urls:
                # select a random resultpage
                url = random.choice(results_urls)
                # open it with the browser to check if results are visible
                page.goto(url)
                try:
                    page.wait_for_selector("#jsonlResults", timeout=20000)
                    download_url = page.query_selector("#jsonlResults").get_attribute(
                        "href"
                    )
                    download_urls.append(download_url)
                    results_urls.remove(url)
                    pbar.update(1)
                    # user output
                    tqdm.write(
                        "{}: Result {} of {} collected.".format(
                            datetime.datetime.now().strftime("%H:%M:%S"),
                            counter,
                            total_downloads,
                            float(counter / total_downloads),
                        )
                    )
                    counter += 1
                except TimeoutError:
                    continue

    return download_urls


def download_and_parse(download_urls: list, hdf_name_results: str) -> None:
    """This function downloads and parses the JSON from the result urls and stores it in the hdf storage

    Args:
        download_urls (list): List of urls to JSON download links.
        hdf_name_results_str (_type_): Name of the hdf storage to write to.
    """
    pass


def main(fasta_path: str, database: int, operating_mode: int) -> None:
    """Main function to run the BOLD identification engine.

    Args:
        fasta_path (str): Path to the fasta file.
        database (int): The database to use. Can be database 1-7, see readme for details.
        operating_mode (int): The operating mode to use. Can be 13, see readme for details.
    """
    # user output
    tqdm.write(
        "{}: Reading input fasta.".format(datetime.datetime.now().strftime("%H:%M:%S"))
    )

    # read the input fasta
    fasta_dict, fasta_name, project_directory = parse_fasta(fasta_path)

    # generate a new for the hdf storage to store the downloaded data
    hdf_name_results = project_directory.joinpath(
        "{}_result_storage.h5.lz".format(fasta_name)
    )

    # user output
    tqdm.write(
        "{}: Generating requests.".format(datetime.datetime.now().strftime("%H:%M:%S"))
    )

    # generate the base URL and params for the post request
    base_url, params = build_url_params(database, operating_mode)

    # post requests to BOLD id engine API, collect the results urls
    results_urls = build_post_requests(fasta_dict, base_url, params)

    # user output
    tqdm.write(
        "{}: Waiting for results to load.".format(
            datetime.datetime.now().strftime("%H:%M:%S")
        )
    )

    # collect links to download the json reports
    download_urls = download_json(results_urls)

    # download and parse the data to hdf storage


# main("C:\\Users\\Dominik\\Documents\\GitHub\\BOLDigger3\\tests\\test_1000.fasta", 1, 2)
with requests_html_playwright.HTMLSession() as session:
    r = session.get(
        "https://us-east-1.linodeobjects.com/blastnode-idengine-output/submitted.db_public.bin-tax-derep~mi_0.94~mo_100~maxh_25~order_3-660f3c7fda89423ebfd4101aa4ff5a50.jsonl.gz?AWSAccessKeyId=TCUH3DQWRDLV575HRQO2&Signature=JsbjwLWUY7vRONi9SwWw%2Bz7pf8o%3D&Expires=1729780831",
    )
    r = gzip.decompress(r.content)
    content_str = r.decode("utf-8")

    for line in content_str.splitlines():
        record = json.loads(line)
        print(record)
