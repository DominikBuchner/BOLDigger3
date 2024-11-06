import datetime, sys, time, random, more_itertools, requests_html_playwright, json, gzip
import pandas as pd
import numpy as np
from tqdm import tqdm
from pathlib import Path
from Bio import SeqIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.sync_api import sync_playwright
from playwright._impl._errors import TimeoutError


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


# function to check is some of the sequences have already been downloaded
def already_downloaded(fasta_dict: dict, hdf_name_results: str) -> dict:
    """Funtion to check if any of the sequences have already been downloaded.

    Args:
        fasta_dict (dict): The dictionary with the fasta data.
        hdf_name_results (str): The savename of the hdf data storage.

    Returns:
        dict: The dictionary with the fasta data with already downloaded sequences removed.
    """
    # try to open the hdf file
    try:
        # only collect the ids from the hdf
        idx = pd.read_hdf(hdf_name_results, key="results_unsorted")["id"]

        # remove those ids from the fasta dict
        fasta_dict = {id: seq for (id, seq) in fasta_dict.items() if id not in set(idx)}

        # return the updated fasta dict
        return fasta_dict
    except FileNotFoundError:
        # return the fasta dict unchanged
        return fasta_dict


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


def download_and_parse(
    download_url: list,
    hdf_name_results: str,
    html_session: object,
    database: int,
    operating_mode: int,
) -> None:
    """This function downloads and parses the JSON from the result urls and stores it in the hdf storage

    Args:
        download_urls (list): URL to download the JSON result
        hdf_name_results_str (str): Name of the hdf storage to write to.
        html_session (object): session object to perform the download.
        database (int): database that was queried
        operating_mode (int): operating mode for the BOLD query
    """
    response = html_session.get(download_url)
    response = gzip.decompress(response.content)
    content_str = response.decode("utf-8")

    # store the output dataframe here
    output_dataframe = pd.DataFrame()

    for json_record in content_str.splitlines():
        # save the results here
        json_record_results = []

        json_record = json.loads(json_record)
        # extract the sequence id first
        sequence_id = json_record["seqid"]

        # extract the results for this seq id
        results = json_record.get("results")

        # only parse if results are not empty
        if results:
            # the keys of the results are the process id|primer|bin_uri|x|x
            for key in results.keys():
                process_id, bin_uri = key.split("|")[0], key.split("|")[2]
                pident = results[key].get("pident", np.nan)
                # extract the taxonomy
                taxonomy = results.get(key).get("taxonomy", {})
                taxonomy = [
                    taxonomy.get(taxonomic_level)
                    for taxonomic_level in [
                        "phylum",
                        "class",
                        "order",
                        "family",
                        "genus",
                        "species",
                    ]
                ]

                json_record_results.append(
                    taxonomy + [pident] + [process_id] + [bin_uri]
                )
        else:
            json_record_results.append(["no-match"] * 6 + [0] + [""] + [""])

        # transform the record to dataframe to add it to the hdf storage
        json_record_results = pd.DataFrame(
            data=json_record_results,
            columns=[
                "Phylum",
                "Class",
                "Order",
                "Family",
                "Genus",
                "Species",
                "pct_identity",
                "process_id",
                "bin_uri",
            ],
        )

        # add the sequence id and the timestamp
        json_record_results.insert(0, column="id", value=sequence_id)
        json_record_results["request_date"] = pd.Timestamp.now().strftime("%Y-%m-%d %X")
        json_record_results["pct_identity"] = json_record_results[
            "pct_identity"
        ].astype("float64")

        # add the database and the operating mode
        json_record_results["database"] = database
        json_record_results["operating_mode"] = operating_mode

        # fill emtpy values with strings to make compatible with hdf
        json_record_results.fillna("")

        # append to the output dataframe
        output_dataframe = pd.concat([output_dataframe, json_record_results], axis=0)

    # add the results to the hdf storage
    # set size limits for the columns
    item_sizes = {
        "id": 100,
        "Phylum": 80,
        "Class": 80,
        "Order": 80,
        "Family": 80,
        "Genus": 80,
        "Species": 80,
        "process_id": 25,
        "bin_uri": 25,
        "request_date": 30,
        "database": 5,
        "operating_mode": 5,
    }

    with pd.HDFStore(
        hdf_name_results, mode="a", complib="blosc:blosclz", complevel=9
    ) as hdf_output:
        hdf_output.append(
            key="results_unsorted",
            value=output_dataframe,
            format="t",
            data_columns=True,
            min_itemsize=item_sizes,
            complib="blosc:blosclz",
            complevel=9,
        )


# function to download the results as json
def download_json(
    results_urls: list, hdf_name_results: str, database: int, operating_mode: int
):
    """Function to download the JSON Results from the BOLD id engine download URLs

    Args:
        results_urls (list): List of download urls.
        hdf_name_results (str): Name of the hdf storage to write to.
        database (int): database that was queried
        operating_mode (int): operating mode for the BOLD query
    """
    # start a headless playwright session to render the javascript
    # no async code needed since waiting for the rendering is required anyways
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # define variables for user output
        total_downloads = len(results_urls)
        counter = 1

        # loop over all results urls
        with tqdm(total=len(results_urls), desc="Downloading JSON data") as pbar:
            with requests_html_playwright.HTMLSession() as session:
                while results_urls:
                    # select a random resultpage
                    url = random.choice(results_urls)
                    # open it with the browser to check if results are visible
                    page.goto(url, timeout=20000)
                    try:
                        page.wait_for_selector("#jsonlResults", timeout=60000)

                        download_url = page.query_selector(
                            "#jsonlResults"
                        ).get_attribute("href")
                        # parsing and download function here
                        download_and_parse(
                            download_url,
                            hdf_name_results,
                            session,
                            database,
                            operating_mode,
                        )

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

    # count the total download loops
    download_loop = 1

    # repeat the download for failed requests
    while fasta_dict:
        # function to check if any of the sequences has already been downloaded
        fasta_dict = already_downloaded(fasta_dict, hdf_name_results)

        if download_loop > 1:
            # user output
            tqdm.write(
                "{}: Checking for failed requests.".format(
                    datetime.datetime.now().strftime("%H:%M:%S")
                )
            )

        if fasta_dict:
            # user output
            tqdm.write(
                "{}: Generating requests.".format(
                    datetime.datetime.now().strftime("%H:%M:%S")
                )
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
            download_json(results_urls, hdf_name_results, database, operating_mode)

            # increase the download loops
            download_loop += 1

    tqdm.write(
        "{}: Data download finished.".format(
            datetime.datetime.now().strftime("%H:%M:%S")
        )
    )
