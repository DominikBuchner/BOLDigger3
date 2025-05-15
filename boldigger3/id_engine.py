import datetime, sys, time, random, more_itertools, requests_html_playwright, json, gzip, pickle, os
import numpy as np
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from Bio import SeqIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.sync_api import sync_playwright
from playwright._impl._errors import TimeoutError
from collections import OrderedDict
from boldigger3.exceptions import DownloadFinished
from json.decoder import JSONDecodeError


class BoldIdRequest:
    """A class to represent the data for a BOLD id engine request

    Attributes
    ----------

    Methods
    -------
    """

    def __init__(
        self,
        # base_url: str,
        # params: dict,
        # query_generator: object,
        # timestamp: object,
        # result_url: str,
        # queued: int,
        # processing: int,
        # completed: int,
    ):
        """Constructs the neccessary attribues for the BoldIdRequest object

        Args:
            base_url (str): Represents the base url for the post request
            params (dict): The parameters to send with the post request
            query_generator (object): A generator holding the data to send with the post request
            timestamp (object): Timestamp that is set when the request is sent to BOLD
            result_url (str): The result url to download the data from
            queued (int): Queries that are queued
            processing (int): Queries that are processing
            completed (int): Queries that are completed
        """
        self.base_url = ""
        self.params = {}
        self.query_data = []
        self.result_url = ""
        self.timestamp = None
        self.database = None
        self.operating_mode = None
        self.download_url = ""
        self.queued = ""
        self.processing = ""
        self.completed = ""


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
    fasta_dict = {key: value for key, value in fasta_dict.items()}

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

    # check all sequences for invalid characters
    raise_invalid_fasta = False

    # check if the sequences contain invalid chars
    for key in fasta_dict.keys():
        if not set(fasta_dict[key].seq.upper()).issubset(valid_chars):
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
        # only collect the ids from the hdf as and iterator
        idx_data = pd.read_hdf(
            hdf_name_results,
            key="results_unsorted",
            columns=["id"],
            iterator=True,
            chunksize=1000000,
        )

        # define the idx set to collect from hdf
        idx = set()

        # loop over the chunks and collect the ids
        for chunk in idx_data:
            idx = idx.union(set(chunk["id"].to_list()))

        # remove those ids from the fasta dict
        fasta_dict = {id: seq for (id, seq) in fasta_dict.items() if id not in idx}

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
        8: "DS-IUCNPUB",
    }

    # the operating mode is translated here
    idx_to_operating_mode = {
        1: {"mi": 0.94, "maxh": 25},
        2: {"mi": 0.9, "maxh": 50},
        3: {"mi": 0.75, "maxh": 100},
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


# function to build the download queue
def build_download_queue(
    fasta_dict: dict, download_queue_name: str, database: int, operating_mode: int
) -> None:
    """Function to build the download queue.

    Args:
        fasta_dict (dict): Dict that holds the data in the fasta file.
        download_queue_name (str): String that holds the path where the download queue is saved.
        database (int): Between 1 and 7 referring to the database, see readme for details.
        operating_mode (int): Between 1 and 3 referring to the operating mode, see readme for details

    """
    # initialize the download queue
    download_queue = {"waiting": OrderedDict(), "active": dict()}

    # build the base url and the params
    base_url, params = build_url_params(database, operating_mode)

    # determine the query size from the params
    query_size_dict = {0.94: 1000, 0.9: 200, 0.75: 100}
    query_size = query_size_dict[params["mi"]]

    # split the fasta dict in query sized chunks
    query_data = more_itertools.chunked(fasta_dict.keys(), query_size)

    # produce a generator that holds all sequence and key data to loop over for the post requests
    query_generators = (
        [">{}\n{}\n".format(key, fasta_dict[key].seq) for key in query_subset]
        for query_subset in query_data
    )

    for idx, query_generator in enumerate(query_generators, start=1):
        # initialize the bold id engine request
        bold_request = BoldIdRequest()
        bold_request.base_url = base_url
        bold_request.params = params
        bold_request.query_data = query_generator
        bold_request.database = database
        bold_request.operating_mode = operating_mode
        download_queue["waiting"][idx] = bold_request

    return download_queue


# function to send a post request to the BOLD id engine
def build_post_request(BoldIdRequest: object) -> object:
    """Function to send the POST request for the dataset to the BOLD id engine.

    Args:
        bold_id_request (object): A BoldIdRequest object that holds all the information needed to send the request

    Returns:
        object: Returns the BoldIdRequest object with an added result url
    """
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

        data = "".join(BoldIdRequest.query_data)

        # generate the files to send via the id engine
        files = {"file": ("submitted.fas", data, "text/plain")}

        while True:
            try:
                # submit the post request
                response = session.post(
                    BoldIdRequest.base_url, params=BoldIdRequest.params, files=files
                )

                # fetch the result
                result = json.loads(response.text)
                break
            except JSONDecodeError:
                # user output
                tqdm.write(
                    "{}: Building the request failed. Waiting 60 seconds for repeat.".format(
                        datetime.datetime.now().strftime("%H:%M:%S")
                    )
                )
                # wait 60 seconds
                time.sleep(60)

        result_url = "https://id.boldsystems.org/processing/{}".format(result["sub_id"])

        # append the resulting url
        BoldIdRequest.result_url = result_url
        BoldIdRequest.timestamp = datetime.datetime.now()

        # return the BoldIDRequest object
        return BoldIdRequest


def download_and_parse(
    BoldIdRequest: object,
    hdf_name_results: str,
    html_session: object,
) -> None:
    """This function downloads and parses the JSON from the result urls and stores it in the hdf storage

    Args:
        BoldIdRequest (object): BoldIdRequest object that holds all the data of the current request
        hdf_name_results_str (str): Name of the hdf storage to write to.
        html_session (object): session object to perform the download.
    """
    response = html_session.get(BoldIdRequest.download_url)
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
        json_record_results["database"] = BoldIdRequest.database
        json_record_results["operating_mode"] = BoldIdRequest.operating_mode

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


def download_json(
    active_queue: dict,
    hdf_name_results: str,
):
    """Function to download JSON results from the id engine and store it in hdf format

    Args:
        active_queue (dict): Queue with activebold requests
        hdf_name_results (str): name of the hdf to save the results to
        database (int): database that was queried
        operating_mode (int): operating mode that was used for the BOLD query
        request_id (int):
    """

    # start a headless playwright session to render the javascript
    # no async code needed since waiting for the rendering is required anyways
    with sync_playwright() as p:
        with requests_html_playwright.HTMLSession() as session:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            # loop over the active cue until any request is finished.
            while active_queue:
                for key in active_queue.keys():
                    url = active_queue[key].result_url
                    try:
                        # open the url with the browser to check if the results are fully loaded
                        page.goto(url, timeout=20000)
                    except TimeoutError:
                        continue

                    # try to find the jsonlResults selector
                    try:
                        page.wait_for_selector("#jsonlResults", timeout=20000)

                        download_url = page.query_selector(
                            "#jsonlResults"
                        ).get_attribute("href")
                        # add the download url to the BoldRequest object
                        active_queue[key].download_url = download_url
                        # parsing and download function here
                        download_and_parse(active_queue[key], hdf_name_results, session)
                        # remove the key from the dict
                        active_queue.pop(key)
                        # give user output
                        tqdm.write(
                            "{}: Request ID {} has successfully been downloaded.".format(
                                datetime.datetime.now().strftime("%H:%M:%S"), key
                            )
                        )
                        # return the active queue
                        return active_queue
                    except TimeoutError:
                        try:
                            # give user output and update if it is not found
                            queued = page.query_selector(
                                "#progress-queued"
                            ).text_content()
                            processing = page.query_selector(
                                "#progress-processing"
                            ).text_content()
                            completed = page.query_selector(
                                "#progress-completed"
                            ).text_content()
                            # add everything to the active cue object
                            active_queue[key].queued = queued
                            active_queue[key].processing = processing
                            active_queue[key].completed = completed

                            # give user output
                            tqdm.write(
                                "{}: Status of request ID {}: {}, {}, {}.".format(
                                    datetime.datetime.now().strftime("%H:%M:%S"),
                                    key,
                                    active_queue[key].queued,
                                    active_queue[key].processing,
                                    active_queue[key].completed,
                                )
                            )
                        except AttributeError:
                            continue


def main(fasta_path: str, database: int, operating_mode: int) -> None:
    """Main function to run the BOLD identification engine.

    Args:
        fasta_path (str): Path to the fasta file.
        database (int): The database to use. Can be database 1-8, see readme for details.
        operating_mode (int): The operating mode to use. Can be 1-4, see readme for details.
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

    # generate a name for the download queue
    download_queue_name = project_directory.joinpath(
        "{}_download_queue.pkl".format(fasta_name)
    )

    # check if any data has been downloaded already
    fasta_dict = already_downloaded(fasta_dict, hdf_name_results)

    # if all data has already been downloaded return to stop the function
    if not fasta_dict:
        tqdm.write(
            "{}: All data has already been downloaded.".format(
                datetime.datetime.now().strftime("%H:%M:%S")
            )
        )
        return None

    try:
        with open(download_queue_name, "rb") as download_queue_file:
            download_queue = pickle.load(download_queue_file)
            # user output
            tqdm.write(
                "{}: Found unfinished downloads from previous runs. Continueing download.".format(
                    datetime.datetime.now().strftime("%H:%M:%S")
                )
            )
    except FileNotFoundError:
        # build the download queue
        tqdm.write(
            "{}: Building the download queue.".format(
                datetime.datetime.now().strftime("%H:%M:%S")
            )
        )
        download_queue = build_download_queue(
            fasta_dict, download_queue_name, database, operating_mode
        )
        with open(download_queue_name, "wb") as download_queue_file:
            pickle.dump(download_queue, download_queue_file)
        tqdm.write(
            "{}: Added {} requests to the download queue.".format(
                datetime.datetime.now().strftime("%H:%M:%S"),
                len(download_queue["waiting"]),
            )
        )

    # calculate the total amounts of downloads
    total_downloads = len(download_queue["waiting"]) + len(download_queue["active"])

    # as long as there is anything waiting or active in the download queue, keep the download running
    with tqdm(total=total_downloads, desc="Finished downloads") as pbar:
        while True:
            try:
                if download_queue["waiting"] or download_queue["active"]:
                    # as long as there are not 5 active requests in the download queue
                    # move on request from the waiting queue to the active queue
                    if len(download_queue["active"]) < 4 and download_queue["waiting"]:
                        # retrieve one request from the waiting queue
                        request_id, current_request_object = download_queue[
                            "waiting"
                        ].popitem(last=False)
                        tqdm.write(
                            "{}: Request ID {} has been moved to the active downloads.".format(
                                datetime.datetime.now().strftime("%H:%M:%S"),
                                request_id,
                            )
                        )
                        # add this request to the active queue
                        download_queue["active"][request_id] = build_post_request(
                            current_request_object
                        )
                    # check if any of the active queue objects has finished and can be saved and removed
                    else:
                        # check if any of the active downloads has been finished
                        download_queue["active"] = download_json(
                            download_queue["active"], hdf_name_results
                        )
                        # update the progress bar
                        pbar.update(1)
                    # after every iteration override the download queue as it is changes along the way
                    with open(download_queue_name, "wb") as out_stream:
                        pickle.dump(download_queue, out_stream)
                # if all downloads are finished raise download finished flag
                else:
                    raise DownloadFinished
            except DownloadFinished:
                # check if all downloads are finished: if yes: delete download queue, break the loop
                fasta_dict = already_downloaded(fasta_dict, hdf_name_results)
                # if there is any unfinished download, requeue
                if fasta_dict:
                    tqdm.write(
                        "{}: Requeuing incomplete downloads.".format(
                            datetime.datetime.now().strftime("%H:%M:%S"),
                            request_id,
                        )
                    )
                    download_queue = build_download_queue(
                        fasta_dict, download_queue_name, database, operating_mode
                    )
                    # recalculate the total downloads
                    total_downloads = len(download_queue["active"]) + len(
                        download_queue["waiting"]
                    )
                    # reset the progress bar for the second round of downloads
                    pbar.reset()
                    pbar.total(total_downloads)
                    pbar.refresh()
                else:
                    tqdm.write(
                        "{}: All downloads finished successfully.".format(
                            datetime.datetime.now().strftime("%H:%M:%S"),
                            request_id,
                        )
                    )
                    # finally remove the download queue
                    os.remove(download_queue_name)
                    break
