import datetime, duckdb, sys, pickle, more_itertools, requests_html, json, time, os, 
import pandas as pd
from Bio import SeqIO
from pathlib import Path
from tqdm import tqdm
from collections import OrderedDict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from boldigger3.exceptions import DownloadFinished
from json.decoder import JSONDecodeError
from requests.exceptions import ReadTimeout


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
        # last_checked: object
    ):
        """Constructs the neccessary attribues for the BoldIdRequest object

        Args:
            base_url (str): Represents the base url for the post request
            params (dict): The parameters to send with the post request
            query_generator (object): A generator holding the data to send with the post request
            timestamp (object): Timestamp that is set when the request is sent to BOLD
            result_url (str): The result url to download the data from
            last_checked (object): The last time the download url was checked for updates

        """
        self.base_url = ""
        self.params = {}
        self.query_data = []
        self.result_url = ""
        self.timestamp = None
        self.database = None
        self.operating_mode = None
        self.download_url = ""
        self.last_checked = None


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
                f"{datetime.datetime.now().strftime('%H:%M:%S')}: Sequence {key} contains invalid characters."
            )
            raise_invalid_fasta = True

    if not raise_invalid_fasta:
        return fasta_dict, fasta_name, project_directory
    else:
        sys.exit()


def already_downloaded(fasta_dict: dict, database_path: str) -> dict:
    """Function to check if any of the requests has been downloaded and stored in the duckdb database.

    Args:
        fasta_dict (dict): The dictionary with the fasta data.
        database_path (str): Path to the duckdb database

    Returns:
        dict: The dictionary with the fasta data with already downloaded sequences removed.
    """
    if database_path.is_file():
        # connect to the database
        database = duckdb.connect(database_path)

        # collect all unique values from the id column
        downloaded_ids = database.execute(
            "SELECT DISTINCT id FROM id_engine_results"
        ).fetchall()

        # close the connection
        database.close()

        # flatten the downloaded ids into a set for faster lookup
        downloaded_ids = {row[0] for row in downloaded_ids}

        # filter the fasta_dict
        fasta_dict = {
            id: seq for (id, seq) in fasta_dict.items() if id not in downloaded_ids
        }

        # return the updated dict
        return fasta_dict
    else:
        # return the fasta dict unchanged
        return fasta_dict


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
        1: "public.tax-derep",
        2: "species",
        3: "all.tax-derep",
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
    base_url = f"https://id.boldsystems.org/submission?db={params['db']}&mi={params['mi']}&mo={params['mo']}&maxh={params['maxh']}&order={params['order']}"

    return base_url, params


def build_download_queue(fasta_dict: dict, database: int, operating_mode: int) -> dict:
    """Function to build the download queue.

    Args:
        fasta_dict (dict): Dict that holds the data in the fasta file.
        download_queue_name (str): String that holds the path where the download queue is saved.
        database (int): Between 1 and 7 referring to the database, see readme for details.
        operating_mode (int): Between 1 and 3 referring to the operating mode, see readme for details

    Returns:
        dict: The dictionary with the downloaded queue

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
        [f">{key}\n{fasta_dict[key].seq}\n" for key in query_subset]
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


def build_post_request(BoldIdRequest: object) -> object:
    """Function to send the POST request for the dataset to the BOLD id engine.

    Args:
        bold_id_request (object): A BoldIdRequest object that holds all the information needed to send the request

    Returns:
        object: Returns the BoldIdRequest object with an added result url
    """
    # send the post requests
    with requests_html.HTMLSession() as session:

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
                    BoldIdRequest.base_url,
                    params=BoldIdRequest.params,
                    files=files,
                    timeout=10000,
                )

                # fetch the result
                result = json.loads(response.text)
                break
            except (JSONDecodeError, ReadTimeout):
                # user output
                tqdm.write(
                    f"{datetime.datetime.now().strftime('%H:%M:%S')}: Building the request failed. Waiting 60 seconds for repeat."
                )
                # wait 60 seconds
                time.sleep(60)

        result_url = f"https://id.boldsystems.org/submission/results/{result['sub_id']}"

        # append the resulting url
        BoldIdRequest.result_url = result_url
        BoldIdRequest.timestamp = datetime.datetime.now()

        return BoldIdRequest


def add_no_match(result: object, BoldIdRequest: object, fasta_order: dict) -> dict:
    """Function to add a no match in case BOLD does not return any results

    Args:
        result (object): JSON returned from BOLD.
        BoldIdRequest (object): Id Request object.
        fasta_order (dict): Order of the original fasta file

    Returns:
        dict: A no match row for the dataframe
    """
    seq_id = result["seqid"]

    row = {
        "id": seq_id,
        "phylum": "no-match",
        "class": "no-match",
        "order": "no-match",
        "family": "no-match",
        "subfamily": "no-match",
        "genus": "no-match",
        "species": "no-match",
        "taxid_count": 0.0,
        "pct_identity": 0.0,
        "process_id": "",
        "bin_uri": "",
        "request_date": pd.Timestamp.now().strftime("%Y-%m-%d %X"),
        "database": BoldIdRequest.database,
        "operating_mode": BoldIdRequest.operating_mode,
        "status": "",
        "fasta_order": fasta_order[seq_id],
    }

    return row


def safe_status(record_key, index, default=""):
    return record_key[index] if len(record_key) > index else default


def parse_and_save_data(
    BoldIdRequest: object,
    response: object,
    fasta_order: dict,
    request_id: int,
    database_path: str,
):
    """Function to parse the JSON returned by BOLD and save it as parquet.

    Args:
        BoldIdRequest (object): BoldIdRequest object.
        response (object): http response to parse.
        fasta_order (dict): Order of the original fasta file, can be used to order the table after metadata addition.
        request_id (int): Request id, used to save the file.
        database_path (str): Path to the database to write to.
    """
    # parse out all objects from the response
    json_objects = [json.loads(line) for line in response.iter_lines()]

    # store the resulting tables rows here when parsing the jsons
    rows = []

    for result in json_objects:
        # handle no-matches here
        if not result["results"]:
            # create a no-match hit
            row = add_no_match(result, BoldIdRequest, fasta_order)
            rows.append(row)
            continue

        # parse the json
        seq_id = result["seqid"]

        for record_key, record_data in result["results"].items():
            record_key = record_key.split("|")

            row = {
                "id": seq_id,
                "process_id": record_key[0],
                "bin_uri": record_key[2],
                "status": safe_status(record_key, 4),
                "pct_identity": record_data.get("pident"),
            }

            taxonomy = record_data.get("taxonomy", {})
            row.update(taxonomy)
            rows.append(row)

    # transform to dataframe, drop unused labels, add ordering column, use correct type annotations
    id_engine_result = pd.DataFrame(rows)
    id_engine_result = id_engine_result.drop(
        labels=["subfamily", "taxid_count"], axis=1
    )
    id_engine_result["pct_identity"] = id_engine_result["pct_identity"].astype(
        "float64"
    )
    id_engine_result["request_date"] = pd.Timestamp.now().strftime("%Y-%m-%d %X")
    id_engine_result["database"] = BoldIdRequest.database
    id_engine_result["operating_mode"] = BoldIdRequest.operating_mode
    id_engine_result["fasta_order"] = id_engine_result["id"].map(fasta_order)

    # reorder the columns
    id_engine_result = id_engine_result[
        [
            "id",
            "phylum",
            "class",
            "order",
            "family",
            "genus",
            "species",
            "pct_identity",
            "process_id",
            "bin_uri",
            "request_date",
            "database",
            "operating_mode",
            "status",
            "fasta_order",
        ]
    ]

    # finally stream to parquet to later load into duckdb, update the active queue
    output_file = database_path.joinpath(
        "boldigger3_data", f"request_id_{request_id}.parquet.snappy"
    )

    id_engine_result.to_parquet(output_file)


def download_json(active_queue: dict, fasta_order: dict, project_directory: str):
    """Function to download the JSON results from the id engine and store them in temporary parquet files.

    Args:
        active_queue (dict): Queue with active BOLD requests.
        fasta_order (dict): Dict that can be appended to the results. Needed for creating a sorted duckdb table later.
    """
    # initialize the last check's for the active queue
    for key in active_queue.keys():
        if not active_queue[key].last_checked:
            active_queue[key].last_checked = datetime.datetime.now()

    # check the requests every ten seconds
    with requests_html.HTMLSession() as session:
        # loop over the active queue until any request is finished
        while active_queue:
            for key in active_queue.keys():
                now = datetime.datetime.now()
                # if the url has not been checked in the last 10 seconds, check again, else skip the url
                if now - active_queue[key].last_checked > datetime.timedelta(
                    seconds=15
                ):
                    response = session.get(active_queue[key].result_url)
                    print(active_queue[key].result_url)
                else:
                    continue
                # if there's no data in the response yet, continue
                if response.status_code == 404:
                    active_queue[key].last_checked = datetime.datetime.now()
                    continue
                # if timing is sufficient AND data in the response, parse the response
                else:
                    # parse the response here and save, update the active queue
                    parse_and_save_data(
                        active_queue[key], response, fasta_order, key, project_directory
                    )
                    active_queue.pop(key)

                    # give user output
                    tqdm.write(
                        f"{datetime.datetime.now().strftime('%H:%M:%S')}: Request ID {key} has successfully been downloaded."
                    )

                    # return the active queue
                    return active_queue


def parquet_to_duckdb(project_directory, database_path):
    """Function to stream the parquet output to duckdb.

    Args:
        project_directory (str): Project directory to work in.
        database_path (str): Path to the database.
    """
    # check if duck db already exists
    db_exists = database_path.exists()

    # fetch the parquet path
    parquet_path = project_directory.joinpath("boldigger3_data", "*.parquet.snappy")

    # connect to the database
    database = duckdb.connect(database_path)

    # insert is db exists already, create table if not
    if not db_exists:
        # Create the table
        database.execute(
            f"""
            CREATE TABLE id_engine_results AS
            SELECT * FROM read_parquet('{parquet_path}')
        """
        )
    else:
        database.execute(
            f"""
        INSERT INTO id_engine_results
        SELECT * FROM read_parquet('{parquet_path}')
        """
        )

    # remove all parquet files if finished successfully
    for file in project_directory.joinpath("boldigger3_data").glob("*.parquet.snappy"):
        if file.is_file():
            file.unlink()


def main(fasta_path: str, database: int, operating_mode: int) -> None:
    """Main function to run the BOLD identification engine.

    Args:
        fasta_path (str): Path to the fasta file.
        database (int): The database to use. Can be database 1-8, see readme for details.
        operating_mode (int): The operating mode to use. Can be 1-4, see readme for details.
    """
    # user output
    tqdm.write(f"{datetime.datetime.now().strftime('%H:%M:%S')}: Reading input fasta.")

    # read the input fasta into memory
    fasta_dict, fasta_name, project_directory = parse_fasta(fasta_path)

    # define the order of the fasta dict, can be used to add an order column in the output
    fasta_dict_order = {key: idx for idx, key in enumerate(fasta_dict.keys())}

    # define a name for the duckdb database where the downloaded data will be stored
    database_path = project_directory.joinpath(
        "boldigger3_data", f"{fasta_name}.duckdb"
    )

    # generate a name for the download queue
    download_queue_name = project_directory.joinpath(
        "boldigger3_data", "{}_download_queue.pkl".format(fasta_name)
    )

    # generate a data directory to save the data to, so the working directory won't be cluttered
    data_dir = project_directory.joinpath("boldigger3_data")
    data_dir.mkdir(exist_ok=True)

    # check if any data has been downloaded yet
    fasta_dict = already_downloaded(fasta_dict, database_path)

    # if all data has already been downloaded return to stop the function
    if not fasta_dict:
        tqdm.write(
            "{}: All data has already been downloaded.".format(
                datetime.datetime.now().strftime("%H:%M:%S")
            )
        )
        return None

    # try to open an existing download queue first to finish unfinished downloads
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
        # if no download queue can be found build it
        tqdm.write(
            "{}: Building the download queue.".format(
                datetime.datetime.now().strftime("%H:%M:%S")
            )
        )
        # build the queue
        download_queue = build_download_queue(fasta_dict, database, operating_mode)
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

    # as long as there is data in the download queue continue the download
    with tqdm(total=total_downloads, desc="Finished downloads") as pbar:
        while True:
            try:
                if download_queue["waiting"] or download_queue["active"]:
                    # as long as there are not 4 active requests in the download queue
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
                            download_queue["active"],
                            fasta_dict_order,
                            project_directory,
                        )
                        # update the progress bar
                        pbar.update(1)
                    # after every iteration override the download queue as it is changes along the way
                    with open(download_queue_name, "wb") as out_stream:
                        pickle.dump(download_queue, out_stream)
                # if all downloads are finished raise download finished flag
                else:
                    parquet_to_duckdb(project_directory, database_path)
                    raise DownloadFinished
            except DownloadFinished:
                # check if all downloads are finished: if yes: delete download queue, break the loop
                fasta_dict = already_downloaded(fasta_dict, database_path)
                # if there is any unfinished download, requeue
                if fasta_dict:
                    tqdm.write(
                        "{}: Requeuing incomplete downloads.".format(
                            datetime.datetime.now().strftime("%H:%M:%S")
                        )
                    )
                    download_queue = build_download_queue(
                        fasta_dict, database, operating_mode
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
