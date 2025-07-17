import duckdb, datetime, more_itertools, re, sys
import pandas as pd
import dask.dataframe as dd
import numpy as np
from tqdm import tqdm
from boldigger3.id_engine import parse_fasta
from string import punctuation, digits


def clean_dataframe(dataframe: object) -> object:
    # replace missing values and empty strings in metadata to pd.NA
    metadata_columns = [
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

    dataframe[metadata_columns] = dataframe[metadata_columns].replace(
        [None, "None", ""], pd.NA
    )
    # remove all punctuation and digits except '-', some species names contain a "-"
    specials = re.escape("".join(c for c in punctuation + digits if c != "-"))
    pattern = f"[{specials}]"

    # Levels to clean
    levels = ["phylum", "class", "order", "family", "genus", "species"]

    # replace any occurrence of invalid characters with pd.NA
    dataframe[levels] = dataframe[levels].apply(
        lambda col: col.where(~col.str.contains(pattern, regex=True)).astype("string")
    )

    # replace all empty strings in dataframe with pd.NA
    dataframe = dataframe.replace("", pd.NA)

    # make all object columns strings so we do not get unintended behaviour later
    object_columns = dataframe.select_dtypes(include="object").columns
    dataframe[object_columns] = dataframe[object_columns].astype("string")

    try:
        # extract the lat lon values
        dataframe[["lat", "lon"]] = (
            dataframe["coord"].str.strip("[]").str.split(",", expand=True)
        )
        dataframe["lat"], dataframe["lon"] = dataframe["lat"].astype(
            "float"
        ), dataframe["lon"].astype("float")
    except ValueError:
        dataframe["lat"], dataframe["lon"] = np.nan, np.nan

    # drop coord column
    dataframe = dataframe.drop("coord", axis=1)

    return dataframe


def stream_hits_to_excel(id_engine_db_path, project_directory, fasta_dict, fasta_name):
    # chunk the fasta dicts keys to retrieve from duckdb
    chunks = enumerate(more_itertools.chunked(fasta_dict.keys(), n=10_000), start=1)

    # define the output path
    output_path = project_directory.joinpath("boldigger3_data")

    with duckdb.connect(id_engine_db_path) as connection:
        # retrieve one chunk of a maximum of 10_000 process_ids
        for part, chunk in chunks:
            query = f"""SELECT * FROM final_results 
            WHERE id IN ?
            ORDER BY fasta_order ASC, pct_identity DESC"""
            chunk_data = connection.execute(query, [chunk]).df()
            chunk_data = clean_dataframe(chunk_data)

            # drop the fasta order just before saving
            chunk_data = chunk_data.drop("fasta_order", axis=1)

            chunk_data.to_excel(
                output_path.joinpath(f"{fasta_name}_bold_results_part_{part}.xlsx"),
                index=False,
            )


def get_threshold(hit_for_id: object, thresholds: list) -> object:
    """Function to find a threshold for a given id from the complete dataset.

    Args:
        hit_for_id (object): The hits for the respective id as dataframe.
        thresholds (list): Lists of thresholds to to use for the selection of the top hit.

    Returns:
        object: Single line dataframe containing the top hit
    """
    # find the highest similarity value for the threshold
    threshold = hit_for_id["pct_identity"].max()

    # check for no matches first
    if "no-match" in hit_for_id.astype(str).values:
        return 0, "no-match"
    else:
        # move through the taxonomy if it is no nomatch hit or broken record
        if threshold >= thresholds[0]:
            return thresholds[0], "species"
        elif threshold >= thresholds[1]:
            return thresholds[1], "genus"
        elif threshold >= thresholds[2]:
            return thresholds[2], "family"
        elif threshold >= thresholds[3]:
            return thresholds[3], "order"
        elif threshold >= thresholds[4]:
            return thresholds[4], "class"
        # used for default thresholds --> if no hit matches the defined threshold levels, it's also a no-match
        else:
            return 0, "no-match"


def move_threshold_up(threshold: int, thresholds: list) -> tuple:
    """Function to move the threshold up one taxonomic level.
    Returns a new threshold and level as a tuple.

    Args:
        threshold (int): Current threshold.
        thresholds (list): List of all thresholds.

    Returns:
        tuple: (new_threshold, thresholds)
    """
    levels = ["Species", "Genus", "Family", "Order", "Class"]

    return (
        thresholds[thresholds.index(threshold) + 1],
        levels[thresholds.index(threshold) + 1],
    )


def find_top_hit(hits_for_id: object, thresholds: list) -> object:
    """Funtion to find the top hit for a given ID.

    Args:
        hits_for_id (object): Dataframe with the data for a given ID
        thresholds (list): List of thresholds to perform the top hit selection with.

    Returns:
        object: Single line dataframe with the selected top hit
    """
    # get the thrshold and taxonomic level
    threshold, level = get_threshold(hits_for_id, thresholds)

    # if a nomatch is found, a no-match can directly be retured
    if level == "no-match":
        return_value = hits_for_id.query("species == 'no-match'").head(1)

        # columns to return
        return_value = return_value[
            [
                "id",
                "phylum",
                "class",
                "order",
                "family",
                "genus",
                "species",
                "pct_identity",
                "status",
            ]
        ]

        # fill the missing data with correct types
        data_to_type = {
            "records": 0,
            "selected_level": pd.NA,
            "BIN": pd.NA,
            "flags": pd.NA,
        }
        for key, value in data_to_type.items():
            return_value[key] = value

        return_value = return_value.astype(
            {
                "selected_level": "string[python]",
                "BIN": "string[python]",
                "flags": "string[python]",
            }
        )

        return return_value

    # go through the hits to make the selection
    while True:
        # copy the hits to perform modifications
        hits_above_similarity = hits_for_id.copy()

        # select the hits above similarity
        hits_above_similarity = hits_above_similarity.loc[
            hits_above_similarity["pct_identity"] > threshold
        ]

        # define the levels for the groupby. care about the selector string later
        all_levels = ["phylum", "class", "order", "family", "genus", "species"]
        levels = all_levels[: all_levels.index(level) + 1]

        # only select levels of interest
        hits_above_similarity = hits_above_similarity[levels].copy()

        # group hits by level and then count the appearence
        hits_above_similarity = pd.DataFrame(
            hits_above_similarity.groupby(by=levels, sort=False, dropna=False)
            .size()
            .reset_index(name="count")
        )

        print(level)
        print("\n")
        print(hits_above_similarity)
        break


def gather_top_hits(
    fasta_dict, id_engine_db_path, project_directory, fasta_name, thresholds
):
    # store top hits here until n are reached, flush to parquet inbetween
    top_hits_buffer = []

    with duckdb.connect(id_engine_db_path) as connection:
        # extract the data per query from duckdb
        for query in fasta_dict.keys():
            sql_query = f"SELECT * FROM final_results WHERE id='{query}' ORDER BY fasta_order ASC, pct_identity DESC"
            query = clean_dataframe(connection.execute(sql_query).df())
            # find the top hit
            find_top_hit(query, thresholds)


def main(fasta_path: str, thresholds: list):
    tqdm.write(
        f"{datetime.datetime.now().strftime('%H:%M:%S')}: Removing digits and punctuation from hits."
    )

    # load the fasta data
    fasta_dict, fasta_name, project_directory = parse_fasta(fasta_path)

    # define the id engine database path
    id_engine_db_path = project_directory.joinpath(
        "boldigger3_data", f"{fasta_name}.duckdb"
    )

    tqdm.write(
        f"{datetime.datetime.now().strftime('%H:%M:%S')}: Streaming all hits to excel."
    )

    # # stream the data from duckdb to excel first
    # stream_hits_to_excel(id_engine_db_path, project_directory, fasta_dict, fasta_name)

    tqdm.write(f"{datetime.datetime.now().strftime('%H:%M:%S')}: Calculating top hits.")

    gather_top_hits(
        fasta_dict, id_engine_db_path, project_directory, fasta_name, thresholds
    )
