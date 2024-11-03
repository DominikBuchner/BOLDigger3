import datetime, more_itertools, sys
import pandas as pd
import numpy as np
from tqdm import tqdm
from boldigger3.id_engine import parse_fasta
from pathlib import Path
from tqdm import tqdm
from string import punctuation, digits


# function to combine additional data and hits and sort by input order
def combine_and_sort(
    hdf_name_results: str, fasta_dict: dict, fasta_name: str, project_path: str
) -> object:
    """Combines additional data and hits, sorts the data by fasta input order
    returns the complete data as dataframe.

    Args:
        hdf_name_results (str): Path to the hdf storage.
        fasta_dict (dict): The fasta dict containing the input order
        fasta_name (str): The name of the fasta file that has to be identified
        project_path (str): Path to the project boldigger3 is working in.

    Returns:
        object: Dataframe with combined data.
    """
    # load the additional data from the hdf storage
    additional_data = pd.read_hdf(hdf_name_results, key="additional_data")

    # transform additional data to dict, retain the column names
    additional_data = additional_data.to_dict("tight")
    column_names = additional_data["columns"][1:]
    additional_data = additional_data["data"]

    # parse the additional data into a dict in the form of process_id : [data fields] to rebuild the dataframe
    additional_data = ((record[0], record[1:]) for record in additional_data)
    additional_data = {record: data for record, data in additional_data}

    # extract the original process ids from the downloaded top n hits
    unsorted_results = pd.read_hdf(
        hdf_name_results, key="results_unsorted"
    ).reset_index(drop=True)

    # extract the process ids and remove all data without id
    process_ids = unsorted_results["process_id"]
    process_ids = process_ids[process_ids != ""]

    # create a dataframe with the additional data and duplicate values since each
    # process id only has to be requested once
    additional_data = pd.DataFrame(
        data=[additional_data[record] for record in process_ids],
        columns=column_names,
        index=process_ids.index,
    )

    # merge the unsorted results and the additional data
    complete_dataset = pd.concat([unsorted_results, additional_data], axis=1)

    # build a sorter to sort the the complete dataset by input order
    sorter = {name: idx for idx, name in enumerate(fasta_dict.keys())}

    # add the sorter column to the complete dataset
    complete_dataset["sorter"] = complete_dataset["id"].map(sorter)

    # name the index for sorting
    complete_dataset.index.name = "index"

    # perform the sorting, remove the sorter, reset the index
    complete_dataset = (
        complete_dataset.sort_values(by=["sorter", "index"], ascending=[True, True])
        .drop(labels=["sorter"], axis=1)
        .reset_index(drop=True)
    )

    # save the complete dataset in the result storage only once
    store = pd.HDFStore(hdf_name_results, "r")
    keys = store.keys()
    store.close()

    if "/complete_dataset" not in keys:
        with pd.HDFStore(
            hdf_name_results, mode="a", complib="blosc:blosclz", complevel=9
        ) as hdf_output:
            hdf_output.append(
                key="complete_dataset",
                value=complete_dataset,
                format="t",
                data_columns=True,
                complib="blosc:blosclz",
                complevel=9,
            )

    # save the complete dataset to excel in chunks
    idx_parts = more_itertools.chunked(complete_dataset.index, 1000000)

    for idx, idx_part in enumerate(idx_parts, start=1):
        savename = "{}_bold_results_part_{}.xlsx".format(fasta_name, idx)
        complete_dataset.iloc[idx_part].to_excel(
            project_path.joinpath(savename), index=False
        )

    return complete_dataset


# function to clean the dataset (remove number, special chars)
def clean_dataset(dataset: object) -> object:
    """Funtion to clean the downloaded dataset. Removes names with special characters and numbers

    Args:
        dataset (object): The complete dataset as a dataframe.

    Returns:
        object: The cleaned dataset as a dataframe
    """
    complete_dataset_clean = dataset.copy()

    # remove punctuation and numbers from the taxonomy
    # exclude "-" since it can be part of a species name in rare cases
    # also retains no-matches
    specials = "".join([char for char in punctuation + digits if char != "-"])
    levels = ["Phylum", "Class", "Order", "Family", "Genus", "Species"]

    # clean the dataset
    for level in levels:
        complete_dataset_clean[level] = np.where(
            complete_dataset_clean[level].str.contains("[{}]".format(specials)),
            np.nan,
            complete_dataset_clean[level],
        )

    # return the cleaned dataset
    return complete_dataset_clean


# accepts a dataframe for any individual id
# returns the threshold to filter for and a taxonomic level
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
    if hit_for_id["Species"][0] == "no-match":
        return 0, "no-match"
    else:
        # move through the taxonomy if it is no nomatch hit or broken record
        if threshold >= thresholds[0]:
            return thresholds[0], "Species"
        elif threshold >= thresholds[1]:
            return thresholds[1], "Genus"
        elif threshold >= thresholds[2]:
            return thresholds[2], "Family"
        elif threshold >= thresholds[3]:
            return thresholds[3], "Order"


## function to move the treshold one level up if no hit is found, also return the new tax level
def move_threshold_up(threshold: int, thresholds: list) -> tuple:
    """Function to move the threshold up one taxonomic level.
    Returns a new threshold and level as a tuple.

    Args:
        threshold (int): Current threshold.
        thresholds (list): List of all thresholds.

    Returns:
        tuple: (new_threshold, thresholds)
    """
    levels = ["Species", "Genus", "Family", "Order"]

    return (
        thresholds[thresholds.index(threshold) + 1],
        levels[thresholds.index(threshold) + 1],
    )


# funtion to produce and inclomplete taxonomy hit if the
# BOLD taxonomy is not complete
def return_incomplete_taxonomy(idx: int):
    incomplete_taxonomy = {
        "id": idx,
        "Phylum": "IncompleteTaxonomy",
        "Class": "IncompleteTaxnonmy",
        "Order": "IncompleteTaxonomy",
        "Family": "IncompleteTaxonomy",
        "Genus": "IncompleteTaxonomy",
        "Species": "IncompleteTaxonomy",
        "pct_identity": 0,
        "status": np.nan,
        "records": np.nan,
        "selected_level": np.nan,
        "BIN": np.nan,
        "flags": np.nan,
    }

    incomplete_taxonomy = pd.DataFrame(data=incomplete_taxonomy, index=[0])

    return incomplete_taxonomy


# function to flag the hits
def flag_hits(
    top_hits: object, hits_for_id_above_similarity: object, top_hit: object
) -> list:
    """Funtion to add a list of flag to the hits

    Args:
        top_hits (object): Dataframe with the top hits.
        hits_for_id_above_similarity (object): Dataframe of the hits above the selected similarity.
        top_hit (object): Dataframe with the selected top hit.

    Returns:
        list: Returns a list of flags.
    """
    # predefine the flags to return
    flags = [False, False, False, False, False]

    # flag 1: Reverse BIN taxonomy
    id_method = top_hits["id_method"]

    if (
        id_method.str.contains("BOLD").all()
        or id_method.str.contains("ID").all()
        or id_method.str.contains("Tree").all()
        or id_method.str.contains("BIN").all()
    ):
        flags[0] = "1"

    # flag 2: more than one group above the selected threshold
    if len(hits_for_id_above_similarity.index) > 1:
        flags[1] = "2"

    # flag 3: all of the selected top hits are private
    if top_hits["status"].isin(["private"]).all():
        flags[2] = "3"

    # flag 4: top hit is only represented by one record
    if len(top_hits.index) == 1:
        flags[3] = "4"

    # flag 5: top hit is represented by multiple bins
    if len(top_hit["BIN"].str.split(";").item()) > 1:
        flags[4] = "5"

    flags = [i for i in flags if i]
    flags = "--".join(flags)

    return flags


# function to find the top hit for a given id
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

    # if a no-match is found, return the no-match directly
    if level == "no-match":
        return_value = hits_for_id.query("Species == 'no-match'").head(1)

        return_value = return_value[
            [
                "id",
                "Phylum",
                "Class",
                "Order",
                "Family",
                "Genus",
                "Species",
                "pct_identity",
                "status",
            ]
        ]
        for value in ["records", "selected_level", "BIN", "flags", "Status"]:
            return_value[value] = np.nan

        return return_value

    # loop through the thresholds until a hit is found
    while True:
        # copy the hits for the respective ID to perform modifications
        hits_for_id_above_similarity = hits_for_id.copy()

        # collect the idx here, to push it into the incomplete taxonomy if needed
        idx = hits_for_id_above_similarity.head(1)["id"].item()

        with pd.option_context("future.no_silent_downcasting", True):
            hits_for_id_above_similarity = hits_for_id_above_similarity.replace(
                "", np.nan
            )

        # only select hits above the selected threshold
        hits_for_id_above_similarity = hits_for_id_above_similarity.loc[
            hits_for_id_above_similarity["pct_identity"] >= threshold
        ]

        # if no hit remains move up one level until class
        if len(hits_for_id_above_similarity.index) == 0:
            try:
                threshold, level = move_threshold_up(threshold, thresholds)
                continue
            # if there is incomplete taxonomy, boldigger3 will move through all thresholds but end up here
            # return incomplete taxonomy if that is the case
            except IndexError:
                return return_incomplete_taxonomy(idx)

        # define the levels for the groupby. care about the selector string later
        all_levels = ["Phylum", "Class", "Order", "Family", "Genus", "Species"]
        levels = all_levels[: all_levels.index(level) + 1]

        # only select interesting levels (all levels above and including the selected level)
        hits_for_id_above_similarity = hits_for_id_above_similarity[levels].copy()

        # group the hits by level and then count the appearence
        hits_for_id_above_similarity = pd.DataFrame(
            {
                "count": hits_for_id_above_similarity.groupby(
                    by=levels,
                    sort=False,
                ).size()
            }
        ).reset_index()

        # if the hits still contained np.nan values, groupby will drop them:
        # if theres nothing left after the gruoupby move up one level and continue
        if len(hits_for_id_above_similarity.index) == 0:
            try:
                threshold, level = move_threshold_up(threshold, thresholds)
                continue
            # if there is incomplete taxonomy, boldigger3 will move through all thresholds but end up here
            # return incomplete taxonomy if that is the case
            except IndexError:
                return return_incomplete_taxonomy(idx)

        # sort the hits by count
        hits_for_id_above_similarity = hits_for_id_above_similarity.sort_values(
            "count", ascending=False
        )

        # select the hit with the highest count from the dataframe
        # also return the count to display in the top hit table in the end
        top_hits, top_count = (
            hits_for_id_above_similarity.head(1),
            hits_for_id_above_similarity.head(1)["count"].item(),
        )

        # generate the selector string based on the selected level
        query_string = [
            "{} == '{}'".format(level, top_hits[level].item()) for level in levels
        ]
        query_string = " and ".join(query_string)

        # query for the top hits
        top_hits = hits_for_id.query(query_string)

        # collect the bins from the selected top hit
        if threshold == thresholds[0]:
            top_hit_bins = top_hits["bin_uri"].dropna().unique()
        else:
            top_hit_bins = []

        # select the first match from the top hits table as the top hit
        top_hit = top_hits.head(1).copy()

        # add the record count to the top hit
        top_hit["records"] = top_count

        # add the selected level
        top_hit["selected_level"] = level

        # add the BINs to the top hit
        top_hit["BIN"] = ";".join(top_hit_bins)

        # define level to remove them from low level hits
        levels = ["Class", "Order", "Family", "Genus", "Species"]

        # return species level information if similarity is high enough
        # else remove higher level information form output depending on level
        if threshold == thresholds[0]:
            break
        else:
            top_hit = top_hit.assign(
                **{k: np.nan for k in levels[levels.index(level) + 1 :]}
            )
            break

    # add flags to the hits
    top_hit["flags"] = flag_hits(top_hits, hits_for_id_above_similarity, top_hit)

    # remove all data that is not needed
    top_hit = top_hit[
        [
            "id",
            "Phylum",
            "Class",
            "Order",
            "Family",
            "Genus",
            "Species",
            "pct_identity",
            "status",
            "records",
            "selected_level",
            "BIN",
            "flags",
        ]
    ]

    # return the top hit
    return top_hit


# function to save the data to excel and parquet
def save_results(project_directory: str, fasta_name: str, all_top_hits: object) -> None:
    """Function to save the identification results as Excel and Parquet.

    Args:
        project_directory (str): Project directory to save the results to.
        fasta_name (str): Name of the fasta that has to be identified.
        all_top_hits (object): Dataframe of the selected top hits.
    """
    # generate the savenames
    savename_excel = Path(project_directory).joinpath(
        "{}_identification_result.xlsx".format(fasta_name)
    )
    savename_parquet = Path(project_directory).joinpath(
        "{}_identification_result.parquet.snappy".format(fasta_name)
    )

    # save the data
    all_top_hits.to_excel(savename_excel, index=False)
    all_top_hits.to_parquet(savename_parquet)


# main function to run the data sorting and top hit selection
def main(fasta_path: str, thresholds: list) -> None:
    """Main function to run data sorting and top hit selection of the downloaded data from BOLD.

    Args:
        fasta_path (str): Path to the fasta file that is identified.
        thresholds (list): Thresholds for the different taxonomic levels.
    """
    # user output
    tqdm.write(
        "{}: Loading the data for top hit selection.".format(
            datetime.datetime.now().strftime("%H:%M:%S")
        )
    )

    # read the input fasta
    fasta_dict, fasta_name, project_directory = parse_fasta(fasta_path)

    # generate a new for the hdf storage to store the downloaded data
    hdf_name_results = project_directory.joinpath(
        "{}_result_storage.h5.lz".format(fasta_name)
    )

    # user output
    tqdm.write(
        "{}: Combining results with additional data and sort by input order.".format(
            datetime.datetime.now().strftime("%H:%M:%S")
        )
    )

    # combine downloaded data and additional data
    complete_dataset = combine_and_sort(
        hdf_name_results, fasta_dict, fasta_name, project_directory
    )

    # clean the dataset prior to the calculation of the top hits
    complete_dataset_clean = clean_dataset(complete_dataset)

    # gather the top hits here
    all_top_hits = []

    # Calculate the top hits
    for idx in tqdm(complete_dataset_clean["id"].unique(), desc="Calculating top hits"):
        # select only the respective id
        hits_for_id = (
            complete_dataset_clean.loc[complete_dataset_clean["id"] == idx]
            .copy()
            .reset_index(drop=True)
        )

        # find the top hit
        all_top_hits.append(find_top_hit(hits_for_id, thresholds))

    # concat all top hits
    all_top_hits = pd.concat(all_top_hits, axis=0).reset_index(drop=True)

    # Save top hits in parquet and excel
    tqdm.write(
        "{}: Saving results. This may take a while.".format(
            datetime.datetime.now().strftime("%H:%M:%S")
        )
    )

    # save the results
    save_results(project_directory, fasta_name, all_top_hits)

    # user output
    tqdm.write("{}: Finished.".format(datetime.datetime.now().strftime("%H:%M:%S")))
