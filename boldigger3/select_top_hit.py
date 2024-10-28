import datetime, more_itertools
import pandas as pd
import numpy as np
from tqdm import tqdm
from id_engine import parse_fasta
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
        "ID": idx,
        "Phylum": "IncompleteTaxonomy",
        "Class": "IncompleteTaxnonmy",
        "Order": "IncompleteTaxonomy",
        "Family": "IncompleteTaxonomy",
        "Genus": "IncompleteTaxonomy",
        "Species": "IncompleteTaxonomy",
        "pct_identity": 0,
        "Status": np.nan,
        "records": np.nan,
        "selected_level": np.nan,
        "BIN": np.nan,
        "flags": np.nan,
    }

    incomplete_taxonomy = pd.DataFrame(data=incomplete_taxonomy, index=[0])

    return incomplete_taxonomy


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
        return hits_for_id.query("Species == 'no-match'").head(1)

    # loop through the thresholds until a hit is found
    while True:
        # copy the hits for the respective ID to perform modifications
        hits_for_id_above_similarity = hits_for_id.copy()

        # collect the idx here, to push it into the incomplete taxonomy if needed
        idx = hits_for_id_above_similarity.head(1)["ID"].item()

        with pd.option_context("future.no_silent_downcasting", True):
            hits_for_id_above_similarity = hits_for_id_above_similarity.replace(
                "", np.nan
            )

        # only select hits above the selected threshold
        hits_for_id_above_similarity = hits_for_id_above_similarity.loc[
            hits_for_id_above_similarity["Similarity"] >= threshold
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

    # # gather the top hits here
    # all_top_hits = []

    # # Calculate the top hits
    # for idx in tqdm(complete_dataset_clean["id"].unique(), desc="Calculating top hits"):
    #     # select only the respective id
    #     hits_for_id = (
    #         complete_dataset_clean.loc[complete_dataset_clean["id"] == idx]
    #         .copy()
    #         .reset_index(drop=True)
    #     )

    #     # find the top hit
    #     all_top_hits.append(find_top_hit(hits_for_id, thresholds))

    # Save top hits in parquet and excel
    # TODO


if __name__ == "__main__":
    main(
        "C:\\Users\\Dominik\\Documents\\GitHub\\BOLDigger3\\tests\\test_10.fasta",
        [97, 95, 90, 85],
    )
