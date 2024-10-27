import datetime
import pandas as pd
from id_engine import parse_fasta
from pathlib import Path
from tqdm import tqdm


# function to combine additional data and hits and sort by input order
def combine_and_sort(hdf_name_results: str, fasta_dict: dict) -> object:
    """Combines additional data and hits, sorts the data by fasta input order
    returns the complete data as dataframe.

    Args:
        hdf_name_results (str): Path to the hdf storage.
        fasta_dict (dict): The fasta dict containing the input order

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
    # TODO

    return complete_dataset


# main function to run the data sorting and top hit selection
def main(fasta_path: str) -> None:
    """Main function to run data sorting and top hit selection of the downloaded data from BOLD.

    Args:
        fasta_path (str): Path to the fasta file that is identified.
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
    complete_dataset = combine_and_sort(hdf_name_results, fasta_dict)

    # Calculate the top hits
    # TODO

    # Save top hits in parquet and excel
    # TODO


if __name__ == "__main__":
    main("C:\\Users\\Dominik\\Documents\\GitHub\\BOLDigger3\\tests\\test_10.fasta")
