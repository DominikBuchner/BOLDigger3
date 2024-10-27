import datetime
import pandas as pd
from id_engine import parse_fasta
from pathlib import Path
from tqdm import tqdm


# main function to run the data sorting and top hit selection
def main(fasta_path: str) -> None:
    """Main function to run data sorting and top hit selection of the downloaded data from BOLD.

    Args:
        fasta_path (str): Path to the fasta file that is identified.
    """
    # user output
    tqdm.write(
        "{}: Collecting process ids.".format(
            datetime.datetime.now().strftime("%H:%M:%S")
        )
    )

    # read the input fasta
    fasta_dict, fasta_name, project_directory = parse_fasta(fasta_path)

    # generate a new for the hdf storage to store the downloaded data
    hdf_name_results = project_directory.joinpath(
        "{}_result_storage.h5.lz".format(fasta_name)
    )

    # combine downloaded data and additional data
    # TODO

    # Calculate the top hits
    # TODO

    # Save top hitsin hdf and excel
    # TODO


if __name__ == "__main__":
    main("C:\\Users\\Dominik\\Documents\\GitHub\\BOLDigger3\\tests\\test_10.fasta")
