import argparse, sys, datetime, time
from boldigger3 import id_engine, additional_data_download, select_top_hit
from importlib.metadata import version
from get_pypi_latest_version import GetPyPiLatestVersion


# main function to program the commandline interface
def main() -> None:
    """Function to define the commandline interface."""
    # initialize the default behaviour if boldigger3 is called without any argument
    formatter = lambda prog: argparse.HelpFormatter(prog, max_help_position=35)

    # define the parser
    parser = argparse.ArgumentParser(
        prog="boldigger3",
        description="A Python package to identify and organise sequences with the Barcode of Life Data systems.",
        formatter_class=formatter,
    )

    # display help when no argument is called
    parser.set_defaults(func=lambda x: parser.print_help())

    # add the subparsers
    subparsers = parser.add_subparsers(dest="function")

    # add the identify parser
    parser_identify = subparsers.add_parser(
        "identify", help="Run the BOLD v5 identification engine"
    )

    # add the fasta path argument
    parser_identify.add_argument(
        "fasta_file",
        help="Path to the fasta file or fasta file in the current working directory to be identified.",
        type=str,
    )

    # add the database argument
    parser_identify.add_argument(
        "--db",
        required=True,
        help="Integer that defines which database to use (1 to 7). See readme for details",
        type=int,
        choices=range(1, 8),
    )

    # add the operating mode argument
    parser_identify.add_argument(
        "--mode",
        required=True,
        help="Integer that defines which operating mode to use (1 to 3). See readme for details.",
        type=int,
        choices=range(1, 4),
    )

    # add the optional argument thresholds
    parser_identify.add_argument(
        "--thresholds",
        nargs="+",
        type=int,
        help="Thresholds to use for the selection of the top hit.",
    )

    # add version control
    # get the installed version
    current_version = version("boldigger2")
    obtainer = GetPyPiLatestVersion()
    latest_version = obtainer("boldigger2")

    # give a user warning if the latest version is not installed
    if current_version != latest_version:
        print(
            "{}: Your boldigger3 version is outdated. Consider updating to the latest version.".format(
                datetime.datetime.now().strftime("%H:%M:%S")
            )
        )

    # add the version argument
    parser.add_argument("--version", action="version", version=version("boldigger2"))

    # parse the arguments
    arguments = parser.parse_args()

    # print help if no argument is provided
    if len(sys.argv) == 1:
        arguments.func(arguments)
        sys.exit()

    # only use the threshold provided by the user replace the rest with defaults
    default_thresholds = [97, 95, 90, 85]
    thresholds = []

    for i in range(4):
        try:
            thresholds.append(arguments.thresholds[i])
        except (IndexError, TypeError):
            thresholds.append(default_thresholds[i])

    if arguments.thresholds:
        # give user output
        print(
            "{}: Default thresholds changed!\n{}: Species: {}, Genus: {}, Family: {}, Order: {}".format(
                datetime.datetime.now().strftime("%H:%M:%S"),
                datetime.datetime.now().strftime("%H:%M:%S"),
                *thresholds
            )
        )

    # run the identification engine
    if arguments.function == "identify":
        # run the id engine
        id_engine.main(
            arguments.fasta_file,
            database=arguments.db,
            operating_mode=arguments.mode,
        )
        # download the additional data
        additional_data_download.main(arguments.fasta_file)
        # select the top hit
        select_top_hit.main(arguments.fasta_file, thresholds=thresholds)


# run only if called as a top level script
if __name__ == "__main__":
    main()
