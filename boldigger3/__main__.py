import argparse, sys, datetime, luddite, duckdb
from boldigger3 import id_engine
from boldigger3 import add_metadata
from boldigger3 import select_top_hit
from boldigger3 import download_database
from importlib.metadata import version
from pathlib import Path
from boldigger3.query_cover import percentage, positive_bp, validate_cache


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

    parser_identify.add_argument(
        "db_path", help="Path to the locally downloaded database.", type=str
    )

    # add the database argument
    parser_identify.add_argument(
        "--db",
        required=True,
        help="Integer that defines which database to use (1 to 8). See readme for details",
        type=int,
        choices=range(1, 9),
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

    parser_identify.add_argument(
        "--min-query-cover", type=percentage, default=None,
        help="Minimum BOLD qcovhsp percentage (0-100) for local classification; omitted disables filtering.",
    )
    parser_identify.add_argument(
        "--min-overlap-bp", type=positive_bp, default=100,
        help="BOLD request minimum overlap (mo) in base pairs (default: 100); separate from query coverage.",
    )

    # add the database download parse
    parser_download = subparsers.add_parser(
        "download_db", help="Download the public database."
    )

    # add the output dir argument
    parser_download.add_argument(
        "output_dir",
        help="Directory to download and save the public database.",
        type=str,
    )

    # add version control
    # get the installed version
    current_version = version("boldigger3")
    latest_version = luddite.get_version_pypi("boldigger3")

    # give a user warning if the latest version is not installed
    if current_version != latest_version:
        print(
            "{}: Your boldigger3 version is outdated. Consider updating to the latest version.".format(
                datetime.datetime.now().strftime("%H:%M:%S")
            )
        )

    # add the version argument
    parser.add_argument("--version", action="version", version=version("boldigger3"))

    # parse the arguments
    arguments = parser.parse_args()

    # print help if no argument is provided
    if len(sys.argv) == 1:
        arguments.func(arguments)
        sys.exit()

    # only use the threshold provided by the user replace the rest with defaults
    default_thresholds = [97, 95, 90, 85, 75]
    thresholds = []

    # run the identification engine
    if arguments.function == "identify":
        try:
            validate_cache(arguments.fasta_file, arguments.min_query_cover, arguments.min_overlap_bp)
            # Metadata may already have been materialized by an earlier run.
            cache = Path(arguments.fasta_file).parent / "boldigger3_data" / (Path(arguments.fasta_file).stem + ".duckdb")
            if cache.is_file():
                with duckdb.connect(str(cache), read_only=True) as connection:
                    has_final = ("final_results",) in connection.execute("SHOW TABLES").fetchall()
                if has_final:
                    validate_cache(arguments.fasta_file, arguments.min_query_cover, arguments.min_overlap_bp, "final_results")
        except ValueError as error:
            parser.error(str(error))
        for i in range(5):
            try:
                thresholds.append(arguments.thresholds[i])
            except (IndexError, TypeError):
                thresholds.append(default_thresholds[i])

        # add an virtual treshold of 50 to the thresholds list, so hits with only phylum information can be handled
        thresholds.append(50)

        if arguments.thresholds:
            # give user output
            print(
                "{}: Default thresholds changed!\n{}: Species: {}, Genus: {}, Family: {}, Order: {}".format(
                    datetime.datetime.now().strftime("%H:%M:%S"),
                    datetime.datetime.now().strftime("%H:%M:%S"),
                    *thresholds
                )
            )

        # run the id engine
        id_engine.main(
            arguments.fasta_file,
            database=arguments.db,
            operating_mode=arguments.mode,
            min_overlap_bp=arguments.min_overlap_bp,
        )

        # add additional data via the metadata
        add_metadata.main(fasta_path=arguments.fasta_file, db_path=arguments.db_path)

        # select the top hit
        select_top_hit.main(fasta_path=arguments.fasta_file, thresholds=thresholds,
                            min_query_cover=arguments.min_query_cover,
                            min_overlap_bp=arguments.min_overlap_bp)

    # run the database download
    if arguments.function == "download_db":
        # download and save the database
        download_database.main(output_dir=arguments.output_dir)


# run only if called as a top level script
if __name__ == "__main__":
    main()
