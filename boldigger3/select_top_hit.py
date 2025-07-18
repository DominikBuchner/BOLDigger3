import duckdb, datetime, more_itertools, re, time
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
                engine="xlsxwriter",
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
        else:
            return thresholds[5], "phylum"


def move_threshold_up(threshold: int, thresholds: list) -> tuple:
    """Function to move the threshold up one taxonomic level.
    Returns a new threshold and level as a tuple.

    Args:
        threshold (int): Current threshold.
        thresholds (list): List of all thresholds.

    Returns:
        tuple: (new_threshold, thresholds)
    """
    levels = ["species", "genus", "family", "order", "class", "phylum"]

    return (
        thresholds[thresholds.index(threshold) + 1],
        levels[thresholds.index(threshold) + 1],
    )


def flag_hits(top_hits: object, final_top_hit: object):
    # initialize the flags
    flags = [""] * 5

    # flag 1: Reverse BIN taxonomy
    id_method = top_hits["identification_method"].dropna()

    if (
        not id_method.empty
        and id_method.str.contains("BOLD|ID|Tree|BIN", regex=False).all()
    ):
        flags[0] = "1"

    # flag 2: top hit ratio < 90%
    if final_top_hit["records_ratio"].item() < 0.9:
        flags[1] = "2"

    # flag 3: all of the selected top hits are private
    if top_hits["status"].isin(["private"]).all():
        flags[2] = "3"

    if len(top_hits.index) == 1:
        flags[3] = "4"

    # flag 5: top hit is represented by multiple bins
    if len(final_top_hit["BIN"].str.split("|").item()) > 1:
        flags[4] = "5"

    flags = "|".join(flags)

    return flags


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
        fasta_order = return_value["fasta_order"]
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
            "flags": "||||",
        }
        for key, value in data_to_type.items():
            return_value[key] = value

        # add the fasta order back in
        return_value["fasta_order"] = fasta_order

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

        # drop nas at the selected level
        hits_above_similarity = hits_above_similarity.dropna(subset=level, axis=0)

        # if there's nothing left, move the threshold up and continue to search
        if len(hits_above_similarity.index) == 0:
            threshold, level = move_threshold_up(threshold, thresholds)
            continue

        # sort by count
        hits_above_similarity = hits_above_similarity.sort_values(
            by="count", ascending=False
        )

        # select the top hit and its count
        top_hit, top_count, top_ratio = (
            hits_above_similarity.head(1),
            hits_above_similarity.head(1)["count"].item(),
            hits_above_similarity.head(1)["count"].item()
            / hits_above_similarity["count"].sum(),
        )

        # drop all columns with na values to not pollute the query string
        top_hit = top_hit.dropna(axis=1).drop(labels="count", axis=1)

        # create a query string
        query_string = [
            f"`{level}` == '{top_hit[level].item()}'" for level in top_hit.columns
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
        final_top_hit = top_hits.head(1).copy()

        # add the record count to the top hit
        final_top_hit["records"] = top_count
        final_top_hit["records_ratio"] = top_ratio

        # add the selected level
        final_top_hit["selected_level"] = level

        # add the BINs to the top hit
        final_top_hit["BIN"] = "|".join(top_hit_bins)

        # remove information that is higher then the selected level if neccesarry
        if threshold != thresholds[0]:
            # get the index of the selected level
            idx = all_levels.index(level)
            levels_to_remove = all_levels[idx + 1 :]
            final_top_hit[levels_to_remove] = pd.NA
            final_top_hit[levels_to_remove] = final_top_hit[levels_to_remove].astype(
                "string"
            )
            break
        break

    # add flags to the hits
    final_top_hit["flags"] = flag_hits(top_hits, final_top_hit)

    # only select the data relevant for output
    final_top_hit = final_top_hit[
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
            "records",
            "records_ratio",
            "selected_level",
            "BIN",
            "flags",
            "fasta_order",
        ]
    ]

    return final_top_hit


def gather_top_hits(
    fasta_dict, id_engine_db_path, project_directory, fasta_name, thresholds
):
    # store top hits here until n are reached, flush to parquet inbetween
    top_hits_buffer = []
    buffer_counter = 0

    with duckdb.connect(id_engine_db_path) as connection:
        # extract the data per query from duckdb
        for query in tqdm(fasta_dict.keys(), desc="Top hit calculation"):
            sql_query = f"SELECT * FROM final_results WHERE id='{query}' ORDER BY fasta_order ASC, pct_identity DESC"
            query = clean_dataframe(connection.execute(sql_query).df())
            # find the top hit
            top_hits_buffer.append(find_top_hit(query, thresholds))
            # spill to parquet whenever there are 1k hits in the buffer, ingest parquet later for saving
            if len(top_hits_buffer) >= 1_000:
                parquet_output = project_directory.joinpath(
                    "boldigger3_data",
                    f"{fasta_name}_top_hit_buffer_{buffer_counter}.parquet.snappy",
                )
                top_hits_buffer = pd.concat(top_hits_buffer, axis=0).reset_index(
                    drop=True
                )
                top_hits_buffer.to_parquet(parquet_output)
                buffer_counter += 1
                top_hits_buffer = []

        # final buffer flush
        if top_hits_buffer:
            parquet_output = project_directory.joinpath(
                "boldigger3_data",
                f"{fasta_name}_top_hit_buffer_{buffer_counter}.parquet.snappy",
            )

            top_hits_buffer = pd.concat(top_hits_buffer, axis=0).reset_index(drop=True)
            top_hits_buffer.to_parquet(parquet_output)


def save_results(project_directory, fasta_name):
    # load all data into dask dataframe
    data_paths = project_directory.joinpath("boldigger3_data").glob(
        f"{fasta_name}_top_hit_buffer_*.parquet.snappy"
    )
    all_top_hits = dd.read_parquet([str(f) for f in data_paths])

    # order values globally by fasta order, drop afterwards
    all_top_hits = (
        all_top_hits.set_index("fasta_order", sorted=True)
        .reset_index(drop=True)
        .compute()
    )

    # write the output
    parquet_output = project_directory.joinpath(
        "boldigger3_data", "{}_identification_result.parquet.snappy".format(fasta_name)
    )
    excel_output = project_directory.joinpath(
        "boldigger3_data", "{}_identification_result.xlsx".format(fasta_name)
    )

    # save the data
    all_top_hits.to_excel(excel_output, index=False, engine="xlsxwriter")
    all_top_hits.to_parquet(parquet_output)

    # unlink the buffer files
    for file in project_directory.joinpath("boldigger3_data").glob(
        f"{fasta_name}_top_hit_buffer_*.parquet.snappy"
    ):
        if file.exists():
            file.unlink()


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
    stream_hits_to_excel(id_engine_db_path, project_directory, fasta_dict, fasta_name)

    tqdm.write(f"{datetime.datetime.now().strftime('%H:%M:%S')}: Calculating top hits.")

    gather_top_hits(
        fasta_dict, id_engine_db_path, project_directory, fasta_name, thresholds
    )
    tqdm.write(
        f"{datetime.datetime.now().strftime('%H:%M:%S')}: Saving results. This may take a while."
    )

    save_results(project_directory, fasta_name)

    tqdm.write(f"{datetime.datetime.now().strftime('%H:%M:%S')}: Finished.")
