import duckdb, importlib, datetime
from pathlib import Path
from boldigger3.id_engine import parse_fasta


def merge_in_additional_data(id_engine_db: str, metadata_db: str) -> None:
    """Function to merge in the additional metadata into the ID engine results

    Args:
        id_engine_db (str): Path to the id engine database.
        metadata_db (str): Path to the metadata database.
    """
    # connect to both databases
    id_engine_con = duckdb.connect(id_engine_db)

    # attach metadata db to the id engine connection
    id_engine_con.execute(f"ATTACH DATABASE '{metadata_db}' AS metadata")

    # check if the table exists
    tables = id_engine_con.execute("SHOW TABLES").fetchall()
    tables = [name[0] for name in tables]
    if "final_results" in tables:
        print(
            f"{datetime.datetime.now().strftime('%H:%M:%S')}: Metadata has already been added in a previous run."
        )

    # SQL Command definition
    sql_command = f"""
    CREATE TABLE IF NOT EXISTS final_results AS
    SELECT *
    FROM id_engine_results
    LEFT JOIN metadata.bold_public
    ON id_engine_results.process_id = metadata.bold_public.processid
    ORDER BY id_engine_results.fasta_order ASC, id_engine_results.pct_identity DESC
    """

    # perform the action
    id_engine_con.execute(sql_command)

    # update the status - needed for DB 2
    update_status = """
    UPDATE final_results
    SET status = CASE
        WHEN processid IS NULL THEN 'private'
        ELSE 'public'
    END;
    """

    # Example execution using DuckDB connection 'con':
    id_engine_con.execute(update_status)

    id_engine_con.close()


def main(
    fasta_path: str,
) -> None:
    """Function to add the metadata to the ID engine results.

    Args:
        fasta_path (str): _description_
    """
    # find the metadata database
    spec = importlib.util.find_spec("boldigger3").origin
    boldigger3_path = Path(spec).parent
    metadata_db_path = next(boldigger3_path.joinpath("database").glob("*.duckdb"))

    # find the id engine database that was downloadedy
    fasta_dict, fasta_name, project_directory = parse_fasta(fasta_path)
    id_engine_db_path = project_directory.joinpath(
        "boldigger3_data", f"{fasta_name}.duckdb"
    )

    print(
        f"{datetime.datetime.now().strftime('%H:%M:%S')}: Adding metadata to the ID engine results.".format()
    )
    # merge in the additional data
    merge_in_additional_data(id_engine_db_path, metadata_db_path)
