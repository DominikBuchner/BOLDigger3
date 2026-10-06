"""Local coverage filtering and non-destructive cache preflight."""
import argparse
import math
from pathlib import Path

import duckdb
import pandas as pd

ALIGNMENT_FIELDS = {
    "query_cover": "qcovhsp",
    "alignment_length": "length",
    "qstart": "qstart",
    "qend": "qend",
    "gapopen": "gapopen",
}


def percentage(value):
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise argparse.ArgumentTypeError("query coverage must be a number from 0 to 100")
    if not math.isfinite(number) or not 0 <= number <= 100:
        raise argparse.ArgumentTypeError("query coverage must be a finite percentage from 0 to 100")
    return number


def positive_bp(value):
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        raise argparse.ArgumentTypeError("minimum overlap must be a positive integer in bp")
    if isinstance(value, bool) or number < 1 or (not isinstance(value, str) and number != value):
        raise argparse.ArgumentTypeError("minimum overlap must be a positive integer in bp")
    return number


def coverage_error():
    return ValueError(
        "Cached results lack query_cover (BOLD qcovhsp). Use a new FASTA filename "
        "in a fresh output directory to query BOLD with coverage support. "
        "The existing database has not been changed."
    )


def annotate_hits(hits, min_query_cover=None):
    """Keep every hit; unknown/out-of-range coverage fails an active filter."""
    if min_query_cover is None:
        return hits.assign(passes_query_cover_filter=True)
    min_query_cover = percentage(min_query_cover)
    if "query_cover" not in hits.columns:
        raise coverage_error()
    coverage = pd.to_numeric(hits["query_cover"], errors="coerce")
    passes = (coverage.between(0, 100) & coverage.ge(min_query_cover)).fillna(False)
    return hits.assign(passes_query_cover_filter=passes)


def validate_cache(fasta_path, min_query_cover=None, min_overlap_bp=100,
                   table="id_engine_results"):
    """Inspect existing cache read-only, before downloads or metadata writes."""
    fasta = Path(fasta_path)
    path = fasta.parent / "boldigger3_data" / (fasta.stem + ".duckdb")
    if not path.is_file():
        return
    if table not in {"id_engine_results", "final_results"}:
        raise ValueError("Unexpected cache table")
    with duckdb.connect(str(path), read_only=True) as connection:
        columns = {row[0] for row in connection.execute(f"DESCRIBE {table}").fetchall()}
        if min_query_cover is not None and "query_cover" not in columns:
            raise coverage_error()
        # Old vanilla caches used mo=100. New caches record it explicitly.
        if "min_overlap_bp" in columns:
            values = connection.execute(f"SELECT DISTINCT min_overlap_bp FROM {table}").fetchall()
            mismatch = any(row[0] != min_overlap_bp for row in values)
        else:
            mismatch = min_overlap_bp != 100
        if mismatch:
            raise ValueError("Cached request overlap differs from --min-overlap-bp; use a fresh output directory.")
