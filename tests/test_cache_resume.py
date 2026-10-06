import pandas as pd
import duckdb
import pytest

from boldigger3 import id_engine
from boldigger3.query_cover import annotate_hits
from test_query_cover import Response, saved_json


def test_missing_column_fails():
    with pytest.raises(ValueError, match="lack query_cover"):
        annotate_hits(pd.DataFrame({"id":["q"]}), 90)


def test_old_cache_resume_and_missing_fields(tmp_path):
    out = tmp_path / "boldigger3_data"
    out.mkdir()
    request = id_engine.BoldIdRequest()
    request.database, request.operating_mode, request.params = 3, 3, {"mo":100}
    obj = saved_json()[0]
    id_engine.parse_and_save_data(request, Response([obj]), {"mixed":0}, 1, tmp_path, "tiny")
    cache = out / "tiny.duckdb"
    id_engine.parquet_to_duckdb(tmp_path, cache)
    with duckdb.connect(str(cache)) as con:
        for column in ["query_cover","alignment_length","qstart","qend","gapopen","min_overlap_bp"]:
            con.execute(f'ALTER TABLE id_engine_results DROP COLUMN "{column}"')
    new = {"seqid":"missing", "results":obj["results"]}
    for record in new["results"].values():
        for field in ["qcovhsp","length","qstart","qend","gapopen"]:
            record.pop(field)
    id_engine.parse_and_save_data(request, Response([new]), {"missing":1}, 2, tmp_path, "tiny")
    id_engine.parquet_to_duckdb(tmp_path, cache)
    with duckdb.connect(str(cache)) as con:
        rows = con.execute("SELECT * FROM id_engine_results").df()
    assert len(rows) == 10
    assert rows.query_cover.isna().all()
    assert rows.min_overlap_bp.eq(100).all()
    assert not annotate_hits(rows, 90).passes_query_cover_filter.any()


def test_all_no_match_parser(tmp_path):
    (tmp_path / "boldigger3_data").mkdir()
    request = id_engine.BoldIdRequest()
    request.database, request.operating_mode, request.params = 3, 3, {"mo":128}
    id_engine.parse_and_save_data(request, Response([{"seqid":"q", "results":{}}]), {"q":0}, 1, tmp_path, "tiny")
    frame = pd.read_parquet(tmp_path / "boldigger3_data" / "request_id_1_tiny.parquet.snappy")
    assert frame.query_cover.isna().all()
    assert frame.min_overlap_bp.item() == 128
