"""Additional boundaries and CLI routing, using synthetic inputs only."""
import argparse
from collections import OrderedDict
import pickle
import sys

import pandas as pd
import pytest

from boldigger3 import __main__ as cli, add_metadata, id_engine, select_top_hit
from boldigger3.query_cover import annotate_hits, positive_bp
from test_select_top_hit import make_hits, THRESHOLDS
from test_query_cover import Response, metadata, saved_json


@pytest.mark.parametrize("coverage,passes", [(89.999, False), (90, True), (90.001, True)])
def test_inclusive_boundary(coverage, passes):
    result = annotate_hits(make_hits([{"query_cover": coverage}]), 90)
    assert bool(result.passes_query_cover_filter.item()) is passes


@pytest.mark.parametrize("threshold,expected", [(0, [True, True, True, False]),
                                               (100, [False, False, True, False])])
def test_extreme_thresholds(threshold, expected):
    hits = make_hits([{"query_cover": value} for value in [0, 99.999, 100, None]])
    assert annotate_hits(hits, threshold).passes_query_cover_filter.tolist() == expected


def test_no_filter_accepts_legacy_hits_without_coverage():
    hits = make_hits([{}, {"species": "Other species"}])
    assert annotate_hits(hits).passes_query_cover_filter.all()
    pd.testing.assert_frame_equal(select_top_hit.classify_hits(hits, THRESHOLDS),
                                  select_top_hit.find_top_hit(hits, THRESHOLDS))


@pytest.mark.parametrize("value", [1.5, True, float("inf"), float("nan"), None])
def test_overlap_rejects_invalid_api_values(value):
    with pytest.raises(argparse.ArgumentTypeError):
        positive_bp(value)


@pytest.mark.parametrize("threshold,overlap", [(None, 128), (90, 100), (90, 128)])
def test_cli_keeps_coverage_and_overlap_separate(tmp_path, monkeypatch, threshold, overlap):
    arguments = ["boldigger3", "identify", str(tmp_path / "tiny.fas"),
                 str(tmp_path / "meta.ddb"), "--db", "3", "--mode", "3"]
    if threshold is not None:
        arguments += ["--min-query-cover", str(threshold)]
    if overlap != 100:
        arguments += ["--min-overlap-bp", str(overlap)]
    monkeypatch.setattr(sys, "argv", arguments)
    monkeypatch.setattr(cli.luddite, "get_version_pypi", lambda name: cli.version(name))
    calls = {}
    monkeypatch.setattr(id_engine, "main", lambda *args, **kwargs: calls.update(engine=kwargs))
    monkeypatch.setattr(cli.add_metadata, "main", lambda **kwargs: None)
    monkeypatch.setattr(select_top_hit, "main", lambda **kwargs: calls.update(classifier=kwargs))
    cli.main()
    assert calls["engine"]["database"] == 3
    assert calls["engine"]["min_overlap_bp"] == overlap
    assert calls["classifier"]["min_query_cover"] == threshold
    assert id_engine.build_url_params(3, 3, overlap)[1]["mo"] == overlap


def test_parser_coerces_invalid_coverage_to_missing(tmp_path):
    (tmp_path / "boldigger3_data").mkdir()
    request = id_engine.BoldIdRequest()
    request.database, request.operating_mode, request.params = 3, 3, {"mo": 100}
    obj = saved_json()[0]
    for record in obj["results"].values():
        record["qcovhsp"] = "unavailable"
    id_engine.parse_and_save_data(request, Response([obj]), {"mixed": 0}, 1, tmp_path, "tiny")
    frame = pd.read_parquet(tmp_path / "boldigger3_data/request_id_1_tiny.parquet.snappy")
    assert frame.query_cover.isna().all()
    assert not annotate_hits(frame, 0).passes_query_cover_filter.any()


def test_pending_queue_rejects_changed_overlap(tmp_path):
    fasta = tmp_path / "tiny.fas"
    fasta.write_text(">synthetic\n" + "ACGT" * 60 + "\n")
    out = tmp_path / "boldigger3_data"
    out.mkdir()
    request = id_engine.BoldIdRequest()
    request.database, request.operating_mode, request.params = 3, 3, {"mo": 128}
    queue = {"waiting": OrderedDict([(0, request)]), "active": {}}
    with (out / "tiny_download_queue.pkl").open("wb") as stream:
        pickle.dump(queue, stream)
    with pytest.raises(ValueError, match="different minimum overlap"):
        id_engine.main(str(fasta), database=3, operating_mode=3, min_overlap_bp=100)


def test_quoted_query_id_is_preserved(tmp_path):
    query_id = "synthetic'query"
    fasta = tmp_path / "tiny.fas"
    fasta.write_text(f">{query_id}\n" + "ACGT" * 60 + "\n")
    out = tmp_path / "boldigger3_data"
    out.mkdir()
    meta = tmp_path / "meta.ddb"
    metadata(meta)
    request = id_engine.BoldIdRequest()
    request.database, request.operating_mode, request.params = 3, 3, {"mo": 100}
    obj = saved_json()[0]
    obj["seqid"] = query_id
    id_engine.parse_and_save_data(request, Response([obj]), {query_id: 0}, 1, tmp_path, "tiny")
    id_engine.parquet_to_duckdb(tmp_path, out / "tiny.duckdb")
    add_metadata.main(str(fasta), str(meta))
    select_top_hit.main(str(fasta), THRESHOLDS, min_query_cover=90)
    final = pd.read_parquet(out / "tiny_identification_result.parquet.snappy")
    assert final.id.item() == query_id
    assert final.records.item() == 2
