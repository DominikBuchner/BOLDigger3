"""Offline parser, metadata, CLI and diagnostic integration.

All response identifiers, BINs, sequence content and metadata are synthetic.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

import duckdb
import pandas as pd
import pytest

from boldigger3 import id_engine, select_top_hit, add_metadata
from boldigger3.query_cover import annotate_hits, percentage, positive_bp, validate_cache
from test_select_top_hit import make_hits, THRESHOLDS


CLI = [sys.executable, "-c",
       "import luddite; from importlib.metadata import version; "
       "luddite.get_version_pypi = lambda name: version(name); "
       "from boldigger3.__main__ import main; main()"]


@pytest.mark.parametrize("value", [0, 90, 100, "94.5"])
def test_percentage_valid(value):
    assert percentage(value) == float(value)


@pytest.mark.parametrize("value", [-1, 101, "nan", "inf", "x"])
def test_percentage_invalid(value):
    with pytest.raises(argparse.ArgumentTypeError):
        percentage(value)


def test_candidate_gate(monkeypatch):
    hits = make_hits([{"query_cover": q} for q in [100, 94, 89, 72]])
    original = select_top_hit.find_top_hit
    seen = []
    def spy(candidates, thresholds):
        seen.extend(candidates.query_cover.tolist())
        return original(candidates, thresholds)
    monkeypatch.setattr(select_top_hit, "find_top_hit", spy)
    result = select_top_hit.classify_hits(hits, THRESHOLDS, 90)
    assert seen == [100, 94]
    assert result.records.item() == 2
    assert result.records_ratio.item() == 1
    assert annotate_hits(hits, 90).passes_query_cover_filter.tolist() == [True, True, False, False]


def test_invalid_coverage_and_boundary():
    hits = make_hits([{"query_cover": q} for q in [90, None, "garbage", -1, 101, float("inf"), 0]])
    assert annotate_hits(hits, 90).passes_query_cover_filter.tolist() == [True, False, False, False, False, False, False]
    assert annotate_hits(hits, 0).passes_query_cover_filter.tolist()[-1]
    assert annotate_hits(hits, None).passes_query_cover_filter.all()


def test_all_rejected(monkeypatch):
    hits = make_hits([{"query_cover": 89}])
    monkeypatch.setattr(select_top_hit, "find_top_hit", lambda *args: pytest.fail("rejected hits entered classification"))
    result = select_top_hit.classify_hits(hits, THRESHOLDS, 90)
    assert result.query_cover_filter_status.item() == "no_hit_above_min_query_cover"
    assert result.status.item() == "unresolved"
    assert result[["phylum", "class", "order", "family", "genus", "species", "pct_identity", "selected_level", "BIN"]].isna().all().all()
    assert result.records.item() == 0


def test_baseline_equality():
    for rows in [[{}], [{"species": "Another species"}, {}], [{"pct_identity": 92}], [{"pct_identity": 40}]]:
        hits = make_hits(rows)
        pd.testing.assert_frame_equal(select_top_hit.classify_hits(hits, THRESHOLDS), select_top_hit.find_top_hit(hits, THRESHOLDS))


def metadata(path):
    with duckdb.connect(str(path)) as con:
        con.execute('CREATE TABLE bold_public (processid VARCHAR, sex VARCHAR, life_stage VARCHAR, inst VARCHAR, "country/ocean" VARCHAR, identified_by VARCHAR, identification_method VARCHAR, coord VARCHAR, nuc VARCHAR, marker_code VARCHAR)')
        con.execute("INSERT INTO bold_public VALUES ('P1','F','adult','test','Belgium','test','Morphology','[50,4]','ACGT','COI-5P')")


def saved_json():
    tax = dict(phylum="Arthropoda", **{"class": "Insecta"}, order="Diptera", family="Drosophilidae", genus="Drosophila", species="Drosophila melanogaster", subfamily=None, taxid_count=1)
    def hit(q, other=False):
        return dict(qcovhsp=q, length=200, qstart=1, qend=200, gapopen=0, pdist=0.5 if other else 1.0,
                    taxonomy={**tax, **(dict(phylum="Annelida", **{"class": "Clitellata"}, order="Haplotaxida", family="Lumbricidae", genus="Lumbricus", species="Lumbricus terrestris") if other else {})})
    return [dict(seqid="mixed", results={"P3|x|BOLD:CCC|x|public":hit(72, True), "P4|x|BOLD:DDD|x|public":hit(89, True), "P5|x|BOLD:EEE|x|public":hit(80, True), "P1|x|BOLD:AAA|x|public":hit("100"), "P2|x|BOLD:BBB|x|public":hit(94)}),
            dict(seqid="rejected", results={"P6|x|BOLD:FFF|x|public":hit(72)}),
            dict(seqid="nomatch", results={})]


class Response:
    def __init__(self, objects):
        self.objects = objects
    def iter_lines(self):
        return [json.dumps(obj).encode() for obj in self.objects]


def test_offline_cli_pipeline(tmp_path):
    fasta = tmp_path / "tiny.fas"
    fasta.write_text("".join(f">{key}\n" + "ACGT" * 60 + "\n" for key in ["mixed", "rejected", "nomatch"]))
    out = tmp_path / "boldigger3_data"
    out.mkdir()
    meta = tmp_path / "meta.ddb"
    metadata(meta)
    request = id_engine.BoldIdRequest()
    request.database, request.operating_mode = 3, 3
    request.params = {"mo":100}
    objects = saved_json()
    (tmp_path / "saved.jsonl").write_text("\n".join(json.dumps(obj) for obj in objects))
    id_engine.parse_and_save_data(request, Response(objects), {"mixed":0, "rejected":1, "nomatch":2}, 1, tmp_path, "tiny")
    cache = out / "tiny.duckdb"
    id_engine.parquet_to_duckdb(tmp_path, cache)
    add_metadata.main(str(fasta), str(meta))
    with duckdb.connect(str(cache)) as con:
        raw = con.execute("SELECT * FROM final_results").df()
    assert raw.query_cover.dropna().tolist() == [72, 89, 80, 100, 94, 72]
    assert raw.alignment_length.dropna().eq(200).all()
    baseline = select_top_hit.classify_hits(select_top_hit.clean_dataframe(raw[raw.id.eq("mixed")].copy()), THRESHOLDS)
    assert baseline.phylum.item() == "Annelida"
    for threshold in [90, 80, None]:
        command = [*CLI, "identify", str(fasta), str(meta), "--db", "3", "--mode", "3"]
        if threshold is not None:
            command += ["--min-query-cover", str(threshold)]
        run = subprocess.run(command, capture_output=True, text=True, timeout=60)
        assert run.returncode == 0, run.stdout + run.stderr
        all_hits = pd.read_excel(out / "tiny_bold_results_part_1.xlsx")
        final = pd.read_parquet(out / "tiny_identification_result.parquet.snappy")
        assert len(all_hits) == 7
        assert final.id.tolist() == ["mixed", "rejected", "nomatch"]
        if threshold == 90:
            mixed = final[final.id.eq("mixed")].iloc[0]
            assert mixed.phylum == "Arthropoda"
            assert mixed.records == 2 and mixed.records_ratio == 1
            assert mixed.BIN == "BOLD:AAA|BOLD:BBB"
            assert mixed["flags"] == "||||5"
            assert all_hits[all_hits.id.eq("mixed")].passes_query_cover_filter.tolist() == [False, False, False, True, True]
            assert final.loc[final.id.eq("rejected"), "query_cover_filter_status"].item() == "no_hit_above_min_query_cover"
            assert final.loc[final.id.eq("nomatch"), "query_cover_filter_status"].item() == "no_match"
            final.to_csv(tmp_path / "filtered.csv", index=False)
            all_hits.to_csv(tmp_path / "all.csv", index=False)
        elif threshold is None:
            assert final.loc[final.id.eq("mixed"), "phylum"].item() == "Annelida"
            assert "min_query_cover" not in final
            assert all_hits.passes_query_cover_filter.all()


def test_cache_preflight(tmp_path):
    out = tmp_path / "boldigger3_data"
    out.mkdir()
    fasta = tmp_path / "old.fas"
    cache = out / "old.duckdb"
    with duckdb.connect(str(cache)) as con:
        con.execute("CREATE TABLE id_engine_results (id VARCHAR)")
    before = cache.read_bytes()
    with pytest.raises(ValueError, match="lack query_cover"):
        validate_cache(fasta, 90)
    validate_cache(fasta, None)
    with pytest.raises(ValueError, match="overlap"):
        validate_cache(fasta, None, 128)
    assert cache.read_bytes() == before


def test_overlap_and_cli():
    assert id_engine.build_url_params(3, 3)[1]["mo"] == 100
    url, params = id_engine.build_url_params(3, 3, 128)
    assert params["mo"] == 128 and "mo=128" in url
    assert positive_bp("128") == 128
    for value in ["0", "-1", "1.5"]:
        with pytest.raises(argparse.ArgumentTypeError):
            positive_bp(value)
    help_run = subprocess.run([*CLI, "identify", "--help"], capture_output=True, text=True, timeout=30)
    assert help_run.returncode == 0 and "--min-query-cover" in help_run.stdout and "--min-overlap-bp" in help_run.stdout
    for value in ["-1", "101", "nan"]:
        run = subprocess.run([*CLI, "identify", "x", "y", "--db", "3", "--mode", "3", "--min-query-cover", value], capture_output=True, text=True, timeout=30)
        assert run.returncode == 2 and "percentage" in run.stderr
