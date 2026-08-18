import pytest
import pandas as pd
import numpy as np
from boldigger3.select_top_hit import get_threshold, move_threshold_up, flag_hits, find_top_hit

THRESHOLDS = [97, 95, 90, 85, 75, 50]


def make_hits(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal hits DataFrame compatible with find_top_hit / flag_hits."""
    defaults = {
        "id": "seq1",
        "phylum": "Arthropoda",
        "class": "Insecta",
        "order": "Diptera",
        "family": "Drosophilidae",
        "genus": "Drosophila",
        "species": "Drosophila melanogaster",
        "pct_identity": 99.0,
        "bin_uri": "BOLD:AAA0001",
        "status": "public",
        "fasta_order": 0,
        "identification_method": "Morphology",
        "process_id": "PROC001",
    }
    data = [{**defaults, **row} for row in rows]
    df = pd.DataFrame(data)
    str_cols = ["id", "phylum", "class", "order", "family", "genus", "species",
                "bin_uri", "status", "identification_method", "process_id"]
    for col in str_cols:
        df[col] = df[col].astype("string")
    return df


# ---------------------------------------------------------------------------
# get_threshold
# ---------------------------------------------------------------------------

class TestGetThreshold:
    def test_species_level(self):
        df = make_hits([{"pct_identity": 98.0}])
        assert get_threshold(df, THRESHOLDS) == (97, "species")

    def test_genus_level(self):
        df = make_hits([{"pct_identity": 96.0}])
        assert get_threshold(df, THRESHOLDS) == (95, "genus")

    def test_family_level(self):
        df = make_hits([{"pct_identity": 92.0}])
        assert get_threshold(df, THRESHOLDS) == (90, "family")

    def test_order_level(self):
        df = make_hits([{"pct_identity": 87.0}])
        assert get_threshold(df, THRESHOLDS) == (85, "order")

    def test_class_level(self):
        df = make_hits([{"pct_identity": 76.0}])
        assert get_threshold(df, THRESHOLDS) == (75, "class")

    def test_phylum_level(self):
        df = make_hits([{"pct_identity": 60.0}])
        assert get_threshold(df, THRESHOLDS) == (50, "phylum")

    def test_no_match(self):
        df = make_hits([{
            "pct_identity": 0.0,
            "phylum": "no-match", "class": "no-match", "order": "no-match",
            "family": "no-match", "genus": "no-match", "species": "no-match",
        }])
        assert get_threshold(df, THRESHOLDS) == (0, "no-match")

    def test_exact_threshold_is_species(self):
        df = make_hits([{"pct_identity": 97.0}])
        assert get_threshold(df, THRESHOLDS) == (97, "species")

    def test_uses_maximum_of_multiple_hits(self):
        # lowest hit is genus-level, but the max is species-level
        df = make_hits([
            {"pct_identity": 99.0},
            {"pct_identity": 93.0},
        ])
        assert get_threshold(df, THRESHOLDS) == (97, "species")


# ---------------------------------------------------------------------------
# move_threshold_up
# ---------------------------------------------------------------------------

class TestMoveThresholdUp:
    def test_species_to_genus(self):
        assert move_threshold_up(97, THRESHOLDS) == (95, "genus")

    def test_genus_to_family(self):
        assert move_threshold_up(95, THRESHOLDS) == (90, "family")

    def test_family_to_order(self):
        assert move_threshold_up(90, THRESHOLDS) == (85, "order")

    def test_order_to_class(self):
        assert move_threshold_up(85, THRESHOLDS) == (75, "class")

    def test_class_to_phylum(self):
        assert move_threshold_up(75, THRESHOLDS) == (50, "phylum")


# ---------------------------------------------------------------------------
# flag_hits
# ---------------------------------------------------------------------------

def make_final_top_hit(records_ratio: float = 0.95, bins: str = "BOLD:AAA0001") -> pd.DataFrame:
    return pd.DataFrame({"records_ratio": [records_ratio], "BIN": [bins]})


class TestFlagHits:
    def test_no_flags(self):
        top_hits = make_hits([
            {"identification_method": "Morphology", "status": "public"},
            {"identification_method": "Morphology", "status": "public"},
        ])
        flags = flag_hits(top_hits, make_final_top_hit())
        assert flags == "||||"

    def test_flag1_reverse_bin_taxonomy(self):
        top_hits = make_hits([
            {"identification_method": "BOLD Taxonomy"},
            {"identification_method": "BOLD Taxonomy"},
        ])
        flags = flag_hits(top_hits, make_final_top_hit())
        assert flags.split("|")[0] == "1"

    def test_flag1_not_raised_when_mixed_methods(self):
        top_hits = make_hits([
            {"identification_method": "BOLD Taxonomy"},
            {"identification_method": "Morphology"},
        ])
        flags = flag_hits(top_hits, make_final_top_hit())
        assert flags.split("|")[0] == ""

    def test_flag1_not_raised_when_all_na(self):
        top_hits = make_hits([
            {"identification_method": pd.NA},
            {"identification_method": pd.NA},
        ])
        flags = flag_hits(top_hits, make_final_top_hit())
        assert flags.split("|")[0] == ""

    def test_flag2_low_ratio(self):
        top_hits = make_hits([
            {"identification_method": "Morphology", "status": "public"},
            {"identification_method": "Morphology", "status": "public"},
        ])
        flags = flag_hits(top_hits, make_final_top_hit(records_ratio=0.5))
        assert flags.split("|")[1] == "2"

    def test_flag2_not_raised_at_boundary(self):
        top_hits = make_hits([{"identification_method": "Morphology", "status": "public"}] * 2)
        flags = flag_hits(top_hits, make_final_top_hit(records_ratio=0.9))
        assert flags.split("|")[1] == ""

    def test_flag3_all_private(self):
        top_hits = make_hits([
            {"identification_method": "Morphology", "status": "private"},
            {"identification_method": "Morphology", "status": "private"},
        ])
        flags = flag_hits(top_hits, make_final_top_hit())
        assert flags.split("|")[2] == "3"

    def test_flag3_not_raised_when_mixed_status(self):
        top_hits = make_hits([
            {"identification_method": "Morphology", "status": "private"},
            {"identification_method": "Morphology", "status": "public"},
        ])
        flags = flag_hits(top_hits, make_final_top_hit())
        assert flags.split("|")[2] == ""

    def test_flag4_unique_hit(self):
        top_hits = make_hits([{"identification_method": "Morphology", "status": "public"}])
        flags = flag_hits(top_hits, make_final_top_hit())
        assert flags.split("|")[3] == "4"

    def test_flag4_not_raised_with_multiple_hits(self):
        top_hits = make_hits([{"identification_method": "Morphology", "status": "public"}] * 2)
        flags = flag_hits(top_hits, make_final_top_hit())
        assert flags.split("|")[3] == ""

    def test_flag5_multiple_bins(self):
        top_hits = make_hits([{"identification_method": "Morphology", "status": "public"}] * 2)
        flags = flag_hits(top_hits, make_final_top_hit(bins="BOLD:AAA0001|BOLD:AAA0002"))
        assert flags.split("|")[4] == "5"

    def test_flag5_not_raised_with_single_bin(self):
        top_hits = make_hits([{"identification_method": "Morphology", "status": "public"}] * 2)
        flags = flag_hits(top_hits, make_final_top_hit(bins="BOLD:AAA0001"))
        assert flags.split("|")[4] == ""


# ---------------------------------------------------------------------------
# find_top_hit
# ---------------------------------------------------------------------------

class TestFindTopHit:
    def test_species_level_most_common_wins(self):
        hits = make_hits([
            {"species": "Drosophila melanogaster", "pct_identity": 99.0},
            {"species": "Drosophila melanogaster", "pct_identity": 98.5},
            {"species": "Drosophila melanogaster", "pct_identity": 98.0},
            {"species": "Drosophila simulans", "pct_identity": 98.9},
        ])
        result = find_top_hit(hits, THRESHOLDS)
        assert result["selected_level"].item() == "species"
        assert result["species"].item() == "Drosophila melanogaster"
        assert result["records"].item() == 3

    def test_no_match_returned_directly(self):
        hits = make_hits([{
            "pct_identity": 0.0,
            "phylum": "no-match", "class": "no-match", "order": "no-match",
            "family": "no-match", "genus": "no-match", "species": "no-match",
        }])
        result = find_top_hit(hits, THRESHOLDS)
        assert result["species"].item() == "no-match"
        assert result["pct_identity"].item() == 0.0

    def test_falls_back_to_genus_when_species_missing(self):
        hits = make_hits([
            {"species": pd.NA, "genus": "Drosophila", "pct_identity": 99.0},
            {"species": pd.NA, "genus": "Drosophila", "pct_identity": 98.5},
        ])
        result = find_top_hit(hits, THRESHOLDS)
        assert result["selected_level"].item() == "genus"
        assert result["genus"].item() == "Drosophila"
        assert pd.isna(result["species"].item())

    def test_species_info_cleared_below_species_threshold(self):
        # pct_identity between genus and species threshold → species should be NA in output
        hits = make_hits([
            {"species": "Drosophila melanogaster", "pct_identity": 96.0},
            {"species": "Drosophila melanogaster", "pct_identity": 95.5},
        ])
        result = find_top_hit(hits, THRESHOLDS)
        assert result["selected_level"].item() == "genus"
        assert pd.isna(result["species"].item())

    def test_bin_collected_at_species_level(self):
        hits = make_hits([
            {"species": "Drosophila melanogaster", "pct_identity": 99.0, "bin_uri": "BOLD:AAA0001"},
            {"species": "Drosophila melanogaster", "pct_identity": 98.5, "bin_uri": "BOLD:AAA0001"},
        ])
        result = find_top_hit(hits, THRESHOLDS)
        assert "BOLD:AAA0001" in result["BIN"].item()

    def test_bin_empty_below_species_threshold(self):
        hits = make_hits([
            {"species": "Drosophila melanogaster", "pct_identity": 96.0, "bin_uri": "BOLD:AAA0001"},
            {"species": "Drosophila melanogaster", "pct_identity": 95.5, "bin_uri": "BOLD:AAA0001"},
        ])
        result = find_top_hit(hits, THRESHOLDS)
        assert result["BIN"].item() == ""

    def test_fasta_order_preserved(self):
        hits = make_hits([
            {"pct_identity": 99.0, "fasta_order": 42},
            {"pct_identity": 98.0, "fasta_order": 42},
        ])
        result = find_top_hit(hits, THRESHOLDS)
        assert result["fasta_order"].item() == 42

    def test_below_phylum_threshold_does_not_crash(self):
        # pct_identity is below even the virtual phylum threshold (50) -
        # previously raised IndexError in move_threshold_up
        hits = make_hits([{"pct_identity": 30.0}])
        result = find_top_hit(hits, THRESHOLDS)
        assert pd.isna(result["selected_level"].item())
        assert pd.isna(result["phylum"].item())
        assert result["records"].item() == 0

    def test_all_na_phylum_does_not_crash(self):
        # no hit has usable phylum information at all - previously raised
        # IndexError in move_threshold_up once the phylum level was reached
        hits = make_hits([
            {"pct_identity": 60.0, "phylum": pd.NA},
            {"pct_identity": 55.0, "phylum": pd.NA},
        ])
        result = find_top_hit(hits, THRESHOLDS)
        assert pd.isna(result["selected_level"].item())
        assert pd.isna(result["phylum"].item())
        assert result["records"].item() == 0
