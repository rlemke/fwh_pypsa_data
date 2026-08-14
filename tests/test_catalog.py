# SPDX-License-Identifier: Apache-2.0
"""The catalogue, against a real copy of PyPSA-Eur's versions.csv.

Offline: the sample is checked in, so these assert on the shape of upstream's
own table rather than on the network. When upstream changes the table the
counts move — that is a signal worth seeing, not a flake, because the whole
port is driven by this file.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src" / "pypsa_data" / "tools"))
sys.path.insert(0, str(_ROOT / "src"))

from _pypsa_data_tools import catalog  # noqa: E402

SAMPLE = (_ROOT / "tests" / "versions.sample.csv").read_text()


@pytest.fixture
def entries():
    return catalog.parse_catalog(SAMPLE)


def test_the_catalogue_is_what_drives_the_port(entries):
    """59 datasets across primary+archive — the reason 73 near-identical
    Snakemake rules exist and one `foreach` replaces them."""
    s = catalog.summarise(entries)
    assert s["datasets"] == 59
    assert s["primary"] > 50 and s["archive"] > 50
    assert s["verifiable_pairs"] == 59


def test_build_rows_and_url_less_rows_are_dropped(entries):
    """`build` rows describe workflow products, not downloads."""
    assert all(e.source in (catalog.PRIMARY, catalog.ARCHIVE) for e in entries)
    assert all(e.url.startswith("http") for e in entries)


def test_select_returns_one_row_per_dataset_version(entries):
    chosen = catalog.select(entries, prefer=catalog.ARCHIVE)
    keys = [(d.name, d.version) for d in chosen]
    assert len(keys) == len(set(keys)), "a dataset+version was selected twice"


def test_preference_is_honoured_but_never_drops_a_dataset(entries):
    """A preference is a preference. Silently losing a dataset because the
    preferred mirror lacks a row would be a missing input three steps later."""
    archive_first = catalog.select(entries, prefer=catalog.ARCHIVE)
    primary_first = catalog.select(entries, prefer=catalog.PRIMARY)
    assert {d.name for d in archive_first} == {d.name for d in primary_first}
    assert sum(1 for d in archive_first if d.source == catalog.ARCHIVE) > sum(
        1 for d in primary_first if d.source == catalog.ARCHIVE
    )


def test_names_filter(entries):
    only = catalog.select(entries, names=["aquifer_data"])
    assert only and {d.name for d in only} == {"aquifer_data"}


def test_an_unknown_preference_is_rejected(entries):
    with pytest.raises(ValueError, match="prefer must be"):
        catalog.select(entries, prefer="whatever")


def test_archive_detection_drives_unpacking(entries):
    """Suffix-based on purpose: the catalogue does not say. Guessing wrong
    costs an extra listing; NOT unpacking leaves the model holding a zip."""
    zips = [d for d in entries if d.filename.lower().endswith(".zip")]
    assert zips and all(d.is_archive for d in zips)
    csvs = [d for d in entries if d.filename.lower().endswith(".csv")]
    assert all(not d.is_archive for d in csvs)


def test_mirror_pairs_are_same_dataset_and_version(entries):
    """A primary compared against a DIFFERENT version's archive would report a
    mismatch that means nothing."""
    for primary, archive in catalog.pairs_for_verification(entries):
        assert primary.name == archive.name and primary.version == archive.version
        assert primary.source == catalog.PRIMARY and archive.source == catalog.ARCHIVE
        assert primary.url != archive.url


def test_filename_survives_a_query_string():
    ds = catalog.Dataset("x", "v1", "primary", "https://h/a/b/file.zip?token=abc")
    assert ds.filename == "file.zip" and ds.is_archive


# ---------------------------------------------------------------------------
# Handlers + FFL
# ---------------------------------------------------------------------------


def test_catalog_handler_returns_the_declared_shape(tmp_path):
    from pypsa_data.handlers.catalog import catalog_handlers

    csv_path = tmp_path / "versions.csv"
    csv_path.write_text(SAMPLE)
    out = catalog_handlers.handle(
        {"_facet_name": "pypsa.data.Catalog", "csv_path": str(csv_path)}
    )
    assert set(out) == {"datasets", "count", "summary"}
    assert out["count"] == len(out["datasets"]) > 50
    first = out["datasets"][0]
    assert {"name", "version", "source", "url", "filename", "is_archive"} <= set(first)


def test_mirror_pairs_handler(tmp_path):
    from pypsa_data.handlers.catalog import catalog_handlers

    csv_path = tmp_path / "versions.csv"
    csv_path.write_text(SAMPLE)
    out = catalog_handlers.handle(
        {"_facet_name": "pypsa.data.MirrorPairs", "csv_path": str(csv_path)}
    )
    assert out["count"] == 59
    assert {"dataset", "primary_url", "archive_url", "filename"} <= set(out["pairs"][0])


def test_a_missing_catalogue_says_where_to_get_one(tmp_path):
    from pypsa_data.handlers.catalog import catalog_handlers

    with pytest.raises(FileNotFoundError, match="versions.csv"):
        catalog_handlers.handle(
            {"_facet_name": "pypsa.data.Catalog", "csv_path": str(tmp_path / "nope.csv")}
        )


def test_every_declared_event_facet_has_a_handler():
    import re

    from pypsa_data.handlers.catalog import catalog_handlers

    src = (_ROOT / "src" / "pypsa_data" / "ffl" / "pypsa_data.ffl").read_text()
    declared = {f"pypsa.data.{m}" for m in re.findall(r"event facet\s+(\w+)\s*\(", src)}
    assert declared == set(catalog_handlers.facet_names())


def test_the_ffl_compiles():
    from facetwork import parse
    from facetwork.validator import validate

    src = (_ROOT / "src" / "pypsa_data" / "ffl" / "pypsa_data.ffl").read_text()
    result = validate(parse(src))
    assert result.is_valid, "; ".join(e.message for e in result.errors)


def test_fanout_yields_aggregate():
    """`+=`, not `=`: a bare assign would leave one entry from a 59-way
    fan-out while every fetch still ran."""
    src = (_ROOT / "src" / "pypsa_data" / "ffl" / "pypsa_data.ffl").read_text()
    assert "stored += one.stored" in src
    assert "verdicts += one.verdict" in src
