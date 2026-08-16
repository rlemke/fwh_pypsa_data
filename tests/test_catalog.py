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
    assert s["verifiable_pairs"] == 58


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
    assert out["count"] == 58
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


# ---------------------------------------------------------------------------
# Tag-aware selection — upstream's `dataset_version` rule, not a superset of it
# ---------------------------------------------------------------------------


def test_default_selects_one_download_per_dataset(entries):
    """Upstream's `dataset_version` filters on source + the `supported` and
    `latest` tags and then `.squeeze()`s to a SINGLE row, so a full run is one
    download per dataset. Ignoring the tags is not a harmless superset: it
    fetches 92 files for 59 datasets."""
    chosen = catalog.select(entries)
    assert len(chosen) == 59
    assert len({d.name for d in chosen}) == 59


def test_all_versions_is_the_escape_hatch(entries):
    """Reproducing an older model needs the versions the default drops."""
    every = catalog.select(entries, versions=catalog.V_ALL, supported_only=False)
    assert len(every) == 92
    costs = [d for d in every if d.name == "costs"]
    assert len(costs) > 1, "the escape hatch must keep every version of a dataset"
    assert len([d for d in catalog.select(entries) if d.name == "costs"]) == 1


def test_deprecated_and_unsupported_versions_are_not_fetched(entries):
    """The tags column is exactly the supported-version marker. Fetching a
    version upstream marks `not-supported` builds a model from data upstream
    says will not work."""
    chosen = catalog.select(entries)
    for d in chosen:
        assert "deprecated" not in d.tag_set
        assert "not-supported" not in d.tag_set


def test_the_moving_unversioned_row_does_not_duplicate_the_pinned_one(entries):
    """Eleven datasets carry a dated row AND a version=`unknown` row — the
    moving un-versioned primary. Keyed on (dataset, version) those look like
    two versions, so `prefer="archive"` could not dedupe them and the moving
    target was fetched next to the pinned mirror copy of the same thing."""
    for name in ("wdpa", "worldbank_urban_population", "eurostat_household_balances"):
        picked = [d for d in catalog.select(entries) if d.name == name]
        assert len(picked) == 1, f"{name}: {[(d.version, d.source) for d in picked]}"


def test_a_dataset_whose_rows_all_fail_the_filter_is_not_dropped():
    """Over-fetching is recoverable; silently retrieving nothing for a dataset
    the caller asked for surfaces as a missing input three steps later."""
    csv_text = (
        "dataset,version,source,tags,added,note,url\n"
        "orphan,1.0,primary,deprecated not-supported,2020-01-01,,https://x/o-1.0.zip\n"
        "orphan,0.9,archive,deprecated not-supported,2020-01-01,,https://y/o-0.9.zip\n"
    )
    got = catalog.select(catalog.parse_catalog(csv_text))
    assert {d.name for d in got} == {"orphan"}


def test_an_unknown_versions_policy_is_rejected(entries):
    with pytest.raises(ValueError, match="versions must be"):
        catalog.select(entries, versions="newest")


def test_summarise_reports_what_a_full_run_would_cost(entries):
    """The count that decides whether a run fits the link is the number of
    downloads, not the number of rows in the table."""
    s = catalog.summarise(entries)
    assert s["downloads_default"] == 59
    assert s["downloads_all_versions"] == 92
    assert s["downloads_default"] < s["rows"]


def test_mirror_pairs_can_be_restricted_to_names(entries):
    """Verification fetches BOTH sides of a pair, so all 59 is several GB.
    Without a filter the check is all-or-nothing, i.e. nothing on a slow link."""
    one = catalog.pairs_for_verification(entries, names=["aquifer_data"])
    assert [p.name for p, _ in one] == ["aquifer_data"]
    assert len(catalog.pairs_for_verification(entries)) == 58


def test_mirror_pairs_ignore_the_tags(entries):
    """A mirror of a deprecated version is still supposed to match what it
    mirrors — verification asks a different question from selection."""
    pairs = catalog.pairs_for_verification(entries)
    assert any("deprecated" in p.tag_set or "not-supported" in p.tag_set for p, _ in pairs)


def test_handler_payload_carries_the_selection_policy(tmp_path):
    """The FFL params have to reach `select` under exactly these payload keys —
    a typo here is silent: the run just fetches the wrong set."""
    from pypsa_data.handlers.catalog import catalog_handlers

    csv_path = tmp_path / "versions.csv"
    csv_path.write_text(SAMPLE)

    def run(**extra):
        return catalog_handlers.handle(
            {"_facet_name": "pypsa.data.Catalog", "csv_path": str(csv_path), **extra}
        )

    assert run()["count"] == 59
    assert run(versions="all", supported_only=False)["count"] == 92
    # supported_only=False must not silently re-enable every version
    assert run(supported_only=False)["count"] == 59


def test_mirror_pairs_handler_takes_names(tmp_path):
    from pypsa_data.handlers.catalog import catalog_handlers

    csv_path = tmp_path / "versions.csv"
    csv_path.write_text(SAMPLE)
    out = catalog_handlers.handle(
        {
            "_facet_name": "pypsa.data.MirrorPairs",
            "csv_path": str(csv_path),
            "names": ["aquifer_data"],
        }
    )
    assert out["count"] == 1 and out["pairs"][0]["dataset"] == "aquifer_data"


def test_a_side_that_is_not_a_plain_url_is_excluded_from_comparison():
    """`ons_lad`'s primary is an ArcGIS /query endpoint: a bare GET returns the
    HTML query form with HTTP 200. Comparing that against the archive's real
    GeoJSON reports "the mirror drifted", which is a false alarm wearing this
    check's own clothes — it is what made the first run of this check wrong."""
    entries = catalog.parse_catalog(SAMPLE)
    assert "ons_lad" not in {p.name for p, _ in catalog.pairs_for_verification(entries)}
    assert catalog.pairs_for_verification(entries, names=["ons_lad"]) == []


def test_a_template_url_falls_back_to_the_other_source(entries):
    """WDPA's primary URL carries a `{bYYYY}` that retrieve.smk substitutes each
    month. Fetched as written it is not an address at all, so preferring the
    primary must yield the archive rather than a download of the literal."""
    picked = catalog.select(entries, names=["wdpa", "wdpa_marine"], prefer=catalog.PRIMARY)
    assert picked and all(d.source == catalog.ARCHIVE for d in picked)
    assert all(not d.unfetchable_reason for d in picked)


def test_the_default_path_has_nothing_unfetchable(entries):
    """Every archive row is a static file, which is why the fan-out works: the
    request-shape problem is confined to `prefer="primary"`."""
    assert not [d for d in catalog.select(entries) if d.unfetchable_reason]
    assert not [d for d in catalog.select(entries, prefer=catalog.PRIMARY) if d.unfetchable_reason]
