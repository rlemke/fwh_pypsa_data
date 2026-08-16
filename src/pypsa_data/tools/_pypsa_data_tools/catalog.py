# SPDX-License-Identifier: Apache-2.0
"""The PyPSA-Eur dataset catalogue — `data/versions.csv`, read as data.

PyPSA-Eur's `rules/retrieve.smk` is 1,631 lines and 73 rules, 58 of which are
`storage(...)` downloads. Almost none of the URLs are in that file: they come
from `data/versions.csv`, a table of

    dataset, version, source, tags, added, note, url

with `source` either **primary** (the upstream publisher) or **archive**
(PyPSA's own mirror at data.pypsa.org). 59 datasets, 164 rows, 158 URLs.

So the retrieval layer is already data — the Snakemake rules are 73 near-copies
of "download this row and copy it into place". That is why the FFL port needs
almost no code: this module turns the table into a work list, and the built-in
`fw.http` / `fw.archive` facets do the rest with no handler at all.

Two things the table makes possible that upstream does not do:

* **Fallback.** A dataset usually has both a primary and an archive row. When
  the publisher moves or removes a file — which is why the mirror exists —
  the archive is the answer, and picking it is a per-dataset decision rather
  than an edit to a rule.
* **Verification.** Nothing checks that the mirror still matches what it
  mirrors. Two URLs for the same `(dataset, version)` should deliver identical
  bytes; if they do not, one of them is wrong and every model built from it
  inherits the discrepancy. `pairs_for_verification` produces exactly those
  candidates.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import asdict, dataclass
from typing import Any

logger = logging.getLogger(__name__)

PRIMARY = "primary"
ARCHIVE = "archive"
#: `build` rows describe something produced by the workflow, not downloaded.
BUILD = "build"

#: Tag tokens. The `tags` column is SPACE-SEPARATED and upstream one-hot encodes
#: it (`_helpers.load_data_versions`), so these are exact tokens, never
#: substrings — `not-supported` is its own token and does not match `supported`.
TAG_LATEST = "latest"
TAG_SUPPORTED = "supported"

#: `versions=` policies for :func:`select`.
V_LATEST = "latest"
V_ALL = "all"

#: Upstream's own catalogue, so a run without a local checkout still works.
DEFAULT_CATALOG_URL = (
    "https://raw.githubusercontent.com/PyPSA/pypsa-eur/master/data/versions.csv"
)

_ARCHIVE_SUFFIXES = (".zip", ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz", ".gz")


@dataclass(frozen=True)
class Dataset:
    """One retrievable dataset at one version, from one source."""

    name: str
    version: str
    source: str
    url: str
    tags: str = ""
    note: str = ""

    @property
    def filename(self) -> str:
        """Basename to store it under, from the URL."""
        tail = self.url.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
        return tail or f"{self.name}-{self.version}"

    @property
    def tag_set(self) -> frozenset[str]:
        """The `tags` cell as tokens, the way upstream reads it."""
        return frozenset(self.tags.split())

    @property
    def is_archive(self) -> bool:
        """Whether the download needs unpacking.

        Suffix-based, deliberately: the catalogue does not say, and guessing
        wrong only means an extra `fw.archive.List` that finds nothing —
        whereas *not* unpacking a zip leaves the model with a zip.
        """
        return self.filename.lower().endswith(_ARCHIVE_SUFFIXES)

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["filename"] = self.filename
        d["is_archive"] = self.is_archive
        return d


def parse_catalog(text: str) -> list[Dataset]:
    """Every row of versions.csv that names a URL.

    `build` rows and rows without a URL are dropped — they describe workflow
    products, not downloads.
    """
    out: list[Dataset] = []
    for row in csv.DictReader(io.StringIO(text)):
        url = (row.get("url") or "").strip()
        source = (row.get("source") or "").strip()
        if not url.startswith("http") or source == BUILD:
            continue
        out.append(
            Dataset(
                name=(row.get("dataset") or "").strip(),
                version=(row.get("version") or "").strip(),
                source=source,
                url=url,
                tags=(row.get("tags") or "").strip(),
                note=(row.get("note") or "").strip(),
            )
        )
    return out


def select(
    catalog: list[Dataset],
    *,
    prefer: str = ARCHIVE,
    names: list[str] | None = None,
    versions: str = V_LATEST,
    supported_only: bool = True,
) -> list[Dataset]:
    """What to retrieve: **one row per dataset** by default, as upstream does.

    `prefer` defaults to **archive**: the mirror exists because primaries move,
    and a reproducible pipeline wants the copy that is still going to be there.
    Pass ``prefer="primary"`` to pull from the original publishers instead.
    Falls back to the other source when the preferred one has no row, so a
    preference never silently drops a dataset.

    The defaults mirror upstream's `dataset_version` (`rules/common.smk`), which
    selects on `source`, the `supported` tag and the `latest` tag and then
    `.squeeze()`s the result to a single row. Ignoring the tags is not a
    harmless superset: on the real catalogue it turns a 59-download run into
    **94**, and what the extra 35 are is the point — deprecated versions,
    versions tagged `not-supported`, and (for eleven datasets) a second
    `latest` row at version `unknown`, which is the *moving un-versioned
    primary*. Keying on `(dataset, version)` made `unknown` look like a version
    of its own, so `prefer="archive"` could not dedupe it and the moving target
    was fetched alongside the pinned mirror copy it duplicates.

    - ``versions="latest"`` (default) keeps rows tagged `latest` and keys on the
      dataset, so a dataset yields exactly one download.
    - ``versions="all"`` is the escape hatch: every version, keyed on
      `(dataset, version)` — for reproducing an older model, or auditing.
    - ``supported_only=True`` (default) drops rows not tagged `supported`, which
      is upstream's "Limit to supported versions only".

    A dataset with no row surviving the tag filters is NOT dropped: its rows are
    used unfiltered and a warning names it. Silently retrieving nothing for a
    dataset the caller asked for is the one outcome worse than over-fetching.
    """
    if prefer not in (PRIMARY, ARCHIVE):
        raise ValueError(f"prefer must be {PRIMARY!r} or {ARCHIVE!r}, got {prefer!r}")
    if versions not in (V_LATEST, V_ALL):
        raise ValueError(f"versions must be {V_LATEST!r} or {V_ALL!r}, got {versions!r}")
    wanted = set(names) if names else None
    other = PRIMARY if prefer == ARCHIVE else ARCHIVE

    def keeps(ds: Dataset) -> bool:
        tags = ds.tag_set
        if supported_only and TAG_SUPPORTED not in tags:
            return False
        return not (versions == V_LATEST and TAG_LATEST not in tags)

    # Group by dataset first so the "nothing survived the filter" fallback can
    # see all of a dataset's rows.
    by_name: dict[str, list[Dataset]] = {}
    for ds in catalog:
        if wanted is not None and ds.name not in wanted:
            continue
        by_name.setdefault(ds.name, []).append(ds)

    chosen: list[Dataset] = []
    for name, rows in sorted(by_name.items()):
        kept = [d for d in rows if keeps(d)]
        collapse = versions == V_LATEST
        if not kept:
            logger.warning(
                "%s: no row tagged %s — retrieving every version instead",
                name,
                " + ".join(
                    t for t, on in ((TAG_LATEST, versions == V_LATEST),
                                    (TAG_SUPPORTED, supported_only)) if on
                ),
            )
            kept, collapse = rows, False

        # One key per dataset when a `latest` was requested; per version otherwise.
        by_key: dict[str, dict[str, Dataset]] = {}
        for d in kept:
            by_key.setdefault("" if collapse else d.version, {})[d.source] = d

        for version, sources in sorted(by_key.items()):
            pick = sources.get(prefer) or sources.get(other)
            if pick is None:  # pragma: no cover — only if a row has an unknown source
                logger.warning("no usable source for %s %s: %s", name, version, sorted(sources))
                continue
            if pick.source != prefer:
                logger.info(
                    "%s %s: no %s row, falling back to %s",
                    name,
                    pick.version,
                    prefer,
                    pick.source,
                )
            chosen.append(pick)
    return chosen


def pairs_for_verification(
    catalog: list[Dataset], *, names: list[str] | None = None
) -> list[tuple[Dataset, Dataset]]:
    """(primary, archive) pairs for the same dataset AND version.

    These should deliver identical bytes. Nothing upstream checks that, and a
    mirror that has drifted from its source is the kind of thing every model
    downstream inherits silently.

    `names` restricts the check to those datasets. Verification fetches BOTH
    sides of every pair, so the full set is several GB — without a filter the
    check is all-59-or-nothing, which on a metered or slow link means nothing.
    Unlike :func:`select` this deliberately ignores the tags: a mirror of a
    deprecated version is still supposed to match what it mirrors.
    """
    wanted = set(names) if names else None
    by_key: dict[tuple[str, str], dict[str, Dataset]] = {}
    for ds in catalog:
        if wanted is not None and ds.name not in wanted:
            continue
        by_key.setdefault((ds.name, ds.version), {})[ds.source] = ds
    return [
        (s[PRIMARY], s[ARCHIVE])
        for _key, s in sorted(by_key.items())
        if PRIMARY in s and ARCHIVE in s
    ]


def summarise(catalog: list[Dataset]) -> dict[str, int]:
    """Counts worth printing before pulling several GB.

    `downloads_default` is the one that matters: how many files the default
    selection will actually fetch, as against `rows` in the table.
    """
    return {
        "rows": len(catalog),
        "datasets": len({d.name for d in catalog}),
        "primary": sum(1 for d in catalog if d.source == PRIMARY),
        "archive": sum(1 for d in catalog if d.source == ARCHIVE),
        "archives_to_unpack": sum(1 for d in catalog if d.is_archive),
        "verifiable_pairs": len(pairs_for_verification(catalog)),
        "downloads_default": len(select(catalog)),
        "downloads_all_versions": len(select(catalog, versions=V_ALL, supported_only=False)),
    }
