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
) -> list[Dataset]:
    """One row per (dataset, version), honouring a source preference.

    `prefer` defaults to **archive**: the mirror exists because primaries move,
    and a reproducible pipeline wants the copy that is still going to be there.
    Pass ``prefer="primary"`` to pull from the original publishers instead.
    Falls back to the other source when the preferred one has no row, so a
    preference never silently drops a dataset.
    """
    if prefer not in (PRIMARY, ARCHIVE):
        raise ValueError(f"prefer must be {PRIMARY!r} or {ARCHIVE!r}, got {prefer!r}")
    wanted = set(names) if names else None

    by_key: dict[tuple[str, str], dict[str, Dataset]] = {}
    for ds in catalog:
        if wanted is not None and ds.name not in wanted:
            continue
        by_key.setdefault((ds.name, ds.version), {})[ds.source] = ds

    chosen: list[Dataset] = []
    for (name, version), sources in sorted(by_key.items()):
        pick = sources.get(prefer) or sources.get(PRIMARY if prefer == ARCHIVE else ARCHIVE)
        if pick is None:  # pragma: no cover — only if a row has an unknown source
            logger.warning("no usable source for %s %s: %s", name, version, sorted(sources))
            continue
        if pick.source != prefer:
            logger.info(
                "%s %s: no %s row, falling back to %s", name, version, prefer, pick.source
            )
        chosen.append(pick)
    return chosen


def pairs_for_verification(catalog: list[Dataset]) -> list[tuple[Dataset, Dataset]]:
    """(primary, archive) pairs for the same dataset AND version.

    These should deliver identical bytes. Nothing upstream checks that, and a
    mirror that has drifted from its source is the kind of thing every model
    downstream inherits silently.
    """
    by_key: dict[tuple[str, str], dict[str, Dataset]] = {}
    for ds in catalog:
        by_key.setdefault((ds.name, ds.version), {})[ds.source] = ds
    return [
        (s[PRIMARY], s[ARCHIVE])
        for _key, s in sorted(by_key.items())
        if PRIMARY in s and ARCHIVE in s
    ]


def summarise(catalog: list[Dataset]) -> dict[str, int]:
    """Counts worth printing before pulling several GB."""
    return {
        "rows": len(catalog),
        "datasets": len({d.name for d in catalog}),
        "primary": sum(1 for d in catalog if d.source == PRIMARY),
        "archive": sum(1 for d in catalog if d.source == ARCHIVE),
        "archives_to_unpack": sum(1 for d in catalog if d.is_archive),
        "verifiable_pairs": len(pairs_for_verification(catalog)),
    }
