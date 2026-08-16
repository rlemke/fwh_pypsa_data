# SPDX-License-Identifier: Apache-2.0
"""The only handlers in this package.

Everything else the workflow does — fetching, unpacking, writing a manifest —
is a built-in `fw.*` facet. That asymmetry IS the result: PyPSA-Eur needs 73
Snakemake rules for the same layer because a rule's outputs are static, so one
download equals one rule.
"""

from __future__ import annotations

from typing import Any

from ..shared.pypsa_data_utils import catalog as _catalog

NAMESPACE = "pypsa.data"


def _read(csv_path: str) -> list:
    """Catalogue text via the storage backend, so `csv_path` may be s3://."""
    from facetwork.runtime.storage import get_storage_backend

    fs = get_storage_backend(csv_path)
    if not fs.exists(csv_path):
        raise FileNotFoundError(
            f"catalogue not found: {csv_path} — fetch versions.csv first "
            f"(default: {_catalog.DEFAULT_CATALOG_URL})"
        )
    with fs.open(csv_path, "rb") as fh:
        return _catalog.parse_catalog(fh.read().decode("utf-8", errors="replace"))


def handle_catalog(payload: dict[str, Any]) -> dict[str, Any]:
    entries = _read(payload["csv_path"])
    names = payload.get("names") or None
    versions = payload.get("versions") or _catalog.V_LATEST
    supported_only = payload.get("supported_only")
    chosen = _catalog.select(
        entries,
        prefer=payload.get("prefer") or _catalog.ARCHIVE,
        names=names,
        versions=versions,
        supported_only=True if supported_only is None else bool(supported_only),
    )
    step_log = payload.get("_step_log")
    if callable(step_log):
        # Say what was skipped, not just what was picked: the gap between the
        # two is deprecated/unsupported versions and moving un-versioned rows,
        # and it is the difference between a 59- and a 94-download run.
        step_log(
            f"catalogue: {len(chosen)} download(s) selected of {len(entries)} rows "
            f"(versions={versions}, supported_only="
            f"{True if supported_only is None else bool(supported_only)})"
        )
    return {
        "datasets": [d.as_dict() for d in chosen],
        "count": len(chosen),
        "summary": _catalog.summarise(entries),
    }


def handle_mirror_pairs(payload: dict[str, Any]) -> dict[str, Any]:
    pairs = _catalog.pairs_for_verification(
        _read(payload["csv_path"]), names=payload.get("names") or None
    )
    out = [
        {
            "dataset": p.name,
            "version": p.version,
            "filename": p.filename,
            "primary_url": p.url,
            "archive_url": a.url,
        }
        for p, a in pairs
    ]
    return {"pairs": out, "count": len(out)}


_DISPATCH = {
    f"{NAMESPACE}.Catalog": handle_catalog,
    f"{NAMESPACE}.MirrorPairs": handle_mirror_pairs,
}


def handle(payload: dict) -> dict:
    facet = payload["_facet_name"]
    fn = _DISPATCH.get(facet)
    if fn is None:
        raise KeyError(f"no handler for {facet!r} in {__name__}")
    return fn(payload)


def facet_names() -> list[str]:
    return sorted(_DISPATCH)


def register_handlers(runner) -> None:
    for facet in facet_names():
        runner.register_handler(
            facet_name=facet, module_uri=f"file://{__file__}", entrypoint="handle"
        )
