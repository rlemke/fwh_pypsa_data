# fwh_pypsa_data

PyPSA-Eur's data-retrieval layer as an FFL workflow — the port of
[`rules/retrieve.smk`](https://github.com/PyPSA/pypsa-eur/blob/master/rules/retrieve.smk).

## What the port shows

Upstream's retrieve layer is **1,631 lines, 73 rules**, 58 of them
`storage(...)` downloads. Almost none of the URLs are in that file: they come
from `data/versions.csv` — 59 datasets × {primary, archive} × version.

So the layer is **already data**, and the 73 rules are the same rule written 73
times, because a Snakemake rule's outputs are static: one download, one rule.

Here the catalogue *is* the work list and the fan-out is one `foreach`:

```ffl
cat     = Catalog(csv_path = $.csv_path, prefer = "archive")
fetched = FetchEach(datasets = cat.datasets, out_dir = $.out_dir, concurrency = 4)
```

`Catalog` and `MirrorPairs` are the **only** handlers in this package. Fetching,
unpacking and the manifest are built-in `fw.http` / `fw.archive` / `fw.file`
facets — no handler code at all.

**Which row per dataset.** The table has 153 retrievable rows for 59 datasets,
so a work list is a selection, not a listing. `Catalog` applies upstream's own
rule (`dataset_version` in `rules/common.smk`: match the source, require the
`supported` and `latest` tags, then `.squeeze()` to one row) and so fetches
**59 files — one per dataset**. `versions="all"` plus `supported_only=false` is
the escape hatch for reproducing an older model, and fetches 94. The 35-file
difference is deprecated versions, versions upstream tags `not-supported`, and
eleven `version=unknown` rows — the *moving un-versioned primary*, which keyed
on `(dataset, version)` looks like a version of its own and so survived a
`prefer="archive"` that was meant to pin it.

## What it adds

`retrieve.smk` contains no `md5`, `sha256`, `checksum`, `ancient()` or
`protected()` — grepped, not assumed — and its rules declare **no file inputs**,
so once an output exists nothing can make it stale (thesis §13.3).

* `fw.http.Fetch` decides reuse on a conditional GET or a recorded digest.
* `VerifyMirror` checks something upstream cannot: that PyPSA's archive mirror
  still delivers the same bytes as the primary it mirrors. **59 such pairs
  exist and nothing compares them.** They cover 42 of the 59 datasets: for the
  other 17 the primary sits at version `unknown` while the archive is pinned to
  a date, so there is no same-version pair to compare and the mirror of those
  17 cannot be verified this way at all. Pairing them anyway would report every
  upstream update as mirror drift, which is a different claim. Pass `names` to
  check a subset — both sides of every pair are fetched, so all 59 is several GB.

## Status — honest scope

The catalogue logic and both workflows are complete and tested **offline**
against a checked-in copy of upstream's `versions.csv` (24 tests, no network) —
including the selection rule, so the "59 downloads, not 94" claim is asserted
against upstream's real table rather than described.

Retrieval was verified end to end on one real dataset (`eez`, 26 MB from
PyPSA's mirror, digest recorded), and a six-name subset fan-out ran cold in
12.2s and warm in 1.47s, with a `max_age_hours=0` probe confirming the
conditional GET (`304`, digest unchanged, body not re-transferred).
**A full run was not performed**: the 59 datasets are several GB of ERA5
weather, Eurostat balances, JRC land cover, WDPA, EEZ and powerplant data, and
the machine this was built on does not have the bandwidth. None of it is
OpenStreetMap, so none can be served from a local OSM cache.

Run it where bandwidth allows:

```bash
fw ffl run --primary src/pypsa_data/ffl/pypsa_data.ffl \
  --workflow pypsa.data.RetrieveDatasets \
  --inputs '{"csv_path": "versions.csv", "out_dir": "s3://afl-cache/pypsa/data",
             "names": ["country_hdd"], "concurrency": 2}'
```

`names` restricts the fan-out — start small. The mirror check takes the same
filter, which is what makes it runnable at all on a limited link:

```bash
fw ffl run --primary src/pypsa_data/ffl/pypsa_data.ffl \
  --workflow pypsa.data.VerifyMirror \
  --inputs '{"csv_path": "versions.csv", "out_dir": "s3://afl-cache/pypsa/verify",
             "names": ["aquifer_data"], "concurrency": 2}'
```

## Licence

Apache-2.0. PyPSA-Eur is MIT; each dataset carries its own terms.
