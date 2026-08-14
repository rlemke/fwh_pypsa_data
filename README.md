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

## What it adds

`retrieve.smk` contains no `md5`, `sha256`, `checksum`, `ancient()` or
`protected()` — grepped, not assumed — and its rules declare **no file inputs**,
so once an output exists nothing can make it stale (thesis §13.3).

* `fw.http.Fetch` decides reuse on a conditional GET or a recorded digest.
* `VerifyMirror` checks something upstream cannot: that PyPSA's archive mirror
  still delivers the same bytes as the primary it mirrors. **59 such pairs
  exist and nothing compares them.**

## Status — honest scope

The catalogue logic and both workflows are complete and tested **offline**
against a checked-in copy of upstream's `versions.csv` (15 tests, no network).

Retrieval was verified end to end on one real dataset (`eez`, 26 MB from
PyPSA's mirror, digest recorded). **A full run was not performed**: the 59
datasets are several GB of ERA5 weather, Eurostat balances, JRC land cover,
WDPA, EEZ and powerplant data, and the machine this was built on does not have
the bandwidth. None of it is OpenStreetMap, so none can be served from a local
OSM cache.

Run it where bandwidth allows:

```bash
fw ffl run --primary src/pypsa_data/ffl/pypsa_data.ffl \
  --workflow pypsa.data.RetrieveDatasets \
  --inputs '{"csv_path": "versions.csv", "out_dir": "s3://afl-cache/pypsa/data",
             "names": ["country_hdd"], "concurrency": 2}'
```

`names` restricts the fan-out — start small.

## Licence

Apache-2.0. PyPSA-Eur is MIT; each dataset carries its own terms.
