# Reference snapshots

Published copies of lookups that are slow to run live, fetched by the SDK from
`raw.githubusercontent.com/spgi-ci/spgci-python/master/reference/`.

| Folder | Served by | Refresh with |
| --- | --- | --- |
| `pe/` | `PowerEvaluator.get_reference_data` | `python3 reference/pe/refresh_reference_data.py` |
| `chemicals/`, `oil_ngl_analytics/` | `get_unique_values` in **agent mode** | `python3 reference/refresh_unique_values.py` |

Both need `SPGCI_USERNAME` / `SPGCI_PASSWORD`, and both expire: a snapshot older
than 14 days is ignored and the SDK queries the API instead. Refresh weekly and
commit the changes.

## `get_unique_values` snapshots

Shared code is `spgci/reference_data.py`. A class opts in with
`_REFERENCE_NAMESPACE`, `_REFERENCE_SNAPSHOTS` and one call at the top of its
`get_unique_values`.

### Two layouts, side by side

Files are gzipped csv (`<dataset>__<group>.csv.gz`; no parquet, so no new
dependency), listed with their size, dtypes, sha256 and `generated_at` in
`manifest.json`.

| Layout | What it is | Pro | Con |
| --- | --- | --- | --- |
| `grouped` | a few small files per dataset, each a hand-picked group of columns (`_REFERENCE_SNAPSHOTS` in the class) | tiny downloads | only answers requests that fit inside one group |
| `full` | one file per dataset (`<dataset>__all`) with the distinct combinations of **every** dimension column | answers any column subset and filter | bigger download and parse |

The `full` column list is not written by hand: it is every string filter parameter
of the dataset's `get_*` method (`reference_data.snapshot_specs`), minus the class's
`_REFERENCE_FULL_EXCLUDE`, so it stays complete when a column is added. Dates,
numbers and event ids are never in either layout.

By default (`SPGCI_REFERENCE_LAYOUT=auto`) the smallest snapshot that has every
needed column answers. Set `grouped` or `full` to force one, e.g. to compare them.
When one wins, delete the other from the spec and rerun the refresh.

### What is served

- Any set of columns that one snapshot contains (duplicates are dropped).
- A `filter_exp` too, when it is what `ci.utilities.build_filter_expression` writes
  for strings (`field: "x"`, `field in ("x","y")`, joined by `AND`), every column in
  it is in the snapshot, and **every value matches a snapshot value exactly**. A
  value the snapshot doesn't have (a different case, a wildcard, something new), an
  `OR`, a range, or an empty result all go to the API, so the server's matching
  rules never have to be guessed.
- Everything else goes to the API as before, as does any call outside agent mode.

`df.attrs["source"]` is `"cache"` (local copy in `~/.cache/spgci/<namespace>/`,
override with `SPGCI_CACHE_DIR`), `"github"` or `"api"`; `df.attrs["snapshot"]`
names the file. `get_unique_values(..., use_snapshot=False)` always queries the API.
If GitHub can't be reached the SDK stops trying for 10 minutes. Point
`SPGCI_REFERENCE_URL` at a mirror to use a different host.

### Refreshing

```
python3 reference/refresh_unique_values.py                  # everything
python3 reference/refresh_unique_values.py --only chemicals --datasets capacity trade
python3 reference/refresh_unique_values.py --layouts full   # skips the cross-check below
```

Snapshots are never truncated. A lookup that fails, or exceeds `--max-rows`
(250,000) or `--max-mb` (30), is skipped and its previous snapshot is kept until it
expires. Each `full` file is checked against the `grouped` files fetched in the same
run (every grouped file must equal the same columns of the full one); if they
disagree, e.g. because the API omits rows that are empty in some grouped column, the
full file is **not** published and the script exits non-zero. Rows are sorted, so
unchanged data produces a byte-identical file.

### Comparing

```
python3 tests/perf_unique_values.py                         # api vs grouped vs full
python3 tests/perf_unique_values.py --datasets capacity --layouts full --repeats 5
```

For each dataset it times each published group, a single column, a request that
spans two groups and a filtered request, as `api`, `cold` (empty cache, downloaded
from GitHub) and `warm`, and checks every snapshot answer against the API's answer
from the same moment. `MISS` means the layout couldn't answer that request.
