"""Refresh the published ``get_unique_values`` snapshots (see ``spgci.reference_data``).

For every client class listed in ``CLASSES`` this runs each lookup in its
``reference_data.snapshot_specs`` live and rewrites
``reference/<namespace>/<dataset>__<group>.csv.gz`` plus ``manifest.json``. Two
layouts are published side by side:

  grouped  a few small files per dataset (the class's ``_REFERENCE_SNAPSHOTS``)
  full     one file per dataset: the distinct combinations of every dimension column

Agents ignore a snapshot older than 14 days, so run this at least weekly and commit
the result.

    cd <repo root>
    python3 reference/refresh_unique_values.py                         # everything
    python3 reference/refresh_unique_values.py --only chemicals        # one namespace
    python3 reference/refresh_unique_values.py --datasets capacity trade --layouts full
    python3 reference/refresh_unique_values.py --missing --timeout 600 --workers 2

The wide ``full`` lookups are heavy for the API. If lookups time out, raise
``--timeout`` (the SDK retries a timeout 3 times, so a lookup that can't finish costs
3x as long before it is skipped) and/or lower ``--workers`` / ``--page-workers``, then
rerun with ``--missing`` to fetch only what isn't published yet.

Nothing is truncated or overwritten with something worse:

* Each ``grouped`` snapshot is written, and the manifest updated, the moment it is
  fetched, so an interrupted run keeps everything finished so far.
* A lookup that fails, or exceeds ``--max-rows`` / ``--max-mb``, is reported and
  skipped; its previously published snapshot (if any) is kept until it expires.
* A column whose values are lists or objects can't be stored in a csv; it is left out
  of that snapshot (reported), which is exactly the lookup without that column.
* A ``full`` snapshot is checked against the ``grouped`` ones of its dataset (fetched
  in this run, else the published files): every grouped file must equal the same
  columns of the full one. If they disagree, or the check can't run, the full
  snapshot is NOT published.

Requires SPGCI_USERNAME / SPGCI_PASSWORD in the environment. Never commit
credentials. Run with agent mode off, so large lookups can paginate.

To publish another class: give it ``_REFERENCE_NAMESPACE`` / ``_REFERENCE_SNAPSHOTS``,
call ``reference_data.lookup_unique_values`` from its ``get_unique_values``, and
add it to ``CLASSES``.
"""

import argparse
import functools
import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import pandas as pd  # noqa: E402
import spgci as ci  # noqa: E402
from spgci import reference_data  # noqa: E402

CLASSES = [ci.Chemicals, ci.OilNGLAnalytics, ci.AgriAndFood]
WORKERS = 4  # lookups in flight; each one already pages with config.parallelism
TIMEOUT = 300  # seconds per request (the SDK's default is 60)
MAX_ROWS = 250_000
MAX_MB = 30  # GitHub warns at 50 MB and refuses 100 MB


def jobs(cls, datasets=None, layouts=None):
    """``(layout, snapshot name, dataset, columns)`` for everything ``cls`` publishes."""
    out = []
    for layout, spec in reference_data.snapshot_specs(cls).items():
        for dataset, groups in spec.items():
            for group, columns in groups.items():
                if (datasets and dataset not in datasets) or (
                    layouts and layout not in layouts
                ):
                    continue
                out.append((layout, f"{dataset}__{group}", dataset, list(columns)))
    names = [j[1] for j in out]
    assert len(names) == len(set(names)), "snapshot names must be unique"
    # dataset by dataset (grouped before full), so datasets finish, and publish, one
    # after another instead of all of the heavy ``full`` lookups landing at the end
    order = {d: i for i, d in enumerate(cls._REFERENCE_SNAPSHOTS)}
    return sorted(out, key=lambda job: order[job[2]])


def load_manifest(directory):
    try:
        return json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"snapshots": {}}


class Publisher:
    """Writes snapshots into one namespace's folder. ``manifest.json`` is rewritten
    after every snapshot, so whatever finished before an interrupt is kept."""

    def __init__(self, directory, wanted, generated_at, max_bytes):
        self.directory, self.generated_at, self.max_bytes = (
            directory,
            generated_at,
            max_bytes,
        )
        self._lock = threading.Lock()
        self.snapshots = {
            n: e
            for n, e in load_manifest(directory)["snapshots"].items()
            if f"{n}.csv.gz" in wanted and (directory / f"{n}.csv.gz").exists()
        }

    def is_published(self, name):
        return name in self.snapshots

    def add(self, layout, name, dataset, columns, df):
        with self._lock:
            entry = reference_data.write_snapshot(
                self.directory,
                name,
                dataset,
                columns,
                df,
                self.generated_at,
                layout=layout,
                max_bytes=self.max_bytes,
            )
            self.snapshots[name] = entry
            self.save()
            return entry

    def save(self):
        reference_data.write_manifest(
            self.directory, dict(sorted(self.snapshots.items())), self.generated_at
        )


def is_container(value):
    return isinstance(value, (list, tuple, set, dict))


def fetch(cls, job, max_rows, publisher):
    """Run one lookup. ``grouped`` snapshots are published right here; ``full`` ones
    are returned for the cross-check. Returns ``{name, df, columns, reason, dropped}``.
    """
    layout, name, dataset, columns = job
    out = {"name": name, "df": None, "columns": columns, "reason": None, "dropped": []}
    start = time.perf_counter()
    try:
        df = cls().get_unique_values(dataset, columns, use_snapshot=False)
        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"lookup did not return rows: {df!r}")
        # A csv can't hold lists. Grouping by the other columns is the same as
        # dropping those columns from the result and de-duplicating.
        listy = [c for c in df.columns if df[c].map(is_container).any()]
        if listy:
            df = df.drop(columns=listy).drop_duplicates()
            out["dropped"] = listy
        out["columns"] = [c for c in columns if c in df.columns]
        if not out["columns"]:
            raise ValueError(f"every column holds lists: {listy}")
        if len(df) > max_rows:
            raise ValueError(f"{len(df)} rows exceeds --max-rows {max_rows}")
        out["df"] = df
        note = f"  (left out list-valued {listy})" if listy else ""
        print(
            f"  fetched {name}: {len(df)} rows in {time.perf_counter() - start:.0f}s{note}"
        )
        if layout == "grouped":
            entry = publisher.add(layout, name, dataset, out["columns"], df)
            print(
                f"  published {name}: {entry['rows']} rows, {entry['bytes'] / 1024:,.0f} KB"
            )
    except Exception as e:  # keep going: one bad lookup must not sink the run
        out["reason"] = f"{type(e).__name__}: {e}"
        print(
            f"  FAILED {name} after {time.perf_counter() - start:.0f}s: {out['reason']}"
        )
    return out


def row_set(df, columns):
    """Comparable rows: a missing value is None from the API but NaN after a csv."""
    sub = df[columns].drop_duplicates()
    return set(map(tuple, sub.where(sub.notna(), "").astype(str).values.tolist()))


def published_frame(directory, entry):
    return pd.read_csv(
        directory / entry["file"], dtype=str, keep_default_na=False, na_values=[""]
    )


def cross_check(full_df, grouped):
    """``None`` if every grouped frame equals the same columns of ``full_df``, else why
    not. ``grouped`` is ``[(name, frame)]``; columns left out of either are skipped."""
    for name, frame in grouped:
        cols = [c for c in frame.columns if c in full_df.columns]
        if not cols:
            continue
        want, have = row_set(frame, cols), row_set(full_df, cols)
        if want != have:
            sample = sorted(want - have)[:2] or sorted(have - want)[:2]
            return (
                f"disagrees with {name}: {len(want - have)} of its {len(want)} rows "
                f"missing from full, {len(have - want)} extra (e.g. {sample})"
            )
    return None


def check_full(directory, publisher, dataset, full_df, fetched):
    """Cross-check a fetched ``full`` frame against the grouped snapshots of its
    dataset: the ones fetched in this run (``fetched``, name -> frame), else the
    published files. Returns ``(reason or None, whether anything was compared)``."""
    grouped = list(fetched.items())
    for name, entry in publisher.snapshots.items():
        if (
            entry["dataset"] == dataset
            and entry.get("layout") == "grouped"
            and name not in fetched
        ):
            grouped.append((name, published_frame(directory, entry)))
    if not grouped:
        return None, False
    return cross_check(full_df, grouped), True


def refresh(
    cls,
    datasets,
    max_rows,
    workers,
    layouts=None,
    max_mb=MAX_MB,
    missing=False,
):
    namespace = cls._REFERENCE_NAMESPACE
    directory = HERE / namespace
    generated_at = datetime.now(timezone.utc)
    wanted = {f"{name}.csv.gz" for _, name, _, _ in jobs(cls)}
    publisher = Publisher(directory, wanted, generated_at, int(max_mb * 1024 * 1024))

    for path in sorted(directory.glob("*.csv*") if directory.is_dir() else []):
        if path.name not in wanted:
            path.unlink()
            print(f"  removed {path.name} (no longer published)")

    todo = jobs(cls, datasets, layouts)
    if missing:
        skipping = [j for j in todo if publisher.is_published(j[1])]
        todo = [j for j in todo if not publisher.is_published(j[1])]
        print(f"  --missing: {len(skipping)} already published, {len(todo)} to fetch")
    print(f"  fetching {len(todo)} lookups, {workers} at a time...")

    # A dataset is finished when all of its lookups are: its ``full`` snapshot is then
    # cross-checked and published right away, so an interrupted run keeps every dataset
    # that completed.
    lock = threading.Lock()
    results, failures, dropped = {}, {}, {}
    remaining = {}
    for _, _, dataset, _ in todo:
        remaining[dataset] = remaining.get(dataset, 0) + 1

    def finish(dataset):
        fetched = {
            name: results[name]["df"]
            for layout, name, d, _ in todo
            if d == dataset and layout == "grouped" and not results[name]["reason"]
        }
        for layout, name, d, _ in todo:
            if d != dataset or layout != "full" or results[name]["reason"]:
                continue
            result = results[name]
            try:
                reason, checked = check_full(
                    directory, publisher, dataset, result["df"], fetched
                )
                if reason is None:
                    entry = publisher.add(
                        layout, name, dataset, result["columns"], result["df"]
                    )
                    note = (
                        "" if checked else "  (not cross-checked: nothing to compare)"
                    )
                    print(
                        f"  published {name}: {entry['rows']} rows, "
                        f"{entry['bytes'] / 1024:,.0f} KB{note}"
                    )
                    continue
            except Exception as ex:
                reason = f"cross-check or write failed: {type(ex).__name__}: {ex}"
            failures[name] = reason
            print(f"  WITHHELD {name}: {reason}")
        for _, name, d, _ in todo:  # free the frames
            if d == dataset:
                results[name]["df"] = None

    def run(job):
        result = fetch(cls, job, max_rows, publisher)
        dataset = job[2]
        with lock:
            results[job[1]] = result
            if result["reason"]:
                failures[job[1]] = result["reason"]
            if result["dropped"]:
                dropped[job[1]] = result["dropped"]
            remaining[dataset] -= 1
            done = remaining[dataset] == 0
        if done:
            finish(dataset)
        return result

    ci.parallel(*[functools.partial(run, job) for job in todo], max_workers=workers)

    publisher.save()
    for name, columns in dropped.items():
        print(f"  note: {name} was published without list-valued column(s) {columns}")
    if dropped:
        print(
            "  Requests using those columns go to the API. To stop fetching them, remove "
            "them from the class's _REFERENCE_SNAPSHOTS / add them to _REFERENCE_FULL_EXCLUDE."
        )
    for name, reason in failures.items():
        print(f"  SKIPPED {name}: {reason}")
    print(
        f"{namespace}: {len(todo) - len(failures)} refreshed, {len(failures)} skipped"
    )
    return len(failures)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--only", nargs="+", help="namespaces to refresh, e.g. chemicals")
    ap.add_argument("--datasets", nargs="+", help="only these datasets")
    ap.add_argument("--layouts", nargs="+", choices=reference_data.LAYOUTS)
    ap.add_argument(
        "--missing", action="store_true", help="skip what is already published"
    )
    ap.add_argument("--max-rows", type=int, default=MAX_ROWS)
    ap.add_argument("--max-mb", type=float, default=MAX_MB)
    ap.add_argument("--workers", type=int, default=WORKERS, help="lookups at a time")
    ap.add_argument(
        "--page-workers", type=int, help="pages at a time per lookup (default: SDK's)"
    )
    ap.add_argument(
        "--timeout", type=float, default=TIMEOUT, help="seconds per request"
    )
    args = ap.parse_args()

    if not (ci.config.get_token() or (ci.config.username and ci.config.password)):
        sys.exit("Set SPGCI_USERNAME and SPGCI_PASSWORD first.")
    if ci.config.is_agent:
        sys.exit("Unset SPGCI_AGENTMODE: agent mode caps pagination.")
    ci.config.timeout = args.timeout
    if args.page_workers:
        ci.config.parallelism = args.page_workers

    failed = 0
    for cls in CLASSES:
        if args.only and cls._REFERENCE_NAMESPACE not in args.only:
            continue
        print(cls._REFERENCE_NAMESPACE)
        failed += refresh(
            cls,
            args.datasets,
            args.max_rows,
            args.workers,
            args.layouts,
            args.max_mb,
            args.missing,
        )
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
