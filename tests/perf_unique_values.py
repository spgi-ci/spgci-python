"""Compare ``get_unique_values`` latency: live API vs the two snapshot layouts.

Not collected by pytest (it needs credentials and the network). For every dataset it
issues a handful of requests (each published group, a single column, a request that
spans two groups, and a filtered request) and times each one, in agent mode:

    api      ``use_snapshot=False``: the call an agent waits on today
    grouped  the small hand-picked files        (``SPGCI_REFERENCE_LAYOUT=grouped``)
    full     one file per dataset, every column (``SPGCI_REFERENCE_LAYOUT=full``)

``cold`` starts from an empty cache (manifest and file come from GitHub), ``warm``
reuses what was downloaded. Each snapshot answer is compared with the API's answer
from the same moment, so a layout that is fast but wrong shows up as ``DIFF``; a
request a layout can't answer exactly is a ``MISS`` and is left to the API.

    python tests/perf_unique_values.py                          # everything
    python tests/perf_unique_values.py --classes chemicals --datasets capacity trade
    python tests/perf_unique_values.py --layouts full --repeats 5 --out perf.csv
    python tests/perf_unique_values.py --skip-api               # snapshot legs only
    python tests/perf_unique_values.py --serve-local            # offline wiring check

The snapshots must already be published (``reference/refresh_unique_values.py``,
committed and pushed) for ``cold`` to hit GitHub; otherwise every case is a ``MISS``.
``--serve-local`` serves ``./reference`` from localhost instead, which exercises the
code path but says nothing about GitHub latency.

Uses a throwaway ``SPGCI_CACHE_DIR``, so your real cache is untouched. Needs
SPGCI_USERNAME / SPGCI_PASSWORD unless ``--skip-api``. Exits 3 if any snapshot
answer differs from the API's.
"""

import argparse
import contextlib
import functools
import http.server
import json
import os
import statistics
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
import spgci as ci  # noqa: E402
import spgci.config  # noqa: E402
from spgci import reference_data  # noqa: E402

CLASSES = {
    c._REFERENCE_NAMESPACE: c
    for c in (ci.Chemicals, ci.OilNGLAnalytics, ci.AgriAndFood)
}
SHORT = {"grouped": "g", "full": "f"}


@dataclass
class Request:
    dataset: str
    kind: str
    columns: List[str]
    filter_exp: Optional[str] = None


class NoApi(Exception):
    """Raised instead of calling the API during a snapshot leg."""


@contextlib.contextmanager
def api_blocked(client):
    """A snapshot leg must never fall back to the (slow, billable) API: when no snapshot
    can answer, the call raises ``NoApi`` and the request is reported as a ``MISS``."""
    module = sys.modules[type(client).__module__]
    original = module.get_data

    def blocked(*args, **kwargs):
        raise NoApi()

    module.get_data = blocked
    try:
        yield
    finally:
        module.get_data = original


def timed(fn: Callable[[], Any]) -> Tuple[float, Any]:
    start = time.perf_counter()
    result = fn()
    return time.perf_counter() - start, result


def rows(df: pd.DataFrame, columns: List[str]) -> set:
    """Comparable rows: the API returns None where a csv round trip gives NaN."""
    sub = df[columns]
    return set(map(tuple, sub.where(sub.notna(), "").astype(str).values.tolist()))


def set_layout(layout: Optional[str]) -> None:
    if layout is None:
        os.environ.pop("SPGCI_REFERENCE_LAYOUT", None)
    else:
        os.environ["SPGCI_REFERENCE_LAYOUT"] = layout


# -- which requests to time ----------------------------------------------------


def filtered_request(
    client, dataset: str, full_columns: List[str]
) -> Optional[Request]:
    """A realistic "step 2" call, with values taken from the published full snapshot."""
    set_layout("full")
    table = reference_data.store(client._REFERENCE_NAMESPACE).lookup(
        dataset, full_columns
    )
    if table is None:  # not published: nothing to take values from
        return None
    counts = table.nunique()
    filterable = counts[(counts >= 2) & (counts <= 500)]
    if filterable.empty or len(full_columns) < 2:
        return None
    by = filterable.idxmin()
    values = sorted(table[by].dropna().unique())[:2]
    target = counts.drop(by).idxmax()
    expression = ci.utilities.build_filter_expression({by: values})
    return Request(dataset, f"filter {by}", [target], expression)


def requests_for(client, cls, dataset: str, filtered: bool) -> List[Request]:
    specs = reference_data.snapshot_specs(cls)
    groups, full = specs["grouped"][dataset], specs["full"][dataset]["all"]
    out = [Request(dataset, f"group {g}", list(c)) for g, c in groups.items()]
    first = next(iter(groups.values()))
    out.append(Request(dataset, "single", [first[0]]))
    for i, a in enumerate(full):  # the first pair that no single group contains
        pair = next(
            (
                [a, b]
                for b in full[i + 1 :]
                if not any({a, b} <= set(c) for c in groups.values())
            ),
            None,
        )
        if pair:
            out.append(Request(dataset, "spans groups", pair))
            break
    if filtered:
        req = filtered_request(client, dataset, full)
        if req:
            out.append(req)
    return out


# -- timing one request ----------------------------------------------------------


def snapshot_kb(
    store: reference_data.ReferenceStore, name: Optional[str]
) -> Optional[float]:
    try:
        wrapper = json.loads((store.directory / "manifest.json").read_text())
        return wrapper["manifest"]["snapshots"][name]["bytes"] / 1024
    except (OSError, ValueError, KeyError, TypeError):
        return None


def measure_api(client, req: Request, repeats: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    try:
        times = []
        for _ in range(repeats):
            dt, df = timed(
                lambda: client.get_unique_values(
                    req.dataset, req.columns, req.filter_exp, use_snapshot=False
                )
            )
            times.append(dt)
        out.update(api=statistics.median(times), rows=len(df), api_df=df)
    except Exception as e:
        out["api_error"] = f"{type(e).__name__}: {e}"[:80]
    return out


def measure_layout(
    client, req: Request, layout: str, repeats: int, api_df
) -> Dict[str, Any]:
    key = SHORT[layout]
    store = reference_data.store(client._REFERENCE_NAMESPACE)
    set_layout(layout)
    store.clear_local()
    call = lambda: client.get_unique_values(req.dataset, req.columns, req.filter_exp)
    with api_blocked(client):
        try:
            cold, df = timed(call)
        except NoApi:  # not published, stale, unreachable, or a request this
            return {f"{key}.result": "MISS"}  # layout can't answer exactly
        except Exception as e:
            return {f"{key}.result": f"ERROR {type(e).__name__}"}
        if df.attrs.get("source") != "github":
            return {f"{key}.result": f"MISS (served from {df.attrs.get('source')})"}
        out: Dict[str, Any] = {f"{key}.cold": cold}
        times = [timed(call)[0] for _ in range(repeats)]
    out[f"{key}.warm"] = statistics.median(times)
    out[f"{key}.KB"] = snapshot_kb(store, df.attrs.get("snapshot"))
    out.setdefault("rows", len(df))
    if api_df is None:
        out[f"{key}.result"] = "ok"
        return out
    only_api = rows(api_df, req.columns) - rows(df, req.columns)
    only_snap = rows(df, req.columns) - rows(api_df, req.columns)
    out[f"{key}.result"] = (
        "match"
        if not (only_api or only_snap)
        else f"DIFF -{len(only_api)}/+{len(only_snap)}"
    )
    return out


def run_request(client, req, layouts, repeats, skip_api) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "dataset": req.dataset,
        "kind": req.kind,
        "columns": ", ".join(req.columns),
    }
    api_df = None
    if not skip_api:
        measured = measure_api(client, req, repeats)
        api_df = measured.pop("api_df", None)
        row.update(measured)
    for layout in layouts:
        row.update(measure_layout(client, req, layout, repeats, api_df))
    return row


# -- reporting ------------------------------------------------------------------


def ms(seconds) -> str:
    return "-" if pd.isna(seconds) else f"{seconds * 1000:,.0f}ms"


def kb(value) -> str:
    return "-" if pd.isna(value) else f"{value:,.0f}"


def speedup(api, snap) -> str:
    return "-" if pd.isna(api) or pd.isna(snap) or not snap else f"{api / snap:,.0f}x"


def report(results: List[Dict[str, Any]], layouts: List[str]) -> pd.DataFrame:
    df = pd.DataFrame(results)
    for col in ("api", "rows", "api_error"):
        if col not in df:
            df[col] = None
    show = pd.DataFrame(
        {
            "dataset": df["dataset"],
            "request": df["kind"] + " [" + df["columns"].str.slice(0, 38) + "]",
            "rows": df["rows"].map(lambda v: "-" if pd.isna(v) else int(v)),
            "api": df["api"].map(ms),
        }
    )
    for layout in layouts:
        k = SHORT[layout]
        for col in ("cold", "warm", "KB", "result"):
            if f"{k}.{col}" not in df:
                df[f"{k}.{col}"] = None
        show[f"{k}.cold"] = df[f"{k}.cold"].map(ms)
        show[f"{k}.warm"] = df[f"{k}.warm"].map(ms)
        show[f"{k}.KB"] = df[f"{k}.KB"].map(kb)
        show[f"{k}.result"] = df[f"{k}.result"].fillna("")
    if df["api_error"].notna().any():
        show["api error"] = df["api_error"].fillna("")
    with pd.option_context("display.width", 300, "display.max_colwidth", 80):
        print(show.to_string(index=False))
    print("\n  g = grouped layout, f = full layout.  KB = size of the file served.\n")

    n = len(df)
    for layout in layouts:
        k = SHORT[layout]
        served = df[df[f"{k}.cold"].notna()]
        compared = served[served["api"].notna()]
        wrong = df[f"{k}.result"].fillna("").str.startswith("DIFF").sum()
        line = f"{layout:8} answered {len(served)}/{n} requests ({wrong} differing from the API)"
        if len(compared):
            line += (
                f" | median api {ms(compared['api'].median())}"
                f" -> cold {ms(compared[f'{k}.cold'].median())}"
                f" ({speedup(compared['api'].median(), compared[f'{k}.cold'].median())})"
                f", warm {ms(compared[f'{k}.warm'].median())}"
                f" ({speedup(compared['api'].median(), compared[f'{k}.warm'].median())})"
            )
        if len(served) and served[f"{k}.KB"].notna().any():
            line += (
                f" | file served: median {kb(served[f'{k}.KB'].median())} KB,"
                f" max {kb(served[f'{k}.KB'].max())} KB"
            )
        print(line)
    if len(layouts) == 2:
        both = df[df["g.cold"].notna() & df["f.cold"].notna()]
        if len(both):
            print(
                f"\nOn the {len(both)} requests both layouts answered: "
                f"cold grouped {ms(both['g.cold'].median())} vs full {ms(both['f.cold'].median())}"
                f" | warm grouped {ms(both['g.warm'].median())} vs full {ms(both['f.warm'].median())}"
            )
    return df


def serve_local(directory: Path) -> Tuple[str, http.server.ThreadingHTTPServer]:
    class Quiet(http.server.SimpleHTTPRequestHandler):
        def log_message(self, *args):
            pass

    handler = functools.partial(Quiet, directory=str(directory))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return f"http://127.0.0.1:{server.server_port}", server


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument(
        "--classes", nargs="+", choices=sorted(CLASSES), help="default: all"
    )
    ap.add_argument("--datasets", nargs="+", help="only these datasets")
    ap.add_argument("--layouts", nargs="+", choices=reference_data.LAYOUTS)
    ap.add_argument("--repeats", type=int, default=3, help="timed calls per leg")
    ap.add_argument("--limit", type=int, help="stop after this many requests per class")
    ap.add_argument(
        "--no-filtered", action="store_true", help="skip the filtered request"
    )
    ap.add_argument("--skip-api", action="store_true", help="time snapshot legs only")
    ap.add_argument(
        "--base-url", help="where snapshots are published (default: GitHub)"
    )
    ap.add_argument(
        "--serve-local", action="store_true", help="serve ./reference locally"
    )
    ap.add_argument("--reference-dir", type=Path, default=ROOT / "reference")
    ap.add_argument("--out", help="also write results to this csv")
    ap.add_argument(
        "--timeout", type=float, default=300, help="seconds per API request (SDK: 60)"
    )
    args = ap.parse_args(argv)
    args.repeats = max(1, args.repeats)
    layouts = args.layouts or list(reference_data.LAYOUTS)

    has_credentials = spgci.config.get_token() or (
        spgci.config.username and spgci.config.password
    )
    if not args.skip_api and not has_credentials:
        print("Set SPGCI_USERNAME and SPGCI_PASSWORD, or pass --skip-api.")
        return 2

    keys = ("SPGCI_CACHE_DIR", "SPGCI_REFERENCE_URL", "SPGCI_REFERENCE_LAYOUT")
    saved = {k: os.environ.get(k) for k in keys}
    was_agent = spgci.config.is_agent
    had_timeout = getattr(spgci.config, "timeout", None)
    spgci.config.timeout = args.timeout
    server = None
    results: List[Dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as cache:
        os.environ["SPGCI_CACHE_DIR"] = cache
        spgci.config.is_agent = True
        try:
            if args.serve_local:
                url, server = serve_local(args.reference_dir)
                os.environ["SPGCI_REFERENCE_URL"] = url
                print(f"serving {args.reference_dir} at {url} (not GitHub latency)")
            elif args.base_url:
                os.environ["SPGCI_REFERENCE_URL"] = args.base_url
            print(f"snapshots from {reference_data.base_url()}\n")

            for namespace in args.classes or CLASSES:
                cls = CLASSES[namespace]
                client = cls()
                done = 0
                for dataset in cls._REFERENCE_SNAPSHOTS:
                    if args.datasets and dataset not in args.datasets:
                        continue
                    for req in requests_for(client, cls, dataset, not args.no_filtered):
                        if args.limit and done >= args.limit:
                            break
                        done += 1
                        print(
                            f"[{namespace}] {dataset}: {req.kind}",
                            file=sys.stderr,
                            flush=True,
                        )
                        results.append(
                            run_request(
                                client, req, layouts, args.repeats, args.skip_api
                            )
                        )
        finally:
            spgci.config.is_agent = was_agent
            if had_timeout is None:
                del spgci.config.timeout
            else:
                spgci.config.timeout = had_timeout
            for k, old in saved.items():
                if old is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = old
            if server:
                server.shutdown()

    if not results:
        print("No matching requests.")
        return 1
    df = report(results, layouts)
    if args.out:
        df.drop(columns=["api_df"], errors="ignore").to_csv(args.out, index=False)
    differing = any(
        str(v).startswith("DIFF")
        for k in (f"{SHORT[layout]}.result" for layout in layouts)
        for v in df.get(k, [])
    )
    return 3 if differing else 0


if __name__ == "__main__":
    sys.exit(main())
