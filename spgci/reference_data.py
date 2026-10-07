# Copyright 2026 S&P Global Energy (previously S&P Global Commodity Insights)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Published snapshots of ``get_unique_values`` results, for agent mode.

A client class opts in with a few class attributes and a short call at the top of
its ``get_unique_values``::

    _REFERENCE_NAMESPACE = "chemicals"
    _REFERENCE_SNAPSHOTS = {"capacity": {"series": ["commodity", "concept"]}}

    snap = reference_data.lookup_unique_values(
        self._REFERENCE_NAMESPACE, dataset, columns, filter_exp, date_columns
    )
    if snap is not None:
        return snap

Snapshots live in ``reference/<namespace>/`` of the SDK's GitHub repo, next to a
``manifest.json`` that describes them. They are written by
``reference/refresh_unique_values.py`` as gzipped csv (standard library only: no
parquet dependency) in two layouts, published side by side:

``grouped``
    A few small files per dataset, each the distinct combinations of a hand-picked
    group of columns (``_REFERENCE_SNAPSHOTS``).
``full``
    One file per dataset with the distinct combinations of *every* dimension column
    (derived from the dataset's ``get_*`` signature by ``snapshot_specs``).

A lookup is answered, in order, from the local copy in
``~/.cache/spgci/<namespace>/``, then from GitHub (downloaded, verified against the
manifest's sha256, cached), otherwise ``None`` and the caller queries the API as it
always did. ``SPGCI_REFERENCE_LAYOUT`` (``auto``, ``grouped``, ``full``) restricts
which layout may answer; ``auto`` uses the smallest snapshot that covers the request.

A snapshot answers a request when it contains every column that is requested or
filtered on: the result is then a pandas ``filter`` and ``drop_duplicates`` over it.
Filters are served only in the form ``build_filter_expression`` produces
(``field: "x"`` / ``field in ("x","y")`` joined by ``AND``) and only when every
value matches a snapshot value exactly; anything else, or an empty result, is left
to the API. A snapshot older than ``MAX_AGE`` is ignored.
``df.attrs["source"]`` is ``"cache"``, ``"github"`` or (set by the caller)
``"api"``; ``df.attrs["snapshot"]`` names the snapshot used.

``reference/pe`` (``PowerEvaluator.get_reference_data``) predates this module and
is intentionally independent of it.
"""

from __future__ import annotations

import gzip
import hashlib
import inspect
import io
import json
import os
import re
import shutil
import tempfile
import threading
import warnings
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    get_args,
)

import pandas as pd
import requests
import spgci.config
from pandas import DataFrame

DEFAULT_BASE_URL = (
    "https://raw.githubusercontent.com/spgi-ci/spgci-python/master/reference"
)
#: A published snapshot older than this is ignored in favor of the API.
MAX_AGE = timedelta(days=14)
#: How long the local copy of the manifest is trusted before it is fetched again.
CACHE_TTL = timedelta(hours=24)
#: After GitHub fails (or serves inconsistent files), don't ask again for this long.
RETRY_AFTER = timedelta(minutes=10)
#: ``(connect, read)`` seconds for each GitHub request.
FETCH_TIMEOUT = (3.05, 5)
LAYOUTS = ("grouped", "full")


def base_url() -> str:
    """Where snapshots are published; override with ``SPGCI_REFERENCE_URL``."""
    return (os.getenv("SPGCI_REFERENCE_URL") or DEFAULT_BASE_URL).rstrip("/")


def cache_root() -> Path:
    return Path(os.getenv("SPGCI_CACHE_DIR") or str(Path.home() / ".cache" / "spgci"))


def layout() -> str:
    """``SPGCI_REFERENCE_LAYOUT``: ``grouped``, ``full`` or (default) ``auto``."""
    value = (os.getenv("SPGCI_REFERENCE_LAYOUT") or "auto").strip().lower()
    return value if value in LAYOUTS else "auto"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _atomic_write(path: Path, data: bytes) -> None:
    """Write via a temp file so concurrent readers never see a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f"{path.name}.")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def normalize_columns(columns: Union[str, Iterable[str], None]) -> Optional[List[str]]:
    """``"a, b"`` or ``["a", "b"]`` -> ``["a", "b"]`` (deduplicated, order kept)."""
    if columns is None:
        return None
    parts = columns.split(",") if isinstance(columns, str) else list(columns)
    cols = [str(c).strip() for c in parts if str(c).strip()]
    return list(dict.fromkeys(cols)) or None


def parse_datetime_columns(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    """Convert ``columns`` (those present) to UTC datetimes, as the API path does."""
    for c in columns:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, format="ISO8601", errors="coerce")
    return df


# -- filters ------------------------------------------------------------------

_STR = r'"[^"]*"'
_TERM = re.compile(
    rf"\s*([A-Za-z_][\w.]*)\s*(?::\s*({_STR})|\s+in\s*\(\s*({_STR}(?:\s*,\s*{_STR})*)\s*\))\s*"
)
_AND = re.compile(r"AND\s+")


def parse_filter(filter_exp: str) -> Optional[List[Tuple[str, List[str]]]]:
    """``[(field, [values]), ...]`` (all of them must hold) for the grammar that
    ``ci.utilities.build_filter_expression`` emits for strings, else ``None``."""
    out: List[Tuple[str, List[str]]] = []
    pos = 0
    while True:
        m = _TERM.match(filter_exp, pos)
        if m is None:
            return None
        values = (
            [m.group(2)] if m.group(2) is not None else re.findall(_STR, m.group(3))
        )
        out.append((m.group(1), [v[1:-1] for v in values]))
        pos = m.end()
        if pos == len(filter_exp):
            return out
        a = _AND.match(filter_exp, pos)
        if a is None:
            return None
        pos = a.end()


# -- the reader -----------------------------------------------------------------


class ReferenceStore:
    """Reads one namespace's published snapshots. See the module docstring."""

    def __init__(self, namespace: str):
        self.namespace = namespace
        self._unavailable_until: Optional[datetime] = None
        # What this process holds: downloads (the fallback when the cache directory
        # is read-only) and parsed frames (so repeat lookups skip the csv parse).
        self._memory: Dict[str, Any] = {}
        self._frames: Dict[Tuple[str, str, Tuple[str, ...]], DataFrame] = {}

    @property
    def directory(self) -> Path:
        return cache_root() / self.namespace

    def clear_local(self) -> None:
        """Forget the local copies (and any 'GitHub is down' memory)."""
        self._unavailable_until = None
        self._memory = {}
        self._frames = {}
        shutil.rmtree(self.directory, ignore_errors=True)

    # -- lookup -------------------------------------------------------------

    def lookup(
        self,
        dataset: str,
        columns: Union[str, Sequence[str], None],
        date_columns: Iterable[str] = (),
        filter_exp: Optional[str] = None,
    ) -> Optional[DataFrame]:
        """Unique combinations of ``columns`` in ``dataset`` (restricted by
        ``filter_exp``), or ``None`` when no fresh snapshot can answer exactly."""
        wanted = normalize_columns(columns)
        if wanted is None:
            return None
        predicates: List[Tuple[str, List[str]]] = []
        if filter_exp is not None:
            parsed = parse_filter(filter_exp)
            if parsed is None:
                return None
            predicates = parsed
        needed = list(dict.fromkeys([*wanted, *(field for field, _ in predicates)]))

        now = _utcnow()
        manifest = self._manifest(now)
        if manifest is None:
            return None
        match = self._match(manifest, dataset, needed, now)
        if match is None:
            return None
        name, entry = match
        loaded = self._load(name, entry, date_columns, now)
        if loaded is None:
            return None
        df, source = loaded
        try:
            if predicates:
                df = self._apply(df, predicates)
                if df is None:
                    return None
            out = df[wanted].drop_duplicates().reset_index(drop=True)
        except (ValueError, KeyError, TypeError):
            return None
        out.attrs["source"] = source
        out.attrs["snapshot"] = name
        return out

    @staticmethod
    def _apply(
        df: DataFrame, predicates: List[Tuple[str, List[str]]]
    ) -> Optional[DataFrame]:
        """Rows matching every predicate, or ``None`` if the snapshot can't be
        trusted to agree with the server: a value that isn't an exact snapshot value
        (the server may match case-insensitively or by pattern), a non-string
        column, or no rows at all."""
        mask = pd.Series(True, index=df.index)
        for field, values in predicates:
            col = df[field]
            if col.dtype != object and not pd.api.types.is_string_dtype(col.dtype):
                return None
            if not set(values) <= set(col.dropna().unique()):
                return None
            mask &= col.isin(values)
        df = df[mask]
        return None if df.empty else df

    @staticmethod
    def _match(
        manifest: Dict[str, Any], dataset: str, needed: List[str], now: datetime
    ) -> Optional[Tuple[str, Dict[str, Any]]]:
        """The smallest fresh snapshot of ``dataset`` that contains ``needed``."""
        only = layout()
        best: Optional[Tuple[Tuple[int, int], str, Dict[str, Any]]] = None
        for name, entry in manifest["snapshots"].items():
            try:
                if entry["dataset"] != dataset:
                    continue
                if only != "auto" and entry.get("layout", "grouped") != only:
                    continue
                if not set(needed) <= set(entry["columns"]):
                    continue
                if now - _parse_utc(entry["generated_at"]) > MAX_AGE:
                    continue
                size = (int(entry.get("rows", 0)), len(entry["columns"]))
            except (KeyError, TypeError, ValueError):
                continue
            if best is None or size < best[0]:
                best = (size, name, entry)
        return None if best is None else (best[1], best[2])

    # -- manifest, csv and parsed frames, local first --------------------------------

    def _load(
        self,
        name: str,
        entry: Dict[str, Any],
        date_columns: Iterable[str],
        now: datetime,
    ) -> Optional[Tuple[DataFrame, str]]:
        key = (name, str(entry.get("sha256")), tuple(sorted(date_columns)))
        frame = self._frames.get(key)
        if frame is not None:
            return frame, "cache"
        loaded = self._csv(name, entry, now)
        if loaded is None:
            return None
        data, source = loaded
        try:
            frame = self._parse(data, entry, name, date_columns)
        except (ValueError, OSError, EOFError, zlib.error):
            return None
        self._frames[key] = frame
        return frame, source

    @staticmethod
    def _parse(
        data: bytes, entry: Dict[str, Any], name: str, date_columns: Iterable[str]
    ) -> DataFrame:
        if str(entry.get("file", f"{name}.csv")).endswith(".gz"):
            data = gzip.decompress(data)
        date_columns = set(date_columns)
        dtypes: Dict[str, Any] = {}
        for col, dtype in (entry.get("dtypes") or {}).items():
            if col in date_columns or str(dtype).startswith("datetime"):
                continue
            dtypes[col] = str if dtype == "object" else dtype
        # Values such as the country code "NA" must not become NaN: only an
        # empty field is missing.
        kwargs: Dict[str, Any] = dict(keep_default_na=False, na_values=[""])
        try:
            df = pd.read_csv(io.BytesIO(data), dtype=dtypes, **kwargs)
        except (ValueError, TypeError):
            df = pd.read_csv(io.BytesIO(data), **kwargs)
        return parse_datetime_columns(df, date_columns)

    def _manifest(self, now: datetime) -> Optional[Dict[str, Any]]:
        path = self.directory / "manifest.json"
        candidates = [self._memory.get("manifest")]
        try:
            wrapper = json.loads(path.read_text(encoding="utf-8"))
            candidates.append((_parse_utc(wrapper["fetched_at"]), wrapper["manifest"]))
        except (OSError, ValueError, KeyError, TypeError):
            pass
        held = [c for c in candidates if c and _valid_manifest(c[1])]
        cached = max(held, key=lambda c: c[0]) if held else None
        if cached and now - cached[0] <= CACHE_TTL:
            return cached[1]

        # A stale local manifest still serves while GitHub is out of reach:
        # each snapshot is age-checked on its own ``generated_at`` anyway.
        stale = cached[1] if cached else None
        if self._is_unavailable(now):
            return stale
        fetched = self._get(f"{base_url()}/{self.namespace}/manifest.json")
        try:
            manifest = json.loads(fetched) if fetched is not None else None
        except ValueError:
            manifest = None
        if not _valid_manifest(manifest):
            self._unavailable_until = now + RETRY_AFTER
            return stale
        self._memory["manifest"] = (now, manifest)
        try:
            _atomic_write(
                path,
                json.dumps(
                    {"fetched_at": now.isoformat(), "manifest": manifest}
                ).encode("utf-8"),
            )
        except OSError:
            pass  # read-only or ephemeral file system: memory still serves
        return manifest

    def _csv(
        self, name: str, entry: Dict[str, Any], now: datetime
    ) -> Optional[Tuple[bytes, str]]:
        """``(file bytes, "cache" | "github")``. A local copy is only trusted when
        its hash matches the manifest, so a republished snapshot is picked up."""
        file = str(entry.get("file", f"{name}.csv"))
        path = self.directory / file
        expected = entry.get("sha256")
        try:
            data = path.read_bytes()
            if _sha256(data) == expected:
                return data, "cache"
        except OSError:
            pass
        held = self._memory.get(name)
        if held is not None and _sha256(held) == expected:
            return held, "cache"

        if self._is_unavailable(now):
            return None
        data = self._get(f"{base_url()}/{self.namespace}/{file}")
        if data is None or _sha256(data) != expected:
            # Unreachable, or the CDN is briefly serving a manifest and a file
            # from different commits: either way, leave it alone for a while.
            self._unavailable_until = now + RETRY_AFTER
            return None
        self._memory[name] = data
        try:
            _atomic_write(path, data)
        except OSError:
            pass
        return data, "github"

    def _is_unavailable(self, now: datetime) -> bool:
        return self._unavailable_until is not None and now < self._unavailable_until

    @staticmethod
    def _get(url: str) -> Optional[bytes]:
        try:
            r = requests.get(
                url,
                verify=spgci.config.verify_ssl,
                proxies=spgci.config.proxies,
                timeout=FETCH_TIMEOUT,
            )
            r.raise_for_status()
            return r.content
        except requests.exceptions.RequestException:
            return None


def _valid_manifest(manifest: Any) -> bool:
    return isinstance(manifest, dict) and isinstance(manifest.get("snapshots"), dict)


_stores: Dict[str, ReferenceStore] = {}
_stores_lock = threading.Lock()


def store(namespace: str) -> ReferenceStore:
    """The shared store for ``namespace`` (it remembers GitHub outages)."""
    with _stores_lock:
        if namespace not in _stores:
            _stores[namespace] = ReferenceStore(namespace)
        return _stores[namespace]


def lookup_unique_values(
    namespace: str,
    dataset: str,
    columns: Union[str, Sequence[str], None],
    filter_exp: Optional[str] = None,
    date_columns: Iterable[str] = (),
) -> Optional[DataFrame]:
    """What ``get_unique_values`` calls first: a snapshot-backed answer, or ``None``
    to proceed with the API. Only used in agent mode."""
    if not spgci.config.is_agent:
        return None
    try:
        return store(namespace).lookup(dataset, columns, date_columns, filter_exp)
    except Exception as e:  # a cache must never be the reason a lookup fails
        warnings.warn(f"Reference snapshot lookup failed ({e!r}); using the API.")
        return None


# -- what to publish (used by the refresh and perf scripts, and the tests) --------


def _camel(snake: str) -> str:
    head, *rest = snake.split("_")
    return head + "".join(p.capitalize() for p in rest)


def dimension_columns(method: Callable[..., Any]) -> List[str]:
    """API names of a ``get_*`` method's string filter parameters: the dataset's
    metadata dimensions (never dates, numbers or ids typed as numbers)."""
    return [
        _camel(name)
        for name, p in inspect.signature(method).parameters.items()
        if "Series[str]" in str(p.annotation)
    ]


def snapshot_specs(cls: Any) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
    """``{layout: {dataset: {snapshot group: columns}}}`` for a client class.

    ``grouped`` is the class's hand-picked ``_REFERENCE_SNAPSHOTS``. ``full`` holds,
    for each dataset, every dimension column of its ``get_*`` method (minus the
    class's ``_REFERENCE_FULL_EXCLUDE``), so it stays complete as columns are added.
    ``_REFERENCE_METHODS`` maps datasets whose method name isn't ``get_<dataset>``.
    """
    methods = getattr(cls, "_REFERENCE_METHODS", {})
    exclude = set(getattr(cls, "_REFERENCE_FULL_EXCLUDE", ()))
    full: Dict[str, Dict[str, List[str]]] = {}
    for dataset in get_args(cls._datasets):
        method = getattr(cls, methods.get(dataset, f"get_{dataset.replace('-', '_')}"))
        full[dataset] = {
            "all": [c for c in dimension_columns(method) if c not in exclude]
        }
    return {"grouped": cls._REFERENCE_SNAPSHOTS, "full": full}


# -- writing (used by reference/refresh_unique_values.py) ---------------------


def encode_csv(df: DataFrame) -> bytes:
    """Gzipped csv with a fixed header, so unchanged data is byte-identical."""
    text = df.to_csv(index=False).replace("\r\n", "\n")
    return gzip.compress(text.encode("utf-8"), compresslevel=9, mtime=0)


def write_snapshot(
    directory: Path,
    name: str,
    dataset: str,
    columns: Sequence[str],
    df: DataFrame,
    generated_at: datetime,
    layout: str = "grouped",
    max_bytes: Optional[int] = None,
) -> Dict[str, Any]:
    """Write ``<name>.csv.gz`` into ``directory`` and return its manifest entry.

    Rows are sorted so an unchanged dataset reproduces the same file.
    """
    columns = list(columns)
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"{name}: response has no column(s) {missing}")
    out = df[columns].drop_duplicates()
    try:
        out = out.sort_values(columns, kind="stable", na_position="last")
    except TypeError:
        pass  # mixed types in one column: keep the API's order
    out = out.reset_index(drop=True)
    data = encode_csv(out)
    if max_bytes is not None and len(data) > max_bytes:
        raise ValueError(f"{name}: {len(data):,} bytes exceeds the {max_bytes:,} cap")
    directory.mkdir(parents=True, exist_ok=True)
    file = f"{name}.csv.gz"
    (directory / file).write_bytes(data)
    return {
        "dataset": dataset,
        "layout": layout,
        "file": file,
        "columns": columns,
        "rows": len(out),
        "bytes": len(data),
        "dtypes": {c: str(t) for c, t in out.dtypes.items()},
        "sha256": _sha256(data),
        "generated_at": generated_at.isoformat(),
    }


def write_manifest(
    directory: Path, snapshots: Dict[str, Dict[str, Any]], generated_at: datetime
) -> Dict[str, Any]:
    manifest = {"generated_at": generated_at.isoformat(), "snapshots": snapshots}
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    return manifest
