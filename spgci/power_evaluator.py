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

from __future__ import annotations

import io
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Mapping, Optional, Union

import pandas as pd
import requests
import spgci.config
from pandas import DataFrame
from requests import Response
from spgci.api_client import get_data, post_data

PeReferenceDataset = Literal[
    "metrics",
    "cases",
    "prime_mover_fuel",
    "fuel",
    "balancing_authority",
    "interconnect",
    "nerc_region",
    "state",
    "market_settlement_type",
    "solar_type",
    "zone",
    "county",
    "owner",
    "ultimate_parent_company",
    "plant",
]

# dataset -> ``PowerEvaluator.submit`` arguments that produce it. Shared with
# ``reference/pe/refresh_reference_data.py`` so the live fallback and the
# published snapshots cannot drift apart.
_REFERENCE_LOOKUPS: Dict[str, Dict[str, Any]] = {
    "metrics": dict(series=["metric", "metric_description", "expose_as_parameter"]),
    "cases": dict(series=["Scenario", "Vintage", "Method"]),
    "prime_mover_fuel": dict(measure=["Fuel"], series=["Prime_Mover"]),
    "fuel": dict(series=["Fuel"]),
    "balancing_authority": dict(series=["Balancing_Authority"]),
    "interconnect": dict(series=["Interconnect"]),
    "nerc_region": dict(series=["NERC_Region"]),
    "state": dict(series=["State"]),
    "market_settlement_type": dict(series=["Market_Settlement_Type"]),
    "solar_type": dict(series=["Solar_Type"]),
    "zone": dict(series=["Zone"]),
    "county": dict(series=["County"]),
    "owner": dict(series=["Owner"]),
    "ultimate_parent_company": dict(series=["Ultimate_Parent_Company"]),
    "plant": dict(series=["Plant"]),
}

_REFERENCE_BASE_URL = (
    "https://raw.githubusercontent.com/spgi-ci/spgci-python/master/reference/pe"
)
#: Published snapshots older than this are ignored in favor of a live lookup.
_REFERENCE_MAX_AGE = timedelta(days=14)
#: How long a local copy is reused before it is fetched again.
_REFERENCE_CACHE_TTL = timedelta(hours=24)
_REFERENCE_FETCH_TIMEOUT = 5


def _reference_cache_dir() -> Path:
    base = os.getenv("SPGCI_CACHE_DIR") or str(Path.home() / ".cache" / "spgci")
    return Path(base) / "pe"


def _parse_utc(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _timestamp(ts: Any) -> str:
    """Format a curve timestamp; strings and ints (e.g. ``2027``) pass through."""
    if isinstance(ts, (datetime, date)):
        return ts.strftime("%Y-%m-%dT%H:%M:%S")
    return str(ts)


@dataclass
class Curve:
    """One override block: a timestamped curve injected in place of a parameter.

    Parameters
    ----------
    temporal_resolution : str
        Granularity of the timestamps, e.g. ``"annual"``, ``"hourly"``,
        ``"seasonal_shape"``, ``"diurnal_shape"``.
    values : Mapping or pandas.Series
        ``{timestamp: value}``. Timestamps may be strings, ints (years) or
        ``datetime`` objects.
    scope : list[dict], optional
        Restrict the curve to matching rows, e.g. ``[{"State": ["Arizona"]}]``.
    start_date, end_date : str, optional
        Bound the curve to a time window.
    """

    temporal_resolution: str
    values: Mapping[Any, Any]
    scope: Optional[List[Dict[str, Any]]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    def to_block(self) -> Dict[str, Any]:
        block: Dict[str, Any] = {"temporal_resolution": self.temporal_resolution}
        if self.scope is not None:
            block["scope"] = self.scope
        if self.start_date is not None:
            block["start_date"] = self.start_date
        if self.end_date is not None:
            block["end_date"] = self.end_date
        block["values"] = [
            {"timestamp": _timestamp(k), "value": v} for k, v in self.values.items()
        ]
        return block


def _normalize_override(value: Any) -> Any:
    """Apply the sensitivity/implementation nesting rules to an override value.

    - ``Curve``                      -> ``[[block]]`` (one case, one block)
    - list containing ``Curve``/etc. -> each element is one case:
        ``Curve`` -> ``[block]``; list of ``Curve`` -> ``[block, ...]``;
        scalars, ``"default"`` and pre-built lists pass through.
    - anything else passes through untouched.
    """
    if isinstance(value, Curve):
        return [[value.to_block()]]
    if isinstance(value, list):
        out: List[Any] = []
        for case in value:
            if isinstance(case, Curve):
                out.append([case.to_block()])
            elif isinstance(case, list):
                out.append([c.to_block() if isinstance(c, Curve) else c for c in case])
            else:
                out.append(case)
        return out
    return value


class PowerEvaluator:
    """
    Power Evaluator (MaaS) API Client. Valuation and fundamentals engine for
    power assets, nodes, zones and weather grid cells.

    Execution mode is auto-detected by the API (force it with ``run_async``).
    Small requests run synchronously and ``submit`` returns a DataFrame. Heavy
    requests run asynchronously: ``submit`` returns a ``run_id``; poll
    ``get_status(run_id)`` until its ``Message`` is ``"Completed"``, then call
    ``get_result(run_id)``.

    Examples
    --------
    >>> import time
    >>> pe = ci.PowerEvaluator()
    >>> run_id = pe.submit(series=["metric"], run_async=True)
    >>> while pe.get_status(run_id)["Message"] == "InProgress":
    ...     time.sleep(5)
    >>> df = pe.get_result(run_id)

    Valid names for scenarios, vintages, metrics, zones and so on are available
    from ``get_reference_data`` without running a job.
    """

    _path_submit = "powerevaluator/v1/pemaas/RetrieveData"
    _path_status = "powerevaluator/v1/pemaas/RetrieveModelStatus"

    Curve = Curve

    @staticmethod
    def scope(**filters: Union[str, List[str]]) -> Dict[str, List[str]]:
        """Build one scope object. Filters inside it are AND-ed; pass a list of
        scope objects to ``scope=`` to OR them.

        Values may be plain strings, lists, or predicate expressions such as
        ``">=now()"``, ``"IN ('A', 'B')"`` or ``"S%"``.

        Examples
        --------
        >>> PowerEvaluator.scope(state="California", fuel="Solar")
        {'state': ['California'], 'fuel': ['Solar']}
        """
        return {k: [v] if isinstance(v, str) else list(v) for k, v in filters.items()}

    @staticmethod
    def site(
        name: str,
        geometry: str,
        project: Optional[str] = None,
        **parameter_overrides: Any,
    ) -> Dict[str, Any]:
        """Build a named scope object (WKT geometry) with per-site overrides.

        Use with ``series=["project_name", "object_name"]`` to get one result
        group per site.
        """
        obj: Dict[str, Any] = {"object_name": [name], "geometry": [geometry]}
        if project is not None:
            obj["project_name"] = [project]
        if parameter_overrides:
            obj["parameter_overrides"] = {
                k: v.to_block() if isinstance(v, Curve) else v
                for k, v in parameter_overrides.items()
            }
        return obj

    @staticmethod
    def _build_payload(
        *,
        scope: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        grain: Optional[Union[str, List[str]]] = None,
        measure: Optional[Union[str, List[str]]] = None,
        series: Optional[Union[str, List[str]]] = None,
        scenario: Optional[Union[str, List[str]]] = None,
        vintage: Optional[Union[str, List[str]]] = None,
        method: Optional[Union[str, List[str]]] = None,
        prime_mover: Optional[Union[str, List[str]]] = None,
        fuel: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        temporal_resolution: Optional[str] = None,
        timezone: Optional[str] = None,
        as_of: Optional[str] = None,
        overlap_resolution: Optional[str] = None,
        relax_autogroup: Optional[bool] = None,
        market_settlement_type: Optional[Union[str, List[str]]] = None,
        having: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        project_name: Optional[str] = None,
        run_async: Optional[bool] = None,
        **overrides: Any,
    ) -> Dict[str, Any]:
        """Assemble the request body without sending it. See ``submit``."""
        if not measure and not series:
            raise ValueError("At least one of `measure` or `series` is required.")

        if isinstance(scope, dict):
            scope = [scope]
        if isinstance(having, dict):
            having = [having]

        fixed: Dict[str, Any] = {
            "PROJECT_NAME": project_name,
            "SCOPE": scope,
            "GRAIN": grain,
            "START_DATE": start_date,
            "END_DATE": end_date,
            "MEASURE": [measure] if isinstance(measure, str) else measure,
            "SERIES": [series] if isinstance(series, str) else series,
            "SCENARIO": scenario,
            "VINTAGE": vintage,
            "METHOD": method,
            "PRIME_MOVER": prime_mover,
            "FUEL": fuel,
            "TEMPORAL_RESOLUTION": temporal_resolution,
            "TIMEZONE": timezone,
            "AS_OF": as_of,
            "OVERLAP_RESOLUTION": overlap_resolution,
            "RELAX_AUTOGROUP": relax_autogroup,
            "MARKET_SETTLEMENT_TYPE": market_settlement_type,
            "HAVING": having,
            "API_TYPE": None if run_async is None else ("async" if run_async else "sync"),
        }
        body = {k: v for k, v in fixed.items() if v is not None}

        for key, value in overrides.items():
            body[key.upper()] = _normalize_override(value)
        return body

    def submit(
        self,
        *,
        scope: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        grain: Optional[Union[str, List[str]]] = None,
        measure: Optional[Union[str, List[str]]] = None,
        series: Optional[Union[str, List[str]]] = None,
        scenario: Optional[Union[str, List[str]]] = None,
        vintage: Optional[Union[str, List[str]]] = None,
        method: Optional[Union[str, List[str]]] = None,
        prime_mover: Optional[Union[str, List[str]]] = None,
        fuel: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        temporal_resolution: Optional[str] = None,
        timezone: Optional[str] = None,
        as_of: Optional[str] = None,
        overlap_resolution: Optional[str] = None,
        relax_autogroup: Optional[bool] = None,
        market_settlement_type: Optional[Union[str, List[str]]] = None,
        having: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        project_name: Optional[str] = None,
        run_async: Optional[bool] = None,
        payload: Optional[Dict[str, Any]] = None,
        dry_run: bool = False,
        raw: bool = False,
        **overrides: Any,
    ) -> Union[DataFrame, str, Dict[str, Any], Response]:
        """
        Submit a Power Evaluator request.

        Every argument is optional except that at least one of ``measure`` or
        ``series`` must be given. Keyword names are the lower-case form of the
        API's keys (``prime_mover`` -> ``PRIME_MOVER``).

        Parameters
        ----------
        scope : dict or list[dict], optional
            Which entities to evaluate. Build with ``PowerEvaluator.scope(...)``.
            Fields inside one object are AND-ed; separate objects are OR-ed.
        grain : str or list[str], optional
            What each row represents: ``"Asset"``, ``"Node"``, ``"Zone"`` or
            ``"Grid"``. Set it whenever possible; it speeds computation.
        measure : str or list[str], optional
            Metric names or custom SQL expressions, e.g.
            ``"sum(`a`)/sum(`b`) as `ratio`"``.
        series : str or list[str], optional
            Grouping dimensions. Put a dimension or ``"metric"`` here (without
            ``measure``) to introspect the catalog.
        scenario, vintage, method, prime_mover, fuel, market_settlement_type
            Forecast case and technology selectors. Lists produce ``sens_`` columns.
        start_date, end_date : str, optional
            ISO 8601 window.
        temporal_resolution : str, optional
            e.g. ``"hourly"``, ``"monthly"``, ``"annual"``, ``"diurnal_shape"``.
        timezone, as_of, overlap_resolution, relax_autogroup, having, project_name
            See the Power Evaluator FAQ.
        run_async : bool, optional
            ``None`` (default) lets the API choose: small requests run sync and
            return a DataFrame, heavy ones run async and return a ``run_id``.
            ``True`` forces async; ``False`` forces sync, which can time out on
            heavy studies. Sent to the API as ``API_TYPE``.
        payload : dict, optional
            A fully formed request body. When given, all other arguments are
            ignored.
        dry_run : bool, optional
            Return the request body without sending it, by default False.
        raw : bool, optional
            Return the ``requests.Response`` instead of parsed output.
        **overrides
            Any metric/parameter to override, e.g. ``tax_fed=0.18`` (single
            value), ``coe=["default", 0.09, 0.12]`` (sensitivity set), or
            ``energy_price=PowerEvaluator.Curve("annual", {2027: 48, 2028: 52})``
            (injected curve). Keys are upper-cased. The API silently ignores
            overrides on metrics where ``expose_as_parameter`` is false.

        Returns
        -------
        Union[DataFrame, str, dict, Response]
            DataFrame
                Sync execution: the rows from the response ``Data``.
            str
                Async execution: the ``run_id`` to pass to ``get_status`` and
                ``get_result``.
            dict
                The request body if ``dry_run=True`` (or the parsed JSON if the
                response has neither ``Data`` nor ``run_id``).
            Response
                Raw ``requests.Response`` if ``raw=True``.

        Examples
        --------
        >>> pe = ci.PowerEvaluator()
        >>> pe.submit(
        ...     scope=pe.scope(owner="City of Burbank, California"),
        ...     grain="Asset",
        ...     measure=["fair_market_value", "irr"],
        ...     scenario="Market Indicative Case",
        ...     tax_fed=["default", 0.12, 0.18],
        ... )
        """
        body = payload
        if body is None:
            body = self._build_payload(
                scope=scope,
                grain=grain,
                measure=measure,
                series=series,
                scenario=scenario,
                vintage=vintage,
                method=method,
                prime_mover=prime_mover,
                fuel=fuel,
                start_date=start_date,
                end_date=end_date,
                temporal_resolution=temporal_resolution,
                timezone=timezone,
                as_of=as_of,
                overlap_resolution=overlap_resolution,
                relax_autogroup=relax_autogroup,
                market_settlement_type=market_settlement_type,
                having=having,
                project_name=project_name,
                run_async=run_async,
                **overrides,
            )
        if dry_run:
            return body

        response = post_data(path=self._path_submit, body=body, raw=True)
        if raw:
            return response  # type: ignore[return-value]

        j = response.json()  # type: ignore[union-attr]
        if isinstance(j, dict) and "run_id" in j:
            return str(j["run_id"])
        if isinstance(j, dict) and "Data" in j:
            return pd.json_normalize(j["Data"])  # type: ignore[arg-type]
        return j

    def get_status(
        self, run_id: Union[str, int], raw: bool = False
    ) -> Union[Dict[str, Any], Response]:
        """
        Get the status of an async run.

        Parameters
        ----------
        run_id : Union[str, int]
            The run id returned by ``submit``.
        raw : bool, optional
            Return the ``requests.Response`` instead of a dict.

        Returns
        -------
        Union[dict, Response]
            dict with keys ``run_id``, ``Message`` (``"InProgress"`` or
            ``"Completed"``), ``presigned_url`` (set once completed),
            ``Start_time`` and ``End_Time``.
        """
        response = get_data(path=f"{self._path_status}/{run_id}", params={}, raw=True)
        if raw:
            return response  # type: ignore[return-value]

        j = response.json()  # type: ignore[union-attr]
        data = j.get("Data") if isinstance(j, dict) else None
        return data[0] if data else j

    def get_result(self, run_id: Union[str, int]) -> DataFrame:
        """
        Download the result of a completed async run as a DataFrame.

        Parameters
        ----------
        run_id : Union[str, int]
            The run id returned by ``submit``.

        Raises
        ------
        RuntimeError
            If the run is not ``"Completed"`` yet (check ``get_status``).
        """
        status = self.get_status(run_id)
        if not isinstance(status, dict) or status.get("Message") != "Completed":
            raise RuntimeError(f"Run {run_id} is not complete: {status}")

        # The presigned S3 URL carries its own signature; do not send the bearer
        # token through `api_client`.
        resp = requests.get(
            status["presigned_url"],
            verify=spgci.config.verify_ssl,
            proxies=spgci.config.proxies,
            timeout=float(getattr(spgci.config, "timeout", 60)),
        )
        resp.raise_for_status()
        # Spark writes embedded quotes as \" inside quoted fields.
        return pd.read_csv(io.BytesIO(resp.content), escapechar="\\")

    def get_reference_data(
        self,
        dataset: PeReferenceDataset,
        refresh: bool = False,
        use_github: bool = True,
    ) -> DataFrame:
        """
        Get the valid names for a Power Evaluator dimension, for example the
        metric catalog or the scenario / vintage / method combinations.

        Lookups are served from a local copy when possible, then from a snapshot
        published in the SDK's GitHub repo, then from a live synchronous
        lookup through ``submit`` (about 15 to 30 seconds). A published snapshot
        is ignored if its manifest is older than 14 days, so stale data falls
        back to the live lookup. ``df.attrs["source"]`` is ``"cache"``,
        ``"github"`` or ``"api"``.

        Parameters
        ----------
        dataset : PeReferenceDataset
            One of ``"metrics"``, ``"cases"``, ``"prime_mover_fuel"``,
            ``"fuel"``, ``"balancing_authority"``, ``"interconnect"``,
            ``"nerc_region"``, ``"state"``, ``"market_settlement_type"``,
            ``"solar_type"``, ``"zone"``, ``"county"``, ``"owner"``,
            ``"ultimate_parent_company"``, ``"plant"``. For anything else (node,
            asset, ...) use ``submit(series=[...], scope=...)``.
        refresh : bool, optional
            Skip the local copy and the published snapshot, by default False.
        use_github : bool, optional
            Allow fetching the published snapshot, by default True. Set False
            on networks that block GitHub.

        Returns
        -------
        DataFrame
            Values are stored lowercase for most dimensions. Some contain a
            blank value (read as ``NaN``); drop it before using the column as a
            filter. ``prime_mover_fuel`` stores each fuel list as a JSON string.
            In ``metrics``, ``expose_as_parameter`` is ``True``, ``False`` or
            blank (unknown).

        Examples
        --------
        >>> pe = ci.PowerEvaluator()
        >>> pe.get_reference_data("cases").query("scenario == 'Power Crunch'")
        """
        if dataset not in _REFERENCE_LOOKUPS:
            raise ValueError(
                f"Unknown reference dataset {dataset!r}. Valid datasets: "
                f"{', '.join(_REFERENCE_LOOKUPS)}. For other dimensions use "
                "submit(series=[...], scope=...)."
            )

        cache_dir = _reference_cache_dir()
        csv_path = cache_dir / f"{dataset}.csv"
        meta_path = cache_dir / f"{dataset}.json"
        now = datetime.now(timezone.utc)

        if not refresh:
            cached = self._read_reference_cache(csv_path, meta_path, now)
            if cached is not None:
                return cached

        source = "api"
        generated_at: Optional[datetime] = None
        text = ""

        if use_github:
            fetched = self._fetch_reference_snapshot(dataset, now)
            if fetched is not None:
                text, generated_at = fetched
                source = "github"

        if source == "api":
            df = self.submit(run_async=False, **_REFERENCE_LOOKUPS[dataset])
            if not isinstance(df, DataFrame):
                raise RuntimeError(
                    f"Live lookup for {dataset!r} did not return rows: {df!r}"
                )
            text = df.to_csv(index=False)

        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            csv_path.write_text(text, encoding="utf-8")
            meta_path.write_text(
                json.dumps(
                    {
                        "source": source,
                        "fetched_at": now.isoformat(),
                        "generated_at": generated_at.isoformat()
                        if generated_at
                        else None,
                    }
                ),
                encoding="utf-8",
            )
        except OSError:
            pass  # read-only or ephemeral file system: just skip caching

        out = pd.read_csv(io.StringIO(text))
        out.attrs["source"] = source
        return out

    @staticmethod
    def _read_reference_cache(
        csv_path: Path, meta_path: Path, now: datetime
    ) -> Optional[DataFrame]:
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if now - _parse_utc(meta["fetched_at"]) > _REFERENCE_CACHE_TTL:
                return None
            if meta.get("source") == "github":
                if now - _parse_utc(meta["generated_at"]) > _REFERENCE_MAX_AGE:
                    return None
            out = pd.read_csv(csv_path)
        except (OSError, ValueError, KeyError, TypeError):
            return None
        out.attrs["source"] = "cache"
        return out

    @staticmethod
    def _fetch_reference_snapshot(
        dataset: str, now: datetime
    ) -> Optional[tuple[str, datetime]]:
        """Return ``(csv_text, generated_at)`` from GitHub, or ``None`` when the
        snapshot is unreachable, missing, or older than the maximum age."""
        kwargs: Dict[str, Any] = dict(
            verify=spgci.config.verify_ssl,
            proxies=spgci.config.proxies,
            timeout=_REFERENCE_FETCH_TIMEOUT,
        )
        try:
            m = requests.get(f"{_REFERENCE_BASE_URL}/manifest.json", **kwargs)
            m.raise_for_status()
            manifest = m.json()
            generated_at = _parse_utc(manifest["generated_at"])
            if now - generated_at > _REFERENCE_MAX_AGE:
                return None
            if dataset not in manifest["datasets"]:
                return None
            r = requests.get(f"{_REFERENCE_BASE_URL}/{dataset}.csv", **kwargs)
            r.raise_for_status()
            return r.content.decode("utf-8"), generated_at
        except (requests.exceptions.RequestException, ValueError, KeyError, TypeError):
            return None
