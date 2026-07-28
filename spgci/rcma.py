from __future__ import annotations
from typing import Any, Dict, List, Optional, Union
from requests import Response
from spgci.api_client import get_data, post_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date, datetime
import pandas as pd
import re

_VALID_NAME = re.compile(r"^[A-Za-z0-9 ]+$")

class RcmaCase:
    """
    A single editable Scenario Manager case backed by ONE flat dataframe.

    The dataframe is the same shape returned by
    `Rcma.get_scenario_manager_default_data`, i.e. columns:
        - category      (e.g. "percentage", "crudePricing", "productPrice", ...)
        - asset         (e.g. "Wti", "Diesel", "aps", ...)
        - defaultValue

    You never split the frame into capacity/crude/product yourself; the SDK
    does that at POST time based on `category`.

    Notes
    -----
    * "Adding" a crude is just setting its percentage > 0. Crude pricing and
      transportation costs for every crude already exist in the defaults with
      percentage 0, so they ride along automatically. Only pass
      `price`/`transport` if you want to override them, or if the crude is not
      present in the defaults at all (rare - discover valid crude names via
      `Rcma.getscenario_manager_ref_data`).
    * All setters are upserts: if the (category, asset) row exists it is
      updated in place, otherwise a new row is appended.
    * Setters return `self`, so calls can be chained.

    Examples
    --------
    >>> defaults = rcma.get_scenario_manager_default_data(
    ...     refineryid=1015, period=date(2026, 1, 1), paginate=True
    ... )
    >>> case1 = RcmaCase(defaults, name="Case 1")
    >>> case2 = (
    ...     case1.copy(name="Case 2")
    ...     .set_crude("Canadian Heavy", 10)   # drop from 12% to 10%
    ...     .add_crude("Agbami", 2)            # add 2% (price comes from defaults)
    ...     .set_product("Diesel", 140)        # bump diesel price
    ... )
    """

    def __init__(self, df: DataFrame, name: str = "Case 1", enabled: bool = True):
      if not isinstance(name, str) or not _VALID_NAME.fullmatch(name):
          bad = sorted(set(re.findall(r"[^A-Za-z0-9 ]", name))) if isinstance(name, str) else []
          raise ValueError(
              f"Invalid case name {name!r}: may only contain letters, numbers, "
              f"and spaces. Offending character(s): {bad}"
          )
      self.df = df.copy()
      self.name = name
      self.enabled = enabled

    def copy(self, name: Optional[str] = None) -> "RcmaCase":
        """Clone this case (handy for building Case 2 off Case 1)."""
        return RcmaCase(self.df, name if name is not None else self.name, self.enabled)

    # -- generic upsert ---------------------------------------------------

    def set(self, category: str, asset: str, value: float) -> "RcmaCase":
        """Upsert a single (category, asset) value. Returns self for chaining."""
        mask = (self.df["category"] == category) & (self.df["asset"] == asset)
        if mask.any():
            self.df.loc[mask, "defaultValue"] = value
        else:
            self.df = pd.concat(
                [
                    self.df,
                    DataFrame(
                        [{"category": category, "asset": asset, "defaultValue": value}]
                    ),
                ],
                ignore_index=True,
            )
        return self

    def get(self, category: str, asset: str) -> Optional[float]:
        """Read a single value, or None if the row doesn't exist."""
        mask = (self.df["category"] == category) & (self.df["asset"] == asset)
        if not mask.any():
            return None
        return float(self.df.loc[mask, "defaultValue"].iloc[0])

    # -- crudes -----------------------------------------------------------

    def set_crude(
        self,
        name: str,
        pct: Optional[float] = None,
        *,
        price: Optional[float] = None,
        transport: Optional[float] = None,
    ) -> "RcmaCase":
        """
        Set a crude's share, and optionally override its economics.

        ``set_crude("Wti", 17)`` sets Wti to 17%. Pricing/transport are left at
        their defaults unless ``price``/``transport`` are supplied.
        """
        if pct is not None:
            self.set("percentage", name, pct)
        if price is not None:
            self.set("crudePricing", name, price)
        if transport is not None:
            self.set("transportationCosts", name, transport)
        return self

    add_crude = set_crude  # alias: adding a crude == giving it a percentage

    def remove_crude(self, name: str) -> "RcmaCase":
        """Drop a crude from the slate by zeroing its percentage."""
        return self.set("percentage", name, 0)

    def set_crude_slate(
        self, slate: Dict[str, float], zero_others: bool = True
    ) -> "RcmaCase":
        """
        Set the entire crude slate from a ``{name: pct}`` mapping.

        By default every crude not named is zeroed, so the resulting slate is
        exactly what you pass. Economics for each named crude come from the
        defaults already loaded in this case.
        """
        if zero_others:
            self.df.loc[self.df["category"] == "percentage", "defaultValue"] = 0
        for crude_name, pct in slate.items():
            self.set("percentage", crude_name, pct)
        return self

    # -- products ---------------------------------------------------------

    def set_product(
        self,
        name: str,
        price: Optional[float] = None,
        *,
        premium: Optional[float] = None,
        transport: Optional[float] = None,
    ) -> "RcmaCase":
        """Override a product's price / premium-discount / transport cost."""
        if price is not None:
            self.set("productPrice", name, price)
        if premium is not None:
            self.set("premiumsDiscounts", name, premium)
        if transport is not None:
            self.set("transportationCosts", name, transport)
        return self

    # -- capacity ---------------------------------------------------------

    def set_capacity(self, asset: str, value: float) -> "RcmaCase":
        """Set a capacity / utilization value (e.g. reformer, alky, aps)."""
        return self.set("capacityAndUtilization", asset, value)


class Rcma:
    _endpoint = "api/v1/"
    _get_sm_default_data_v_endpoint = "scenario-manager-default-data"
    _refinery_ref_v_endpoint = "scenario-manager-ref-data"
    _scenario_manager_scenarios_v_endpoint = "scenarios"

    def get_scenario_manager_output(
        self,
        scenario_id: Union[list[str], Series[str], str],
        execution_id: Union[list[str], Series[str], str],
        *,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        This end point will give you the cost and margins for the oil refineries

        Parameters
        ----------

         scenario_id: Union[list[str], Series[str], str]
             unique ID for each scenario
         execution_id: Union[list[str], Series[str], str]
             unique ID for each execution
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        filter_params.append(list_to_filter("executionId", execution_id))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/rcma/scenario-manager-output",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_scenario_manager_default_data(
        self,
        refineryid: int,
        period: date,
        *,
        category: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        This end point will give you the default-data for the selected refineryid and period, where refineryid and period both are mandatory filters

        Parameters
        ----------

         period: date
             first day of the quarter
         category: Optional[Union[list[str], Series[str], str]]
             representing group of assets, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(f"period: {period}")
        
        filter_params.append(f"refineryid: {refineryid}")
        filter_params.append(list_to_filter("category", category))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/rcma/scenario-manager-default-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_scenario_manager_ref_data(
        self,
        *,
        region: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        This end point will give you the reference data for refineries

        Parameters
        ----------

         region: Optional[Union[list[str], Series[str], str]]
             region of the refinery, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("region", region))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/rcma/scenario-manager-ref-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_scenario(
        self,
        scenario_id: str,
        *,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve a single saved scenario by its scenario id.

        This calls the path-parameter endpoint `/scenarios/{scenarioId}` (the
        same id returned by `run_scenario`) and returns the scenario metadata as
        a one-row DataFrame. The full nested `parameters` payload (cases,
        crudes, products, capacity) is omitted from the flat frame for
        readability; pass `raw=True` to get the complete object.

        Parameters
        ----------
        scenario_id : str
            The unique scenario id, e.g. "acb94a26-66e3-416e-aea9-5c10115c923f".
            This is the `scenarioId` returned by `run_scenario`.
        raw : bool, optional
            Return the raw `requests.Response` (whose `.json()` includes the
            full `parameters` payload) instead of a DataFrame, by default False.

        Returns
        -------
        Union[pd.DataFrame, Response]
            DataFrame
                One-row DataFrame of the scenario metadata.
            Response
                Raw `requests.Response` object when `raw=True`.

        Examples
        --------
        >>> res = rcma.run_scenario(...)
        >>> scenario_id = res["scenarioId"].iloc[0]
        >>> scenario = rcma.getscenario_manager_scenario(scenario_id)

        Retrieve the full nested payload::

        >>> full = rcma.getscenario_manager_scenario(scenario_id, raw=True).json()
        """

        response = get_data(
            path=f"/{self._scenario_manager_scenarios_v_endpoint}/{scenario_id}",
            params={},
            df_fn=self._convert_scenario_response_to_df,
            raw=raw,
        )
        return response

    def run_scenario(
        self,
        *,
        period: Union[date, datetime, str],
        region: str,
        refinery: str,
        refineryid: int,
        cases: Union["RcmaCase", DataFrame, List[Union["RcmaCase", DataFrame]]],
        scenario_definition_id: str = "2bf7e599-a25a-4b56-98b0-f384c787f3ba",
        version: str = "1",
        name: str = "",
        tags: str = "UI Saved",
        hidden: bool = True,
        display_scenario: bool = True,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Run one or more Scenario Manager cases for a refinery.

        The SDK converts each case's flat dataframe into the Scenario Manager
        POST payload and submits it to `/scenarios`. No business validation is
        performed here - the API is the source of truth and will return an error
        if the payload is invalid.

        Typical workflow
        ----------------
        1. Pull defaults for the refinery/period::

               defaults = rcma.getscenario_manager_default_data(
                   refineryid=1015, period=date(2026, 1, 1), paginate=True
               )

        2. Wrap them in a case and edit with the ergonomic setters::

               base = RcmaCase(defaults, name="Case 1")
               downside = (
                   base.copy(name="Case 2")
                   .set_crude("North Dakota", 10)   # was 21%
                   .add_crude("Wti", 11)            # picks up default price
                   .set_product("Diesel", 140)      # bump diesel price
               )

        3. Run one or more cases::

               rcma.run_scenario(
                   period=date(2026, 1, 1),
                   region="North America",
                   refinery="Baytown | ExxonMobil",
                   refineryid=1015,
                   cases=[base, downside],
               )

        Parameters
        ----------
        period : Union[date, datetime, str]
            Scenario period. A `date`/`datetime` is formatted as YYYY-MM-DD to
            match the UI payload (e.g. "2026-01-01"); a string is passed through.
        region : str
            Region display name, e.g. "North America".
        refinery : str
            Refinery display name, e.g. "Baytown | ExxonMobil".
        refineryid : int
            Refinery identifier, e.g. 1015.
        cases : RcmaCase | DataFrame | list of those
            One or more cases. Each case is either an `RcmaCase` or a flat
            dataframe with columns `category`, `asset`, `defaultValue` (the shape
            returned by `getscenario_manager_default_data`). A bare dataframe is
            treated as a single case named "Case 1". The first case is also used
            to build `baseCaseCrudes`.

            How rows map to the payload (by `category`):
                - `capacityAndUtilization`  -> the capacity object (asset -> value)
                - `percentage`              -> crude share (crudes with pct > 0)
                - `crudePricing`            -> crude price (for active crudes)
                - `productPrice`            -> defines a product
                - `premiumsDiscounts`       -> product premium/discount
                - `transportationCosts`     -> transport cost (shared by crudes &
                  products; matched to whichever asset it belongs to)

        API rules to satisfy in advance
        --------------------------------
        The API validates the payload and rejects it otherwise. For a valid run:
            - Active crude percentages must sum to 100 per case.
            - Each active crude should have a crudePricing and transportationCosts
              value (already present in the defaults).
            - Each product should have productPrice, premiumsDiscounts, and
              transportationCosts (already present in the defaults).
            - Asset names must match the defaults / ref-data (don't invent names).

        scenario_definition_id, version, name, tags, hidden, display_scenario
            Payload metadata; defaults match what the UI sends.
        raw : bool, optional
            Return the raw `requests.Response` instead of a `DataFrame`.

        Returns
        -------
        Union[pd.DataFrame, Response]
        """

        case_list = cases if isinstance(cases, list) else [cases]
        if not case_list:
            raise ValueError("At least one scenario case is required.")

        normalized = [
            self._coerce_case(case, index) for index, case in enumerate(case_list, start=1)
        ]

        body: Dict[str, Any] = {
            "version": version,
            "scenarioDefinitionId": scenario_definition_id,
            "name": name,
            "parameters": {
                "period": period.strftime("%d/%m/%Y") if isinstance(period, (date, datetime)) else str(period),
                "region": region,
                "refinery": refinery,
                "refineryId": refineryid,
                "displayScenario": display_scenario,
                "baseCaseCrudes": self._build_base_case_crudes(normalized[0].df),
                "scenario": {
                    "cases": [
                        self._build_scenario_case(case, case_index)
                        for case_index, case in enumerate(normalized, start=3)
                    ]
                },
            },
            "tags": tags,
            "hidden": hidden,
        }

        response = post_data(
            path=f"/{self._scenario_manager_scenarios_v_endpoint}",
            body=body,
            df_fn=self._convert_scenario_response_to_df,
            raw=raw,
        )
        return response

    @staticmethod
    def _coerce_case(
        case: Union["RcmaCase", DataFrame], index: int
    ) -> "RcmaCase":
        if isinstance(case, RcmaCase):
            return case
        if isinstance(case, DataFrame):
            return RcmaCase(case, name=f"Case {index}")
        raise TypeError(
            "Each case must be an RcmaCase or a flat pandas DataFrame; "
            f"got {type(case).__name__}."
        )

    def _build_scenario_case(
        self, case: "RcmaCase", case_index: int
    ) -> Dict[str, Any]:
        df = case.df
        return {
            "caseKey": f"Case{case_index}",
            "caseName": case.name,
            "isCaseEnabled": case.enabled,
            "capacityAndUtilization": self._build_capacity(df),
            "crudes": self._build_crudes(df),
            "products": self._build_products(df),
        }

    @staticmethod
    def _num(value: Any) -> float:
        """
        Coerce a value to a JSON-safe float.

        Missing cells (NaN/None) become 0.0. This mirrors the UI payload, where
        absent premium/discount and transportation-cost rows are sent as 0, and
        it also keeps `NaN`/`inf` (which are not valid JSON) out of the request.
        """
        if value is None or pd.isna(value):
            return 0.0
        return float(value)

    @staticmethod
    def _build_capacity(df: DataFrame) -> Dict[str, float]:
        cap = df[df["category"] == "capacityAndUtilization"]
        return {
            str(asset): Rcma._num(value)
            for asset, value in zip(cap["asset"], cap["defaultValue"])
        }

    @staticmethod
    def _build_crudes(df: DataFrame) -> List[Dict[str, Any]]:
        wide = (
            df[df["category"].isin(["percentage", "crudePricing", "transportationCosts"])]
            .pivot_table(
                index="asset", columns="category", values="defaultValue", aggfunc="first"
            )
            .rename_axis(None, axis=1)
        )

        for col in ["percentage", "crudePricing", "transportationCosts"]:
            if col not in wide.columns:
                wide[col] = float("nan")

        # A crude is anything with a percentage row; only emit active (pct > 0).
        wide = wide[wide["percentage"].notna()]
        active = wide[wide["percentage"] > 0]

        return [
            {
                "crudeType": str(asset),
                "percentage": Rcma._num(row["percentage"]),
                "crudePricing": Rcma._num(row["crudePricing"]),
                "transportationCosts": Rcma._num(row["transportationCosts"]),
            }
            for asset, row in active.iterrows()
        ]

    @staticmethod
    def _build_products(df: DataFrame) -> List[Dict[str, Any]]:
        wide = (
            df[df["category"].isin(["productPrice", "premiumsDiscounts", "transportationCosts"])]
            .pivot_table(
                index="asset", columns="category", values="defaultValue", aggfunc="first"
            )
            .rename_axis(None, axis=1)
        )

        for col in ["productPrice", "premiumsDiscounts", "transportationCosts"]:
            if col not in wide.columns:
                wide[col] = float("nan")

        # A product is defined by having a productPrice. This also prevents the
        # shared `transportationCosts` category from pulling crude assets in as
        # phantom products.
        wide = wide[wide["productPrice"].notna()]

        return [
            {
                "productType": str(asset),
                "productPrice": Rcma._num(row["productPrice"]),
                "premiumsDiscounts": Rcma._num(row["premiumsDiscounts"]),
                "transportationCosts": Rcma._num(row["transportationCosts"]),
            }
            for asset, row in wide.iterrows()
        ]

    @staticmethod
    def _build_base_case_crudes(df: DataFrame) -> List[Dict[str, Any]]:
        pct = df[df["category"] == "percentage"]
        return [
            {"crudeType": str(asset), "percentage": Rcma._num(value)}
            for asset, value in zip(pct["asset"], pct["defaultValue"])
            if Rcma._num(value) > 0
        ]


    @staticmethod
    def _convert_scenario_response_to_df(resp: Response) -> pd.DataFrame:
        """
        Convert the `/scenarios` POST response into a one-row DataFrame.

        Unlike the GET endpoints, the scenario POST returns the created scenario
        object at the top level (not wrapped in `{"results": [...]}`). The most
        useful thing to hand back is the scenario metadata, especially
        `scenarioId` / `executionId`, which can be passed straight into
        `getscenario_manager_output` to retrieve results.
        """
        j = resp.json()

        # Keep the (large) nested parameters payload out of the flat frame; it
        # is still available via `raw=True` if the caller needs it.
        meta = {k: v for k, v in j.items() if k != "parameters"}
        df = pd.json_normalize(meta)  # type: ignore

        if "lastUpdatedOn" in df.columns:
            df["lastUpdatedOn"] = pd.to_datetime(df["lastUpdatedOn"])  # type: ignore
        if "createdOn" in df.columns:
            df["createdOn"] = pd.to_datetime(df["createdOn"])  # type: ignore
        return df

    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
        j = resp.json()
        df = pd.json_normalize(j["results"])  # type: ignore

        # format="ISO8601" handles mixed fractional-second precision (some rows
        # have microseconds, some don't). errors="coerce" turns out-of-bounds
        # sentinels like "9999-12-31T23:59:59" (the API's "never expires" value,
        # which overflows pandas' nanosecond timestamps) into NaT rather than
        # raising.
        for col in ("yieldDate", "validFrom", "validTo", "period"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="ISO8601", errors="coerce")  # type: ignore
        return df

    def execute_scenario(
        self,
        scenario_id: str,
        *,
        scenario_version: str = "1",
        scenario_definition_id: str = "2bf7e599-a25a-4b56-98b0-f384c787f3ba",
        scenario_definition_version: str = "1",
        product: Optional[str] = None,
        correlation: Optional[str] = None,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Execute (run) a previously saved scenario asynchronously.

        Saving a scenario (`run_scenario` / `POST /scenarios`) only persists the
        definition - it does NOT run the model. This method triggers the actual
        run via the Scenario Manager `/execution/run` endpoint, which queues the
        execution and returns a `RunStatus`. The important field is
        `executionId`, which is minted here and later used with
        `getscenario_manager_output`.

        The run is asynchronous (runs take ~1.5-2 minutes), so the returned
        status typically starts as `WaitingToRun` / `Running`. Poll
        `get_execution_status` until the status reaches `Completed`, then fetch
        results with `getscenario_manager_output`.

        Parameters
        ----------
        scenario_id : str
            The id of a saved scenario (the `scenarioId` returned by
            `run_scenario`).
        scenario_version : str, optional
            Version of the scenario, by default "1".
        scenario_definition_id : str, optional
            Scenario definition id, by default the standard RCMA definition.
        scenario_definition_version : str, optional
            Version of the scenario definition, by default "1".
        product : Optional[str], optional
            Optional product identifier, by default None.
        correlation : Optional[str], optional
            Optional correlation id for tracing the run, by default None.
        raw : bool, optional
            Return the raw `requests.Response` instead of a DataFrame, by
            default False.

        Returns
        -------
        Union[pd.DataFrame, Response]
            One-row DataFrame of the `RunStatus` (executionId, status, queuedOn,
            startedOn, completedOn, progress, message), or the raw Response when
            `raw=True`.

        Examples
        --------
        >>> saved = rcma.run_scenario(...)
        >>> scenario_id = saved["scenarioId"].iloc[0]
        >>> status = rcma.execute_scenario(scenario_id)
        >>> execution_id = status["executionId"].iloc[0]
        """

        body: Dict[str, Any] = {
            "scenarioId": scenario_id,
            "scenarioVersion": scenario_version,
            "scenarioDefinitionId": scenario_definition_id,
            "scenarioDefinitionVersion": scenario_definition_version,
        }
        if product is not None:
            body["product"] = product
        if correlation is not None:
            body["correlation"] = correlation

        response = post_data(
            path=f"/execution/run",
            body=body,
            df_fn=self._convert_run_status_to_df,
            raw=raw,
        )
        return response

    def get_execution_status(
        self,
        execution_id: str,
        *,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Check the status of an asynchronous scenario execution.

        Calls `GET /execution/status/{executionId}`. When `status` is
        `Completed`, the model output has been saved and can be retrieved with
        `getscenario_manager_output`.

        Parameters
        ----------
        execution_id : str
            The execution id returned by `execute_scenario`.
        raw : bool, optional
            Return the raw `requests.Response` instead of a DataFrame, by
            default False.

        Returns
        -------
        Union[pd.DataFrame, Response]
            One-row DataFrame of the status (executionId, status, queuedOn,
            startedOn, completedOn, message). `status` is one of:
                - Running   : execution still in progress, output not available
                - Completed : execution finished, output saved in database
                - Faulted   : an execution error occurred (see `message`)

        Examples
        --------
        >>> status = rcma.get_execution_status(execution_id)
        >>> status["status"].iloc[0]
        'Completed'
        """

        response = get_data(
            path=f"/execution/status/{execution_id}",
            params={},
            df_fn=self._convert_run_status_to_df,
            raw=raw,
        )
        return response

    def execute_scenario_sync(
        self,
        parameters: Dict[str, Any],
        *,
        scenario_definition_id: str = "2bf7e599-a25a-4b56-98b0-f384c787f3ba",
        scenario_definition_version: str = "1",
        name: Optional[str] = None,
        tags: Optional[str] = None,
        product: Optional[str] = None,
        correlation: Optional[str] = None,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Execute a scenario synchronously and return the model output inline.

        Calls `POST /execution/run-sync`. Unlike `execute_scenario`, this blocks
        until the model finishes and returns the result in the same response
        (status is either `Completed` or `Faulted`). Use this for quick runs
        where you don't want to poll; use the async `execute_scenario` +
        `get_execution_status` pair for long-running scenarios.

        Note
        ----
        This takes a raw `parameters` object (the model input JSON), not the
        dataframe-based cases used by `run_scenario`. You can build `parameters`
        with the same structure `run_scenario` produces (period, region,
        refinery, baseCaseCrudes, scenario.cases, ...).

        Parameters
        ----------
        parameters : Dict[str, Any]
            Model input parameters object.
        scenario_definition_id : str, optional
            Scenario definition id, by default the standard RCMA definition.
        scenario_definition_version : str, optional
            Version of the scenario definition, by default "1".
        name : Optional[str], optional
            Scenario name, by default None.
        tags : Optional[str], optional
            Scenario tags, by default None.
        product : Optional[str], optional
            Optional product identifier, by default None.
        correlation : Optional[str], optional
            Optional correlation id, by default None.
        raw : bool, optional
            Return the raw `requests.Response` instead of a DataFrame, by
            default False. The raw response includes the full `result` object.

        Returns
        -------
        Union[pd.DataFrame, Response]
            One-row DataFrame of the sync status (executionId, status,
            completedOn, message). The `result` (model output JSON) is included
            as a column; use `raw=True` to get the full nested object cleanly.

        Examples
        --------
        >>> resp = rcma.execute_scenario_sync(parameters, raw=True).json()
        >>> resp["status"], resp["result"]
        """

        body: Dict[str, Any] = {
            "scenarioDefinitionId": scenario_definition_id,
            "scenarioDefinitionVersion": scenario_definition_version,
            "parameters": parameters,
        }
        if name is not None:
            body["name"] = name
        if tags is not None:
            body["tags"] = tags
        if product is not None:
            body["product"] = product
        if correlation is not None:
            body["correlation"] = correlation

        response = post_data(
            path=f"/execution/run-sync",
            body=body,
            df_fn=self._convert_run_status_to_df,
            raw=raw,
        )
        return response

    @staticmethod
    def _convert_run_status_to_df(resp: Response) -> pd.DataFrame:
        """
        Convert a RunStatus / RunSyncStatus / status response into a one-row
        DataFrame.

        These endpoints return a single status object (not wrapped in
        `{"results": [...]}`). Datetime fields are UTC and parsed with ISO8601
        (errors coerced to NaT so partial/absent timestamps don't raise).
        """
        j = resp.json()
        df = pd.json_normalize(j)  # type: ignore

        for col in ("queuedOn", "startedOn", "completedOn"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="ISO8601", errors="coerce")  # type: ignore
        return df



