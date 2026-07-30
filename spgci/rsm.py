from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from pandas import DataFrame, Series
from requests import Response

from spgci.api_client import get_data, post_data
from spgci.utilities import list_to_filter


_VALID_NAME = re.compile(r"^[A-Za-z0-9 ]+$")


class RsmCase:
    """
    A single editable Scenario Manager case backed by one flat DataFrame.

    The DataFrame has the same shape returned by
    `Rsm.get_scenario_manager_default_data`, with columns such as:

        - category
        - asset
        - defaultValue

    The DataFrame should not be manually split into capacity, crude, and
    product sections. The SDK performs that conversion when the scenario
    payload is built.

    Notes
    -----
    - Adding a crude means setting its percentage above zero.
    - Crude pricing and transportation costs normally already exist in the
      default data, including for crudes whose default percentage is zero.
    - Only provide `price` or `transport` when overriding the default value or
      when the crude is not present in the defaults.
    - Setters are upserts. Existing rows are updated in place and missing rows
      are appended.
    - Setters return `self`, allowing calls to be chained.

    Examples
    --------
    >>> defaults = rsm.get_scenario_manager_default_data(
    ...     refineryid=1015,
    ...     period=date(2026, 1, 1),
    ...     paginate=True,
    ... )
    >>> case1 = RsmCase(defaults, name="Case 1")
    >>> case2 = (
    ...     case1.copy(name="Case 2")
    ...     .set_crude("Canadian Heavy", 10)
    ...     .add_crude("Agbami", 2)
    ...     .set_product("Diesel", 140)
    ... )
    """

    def __init__(
        self,
        df: DataFrame,
        name: str = "Case 1",
        enabled: bool = True,
    ):
        if not isinstance(name, str) or not _VALID_NAME.fullmatch(name):
            bad = (
                sorted(set(re.findall(r"[^A-Za-z0-9 ]", name)))
                if isinstance(name, str)
                else []
            )
            raise ValueError(
                f"Invalid case name {name!r}: may only contain letters, "
                f"numbers, and spaces. Offending character(s): {bad}"
            )

        self.df = df.copy()
        self.name = name
        self.enabled = enabled

    def copy(self, name: Optional[str] = None) -> "RsmCase":
        """
        Clone this case.

        This is useful when building an additional scenario case from a
        previously configured case.
        """
        return RsmCase(
            self.df,
            name=name if name is not None else self.name,
            enabled=self.enabled,
        )

    def set(
        self,
        category: str,
        asset: str,
        value: float,
    ) -> "RsmCase":
        """
        Upsert a single `(category, asset)` value.

        Returns
        -------
        RsmCase
            This case, allowing calls to be chained.
        """
        mask = (
            (self.df["category"] == category)
            & (self.df["asset"] == asset)
        )

        if mask.any():
            self.df.loc[mask, "defaultValue"] = value
        else:
            self.df = pd.concat(
                [
                    self.df,
                    DataFrame(
                        [
                            {
                                "category": category,
                                "asset": asset,
                                "defaultValue": value,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

        return self

    def get(
        self,
        category: str,
        asset: str,
    ) -> Optional[float]:
        """
        Read a single `(category, asset)` value.

        Returns `None` when the row does not exist.
        """
        mask = (
            (self.df["category"] == category)
            & (self.df["asset"] == asset)
        )

        if not mask.any():
            return None

        return float(self.df.loc[mask, "defaultValue"].iloc[0])

    # ------------------------------------------------------------------
    # Crudes
    # ------------------------------------------------------------------

    def set_crude(
        self,
        name: str,
        pct: Optional[float] = None,
        *,
        price: Optional[float] = None,
        transport: Optional[float] = None,
    ) -> "RsmCase":
        """
        Set a crude's percentage and optionally override its economics.

        For example, `set_crude("Wti", 17)` sets Wti to 17 percent. Its price
        and transportation cost remain unchanged unless `price` or `transport`
        is supplied.
        """
        if pct is not None:
            self.set("percentage", name, pct)

        if price is not None:
            self.set("crudePricing", name, price)

        if transport is not None:
            self.set("transportationCosts", name, transport)

        return self

    add_crude = set_crude

    def remove_crude(self, name: str) -> "RsmCase":
        """
        Remove a crude from the active slate by setting its percentage to zero.
        """
        return self.set("percentage", name, 0)

    def set_crude_slate(
        self,
        slate: Dict[str, float],
        zero_others: bool = True,
    ) -> "RsmCase":
        """
        Set the crude slate from a `{crude_name: percentage}` mapping.

        When `zero_others` is true, all existing crude percentages are first
        set to zero. The resulting active slate then contains only the crudes
        supplied in `slate`.

        Existing crude prices and transportation costs remain unchanged.
        """
        if zero_others:
            self.df.loc[
                self.df["category"] == "percentage",
                "defaultValue",
            ] = 0

        for crude_name, pct in slate.items():
            self.set("percentage", crude_name, pct)

        return self

    # ------------------------------------------------------------------
    # Products
    # ------------------------------------------------------------------

    def set_product(
        self,
        name: str,
        price: Optional[float] = None,
        *,
        premium: Optional[float] = None,
        transport: Optional[float] = None,
    ) -> "RsmCase":
        """
        Override a product's price, premium or discount, or transport cost.
        """
        if price is not None:
            self.set("productPrice", name, price)

        if premium is not None:
            self.set("premiumsDiscounts", name, premium)

        if transport is not None:
            self.set("transportationCosts", name, transport)

        return self

    # ------------------------------------------------------------------
    # Capacity
    # ------------------------------------------------------------------

    def set_capacity(
        self,
        asset: str,
        value: float,
    ) -> "RsmCase":
        """
        Set a capacity or utilization value, such as reformer, alky, or aps.
        """
        return self.set("capacityAndUtilization", asset, value)


class Rsm:
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
        Retrieve Scenario Manager refinery cost and margin output.

        Parameters
        ----------
        scenario_id : list[str] | Series[str] | str
            One or more scenario identifiers.
        execution_id : list[str] | Series[str] | str
            One or more execution identifiers.
        filter_exp : str, optional
            Additional filter expression.
        page : int, optional
            Page number, by default 1.
        page_size : int, optional
            Number of rows per page, by default 1000.
        raw : bool, optional
            Return the raw `requests.Response`, by default false.
        paginate : bool, optional
            Retrieve all available pages, by default false.

        Returns
        -------
        DataFrame | Response
            Scenario output as a DataFrame, or the raw response when
            `raw=True`.
        """
        filter_params: List[str] = [
            list_to_filter("scenarioId", scenario_id),
            list_to_filter("executionId", execution_id),
        ]

        filter_params = [value for value in filter_params if value != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = (
                " AND ".join(filter_params)
                + " AND ("
                + filter_exp
                + ")"
            )

        params = {
            "page": page,
            "pageSize": page_size,
            "filter": filter_exp,
        }

        return get_data(
            path="/analytics/v1/rcma/scenario-manager-output",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_scenario_manager_default_data(
        self,
        refineryid: int,
        period: date,
        *,
        category: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve Scenario Manager default data for a refinery and period.

        The `refineryid` and `period` filters are required.

        Parameters
        ----------
        refineryid : int
            Refinery identifier.
        period : date
            Scenario period used by the default-data endpoint.
        category : list[str] | Series[str] | str, optional
            One or more categories to retrieve.
        filter_exp : str, optional
            Additional filter expression.
        page : int, optional
            Page number, by default 1.
        page_size : int, optional
            Number of rows per page, by default 1000.
        raw : bool, optional
            Return the raw `requests.Response`, by default false.
        paginate : bool, optional
            Retrieve all available pages, by default false.

        Returns
        -------
        DataFrame | Response
            Default data as a DataFrame, or the raw response when `raw=True`.
        """
        filter_params: List[str] = [
            f"period: {period}",
            f"refineryid: {refineryid}",
            list_to_filter("category", category),
        ]

        filter_params = [value for value in filter_params if value != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = (
                " AND ".join(filter_params)
                + " AND ("
                + filter_exp
                + ")"
            )

        params = {
            "page": page,
            "pageSize": page_size,
            "filter": filter_exp,
        }

        return get_data(
            path="/analytics/v1/rcma/scenario-manager-default-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_scenario_manager_ref_data(
        self,
        *,
        region: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve Scenario Manager refinery reference data.

        Parameters
        ----------
        region : list[str] | Series[str] | str, optional
            One or more refinery regions.
        filter_exp : str, optional
            Additional filter expression.
        page : int, optional
            Page number, by default 1.
        page_size : int, optional
            Number of rows per page, by default 1000.
        raw : bool, optional
            Return the raw `requests.Response`, by default false.
        paginate : bool, optional
            Retrieve all available pages, by default false.

        Returns
        -------
        DataFrame | Response
            Reference data as a DataFrame, or the raw response when `raw=True`.
        """
        filter_params: List[str] = [
            list_to_filter("region", region),
        ]

        filter_params = [value for value in filter_params if value != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = (
                " AND ".join(filter_params)
                + " AND ("
                + filter_exp
                + ")"
            )

        params = {
            "page": page,
            "pageSize": page_size,
            "filter": filter_exp,
        }

        return get_data(
            path="/analytics/v1/rcma/scenario-manager-ref-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_scenario(
        self,
        scenario_id: str,
        *,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve a saved scenario by its scenario identifier.

        This calls `/scenarios/{scenarioId}` and returns the scenario metadata
        as a one-row DataFrame. The nested `parameters` payload is omitted from
        the DataFrame for readability.

        Pass `raw=True` to retrieve the complete response, including the full
        nested parameters payload.

        Parameters
        ----------
        scenario_id : str
            Scenario identifier returned by `run_scenario`.
        raw : bool, optional
            Return the raw `requests.Response`, by default false.

        Returns
        -------
        DataFrame | Response
            Scenario metadata as a one-row DataFrame, or the raw response when
            `raw=True`.

        Examples
        --------
        >>> saved = rsm.run_scenario(...)
        >>> scenario_id = saved["scenarioId"].iloc[0]
        >>> scenario = rsm.get_scenario(scenario_id)

        Retrieve the complete nested response:

        >>> full = rsm.get_scenario(scenario_id, raw=True).json()
        """
        return get_data(
            path=(
                f"/{self._scenario_manager_scenarios_v_endpoint}/"
                f"{scenario_id}"
            ),
            params={},
            df_fn=self._convert_scenario_response_to_df,
            raw=raw,
        )

    def run_scenario(
        self,
        *,
        period: Union[date, datetime, str],
        region: str,
        refinery: str,
        refineryid: int,
        cases: Union[
            RsmCase,
            DataFrame,
            List[Union[RsmCase, DataFrame]],
        ],
        scenario_definition_id: str = (
            "2bf7e599-a25a-4b56-98b0-f384c787f3ba"
        ),
        version: str = "1",
        name: str = "",
        tags: str = "UI Saved",
        hidden: bool = True,
        display_scenario: bool = True,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Save one or more Scenario Manager cases for a refinery.

        This method converts each case's flat DataFrame into the Scenario
        Manager payload and submits it to `/scenarios`.

        Saving the scenario does not execute the model. Pass the returned
        `scenarioId` to `execute_scenario` to start execution and obtain an
        `executionId`.

        No business validation is performed by this method. The Scenario
        Manager API remains the source of truth and will reject invalid
        payloads.

        Typical workflow
        ----------------
        Retrieve the defaults:

        >>> defaults = rsm.get_scenario_manager_default_data(
        ...     refineryid=1015,
        ...     period=date(2026, 1, 1),
        ...     paginate=True,
        ... )

        Build a base case and an additional case:

        >>> base = RsmCase(defaults, name="Case 1")
        >>> downside = (
        ...     base.copy(name="Case 2")
        ...     .set_crude("North Dakota", 10)
        ...     .add_crude("Wti", 11)
        ...     .set_product("Diesel", 140)
        ... )

        Save both cases:

        >>> saved = rsm.run_scenario(
        ...     period=date(2026, 1, 1),
        ...     region="North America",
        ...     refinery="Baytown | ExxonMobil",
        ...     refineryid=1015,
        ...     cases=[base, downside],
        ... )

        Parameters
        ----------
        period : date | datetime | str
            Scenario period. A `date` or `datetime` is formatted as
            `DD/MM/YYYY` to match the Scenario Manager UI payload. For example,
            July 1, 2026 is sent as `"01/07/2026"`. A string is passed through
            unchanged.
        region : str
            Region display name, such as `"North America"`.
        refinery : str
            Refinery display name, such as `"Baytown | ExxonMobil"`.
        refineryid : int
            Refinery identifier.
        cases : RsmCase | DataFrame | list[RsmCase | DataFrame]
            One or more cases.

            An `RsmCase` preserves its configured name and enabled state.

            A bare DataFrame is treated as a single case and assigned a
            generated name such as `"Case 1"`.

            The first case is also used to construct `baseCaseCrudes`.

            Rows are mapped to the payload according to `category`:

            - `capacityAndUtilization` builds the capacity object.
            - `percentage` identifies crude shares.
            - `crudePricing` identifies crude prices.
            - `productPrice` identifies products and product prices.
            - `premiumsDiscounts` identifies product premiums or discounts.
            - `transportationCosts` supplies transport costs for both crudes
              and products, matched by asset name.
        scenario_definition_id : str, optional
            Scenario definition identifier.
        version : str, optional
            Scenario version, by default `"1"`.
        name : str, optional
            Scenario name.
        tags : str, optional
            Scenario tags, by default `"UI Saved"`.
        hidden : bool, optional
            Whether the saved scenario is hidden, by default true.
        display_scenario : bool, optional
            Whether the scenario should be displayed, by default true.
        raw : bool, optional
            Return the raw `requests.Response`, by default false.

        Returns
        -------
        DataFrame | Response
            Saved scenario metadata as a one-row DataFrame, or the raw response
            when `raw=True`.

        API requirements
        ----------------
        The Scenario Manager API validates the resulting payload. In general:

        - Active crude percentages must sum to 100 for each case.
        - Each active crude should have crude pricing and transportation costs.
        - Each product should have a product price, premium or discount, and
          transportation cost.
        - Asset names should match the default data or reference data.
        """
        case_list = cases if isinstance(cases, list) else [cases]

        if not case_list:
            raise ValueError("At least one scenario case is required.")

        normalized = [
            self._coerce_case(case, index)
            for index, case in enumerate(case_list, start=1)
        ]

        formatted_period = (
            period.strftime("%d/%m/%Y")
            if isinstance(period, (date, datetime))
            else str(period)
        )

        body: Dict[str, Any] = {
            "version": version,
            "scenarioDefinitionId": scenario_definition_id,
            "name": name,
            "parameters": {
                "period": formatted_period,
                "region": region,
                "refinery": refinery,
                "refineryId": refineryid,
                "displayScenario": display_scenario,
                "baseCaseCrudes": self._build_base_case_crudes(
                    normalized[0].df
                ),
                "scenario": {
                    "cases": [
                        self._build_scenario_case(case, case_index)
                        for case_index, case in enumerate(
                            normalized,
                            start=3,
                        )
                    ]
                },
            },
            "tags": tags,
            "hidden": hidden,
        }

        return post_data(
            path=f"/{self._scenario_manager_scenarios_v_endpoint}",
            body=body,
            df_fn=self._convert_scenario_response_to_df,
            raw=raw,
        )

    def execute_scenario(
        self,
        scenario_id: str,
        *,
        scenario_version: str = "1",
        scenario_definition_id: str = (
            "2bf7e599-a25a-4b56-98b0-f384c787f3ba"
        ),
        scenario_definition_version: str = "1",
        product: Optional[str] = None,
        correlation: Optional[str] = None,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Execute a previously saved scenario asynchronously.

        `run_scenario` saves the scenario definition but does not execute the
        model. This method triggers execution through `/execution/run`.

        The response includes the `executionId` used to identify the run. The
        execution may initially be in a waiting or running state.

        This class does not currently expose a status-polling method. Once the
        execution has completed, pass both the `scenarioId` and `executionId`
        to `get_scenario_manager_output`.

        Parameters
        ----------
        scenario_id : str
            Identifier of the saved scenario returned by `run_scenario`.
        scenario_version : str, optional
            Saved scenario version, by default `"1"`.
        scenario_definition_id : str, optional
            Scenario definition identifier.
        scenario_definition_version : str, optional
            Scenario definition version, by default `"1"`.
        product : str, optional
            Optional product identifier.
        correlation : str, optional
            Optional correlation identifier for tracing.
        raw : bool, optional
            Return the raw `requests.Response`, by default false.

        Returns
        -------
        DataFrame | Response
            Execution status as a one-row DataFrame, or the raw response when
            `raw=True`.

        Examples
        --------
        >>> saved = rsm.run_scenario(...)
        >>> scenario_id = saved["scenarioId"].iloc[0]
        >>> status = rsm.execute_scenario(scenario_id)
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

        return post_data(
            path="/execution/run",
            body=body,
            df_fn=self._convert_run_status_to_df,
            raw=raw,
        )

    @staticmethod
    def _coerce_case(
        case: Union[RsmCase, DataFrame],
        index: int,
    ) -> RsmCase:
        """
        Normalize an `RsmCase` or flat DataFrame into an `RsmCase`.
        """
        if isinstance(case, RsmCase):
            return case

        if isinstance(case, DataFrame):
            return RsmCase(case, name=f"Case {index}")

        raise TypeError(
            "Each case must be an RsmCase or a flat pandas DataFrame; "
            f"got {type(case).__name__}."
        )

    def _build_scenario_case(
        self,
        case: RsmCase,
        case_index: int,
    ) -> Dict[str, Any]:
        """
        Convert an `RsmCase` into a Scenario Manager case payload.
        """
        return {
            "caseKey": f"Case{case_index}",
            "caseName": case.name,
            "isCaseEnabled": case.enabled,
            "capacityAndUtilization": self._build_capacity(case.df),
            "crudes": self._build_crudes(case.df),
            "products": self._build_products(case.df),
        }

    @staticmethod
    def _num(value: Any) -> float:
        """
        Coerce a value to a finite, JSON-safe float.

        Missing and non-finite values become `0.0`. This prevents `NaN`,
        positive infinity, and negative infinity from entering the JSON
        request body.
        """
        if value is None or pd.isna(value):
            return 0.0

        number = float(value)

        if not math.isfinite(number):
            return 0.0

        return number

    @staticmethod
    def _build_capacity(df: DataFrame) -> Dict[str, float]:
        """
        Build the capacity and utilization section of the payload.
        """
        capacity = df[
            df["category"] == "capacityAndUtilization"
        ]

        return {
            str(asset): Rsm._num(value)
            for asset, value in zip(
                capacity["asset"],
                capacity["defaultValue"],
            )
        }

    @staticmethod
    def _build_crudes(df: DataFrame) -> List[Dict[str, Any]]:
        """
        Build active crude records from the flat default-data DataFrame.

        An asset is treated as a crude when it has a percentage row. Only
        crudes whose percentage is greater than zero are included.
        """
        wide = (
            df[
                df["category"].isin(
                    [
                        "percentage",
                        "crudePricing",
                        "transportationCosts",
                    ]
                )
            ]
            .pivot_table(
                index="asset",
                columns="category",
                values="defaultValue",
                aggfunc="first",
            )
            .rename_axis(None, axis=1)
        )

        for column in (
            "percentage",
            "crudePricing",
            "transportationCosts",
        ):
            if column not in wide.columns:
                wide[column] = float("nan")

        wide = wide[wide["percentage"].notna()]
        active = wide[wide["percentage"] > 0]

        return [
            {
                "crudeType": str(asset),
                "percentage": Rsm._num(row["percentage"]),
                "crudePricing": Rsm._num(row["crudePricing"]),
                "transportationCosts": Rsm._num(
                    row["transportationCosts"]
                ),
            }
            for asset, row in active.iterrows()
        ]

    @staticmethod
    def _build_products(df: DataFrame) -> List[Dict[str, Any]]:
        """
        Build product records from the flat default-data DataFrame.

        An asset is treated as a product only when it has a `productPrice`
        row. This prevents the shared `transportationCosts` category from
        creating phantom product records for crude assets.
        """
        wide = (
            df[
                df["category"].isin(
                    [
                        "productPrice",
                        "premiumsDiscounts",
                        "transportationCosts",
                    ]
                )
            ]
            .pivot_table(
                index="asset",
                columns="category",
                values="defaultValue",
                aggfunc="first",
            )
            .rename_axis(None, axis=1)
        )

        for column in (
            "productPrice",
            "premiumsDiscounts",
            "transportationCosts",
        ):
            if column not in wide.columns:
                wide[column] = float("nan")

        wide = wide[wide["productPrice"].notna()]

        return [
            {
                "productType": str(asset),
                "productPrice": Rsm._num(row["productPrice"]),
                "premiumsDiscounts": Rsm._num(
                    row["premiumsDiscounts"]
                ),
                "transportationCosts": Rsm._num(
                    row["transportationCosts"]
                ),
            }
            for asset, row in wide.iterrows()
        ]

    @staticmethod
    def _build_base_case_crudes(
        df: DataFrame,
    ) -> List[Dict[str, Any]]:
        """
        Build `baseCaseCrudes` from active percentage rows.
        """
        percentages = df[df["category"] == "percentage"]

        return [
            {
                "crudeType": str(asset),
                "percentage": Rsm._num(value),
            }
            for asset, value in zip(
                percentages["asset"],
                percentages["defaultValue"],
            )
            if Rsm._num(value) > 0
        ]

    @staticmethod
    def _convert_scenario_response_to_df(
        resp: Response,
    ) -> DataFrame:
        """
        Convert a scenario response into a one-row DataFrame.

        The scenario endpoints return the scenario object at the top level
        rather than wrapping it in `{"results": [...]}`.

        The nested `parameters` payload is omitted from the flattened frame.
        It remains available by calling the relevant method with `raw=True`.

        The returned `scenarioId` can be passed to `execute_scenario`, which
        returns the `executionId` required to retrieve scenario output.
        """
        response_json = resp.json()

        metadata = {
            key: value
            for key, value in response_json.items()
            if key != "parameters"
        }

        df = pd.json_normalize(metadata)  # type: ignore

        for column in ("lastUpdatedOn", "createdOn"):
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column],
                    format="ISO8601",
                    errors="coerce",
                )

        return df

    @staticmethod
    def _convert_run_status_to_df(
        resp: Response,
    ) -> DataFrame:
        """
        Convert an execution status response into a one-row DataFrame.

        The execution endpoints return a single status object rather than a
        `{"results": [...]}` wrapper.
        """
        response_json = resp.json()
        df = pd.json_normalize(response_json)  # type: ignore

        for column in ("queuedOn", "startedOn", "completedOn"):
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column],
                    format="ISO8601",
                    errors="coerce",
                )

        return df

    @staticmethod
    def _convert_to_df(resp: Response) -> DataFrame:
        """
        Convert a standard paginated analytics response into a DataFrame.
        """
        response_json = resp.json()
        df = pd.json_normalize(response_json["results"])  # type: ignore

        # `format="ISO8601"` handles mixed fractional-second precision.
        # `errors="coerce"` also converts out-of-bounds sentinel values, such
        # as year 9999 timestamps, to NaT instead of raising.
        for column in (
            "yieldDate",
            "validFrom",
            "validTo",
            "period",
        ):
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column],
                    format="ISO8601",
                    errors="coerce",
                )

        return df