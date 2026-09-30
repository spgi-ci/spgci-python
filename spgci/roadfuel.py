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
from typing import Any, ClassVar, Dict, List, Optional, Union, Literal, Iterator, Tuple
from requests import Response
from spgci.api_client import get_data, post_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from dataclasses import dataclass
import pandas as pd


@dataclass
class BiofuelBlend:
    """
    A single ``referenceBioFuelBlending`` entry for the road-fuel calculator.

    Parameters
    ----------
    reference_biofuel : str
        The biofuel name, e.g. ``"RD-A"`` or ``"B30 Advanced FAME MGO"``.
        Set to ``BiofuelBlend.ALL`` (``"all"``, any case) to expand to every
        biofuel on record for the region/sector, reusing this entry's
        ``liters_biofuel_blended`` / ``emission_factors`` for each one.
    liters_biofuel_blended : float
        The quantity of biofuel blended. Required by the API for every entry.
    emission_factors : Optional[float]
        The emission factor. When omitted it is left out of the request body
        entirely and the API silently defaults it to ``14``.

    Examples
    --------
    >>> BiofuelBlend("RD-A", 1000, emission_factors=14)
    >>> BiofuelBlend(BiofuelBlend.ALL, 1000)
    """

    reference_biofuel: str
    liters_biofuel_blended: float
    emission_factors: Optional[float] = None

    #: Wildcard expanding to every biofuel on record for the region/sector.
    ALL: ClassVar[str] = "all"

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize to the camelCase payload the API expects.

        ``emissionFactors`` is omitted when unset so the API can apply its
        own default of ``14`` — sending an explicit ``null`` is not equivalent.
        """
        d: Dict[str, Any] = {
            "referenceBioFuel": self.reference_biofuel,
            "litersBiofuelBlended": self.liters_biofuel_blended,
        }
        if self.emission_factors is not None:
            d["emissionFactors"] = self.emission_factors
        return d


#: A blend entry may be supplied as a ``BiofuelBlend`` or a raw camelCase dict.
BlendInput = Union[BiofuelBlend, Dict[str, Any]]


def _serialize_blends(
    blends: Union[BlendInput, List[BlendInput]],
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Normalize blend input to JSON-ready payload.

    Accepts a single entry or a list, and either ``BiofuelBlend`` instances or
    raw dicts (passed through untouched). Shape is preserved: a single entry in
    yields a single object out, a list yields a list.
    """
    if isinstance(blends, BiofuelBlend):
        return blends.to_dict()
    if isinstance(blends, dict):
        return blends
    return [b.to_dict() if isinstance(b, BiofuelBlend) else b for b in blends]


class RoadFuel:
    """
    Road Fuel / RED III blending calculator API client.

    Provides access to the road-fuel blending calculator, its reference data
    (biofuels and obligation percentages) 
    """

    _datasets = Literal[
        "biofuel-data",
        "obligation-data",
    ]

    _path_calculate_price = "fuel-calculator/v1/roadblending/calculate-price-api"
    _path_ref_biofuel = "api/v1/calculators/road-fuel/reference/biofuel-data"
    _path_ref_obligation = "api/v1/calculators/road-fuel/reference/obligation-data"

    def get_unique_values(
        self,
        dataset: _datasets,
        columns: Optional[Union[list[str], str]],
        filter_exp: Optional[str] = None,
    ) -> DataFrame:
        """
        Get unique values for specified columns in a dataset, optionally filtered by an expression.

        This method is crucial for data discovery and validation before making actual data queries.
        Use this to understand what values are available in the dataset and what combinations
        actually exist before attempting to filter your main data queries.

        Args:
            dataset (str): The dataset name in kebab-case format:
                - get_biofuel_data → "biofuel-data"
                - get_obligation_data → "obligation-data"
            columns (list[str] or str): Column names to get unique values for.
                - Use camelCase format: ["region", "transportSector"]
                - Can be single string: "region"
                - Can be multiple columns: ["region", "transportSector", "referenceBiofuel"]
            filter_exp (str, optional): Filter expression to limit results to specific subsets.

        Returns:
            pd.DataFrame: DataFrame with unique combinations of the specified columns,
            optionally filtered by the provided expression.
        """

        dataset_to_path = {
            "biofuel-data": self._path_ref_biofuel,
            "obligation-data": self._path_ref_obligation,
        }

        if dataset not in dataset_to_path:
            valid = "\n".join(dataset_to_path.keys())
            print(f"Dataset '{dataset}' not found. Valid Datasets:\n", valid)
            raise ValueError(
                f"dataset '{dataset}' not found ",
            )
        else:
            path = dataset_to_path[dataset]

        col_value = ", ".join(columns) if isinstance(columns, list) else columns or ""
        params = {"GroupBy": col_value, "pageSize": 5000}

        if filter_exp is not None:
            params.update({"filter": filter_exp})

        def to_df(resp: Response) -> pd.DataFrame:
            j = resp.json()
            df = pd.json_normalize(j["aggResultValue"])
            return df

        return get_data(path, params, to_df, paginate=True)

    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
        j = resp.json()
        df = pd.json_normalize(j["results"])
        return df

    @staticmethod
    def _iter_price_combinations(
        resp: Response,
    ) -> Iterator[Tuple[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Yield ``(base, metrics)`` for each combination in a ``calculate-price-api``
        response.

        ``base`` holds combination-level fields: ``combinationId``, ``status``,
        ``requestSummary`` fields (``fuelsUsed`` exploded to ``fuelsUsed.<fuel>``),
        ``ticketType`` (lifted from the ``TicketType`` metric), ``error``, and
        ``errorType``. ``metrics`` is the remaining ``result[]`` entries.
        """
        j = resp.json()
        for combo in j.get("combinations") or []:
            base: Dict[str, Any] = {
                "combinationId": combo.get("combinationId"),
                "status": combo.get("status"),
            }
            for key, val in (combo.get("requestSummary") or {}).items():
                if key == "fuelsUsed" and isinstance(val, dict):
                    for fuel_name, fuel_val in val.items():
                        base[f"fuelsUsed.{fuel_name}"] = fuel_val
                else:
                    base[key] = val

            metrics = combo.get("result") or []
            for m in metrics:
                if m.get("metric") == "TicketType":
                    base["ticketType"] = m.get("value")
            metrics = [
                m for m in metrics if m.get("metric") not in (None, "TicketType")
            ]

            base["error"] = combo.get("error")
            base["errorType"] = combo.get("errorType")
            yield base, metrics

    @staticmethod
    def _convert_price_to_df(resp: Response) -> pd.DataFrame:
        """
        Long format: one row per (combination, metric).

        - Combination-level fields (see ``_iter_price_combinations``) are
          repeated on each row.
        - Each ``result[]`` entry becomes a row with ``metric``, ``label``,
          ``value`` (numeric), ``uom``, and ``currency``. Costs are typically
          EUR — never assume USD.
        - Failed combinations produce a single row with ``status``, ``error``,
          and ``errorType`` populated and metric fields null.
        """
        rows: List[Dict[str, Any]] = []
        for base, metrics in RoadFuel._iter_price_combinations(resp):
            if not metrics:
                rows.append(base)
                continue
            for m in metrics:
                rows.append({
                    **base,
                    "metric": m.get("metric"),
                    "label": m.get("label"),
                    "value": m.get("value"),
                    "uom": m.get("uom") or None,
                    "currency": m.get("currency") or None,
                })

        df = pd.DataFrame(rows)
        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df

    @staticmethod
    def _convert_price_to_wide_df(resp: Response) -> pd.DataFrame:
        """
        Wide format: one row per combination.

        - Combination-level fields (see ``_iter_price_combinations``) are columns.
        - Each metric becomes a numeric column named by its exact ``label``
          (e.g. ``"Cost of buying tickets instead"``), in API order. If two
          metrics share a label, the column is ``"<label> (<metric>)"``.
        - ``currency`` is a single combination-level column taken from the
          cost metrics (multiple distinct currencies are joined with ``", "``).
          Never assume USD.
        - Failed combinations are kept as rows with metric columns null.
        """
        rows: List[Dict[str, Any]] = []
        metric_cols: List[str] = []
        col_for_metric: Dict[str, str] = {}

        for base, metrics in RoadFuel._iter_price_combinations(resp):
            row: Dict[str, Any] = dict(base)
            currencies: List[str] = []
            for m in metrics:
                code = m["metric"]
                if code not in col_for_metric:
                    col = m.get("label") or code
                    if col in col_for_metric.values() or col in base:
                        col = f"{col} ({code})"
                    col_for_metric[code] = col
                    metric_cols.append(col)
                row[col_for_metric[code]] = m.get("value")
                cur = m.get("currency")
                if cur and cur not in currencies:
                    currencies.append(cur)
            row["currency"] = ", ".join(currencies) if currencies else None
            rows.append(row)

        df = pd.DataFrame(rows)
        for col in metric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Combination fields first, then currency, then metrics in API order.
        front = [c for c in df.columns if c not in metric_cols and c != "currency"]
        ordered = front + (["currency"] if "currency" in df.columns else []) + metric_cols
        return df[ordered]

    def calculate_price(
        self,
        *,
        region: str,
        transport_sector: str,
        reference_biofuel_blending: Union[BlendInput, List[BlendInput]],
        fuels_used: Dict[str, float],
        obligation_year: Optional[Union[str, int, List[Union[str, int]]]] = None,
        wide: bool = True,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Calculate RED III road-fuel / maritime blending economics.

        The endpoint supports multi-combination requests: ``obligation_year``
        and ``reference_biofuel_blending`` each accept a single value or a list.
        When either is a list, the API computes every
        ``obligation_year x reference_biofuel_blending`` combination and returns
        one entry per combination. Results are ordered by year, then biofuel
        name alphabetically — not by request order.

        Parameters
        ----------
        region : str
            The market region, e.g. ``"Netherlands"``. Required.
        transport_sector : str
            The transport sector, ``"Road"`` or ``"Maritime"``. Required.
        reference_biofuel_blending : BiofuelBlend, dict, or list of either
            One blending entry or a list of them. Prefer :class:`BiofuelBlend`;
            raw camelCase dicts are passed through unchanged. Set
            ``reference_biofuel`` to ``BiofuelBlend.ALL`` to expand to every
            biofuel on record for the region/sector.
        fuels_used : dict
            Mapping of fuel type to quantity. Fuel keys are sector-constrained:

            - Road sector → ``diesel``, ``gasoline``
            - Maritime sector → ``mgo``, ``vlsfo``, ``hsfo``

            At least one fuel must be provided.
        obligation_year : str, int, or list, optional
            A single obligation year or a list of years. If omitted entirely
            (or ``None``), the API defaults to the current year. An explicitly
            empty value (``""`` / ``[]``) is rejected as invalid input.
        wide : bool, optional
            Return one row per combination (default, True), with one numeric
            column per metric named by its exact label. Set ``False`` for
            one row per (combination, metric) with ``label``, ``uom``, and
            ``currency`` per metric.
        raw : bool, optional
            Return a ``requests.Response`` instead of a ``DataFrame``, by default False.

        Returns
        -------
        Union[pd.DataFrame, Response]
            DataFrame, ``wide=True`` (default)
                One row per combination. Combination fields (``combinationId``,
                ``status``, ``region``, ``transportSector``, ``obligationYear``,
                ``referenceBioFuel``, ``litersBiofuelBlended``,
                ``emissionFactors``, ``fuelsUsed.<fuel>``, ``ticketType``,
                ``error``, ``errorType``), a ``currency`` column, and one
                numeric column per metric, named by its exact ``label``.
            DataFrame, ``wide=False``
                One row per (combination, metric). Same combination fields
                repeated on each row, plus ``metric``, ``label``, ``value``,
                ``uom``, ``currency``.
            Response
                Raw ``requests.Response`` object.

            Failed combinations are kept in both DataFrame formats with
            ``status``, ``error``, and ``errorType`` populated and metrics null.

        Examples
        --------
        **Multiple years, single biofuel**

        >>> ci.RoadFuel().calculate_price(
        ...     region="Netherlands",
        ...     transport_sector="Maritime",
        ...     obligation_year=["2026", "2028"],
        ...     reference_biofuel_blending=BiofuelBlend("RD-A", 1000, emission_factors=14),
        ...     fuels_used={"mgo": 10, "vlsfo": 20, "hsfo": 30},
        ... )

        **Multiple years x multiple biofuels**

        >>> ci.RoadFuel().calculate_price(
        ...     region="Netherlands",
        ...     transport_sector="Maritime",
        ...     obligation_year=["2026", "2028"],
        ...     reference_biofuel_blending=[
        ...         BiofuelBlend("RD-A", 1000, emission_factors=14),
        ...         BiofuelBlend("B30 Advanced FAME MGO", 500, emission_factors=20),
        ...     ],
        ...     fuels_used={"mgo": 10, "vlsfo": 20, "hsfo": 30},
        ... )

        **Every biofuel on record, current year, long format with labels**

        >>> ci.RoadFuel().calculate_price(
        ...     region="Netherlands",
        ...     transport_sector="Maritime",
        ...     reference_biofuel_blending=BiofuelBlend(BiofuelBlend.ALL, 1000),
        ...     fuels_used={"mgo": 10, "vlsfo": 20, "hsfo": 30},
        ...     wide=False,
        ... )
        """

        body: Dict[str, Any] = {
            "region": region,
            "transportSector": transport_sector,
            "referenceBioFuelBlending": _serialize_blends(reference_biofuel_blending),
            "fuelsUsed": fuels_used,
        }

        # Only include obligationYear when supplied, so the API can apply its
        # own "default to the current year" behavior on omission.
        if obligation_year is not None:
            body["obligationYear"] = obligation_year

        df_fn = self._convert_price_to_wide_df if wide else self._convert_price_to_df

        return post_data(
            path=self._path_calculate_price,
            body=body,
            df_fn=df_fn,
            raw=raw,
        )


    def get_biofuel_data(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        transport_sector: Optional[Union[list[str], Series[str], str]] = None,
        reference_biofuel: Optional[Union[list[str], Series[str], str]] = None,
        symbol_c_bate: Optional[Union[list[str], Series[str], str]] = None,
        symbol_description: Optional[Union[list[str], Series[str], str]] = None,
        fx_rate_symbol: Optional[Union[list[str], Series[str], str]] = None,
        uom_factor: Optional[float] = None,
        uom_factor_lt: Optional[float] = None,
        uom_factor_lte: Optional[float] = None,
        uom_factor_gt: Optional[float] = None,
        uom_factor_gte: Optional[float] = None,
        calorific_value: Optional[float] = None,
        calorific_value_lt: Optional[float] = None,
        calorific_value_lte: Optional[float] = None,
        calorific_value_gt: Optional[float] = None,
        calorific_value_gte: Optional[float] = None,
        other_constants: Optional[float] = None,
        other_constants_lt: Optional[float] = None,
        other_constants_lte: Optional[float] = None,
        other_constants_gt: Optional[float] = None,
        other_constants_gte: Optional[float] = None,
        ticket_type: Optional[Union[list[str], Series[str], str]] = None,
        ticket_type_symbol: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the RED III Compliance reference biofuel data.

        Parameters
        ----------
        id : Optional[int]
            The biofuel reference record identifier., by default None.
        id_gt, id_gte, id_lt, id_lte : Optional[int]
            Comparison filters for `id`, by default None.
        region : Optional[Union[list[str], Series[str], str]]
            The geographic region., by default None.
        transport_sector : Optional[Union[list[str], Series[str], str]]
            The transport sector classification., by default None.
        reference_biofuel : Optional[Union[list[str], Series[str], str]]
            The reference biofuel name., by default None.
        symbol_c_bate : Optional[Union[list[str], Series[str], str]]
            The C-Bate price symbol., by default None.
        symbol_description : Optional[Union[list[str], Series[str], str]]
            The symbol description., by default None.
        fx_rate_symbol : Optional[Union[list[str], Series[str], str]]
            The FX rate symbol., by default None.
        uom_factor : Optional[float]
            The unit of measure conversion factor., by default None.
        uom_factor_gt, uom_factor_gte, uom_factor_lt, uom_factor_lte : Optional[float]
            Comparison filters for `uom_factor`, by default None.
        calorific_value : Optional[float]
            The calorific value of the biofuel., by default None.
        calorific_value_gt, calorific_value_gte, calorific_value_lt, calorific_value_lte : Optional[float]
            Comparison filters for `calorific_value`, by default None.
        other_constants : Optional[float]
            Other calculation constants., by default None.
        other_constants_gt, other_constants_gte, other_constants_lt, other_constants_lte : Optional[float]
            Comparison filters for `other_constants`, by default None.
        ticket_type : Optional[Union[list[str], Series[str], str]]
            The compliance ticket type., by default None.
        ticket_type_symbol : Optional[Union[list[str], Series[str], str]]
            The compliance ticket type symbol., by default None.
        filter_exp : Optional[str]
            An additional API filter expression, by default None.
        page : int
            Page number to retrieve, by default 1.
        page_size : int
            Number of records per page, by default 5000.
        raw : bool
            Return the raw API response when True, by default False.
        paginate : bool
            Retrieve all available pages when True, by default False.

        Returns
        -------
        Union[DataFrame, Response]
            A pandas DataFrame or the raw API response.
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        if id_gt is not None:
            filter_params.append(f'id > "{id_gt}"')
        if id_gte is not None:
            filter_params.append(f'id >= "{id_gte}"')
        if id_lt is not None:
            filter_params.append(f'id < "{id_lt}"')
        if id_lte is not None:
            filter_params.append(f'id <= "{id_lte}"')
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("transportSector", transport_sector))
        filter_params.append(list_to_filter("referenceBiofuel", reference_biofuel))
        filter_params.append(list_to_filter("symbolCBate", symbol_c_bate))
        filter_params.append(list_to_filter("symbolDescription", symbol_description))
        filter_params.append(list_to_filter("fxRateSymbol", fx_rate_symbol))
        filter_params.append(list_to_filter("uomFactor", uom_factor))
        if uom_factor_gt is not None:
            filter_params.append(f'uomFactor > "{uom_factor_gt}"')
        if uom_factor_gte is not None:
            filter_params.append(f'uomFactor >= "{uom_factor_gte}"')
        if uom_factor_lt is not None:
            filter_params.append(f'uomFactor < "{uom_factor_lt}"')
        if uom_factor_lte is not None:
            filter_params.append(f'uomFactor <= "{uom_factor_lte}"')
        filter_params.append(list_to_filter("calorificValue", calorific_value))
        if calorific_value_gt is not None:
            filter_params.append(f'calorificValue > "{calorific_value_gt}"')
        if calorific_value_gte is not None:
            filter_params.append(f'calorificValue >= "{calorific_value_gte}"')
        if calorific_value_lt is not None:
            filter_params.append(f'calorificValue < "{calorific_value_lt}"')
        if calorific_value_lte is not None:
            filter_params.append(f'calorificValue <= "{calorific_value_lte}"')
        filter_params.append(list_to_filter("otherConstants", other_constants))
        if other_constants_gt is not None:
            filter_params.append(f'otherConstants > "{other_constants_gt}"')
        if other_constants_gte is not None:
            filter_params.append(f'otherConstants >= "{other_constants_gte}"')
        if other_constants_lt is not None:
            filter_params.append(f'otherConstants < "{other_constants_lt}"')
        if other_constants_lte is not None:
            filter_params.append(f'otherConstants <= "{other_constants_lte}"')
        filter_params.append(list_to_filter("ticketType", ticket_type))
        filter_params.append(list_to_filter("ticketTypeSymbol", ticket_type_symbol))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path=self._path_ref_biofuel,
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_obligation_data(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        transport_sector: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        obligation_percentage: Optional[float] = None,
        obligation_percentage_lt: Optional[float] = None,
        obligation_percentage_lte: Optional[float] = None,
        obligation_percentage_gt: Optional[float] = None,
        obligation_percentage_gte: Optional[float] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the RED III Compliance reference obligation data.

        Parameters
        ----------
        id : Optional[int]
            The obligation reference record identifier., by default None.
        id_gt, id_gte, id_lt, id_lte : Optional[int]
            Comparison filters for `id`, by default None.
        region : Optional[Union[list[str], Series[str], str]]
            The geographic region., by default None.
        transport_sector : Optional[Union[list[str], Series[str], str]]
            The transport sector classification., by default None.
        year : Optional[int]
            The obligation calendar year., by default None.
        year_gt, year_gte, year_lt, year_lte : Optional[int]
            Comparison filters for `year`, by default None.
        obligation_percentage : Optional[float]
            The renewable energy obligation percentage., by default None.
        obligation_percentage_gt, obligation_percentage_gte, obligation_percentage_lt, obligation_percentage_lte : Optional[float]
            Comparison filters for `obligation_percentage`, by default None.
        filter_exp : Optional[str]
            An additional API filter expression, by default None.
        page : int
            Page number to retrieve, by default 1.
        page_size : int
            Number of records per page, by default 5000.
        raw : bool
            Return the raw API response when True, by default False.
        paginate : bool
            Retrieve all available pages when True, by default False.

        Returns
        -------
        Union[DataFrame, Response]
            A pandas DataFrame or the raw API response.
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        if id_gt is not None:
            filter_params.append(f'id > "{id_gt}"')
        if id_gte is not None:
            filter_params.append(f'id >= "{id_gte}"')
        if id_lt is not None:
            filter_params.append(f'id < "{id_lt}"')
        if id_lte is not None:
            filter_params.append(f'id <= "{id_lte}"')
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("transportSector", transport_sector))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(
            list_to_filter("obligationPercentage", obligation_percentage)
        )
        if obligation_percentage_gt is not None:
            filter_params.append(f'obligationPercentage > "{obligation_percentage_gt}"')
        if obligation_percentage_gte is not None:
            filter_params.append(
                f'obligationPercentage >= "{obligation_percentage_gte}"'
            )
        if obligation_percentage_lt is not None:
            filter_params.append(f'obligationPercentage < "{obligation_percentage_lt}"')
        if obligation_percentage_lte is not None:
            filter_params.append(
                f'obligationPercentage <= "{obligation_percentage_lte}"'
            )

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path=self._path_ref_obligation,
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
