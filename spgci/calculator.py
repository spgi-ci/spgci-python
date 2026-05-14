# Copyright 2026 S&P Global Energy (previously S&P Global Commodity Insights)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations
from typing import Any, Dict, List, Optional, Union
from requests import Response
from spgci.api_client import get_data, post_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
import pandas as pd


class Calculator:
    _path_eu_compliance = "fuel-calculator/v1/eu-compliance/calculate-penalty-api"
    _path_on_demand_price = "fuel-calculator/v1/ondemand/calculate-price-api"
    _path_ref_commodities = "api/v1/calculators/on-demand/reference/commodities"
    _path_ref_line_spaces = "api/v1/calculators/on-demand/reference/line-spaces"
    _path_ref_pipelines = "api/v1/calculators/on-demand/reference/pipelines"
    _path_ref_shipping = "api/v1/calculators/alternate-fuel/reference/shipping"

    @staticmethod
    def _convert_to_df(resp: Response) -> DataFrame:
        j = resp.json()
        if isinstance(j, dict) and "results" in j:
            df = pd.json_normalize(j["results"])  # type: ignore
        elif isinstance(j, list):
            df = pd.json_normalize(j)
        else:
            df = pd.json_normalize([j])

        # Explode costBreakdown into separate rows and flatten each breakdown object.
        if "costBreakdown" in df.columns:
            df = df.explode("costBreakdown", ignore_index=True)
            breakdown_df = pd.json_normalize(
                df["costBreakdown"].apply(lambda x: x if isinstance(x, dict) else {})
            )
            df = pd.concat(
                [
                    df.drop(columns=["costBreakdown"]).reset_index(drop=True),
                    breakdown_df.reset_index(drop=True),
                ],
                axis=1,
            )

        return df

    def calculate_eu_compliance_penalty(
        self,
        origin_port: str,
        destination_port: str,
        fuel_blends: List[Dict[str, Any]],
        reporting_period: str,
        include_eu_ets_cost: bool = True,
        ghg_intensity_lng: Optional[float] = None,
        ghg_intensity_b30: Optional[float] = None,
        ghg_intensity_b24: Optional[float] = None,
        ghg_intensity_ucome: Optional[float] = None,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Calculate the EU FuelEU Maritime compliance penalty for a voyage.

        Parameters
        ----------
        origin_port : str
            Origin port of the voyage, e.g. ``"Rotterdam, Europe"``.
        destination_port : str
            Destination port of the voyage, e.g. ``"New York, Americas"``.
        fuel_blends : List[Dict[str, Any]]
            List of fuel blend objects. Each blend is a dict with a ``"fuels"``
            key containing a list of ``{"fuel": str, "consumption": float}`` dicts.

            Example::

                [
                    {
                        "fuels": [
                            {"fuel": "VLSFO", "consumption": 50},
                            {"fuel": "MGO",   "consumption": 50},
                        ]
                    }
                ]

        reporting_period : str
            The FuelEU reporting period, e.g. ``"2025-2029"``.
        include_eu_ets_cost : bool, optional
            Whether to include EU ETS cost in the calculation, by default ``True``.
        ghg_intensity_lng : Optional[float], optional
            Custom GHG intensity value for LNG (gCO2eq/MJ), by default ``None``.
        ghg_intensity_b30 : Optional[float], optional
            Custom GHG intensity value for B30 (gCO2eq/MJ), by default ``None``.
        ghg_intensity_b24 : Optional[float], optional
            Custom GHG intensity value for B24 (gCO2eq/MJ), by default ``None``.
        ghg_intensity_ucome : Optional[float], optional
            Custom GHG intensity value for UCOME (gCO2eq/MJ), by default ``None``.
        raw : bool, optional
            Return a ``requests.Response`` instead of a ``DataFrame``, by default ``False``.

        Returns
        -------
        Union[pd.DataFrame, Response]
            DataFrame
                DataFrame of the ``response.json()``
            Response
                Raw ``requests.Response`` object

        Examples
        --------
        **Simple**

        >>> ci.Calculator().calculate_eu_compliance_penalty(
        ...     origin_port="Rotterdam, Europe",
        ...     destination_port="New York, Americas",
        ...     fuel_blends=[
        ...         {"fuels": [{"fuel": "VLSFO", "consumption": 50}, {"fuel": "MGO", "consumption": 50}]}
        ...     ],
        ...     reporting_period="2025-2029",
        ... )

        **With custom GHG intensity values**

        >>> ci.Calculator().calculate_eu_compliance_penalty(
        ...     origin_port="Rotterdam, Europe",
        ...     destination_port="New York, Americas",
        ...     fuel_blends=[
        ...         {"fuels": [{"fuel": "VLSFO", "consumption": 50}, {"fuel": "MGO", "consumption": 50}]}
        ...     ],
        ...     reporting_period="2025-2029",
        ...     ghg_intensity_lng=76.08,
        ...     ghg_intensity_b30=69.138,
        ...     ghg_intensity_b24=73.6584,
        ...     ghg_intensity_ucome=16.4,
        ... )
        """
        body: Dict[str, Any] = {
            "originPort": origin_port,
            "destinationPort": destination_port,
            "fuelBlends": fuel_blends,
            "includeEUETSCost": include_eu_ets_cost,
            "reportingPeriod": reporting_period,
        }

        ghg: Dict[str, float] = {}
        if ghg_intensity_lng is not None:
            ghg["lng"] = ghg_intensity_lng
        if ghg_intensity_b30 is not None:
            ghg["b30"] = ghg_intensity_b30
        if ghg_intensity_b24 is not None:
            ghg["b24"] = ghg_intensity_b24
        if ghg_intensity_ucome is not None:
            ghg["ucome"] = ghg_intensity_ucome
        if ghg:
            body["ghgIntensityValues"] = ghg

        response = post_data(
            path=self._path_eu_compliance,
            body=body,
            df_fn=self._convert_to_df,
            raw=raw,
        )

        return response

    def calculate_ondemand_price(
        self,
        start_date: str,
        end_date: str,
        commodity: str,
        product_grade: str,
        origin_state: Optional[str] = None,
        origin_city: Optional[str] = None,
        delivery_state: Optional[str] = None,
        delivery_city: Optional[str] = None,
        rvp: Optional[float] = None,
        octane: Optional[float] = None,
        line_space_code: Optional[str] = None,
        pipeline_tariff: Optional[bool] = None,
        field: Optional[str] = None,
        filter_exp: Optional[str] = None,
        sort: Optional[str] = None,
        group_by: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Generate on-demand pricing for refined products.

        Parameters
        ----------
        start_date : str
            Start date in ``YYYY-MM-DD`` format.
        end_date : str
            End date in ``YYYY-MM-DD`` format.
        commodity : str
            Commodity name, e.g. ``"Gasoline"``.
        product_grade : str
            Product grade to calculate, e.g.
            ``"Gasoline Unl 87 USGC Prompt Pipeline"``.
        origin_state : Optional[str], optional
            Origin state, by default ``None``.
        origin_city : Optional[str], optional
            Origin city, by default ``None``.
        delivery_state : Optional[str], optional
            Delivery state, by default ``None``.
        delivery_city : Optional[str], optional
            Delivery city, by default ``None``.
        rvp : Optional[float], optional
            RVP adjustment value, by default ``None``.
        octane : Optional[float], optional
            Octane adjustment value, by default ``None``.
        line_space_code : Optional[str], optional
            Line space code, by default ``None``.
        pipeline_tariff : Optional[bool], optional
            Whether to include pipeline tariff, by default ``None``.
        field : Optional[str], optional
            Pass-thru ``field`` query parameter to choose response fields,
            by default ``None``.
        filter_exp : Optional[str], optional
            Pass-thru ``filter`` query parameter to filter response rows,
            by default ``None``.
        sort : Optional[str], optional
            Pass-thru ``sort`` query parameter, by default ``None``.
        group_by : Optional[str], optional
            Pass-thru ``groupBy`` query parameter, by default ``None``.
        page : int, optional
            Result page number (1-indexed), by default ``1``.
        page_size : int, optional
            Number of rows per page, by default ``1000``.
        raw : bool, optional
            Return a ``requests.Response`` instead of a ``DataFrame``, by default ``False``.

        Returns
        -------
        Union[pd.DataFrame, Response]
            DataFrame
                DataFrame of the ``response.json()``
            Response
                Raw ``requests.Response`` object

        Examples
        --------
        **Simple**

        >>> ci.Calculator().calculate_ondemand_price(
        ...     start_date="2026-02-15",
        ...     end_date="2026-02-17",
        ...     commodity="Gasoline",
        ...     product_grade="Gasoline Unl 87 USGC Prompt Pipeline",
        ... )

        **With Cost Factor Adjustments**

        >>> ci.Calculator().calculate_ondemand_price(
        ...     start_date="2026-02-15",
        ...     end_date="2026-02-17",
        ...     commodity="Gasoline",
        ...     product_grade="Gasoline Unl 87 USGC Prompt Pipeline",
        ...     origin_state="Texas",
        ...     origin_city="Houston",
        ...     delivery_state="New York",
        ...     delivery_city="New York City",
        ...     rvp=3.0,
        ...     octane=5.0,
        ...     line_space_code="Colonial pipeline gasoline Line 3",
        ...     pipeline_tariff=True,
        ... )
        """
        body: Dict[str, Any] = {
            "startDate": start_date,
            "endDate": end_date,
            "commodity": commodity,
            "productGrade": product_grade,
        }

        if origin_state is not None:
            body["originState"] = origin_state
        if origin_city is not None:
            body["originCity"] = origin_city
        if delivery_state is not None:
            body["deliveryState"] = delivery_state
        if delivery_city is not None:
            body["deliveryCity"] = delivery_city

        cost_factor_adjustments: Dict[str, Any] = {}
        if rvp is not None:
            cost_factor_adjustments["rvp"] = rvp
        if octane is not None:
            cost_factor_adjustments["octane"] = octane
        if line_space_code is not None:
            cost_factor_adjustments["lineSpaceCode"] = line_space_code
        if pipeline_tariff is not None:
            cost_factor_adjustments["pipelineTariff"] = pipeline_tariff
        if cost_factor_adjustments:
            body["costFactorAdjustments"] = cost_factor_adjustments

        query_params: List[str] = [f"page={page}", f"page_size={page_size}"]
        if field is not None:
            query_params.append(f"field={field}")
        if filter_exp is not None:
            query_params.append(f"filter={filter_exp}")
        if sort is not None:
            query_params.append(f"sort={sort}")
        if group_by is not None:
            query_params.append(f"groupBy={group_by}")

        path = f"{self._path_on_demand_price}?{'&'.join(query_params)}"

        response = post_data(
            path=path,
            body=body,
            df_fn=self._convert_to_df,
            raw=raw,
        )

        return response

    def get_reference_data_commodities(
        self,
        id: Optional[Union[list[int], Series[int], int]] = None,
        product_code: Optional[Union[list[str], Series[str], str]] = None,
        product: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_id: Optional[Union[list[int], Series[int], int]] = None,
        pipeline_name: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides access to energy commodity classifications and product hierarchies for market standardization and trading operations.

        Parameters
        ----------
        id : Optional[Union[list[int], Series[int], int]]
            Filter by commodity id, by default None
        product_code : Optional[Union[list[str], Series[str], str]]
            Filter by product code, by default None
        product : Optional[Union[list[str], Series[str], str]]
            Filter by product name, by default None
        commodity : Optional[Union[list[str], Series[str], str]]
            Filter by commodity name, by default None
        pipeline_id : Optional[Union[list[int], Series[int], int]]
            Filter by pipeline id, by default None
        pipeline_name : Optional[Union[list[str], Series[str], str]]
            Filter by pipeline name, by default None
        filter_exp : Optional[str], optional
            Pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            Page number, by default 1
        page_size : int, optional
            Number of results per page, by default 1000
        raw : bool, optional
            Return a ``requests.Response`` instead of a ``DataFrame``, by default False
        paginate : bool, optional
            Whether to auto-paginate the response, by default False

        Returns
        -------
        Union[pd.DataFrame, Response]

        Examples
        --------
        >>> ci.Calculator().get_reference_commodities(pipeline_name="Colonial")
        """
        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        filter_params.append(list_to_filter("productCode", product_code))
        filter_params.append(list_to_filter("product", product))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("pipelineName", pipeline_name))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path="api/v1/calculators/on-demand/reference/commodities",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_reference_data_line_spaces(
        self,
        code: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_id: Optional[Union[list[int], Series[int], int]] = None,
        pipeline_name: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides comprehensive pipeline network information including route mappings and geographic coverage for energy transportation analysis.

        Parameters
        ----------
        code : Optional[Union[list[str], Series[str], str]]
            Filter by line space code, by default None
        pipeline_id : Optional[Union[list[int], Series[int], int]]
            Filter by pipeline id, by default None
        pipeline_name : Optional[Union[list[str], Series[str], str]]
            Filter by pipeline name, by default None
        filter_exp : Optional[str], optional
            Pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            Page number, by default 1
        page_size : int, optional
            Number of results per page, by default 1000
        raw : bool, optional
            Return a ``requests.Response`` instead of a ``DataFrame``, by default False
        paginate : bool, optional
            Whether to auto-paginate the response, by default False

        Returns
        -------
        Union[pd.DataFrame, Response]

        Examples
        --------
        >>> ci.Calculator().get_reference_line_spaces(pipeline_name="Colonial")
        """
        filter_params: List[str] = []
        filter_params.append(list_to_filter("code", code))
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("pipelineName", pipeline_name))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path="api/v1/calculators/on-demand/reference/line-spaces",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_reference_data_pipelines(
        self,
        pipeline_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_name: Optional[Union[list[str], Series[str], str]] = None,
        status: Optional[Union[list[str], Series[str], str]] = None,
        origin_city: Optional[Union[list[str], Series[str], str]] = None,
        origin_state: Optional[Union[list[str], Series[str], str]] = None,
        origin_county: Optional[Union[list[str], Series[str], str]] = None,
        destination_city: Optional[Union[list[str], Series[str], str]] = None,
        destination_state: Optional[Union[list[str], Series[str], str]] = None,
        destination_county: Optional[Union[list[str], Series[str], str]] = None,
        tariff: Optional[float] = None,
        tariff_gte: Optional[float] = None,
        tariff_gt: Optional[float] = None,
        tariff_lte: Optional[float] = None,
        tariff_lt: Optional[float] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides comprehensive pipeline network information including route mappings and geographic coverage for energy transportation analysis.

        Parameters
        ----------
        pipeline_id : Optional[Union[list[str], Series[str], str]]
            Filter by pipeline id, by default None
        pipeline_name : Optional[Union[list[str], Series[str], str]]
            Filter by pipeline name, by default None
        status : Optional[Union[list[str], Series[str], str]]
            Filter by status, by default None
        origin_city : Optional[Union[list[str], Series[str], str]]
            Filter by origin city, by default None
        origin_state : Optional[Union[list[str], Series[str], str]]
            Filter by origin state, by default None
        origin_county : Optional[Union[list[str], Series[str], str]]
            Filter by origin county, by default None
        destination_city : Optional[Union[list[str], Series[str], str]]
            Filter by destination city, by default None
        destination_state : Optional[Union[list[str], Series[str], str]]
            Filter by destination state, by default None
        destination_county : Optional[Union[list[str], Series[str], str]]
            Filter by destination county, by default None
        tariff : Optional[float]
            Filter by exact tariff value, by default None
        tariff_gte : Optional[float]
            Filter by ``tariff >= x``, by default None
        tariff_gt : Optional[float]
            Filter by ``tariff > x``, by default None
        tariff_lte : Optional[float]
            Filter by ``tariff <= x``, by default None
        tariff_lt : Optional[float]
            Filter by ``tariff < x``, by default None
        filter_exp : Optional[str], optional
            Pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            Page number, by default 1
        page_size : int, optional
            Number of results per page, by default 1000
        raw : bool, optional
            Return a ``requests.Response`` instead of a ``DataFrame``, by default False
        paginate : bool, optional
            Whether to auto-paginate the response, by default False

        Returns
        -------
        Union[pd.DataFrame, Response]

        Examples
        --------
        >>> ci.Calculator().get_reference_pipeline_tariffs(pipeline_name="Colonial", origin_state="TX")
        """
        filter_params: List[str] = []
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("pipelineName", pipeline_name))
        filter_params.append(list_to_filter("status", status))
        filter_params.append(list_to_filter("originCity", origin_city))
        filter_params.append(list_to_filter("originState", origin_state))
        filter_params.append(list_to_filter("originCounty", origin_county))
        filter_params.append(list_to_filter("destinationCity", destination_city))
        filter_params.append(list_to_filter("destinationState", destination_state))
        filter_params.append(list_to_filter("destinationCounty", destination_county))
        filter_params.append(list_to_filter("tariff", tariff))

        if tariff_gt is not None:
            filter_params.append(f'tariff > "{tariff_gt}"')
        if tariff_gte is not None:
            filter_params.append(f'tariff >= "{tariff_gte}"')
        if tariff_lt is not None:
            filter_params.append(f'tariff < "{tariff_lt}"')
        if tariff_lte is not None:
            filter_params.append(f'tariff <= "{tariff_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path="api/v1/calculators/on-demand/reference/pipelines",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_reference_data_shipping(
        self,
        region: Optional[Union[list[str], Series[str], str]] = None,
        route_desc: Optional[Union[list[str], Series[str], str]] = None,
        load_port: Optional[Union[list[str], Series[str], str]] = None,
        disch_port: Optional[Union[list[str], Series[str], str]] = None,
        fb_port: Optional[Union[list[str], Series[str], str]] = None,
        alt: Optional[Union[list[str], Series[str], str]] = None,
        fb_port_sym: Optional[Union[list[str], Series[str], str]] = None,
        b_port_dist: Optional[Union[list[float], Series[float], float]] = None,
        l_port_dist: Optional[Union[list[float], Series[float], float]] = None,
        fb_dist: Optional[Union[list[float], Series[float], float]] = None,
        bunker_cons: Optional[Union[list[str], Series[str], str]] = None,
        eu_ets_cost: Optional[Union[list[float], Series[float], float]] = None,
        load_eu: Optional[Union[list[bool], Series[bool], bool]] = None,
        disch_eu: Optional[Union[list[bool], Series[bool], bool]] = None,
        load_eca: Optional[Union[list[bool], Series[bool], bool]] = None,
        disch_eca: Optional[Union[list[bool], Series[bool], bool]] = None,
        created_at: Optional[Union[list[str], Series[str], str]] = None,
        updated_at: Optional[Union[list[str], Series[str], str]] = None,
        vlsfo_rate_sym: Optional[Union[list[str], Series[str], str]] = None,
        mgo_rate_sym: Optional[Union[list[str], Series[str], str]] = None,
        ucome_rate_sym: Optional[Union[list[str], Series[str], str]] = None,
        lng_rate_sym: Optional[Union[list[str], Series[str], str]] = None,
        b24_rate_sym: Optional[Union[list[str], Series[str], str]] = None,
        b30_rate_sym: Optional[Union[list[str], Series[str], str]] = None,
        load_port_region: Optional[Union[list[str], Series[str], str]] = None,
        disch_port_region: Optional[Union[list[str], Series[str], str]] = None,
        total_distance: Optional[Union[list[float], Series[float], float]] = None,
        laden_days: Optional[Union[list[float], Series[float], float]] = None,
        laden_fuel_mt: Optional[Union[list[float], Series[float], float]] = None,
        ballast_days: Optional[Union[list[float], Series[float], float]] = None,
        ballast_fuel_mt: Optional[Union[list[float], Series[float], float]] = None,
        loading_fuel_mt: Optional[Union[list[float], Series[float], float]] = None,
        discharging_fuel_mt: Optional[Union[list[float], Series[float], float]] = None,
        waiting_load_fuel_mt: Optional[Union[list[float], Series[float], float]] = None,
        waiting_discharge_fuel_mt: Optional[Union[list[float], Series[float], float]] = None,
        total_fuel: Optional[Union[list[float], Series[float], float]] = None,
        total_voyage_days: Optional[Union[list[float], Series[float], float]] = None,
        filter_exp: Optional[str] = None,
        field: Optional[str] = None,
        sort: Optional[str] = None,
        group_by: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides comprehensive shipping route and port reference information used by alternate-fuel maritime calculators.

        Parameters
        ----------
        filter_exp : Optional[str], optional
            Pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        field : Optional[str], optional
            Pass-thru ``field`` query param to choose response fields or aggregations, by default None
        sort : Optional[str], optional
            Pass-thru ``sort`` query param, e.g. ``"region:desc,loadPort:asc"``, by default None
        group_by : Optional[str], optional
            Pass-thru ``groupBy`` query param, by default None
        page : int, optional
            Page number, by default 1
        page_size : int, optional
            Number of results per page, by default 5000
        raw : bool, optional
            Return a ``requests.Response`` instead of a ``DataFrame``, by default False
        paginate : bool, optional
            Whether to auto-paginate the response, by default False

        Returns
        -------
        Union[pd.DataFrame, Response]

        Examples
        --------
        >>> ci.Calculator().get_reference_data_shipping(region=["APAC", "Europe"], load_eu=True)
        """
        filter_params: List[str] = []
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("routeDesc", route_desc))
        filter_params.append(list_to_filter("loadPort", load_port))
        filter_params.append(list_to_filter("dischPort", disch_port))
        filter_params.append(list_to_filter("fbPort", fb_port))
        filter_params.append(list_to_filter("alt", alt))
        filter_params.append(list_to_filter("fbPortSym", fb_port_sym))
        filter_params.append(list_to_filter("bPortDist", b_port_dist))
        filter_params.append(list_to_filter("lPortDist", l_port_dist))
        filter_params.append(list_to_filter("fbDist", fb_dist))
        filter_params.append(list_to_filter("bunkerCons", bunker_cons))
        filter_params.append(list_to_filter("euEtsCost", eu_ets_cost))
        filter_params.append(list_to_filter("loadEu", load_eu))
        filter_params.append(list_to_filter("dischEu", disch_eu))
        filter_params.append(list_to_filter("loadEca", load_eca))
        filter_params.append(list_to_filter("dischEca", disch_eca))
        filter_params.append(list_to_filter("createdAt", created_at))
        filter_params.append(list_to_filter("updatedAt", updated_at))
        filter_params.append(list_to_filter("vlsfoRateSym", vlsfo_rate_sym))
        filter_params.append(list_to_filter("mgoRateSym", mgo_rate_sym))
        filter_params.append(list_to_filter("ucomeRateSym", ucome_rate_sym))
        filter_params.append(list_to_filter("lngRateSym", lng_rate_sym))
        filter_params.append(list_to_filter("b24RateSym", b24_rate_sym))
        filter_params.append(list_to_filter("b30RateSym", b30_rate_sym))
        filter_params.append(list_to_filter("loadPortRegion", load_port_region))
        filter_params.append(list_to_filter("dischPortRegion", disch_port_region))
        filter_params.append(list_to_filter("totalDistance", total_distance))
        filter_params.append(list_to_filter("ladenDays", laden_days))
        filter_params.append(list_to_filter("ladenFuelMt", laden_fuel_mt))
        filter_params.append(list_to_filter("ballastDays", ballast_days))
        filter_params.append(list_to_filter("ballastFuelMt", ballast_fuel_mt))
        filter_params.append(list_to_filter("loadingFuelMt", loading_fuel_mt))
        filter_params.append(list_to_filter("dischargingFuelMt", discharging_fuel_mt))
        filter_params.append(list_to_filter("waitingLoadFuelMt", waiting_load_fuel_mt))
        filter_params.append(
            list_to_filter("waitingDischargeFuelMt", waiting_discharge_fuel_mt)
        )
        filter_params.append(list_to_filter("totalFuel", total_fuel))
        filter_params.append(list_to_filter("totalVoyageDays", total_voyage_days))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params: Dict[str, Any] = {
            "page": page,
            "pageSize": page_size,
            "filter": filter_exp,
        }

        if field is not None:
            params["field"] = field
        if sort is not None:
            params["sort"] = sort
        if group_by is not None:
            params["groupBy"] = group_by

        response = get_data(
            path=self._path_ref_shipping,
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
