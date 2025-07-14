# Copyright 2025 S&P Global Commodity Insights

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
from requests import Response
from pandas import DataFrame, Series
from typing import Union, Optional, List
from spgci.api_client import get_data, Paginator
from spgci.utilities import list_to_filter
from enum import Enum


class WorldOilSupply:
    """
    World Oil Supply.

    Includes
    --------
    ``RefTypes`` to use with the ``get_reference_data`` method.
    ``get_reference_data()`` to get the list of countries, states, grades, etc.. for the World Oil Supply dataset.
    ``get_ownership()`` to get production ownership by company.
    ``get_cost_of_supplies()`` to get cost of supplies in $/b. Includes Brent equivalent, Brent differential, wellhead and annual production in MBD.
    ``get_production()`` to get the latest historical and forecast oil supply data for individual countries.
    ``get_production_archive()`` to get previously issued historical and forecast oil supply data for individual countries.

    """

    _path = "wos/v2/"

    class RefTypes(Enum):
        """World Oil Supply Reference Data Types"""

        SupplyTypes = "supply-types"
        SupplySubTypes = "supply-sub-types"
        States = "states"
        ScenarioTerms = "scenario-terms"
        ScenarioList = "scenario-list"
        ReserveTypes = "reserve-types"
        ReserveLocations = "reserve-locations"
        Regions = "regions"
        ReferenceYears = "reference-years"
        ProductionTypes = "production-types"
        Padds = "padds"
        OwnershipTypes = "ownership-types"
        OPECCountries = "opec-countries"
        ProductionElements = "production-elements"
        CostProductionElements = "cost-production-elements"
        Grades = "crude-grades"
        GradeElements = "grade-elements"
        Countries = "countries"
        CostOfSuppliesType = "cost-of-supplies-type"
        CompanyTypes = "company-types"
        Companies = "companies"

    @staticmethod
    def _paginate(resp: Response) -> Paginator:
        j = resp.json()
        count = j["metadata"]["count"]
        size = j["metadata"]["pageSize"]

        remainder = count % size
        quotient = count // size
        total_pages = quotient + (1 if remainder > 0 else 0)

        if total_pages <= 1:
            return Paginator(False, "page", 1)

        return Paginator(True, "page", total_pages=total_pages)

    def get_cost_of_supplies(
        self,
        *,
        ref_year: Optional[Union[int, list[int], "Series[int]"]] = None,
        region: Optional[Union[str, list[str], "Series[str]"]] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        reserve_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        supply_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        supply_subtype: Optional[Union[str, list[str], "Series[str]"]] = None,
        reserve_location: Optional[Union[str, list[str], "Series[str]"]] = None,
        cost_of_supplies_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        production_element: Optional[Union[str, list[str], "Series[str]"]] = None,
        cost_production_element: Optional[Union[str, list[str], "Series[str]"]] = None,
        units: Optional[Union[str, list[str], "Series[str]"]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Fetch cost of supplies in $/b. Includes Brent equivalent, Brent differential, wellhead and annual production in MBD.

        Parameters
        ----------
        ref_year : Optional[Union[int, list[int], Series[int]]], optional
            filter by ref_year, by default None
        region : Optional[Union[str, list[str], Series[str]]], optional
            filter by region, by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by country, by default None
        reserve_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by reserveTypeName, by default None
        supply_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by supplyTypeName, by default None
        supply_subtype : Optional[Union[str, list[str], Series[str]]], optional
            filter by supplySubTypeName, by default None
        reserve_location : Optional[Union[str, list[str], Series[str]]], optional
            filter by reserveLocationName, by default None
        cost_of_supplies_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by costOfSuppliesTypeName, by default None
        production_element : Optional[Union[str, list[str], Series[str]]], optional
            filter by productionElementName, by default None
        cost_production_element : Optional[Union[str, list[str], Series[str]]], optional
            filter by costOfProductionElementName, by default None
        units : Optional[Union[str, list[str], Series[str]]], optional
            filter by units, by default None
        filter_exp : Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame``, by default False

        Returns
        -------
        Union[pd.DataFrame, Response]
              DataFrame
                  DataFrame of the ``response.json()``
              Response
                  Raw ``requests.Response`` object
        Examples
        --------
        **Get Production Costs**
        >>> ci.WorldOilSupply().get_cost_of_supplies(cost_of_supplies_type="Production")

        **Get Crude supplies costs in Albania**
        >>> ci.WorldOilSupply().get_cost_of_supplies(supply_type="Crude", country="Albania")

        **Compose Ref Data with Costs**
        >>> sup_sub_types = ci.WorldOilSupply().get_reference_data(type=ci.WorldOilSupply.RefTypes.SupplySubTypes)
        >>> ci.WorldOilSupply().get_cost_of_supplies(supply_subtype=sup_sub_types['supplySubTypeName'][:2])
        """
        endpoint_path = "cost-of-supplies"

        filter_param: List[str] = []

        filter_param.append(list_to_filter("referenceYear", ref_year))
        filter_param.append(list_to_filter("regionName", region))
        filter_param.append(list_to_filter("countryName", country))
        filter_param.append(list_to_filter("reserveTypeName", reserve_type))
        filter_param.append(list_to_filter("supplyTypeName", supply_type))
        filter_param.append(list_to_filter("supplySubTypeName", supply_subtype))
        filter_param.append(list_to_filter("reserveLocationName", reserve_location))
        filter_param.append(list_to_filter("productionElementName", production_element))
        filter_param.append(list_to_filter("units", units))
        filter_param.append(
            list_to_filter("costOfSuppliesTypeName", cost_of_supplies_type)
        )
        filter_param.append(
            list_to_filter("costProductionElementName", cost_production_element)
        )

        filter_param = [fp for fp in filter_param if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_param)
        elif len(filter_param) > 0:
            filter_exp = " AND ".join(filter_param) + " AND (" + filter_exp + ")"

        params = {
            "pageSize": page_size,
            "filter": filter_exp,
            "page": page,
        }
        return get_data(
            path=f"{self._path}{endpoint_path}",
            params=params,
            paginate=paginate,
            paginate_fn=self._paginate,
            raw=raw,
        )

    def get_ownership(
        self,
        *,
        year: Optional[Union[int, list[int], "Series[int]"]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        region: Optional[Union[str, list[str], "Series[str]"]] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        state: Optional[Union[str, list[str], "Series[str]"]] = None,
        padd: Optional[Union[str, list[str], "Series[str]"]] = None,
        reserve_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        supply_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        supply_subtype: Optional[Union[str, list[str], "Series[str]"]] = None,
        reserve_location: Optional[Union[str, list[str], "Series[str]"]] = None,
        production_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        production_element: Optional[Union[str, list[str], "Series[str]"]] = None,
        company: Optional[Union[str, list[str], "Series[str]"]] = None,
        company_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        ownership_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        units: Optional[Union[str, list[str], "Series[str]"]] = None,
        vintage: Optional[int] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Fetch production ownership by company.

        Parameters
        ----------
        year : Optional[Union[int, list[int], Series[int]]], optional
            filter by year, by default None
        year_gt : Optional[int], optional
            filter by ``year > x``, by default None
        year_gte : Optional[int], optional
            filter by ``year >= x``, by default None
        year_lt : Optional[int], optional
            filter by ``year < x``, by default None
        year_lte : Optional[int], optional
            filter by ``year <= x``, by default None
        region : Optional[Union[str, list[str], Series[str]]], optional
            filter by region, by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by country, by default None
        state : Optional[Union[str, list[str], Series[str]]], optional
            filter by state, by default None
        padd : Optional[Union[str, list[str], Series[str]]], optional
            filter by padd, by default None
        reserve_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by reserveTypeName, by default None
        supply_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by supplyTypeName, by default None
        supply_subtype : Optional[Union[str, list[str], Series[str]]], optional
            filter by supplySubTypeName, by default None
        reserve_location : Optional[Union[str, list[str], Series[str]]], optional
            filter by reserveLocationName, by default None
        production_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by productionTypeName, by default None
        production_element : Optional[Union[str, list[str], Series[str]]], optional
            filter by productionElementName, by default None
        company : Optional[Union[str, list[str], Series[str]]], optional
            filter by companyName, by default None
        company_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by companyTypeName, by default None
        ownership_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by ownershipTypeName, by default None
        units : Optional[Union[str, list[str], Series[str]]], optional
            filter by units, by default None
        vintage : Optional[int], optional
            filter by vintage, by default None
        filter_exp : Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame``, by default False

        Returns
        -------
        Union[pd.DataFrame, Response]
              DataFrame
                  DataFrame of the ``response.json()``
              Response
                  Raw ``requests.Response`` object
        Examples
        --------
        **Get Ownership by Company**
        >>> ci.WorldOilSupply().get_ownership(company="ADNOC")

        **Get Ownership by state companies in the Middle East, Africa, in 2040**
        >>> ci.WorldOilSupply().get_ownership(company_type="State", year=2040, region=["Middle East", "Africa"])

        **Compose Ref Data with Ownership**
        >>> res_locations = ci.WorldOilSupply().get_reference_data(type=ci.WorldOilSupply.RefTypes.ReserveLocations)
        >>> ci.WorldOilSupply().get_ownership(reserve_location=res_locations['reserveLocationName'][:2], country='Norway', year=2031)
        """
        endpoint_path = "ownership"

        filter_param: List[str] = []

        filter_param.append(list_to_filter("year", year))
        filter_param.append(list_to_filter("regionName", region))
        filter_param.append(list_to_filter("countryName", country))
        filter_param.append(list_to_filter("stateName", state))
        filter_param.append(list_to_filter("paddName", padd))
        filter_param.append(list_to_filter("reserveTypeName", reserve_type))
        filter_param.append(list_to_filter("supplyTypeName", supply_type))
        filter_param.append(list_to_filter("supplySubTypeName", supply_subtype))
        filter_param.append(list_to_filter("reserveLocationName", reserve_location))
        filter_param.append(list_to_filter("productionTypeName", production_type))
        filter_param.append(list_to_filter("productionElementName", production_element))
        filter_param.append(list_to_filter("vintage", vintage))
        filter_param.append(list_to_filter("units", units))
        filter_param.append(list_to_filter("companyName", company))
        filter_param.append(list_to_filter("companyTypeName", company_type))
        filter_param.append(list_to_filter("ownershipTypeName", ownership_type))

        if year_gt:
            filter_param.append(f"year > {year_gt}")
        if year_gte:
            filter_param.append(f"year >= {year_gte}")
        if year_lt:
            filter_param.append(f"year < {year_lt}")
        if year_lte:
            filter_param.append(f"year <= {year_lte}")

        filter_param = [fp for fp in filter_param if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_param)
        elif len(filter_param) > 0:
            filter_exp = " AND ".join(filter_param) + " AND (" + filter_exp + ")"

        params = {
            "pageSize": page_size,
            "filter": filter_exp,
            "page": page,
        }
        return get_data(
            path=f"{self._path}{endpoint_path}",
            params=params,
            paginate=paginate,
            paginate_fn=self._paginate,
            raw=raw,
        )

    def get_production(
        self,
        *,
        scenario_term_id: int = 2,
        year: Optional[Union[int, list[int], "Series[int]"]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        month: Optional[Union[int, list[int], "Series[int]"]] = None,
        region: Optional[Union[str, list[str], "Series[str]"]] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        state: Optional[Union[str, list[str], "Series[str]"]] = None,
        padd: Optional[Union[str, list[str], "Series[str]"]] = None,
        reserve_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        supply_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        supply_subtype: Optional[Union[str, list[str], "Series[str]"]] = None,
        reserve_location: Optional[Union[str, list[str], "Series[str]"]] = None,
        production_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        grade: Optional[Union[str, list[str], "Series[str]"]] = None,
        production_element: Optional[Union[str, list[str], "Series[str]"]] = None,
        grade_element: Optional[Union[str, list[str], "Series[str]"]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Latest historical and forecast oil supply data for countries.

        Parameters
        ----------
        scenario_term_id : int, optional
            long-term (2) or short-term forecasts (1), by default 2
        year : Optional[Union[str, list[int], Series[int]]], optional
            filter by year, by default None
        year_gt : Optional[int], optional
            filter by ``year > x``, by default None
        year_gte : Optional[int], optional
            filter by ``year >= x``, by default None
        year_lt : Optional[int], optional
            filter by ``year < x``, by default None
        year_lte : Optional[int], optional
            filter by ``year <= x``, by default None
        month : Optional[Union[str, list[int], Series[int]]], optional
            filter by month, by default None
        region : Optional[Union[str, list[str], Series[str]]], optional
            filter by region, by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by country, by default None
        state : Optional[Union[str, list[str], Series[str]]], optional
            filter by state, by default None
        padd : Optional[Union[str, list[str], Series[str]]], optional
            filter by padd, by default None
        reserve_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by reserveTypeName, by default None
        supply_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by supplyTypeName, by default None
        supply_subtype : Optional[Union[str, list[str], Series[str]]], optional
            filter by supplySubTypeName, by default None
        reserve_location : Optional[Union[str, list[str], Series[str]]], optional
            filter by reserveLocation, by default None
        production_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by productionTypeName, by default None
        grade : Optional[Union[str, list[str], Series[str]]], optional
            filter by gradeName, by default None
        production_element : Optional[Union[str, list[str], Series[str]]], optional
            filter by productionElementName, by default None
        grade_element : Optional[Union[str, list[str], Series[str]]], optional
            filter by gradeElementName, by default None
        filter_exp : Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame``, by default False

        Returns
        -------
        Union[pd.DataFrame, Response]
              DataFrame
                  DataFrame of the ``response.json()``
              Response
                  Raw ``requests.Response`` object
        Examples
        --------
        **Get Production by Country**
        >>> ci.WorldOilSupply().get_production(scenario_term_id=1, country="Qatar", year=2050)

        **Get Production of Crude in Middle East, Africa, in 2024**
        >>> ci.WorldOilSupply().get_production(year=2024, region=["Middle East", "Africa"], production_element="Crude")

        **Compose Ref Data with Production**
        >>> opec = ci.WorldOilSupply().get_reference_data(type=ci.WorldOilSupply().RefTypes.OPECCountries)
        >>> filt = opec['effectiveLeaveDate'].isna()
        >>> opec = opec[filt]
        >>> ci.WorldOilSupply().get_production(country=opec['countryName'][3:], production_type="Production", year=2024)
        """
        endpoint_path = "production"

        filter_param: List[str] = []

        filter_param.append(list_to_filter("year", year))
        filter_param.append(list_to_filter("month", month))
        filter_param.append(list_to_filter("regionName", region))
        filter_param.append(list_to_filter("countryName", country))
        filter_param.append(list_to_filter("stateName", state))
        filter_param.append(list_to_filter("paddName", padd))
        filter_param.append(list_to_filter("reserveTypeName", reserve_type))
        filter_param.append(list_to_filter("supplyTypeName", supply_type))
        filter_param.append(list_to_filter("supplySubTypeName", supply_subtype))
        filter_param.append(list_to_filter("reserveLocationName", reserve_location))
        filter_param.append(list_to_filter("productionTypeName", production_type))
        filter_param.append(list_to_filter("gradeName", grade))
        filter_param.append(list_to_filter("productionElementName", production_element))
        filter_param.append(list_to_filter("gradeElementName", grade_element))

        if year_gt:
            filter_param.append(f"year > {year_gt}")
        if year_gte:
            filter_param.append(f"year >= {year_gte}")
        if year_lt:
            filter_param.append(f"year < {year_lt}")
        if year_lte:
            filter_param.append(f"year <= {year_lte}")

        filter_param = [fp for fp in filter_param if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_param)
        elif len(filter_param) > 0:
            filter_exp = " AND ".join(filter_param) + " AND (" + filter_exp + ")"

        params = {
            "scenarioTermId": scenario_term_id,
            "pageSize": page_size,
            "filter": filter_exp,
            "page": page,
        }
        return get_data(
            path=f"{self._path}{endpoint_path}",
            params=params,
            paginate=paginate,
            paginate_fn=self._paginate,
            raw=raw,
        )

    def get_production_archive(
        self,
        *,
        scenario_id: int,
        scenario_term_id: int = 1,
        year: Optional[Union[int, list[int], "Series[int]"]] = None,
        month: Optional[Union[int, list[int], "Series[int]"]] = None,
        region: Optional[Union[str, list[str], "Series[str]"]] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        state: Optional[Union[str, list[str], "Series[str]"]] = None,
        padd: Optional[Union[str, list[str], "Series[str]"]] = None,
        reserve_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        supply_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        supply_subtype: Optional[Union[str, list[str], "Series[str]"]] = None,
        reserve_location: Optional[Union[str, list[str], "Series[str]"]] = None,
        production_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        grade: Optional[Union[str, list[str], "Series[str]"]] = None,
        production_element: Optional[Union[str, list[str], "Series[str]"]] = None,
        grade_element: Optional[Union[str, list[str], "Series[str]"]] = None,
        units: Optional[Union[str, list[str], "Series[str]"]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Previously issued historical and forecast oil supply data.

        Parameters
        ----------
        scenario_id : int
            filter by scenarioId
        scenario_term_id : int, optional
            long-term (2) or short-term forecasts (1), by default 1
        year : Optional[Union[int, list[int], Series[int]]], optional
            filter by year, by default None
        month : Optional[Union[int, list[int], Series[int]]], optional
            filter by month, by default None
        region : Optional[Union[str, list[str], Series[str]]], optional
            filter by region, by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by country, by default None
        state : Optional[Union[str, list[str], Series[str]]], optional
            filter by state, by default None
        padd : Optional[Union[str, list[str], Series[str]]], optional
            filter by PADD, by default None
        reserve_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by ReserveTypeName, by default None
        supply_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by SupplyTypeName, by default None
        supply_subtype : Optional[Union[str, list[str], Series[str]]], optional
            filter by SupplySubTypeName, by default None
        reserve_location : Optional[Union[str, list[str], Series[str]]], optional
            filter by ReserveLocationName, by default None
        production_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by ProductionTypeName, by default None
        grade : Optional[Union[str, list[str], Series[str]]], optional
            filter by GradeName, by default None
        production_element : Optional[Union[str, list[str], Series[str]]], optional
            filter by ProductionElementName, by default None
        grade_element : Optional[Union[str, list[str], Series[str]]], optional
            filter by GradeElementName, by default None
        units : Optional[Union[str, list[str], Series[str]]], optional
            filter by units, by default None
        filter_exp : Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame``, by default False

        Returns
        -------
        Union[pd.DataFrame, Response]
            DataFrame
                DataFrame of the ``response.json()``
            Response
                Raw ``requests.Response`` object
        Examples
        --------
        **Get Production Archive by Country**
        >>> ci.WorldOilSupply().get_production_archive(scenario_id=2243, scenario_term_id=2, country="Qatar", year=2021)

        **Get List of Scenario IDs**
        >>> ci.WorldOilSupply().get_reference_data(type=ci.WorldOilSupply.RefTypes.ScenarioList)

        **Compose Ref Data with Production Archive**
        >>> scenarios = ci.WorldOilSupply().get_reference_data(type=ci.WorldOilSupply.RefTypes.ScenarioList)
        >>> s_id = scenarios['scenarioId'][1]
        >>> ci.WorldOilSupply().get_production_archive(scenario_id=s_id, production_type="Capacity", country="Albania")
        """
        endpoint_path = "production-archive"

        filter_param: List[str] = []

        filter_param.append(list_to_filter("year", year))
        filter_param.append(list_to_filter("month", month))
        filter_param.append(list_to_filter("units", units))
        filter_param.append(list_to_filter("regionName", region))
        filter_param.append(list_to_filter("countryName", country))
        filter_param.append(list_to_filter("stateName", state))
        filter_param.append(list_to_filter("paddName", padd))
        filter_param.append(list_to_filter("reserveTypeName", reserve_type))
        filter_param.append(list_to_filter("supplyTypeName", supply_type))
        filter_param.append(list_to_filter("supplySubTypeName", supply_subtype))
        filter_param.append(list_to_filter("reserveLocationName", reserve_location))
        filter_param.append(list_to_filter("productionTypeName", production_type))
        filter_param.append(list_to_filter("gradeName", grade))
        filter_param.append(list_to_filter("productionElementName", production_element))
        filter_param.append(list_to_filter("gradeElementName", grade_element))

        filter_param = [fp for fp in filter_param if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_param)
        elif len(filter_param) > 0:
            filter_exp = " AND ".join(filter_param) + " AND (" + filter_exp + ")"

        params = {
            "scenarioTermId": scenario_term_id,
            "scenarioId": scenario_id,
            "pageSize": page_size,
            "filter": filter_exp,
            "page": page,
        }
        return get_data(
            path=f"{self._path}{endpoint_path}",
            params=params,
            paginate=paginate,
            paginate_fn=self._paginate,
            raw=raw,
        )

    def get_reference_data(
        self, type: RefTypes, raw: bool = False
    ) -> Union[Response, DataFrame]:
        """
        Fetch reference data for the World Oil Supply dataset.

        Parameters
        ----------
        type : RefTypes
            filter by type
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame``, by default False

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
        >>> ci.WorldOilSupply().get_reference_data(type=ci.WorldOilSupply.RefTypes.Grades)
        """
        endpoint_path = type.value

        params = {"pageSize": 1000}

        return get_data(
            path=f"{self._path}{endpoint_path}",
            params=params,
            paginate=True,
            paginate_fn=self._paginate,
            raw=raw,
        )
