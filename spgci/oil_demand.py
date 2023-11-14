# Copyright 2023 S&P Global Commodity Insights

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


class GlobalOilDemand:
    """
    Global Oil Demand.

    Includes
    --------
    ``RefTypes`` to use with the ``get_reference_data`` method.
    ``get_reference_data()`` to get the list of countries, states, grades, etc.. for the World Oil Supply dataset.
    ``get_ownership()`` to get production ownership by company.
    ``get_cost_of_supplies()`` to get cost of supplies in $/b. Includes Brent equivalent, Brent differential, wellhead and annual production in MBD.
    ``get_production()`` to get the latest historical and forecast oil supply data for individual countries.
    ``get_production_archive()`` to get previously issued historical and forecast oil supply data for individual countries.

    """

    _path = "mgodf/v1/"

    class RefTypes(Enum):
        """Global Oil Demand Reference Data Types"""

        Countries = "countries"
        DataQuality = "dataquality"
        Products = "products"
        Regions = "regions"
        Scenarios = "scenario-list"

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

    def get_demand(
        self,
        *,
        year: Optional[Union[int, list[int], "Series[int]"]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        month: Optional[Union[int, list[int]]] = None,
        product: Optional[Union[str, list[str], "Series[str]"]] = None,
        region: Optional[Union[str, list[str], "Series[str]"]] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        units: Optional[Union[str, list[str], "Series[str]"]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Fetch latest Global Oil Demand forecast by month.

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
        product : Optional[Union[str, list[str], Series[str]]], optional
            filter by productName, by default None
        region : Optional[Union[str, list[str], Series[str]]], optional
            filter by regionName, by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by countryName, by default None
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
        **Simple Example**
        >>> ci.GlobalOilDemand().get_demand(year=2022)

        **Using Lists**
        >>> ci.GlobalOilDemand().get_demand(country="Cambodia", product=["Naphtha", "Ethane"])

        **Compose with Ref Data**
        >>> products = ci.GlobalOilDemand().get_reference_data(type=ci.GlobalOilDemand.RefTypes.Products)
        >>> ci.GlobalOilDemand().get_demand(product=products["productName"][:2], year=2023)
        """
        endpoint_path = "demand"

        filter_param: List[str] = []

        filter_param.append(list_to_filter("year", year))
        filter_param.append(list_to_filter("productName", product))
        filter_param.append(list_to_filter("month", month))
        filter_param.append(list_to_filter("regionName", region))
        filter_param.append(list_to_filter("countryName", country))
        filter_param.append(list_to_filter("units", units))

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

    def get_demand_archive(
        self,
        scenario_id: int,
        *,
        year: Optional[Union[int, list[int], "Series[int]"]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        month: Optional[Union[int, list[int]]] = None,
        product: Optional[Union[str, list[str], "Series[str]"]] = None,
        region: Optional[Union[str, list[str], "Series[str]"]] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        units: Optional[Union[str, list[str], "Series[str]"]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Fetch archive Global Oil Demand forecast by month.

        Parameters
        ----------
        scenario_id : int
            filter by scenarioId
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
        product : Optional[Union[str, list[str], Series[str]]], optional
            filter by productName, by default None
        region : Optional[Union[str, list[str], Series[str]]], optional
            filter by regionName, by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by countryName, by default None
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
        **Simple Example**
        >>> ci.GlobalOilDemand().get_demand_archive(scenario_id=150, country="Norway")

        **Using Lists**
        >>> ci.GlobalOilDemand().get_demand_archive(scenario_id=150, country="Cambodia", product=["Naphtha", "Ethane"])

        **Compose with Ref Data**
        >>> scenarios = ci.GlobalOilDemand().get_reference_data(type=ci.GlobalOilDemand.RefTypes.Scenarios)
        >>> ci.GlobalOilDemand().get_demand_archive(scenario_id=scenarios['scenarioId'].iloc[-1], year=2022)
        """
        endpoint_path = "demand-archive"

        filter_param: List[str] = []

        filter_param.append(list_to_filter("year", year))
        filter_param.append(list_to_filter("productName", product))
        filter_param.append(list_to_filter("month", month))
        filter_param.append(list_to_filter("regionName", region))
        filter_param.append(list_to_filter("countryName", country))
        filter_param.append(list_to_filter("units", units))

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
        >>> ci.GlobalOilDemand().get_reference_data(type=ci.GlobalOilDeman.RefTypes.Products)
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
