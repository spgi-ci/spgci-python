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
from typing import Union, Optional
from requests import Response
from pandas import Series, DataFrame, to_datetime, json_normalize  # type: ignore
from spgci.api_client import get_data, Paginator
from spgci.utilities import odata_list_to_filter, list_to_filter
from urllib.parse import urlencode, quote, parse_qs, urlparse
from datetime import date
from enum import Enum


class WorldRefineryData:
    """
    World Refinery Data.

    Includes
    --------
    ``RefTypes`` to use with the ``get_reference_data`` method.
    ``get_reference_data()`` to get the list of countries, owners, refineries, etc.. used in the World Refinery Database.
    ``get_capacity()`` to get refinery capacity changes.
    ``get_runs()`` to get annual crude runs for individual refineries.
    ``get_yields()`` to get the yields for individual refineries.
    ``get_outages()`` to get refinery outages.
    ``get_outage_alerts()`` to get intra-day refinery outages.
    ``get_ownership()`` to get refinery ownership.
    ``get_margins()`` to get margin data.
    """

    _endpoint = "odata/refinery-data/v2.2/"

    class RefTypes(Enum):
        """World Refinery Database Reference Data Type"""

        CapacityStatuses = "CapacityStatuses"
        Cities = "cities"
        Configurations = "configurations"
        Countries = "countries"
        MarginTypes = "marginTypes"
        Operators = "operators"
        OutageUnits = "outageUnits"
        Owners = "owners"
        PADDs = "PADDs"
        # ProcessUnitCategories = "ProcessUnitCategories"
        ProcessUnits = "ProcessUnits"
        Refineries = "Refineries"
        Regions = "Regions"
        States = "States"

    @staticmethod
    def _padd_to_df(resp: Response) -> DataFrame:
        j = resp.json()
        df = json_normalize(
            j["value"],
            meta=["Id", "Name"],
            record_path="States",
            record_prefix="State.",
        )

        return df

    @staticmethod
    def _ts_to_df(resp: Response) -> DataFrame:
        j = resp.json()
        df = json_normalize(j, record_path="value")

        # drop odata artifacts
        drop = df.columns[df.columns.str.contains("@odata")]
        df = df[df.columns.drop(drop)]  # type: ignore

        if len(df) > 0 and "start_date" in df.columns:
            df["start_date"] = to_datetime(df["start_date"], utc=True)  # type: ignore
        if len(df) > 0 and "end_date" in df.columns:
            df["end_date"] = to_datetime(df["end_date"], utc=True)  # type: ignore

        return df

    @staticmethod
    def _to_df(resp: Response) -> DataFrame:
        j = resp.json()
        df = json_normalize(j["value"])

        # duplicates due to $expand=*
        dupes = ["RefineryId", "OwnerId", "CapacityStatusId", "MarginTypeId"]
        df = df[df.columns.drop(dupes, "ignore")]  # type: ignore

        if len(df) > 0 and "ModifiedDate" in df.columns:
            df["ModifiedDate"] = to_datetime(df["ModifiedDate"], utc=True)  # type: ignore
        if len(df) > 0 and "Date" in df.columns:
            df["Date"] = to_datetime(df["Date"], utc=True)  # type: ignore

        return df

    @staticmethod
    def _paginate(resp: Response) -> Paginator:
        j = resp.json()

        count: int = j["@odata.count"]
        page_size = parse_qs(urlparse(resp.url).query)["pageSize"]

        if not page_size:
            return Paginator(False, "$skip", 0)

        remainder = count % int(page_size[0])
        quotient = count // int(page_size[0])
        total_pages = quotient + (1 if remainder > 0 else 0)

        if total_pages <= 1:
            return Paginator(False, "$skip", total_pages)

        return Paginator(True, "$skip", total_pages, pg_type="odata")

    def get_capacity(
        self,
        *,
        year: Optional[Union[int, list[int], "Series[int]"]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        quarter: Optional[Union[int, list[int], "Series[int]"]] = None,
        refinery_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        owner: Optional[Union[str, list[str], "Series[str]"]] = None,
        capacity_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        capacity_status_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        process_unit: Optional[Union[str, list[str], "Series[str]"]] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        region: Optional[Union[str, list[str], "Series[str]"]] = None,
        filter_exp: Optional[str] = None,
        skip: int = 0,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Fetch historical refinery capacity changes.

        Parameters
        ----------
        year : Optional[Union[int, list[int], Series[int]]], optional
            filter by ``year = x``, by default None
        year_gt : Optional[int], optional
            filter by ``year > x``, by default None
        year_gte : Optional[int], optional
            filter by ``year >= x``, by default None
        year_lt : Optional[int], optional
            filter by ``year < x``, by default None
        year_lte : Optional[int], optional
            filter by ``year <= x``, by default None
        quarter : Optional[Union[int, list[int], Series[int]]], optional
            filter by quarter, by default None
        refinery_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by refineryId, by default None
        owner : Optional[Union[str, list[str], Series[str]]], optional
            filter by Owner/Name, by default None
        capacity_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by capacityId, by default None
        capacity_status_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by capacityStatusId, by default None
        process_unit : Optional[Union[str, list[str], Series[str]]], optional
            filter by ProcessUnit/Name, by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by Refinery/Country/Name, by default None
        region : Optional[Union[str, list[str], Series[str]]], optional
            filter by Refinery/Region/Name, by default None
        filter_exp : Optional[str], optional
            pass-thru ``$filter`` query param to use a handcrafted filter expression, by default None
        skip : int, optional
            pass-thru ``$skip`` query param to skip a certain number of records, by default 0
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame, by default False

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
        >>> ci.WorldRefineryData().get_capacity(year=2008)

        **Using Lists**
        >>> ci.WorldRefineryData().get_capacity(year_gte=2022, process_unit=["Atmos Distillation", "Dist Hydrocracking"])

        **Using Series**
        >>> df = ci.WorldRefineryData().get_reference_data(type=ci.WorldRefineryData.RefTypes.Refineries)
        >>> ci.WorldRefineryData().get_capacity(year=2020, refinery_id=df["Id"][:3])
        """
        endpoint = "capacity"

        filter_params: list[str] = []

        filter_params.append(odata_list_to_filter("Year", year))
        filter_params.append(odata_list_to_filter("Quarter", quarter))
        filter_params.append(odata_list_to_filter("RefineryId", refinery_id))
        filter_params.append(odata_list_to_filter("Owner/Name", owner))
        filter_params.append(odata_list_to_filter("CapacityId", capacity_id))
        filter_params.append(
            odata_list_to_filter("CapacityStatusId", capacity_status_id)
        )
        filter_params.append(odata_list_to_filter("ProcessUnit/Name", process_unit))
        filter_params.append(odata_list_to_filter("Refinery/Country/Name", country))
        filter_params.append(odata_list_to_filter("Refinery/Region/Name", region))

        if year_gt:
            filter_params.append(f"year gt {year_gt}")
        if year_gte:
            filter_params.append(f"year ge {year_gte}")
        if year_lt:
            filter_params.append(f"year lt {year_lt}")
        if year_lte:
            filter_params.append(f"year le {year_lte}")

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {
            "$skip": skip,
            "pageSize": page_size,
            "$count": "true",
            "$expand": "*",
        }

        if filter_exp:
            params["$filter"] = filter_exp

        # odata endpoint will not allow '+' character in URL seemingly
        qs = urlencode(params, quote_via=quote)

        return get_data(
            path=f"{self._endpoint}{endpoint}?{qs}",
            params={},
            # params=params,
            paginate=paginate,
            raw=raw,
            df_fn=self._to_df,
            paginate_fn=self._paginate,
        )

    def get_runs(
        self,
        *,
        year: Optional[Union[int, list[int], "Series[int]"]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        quarter: Optional[Union[int, list[int], "Series[int]"]] = None,
        refinery_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        owner: Optional[Union[str, list[str], "Series[int]"]] = None,
        capacity_status_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        process_unit: Optional[Union[str, list[str], "Series[str]"]] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        region: Optional[Union[str, list[str], "Series[str]"]] = None,
        filter_exp: Optional[str] = None,
        skip: int = 0,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Fetch annual crude runs by refinery.

        Parameters
        ----------
        year : Optional[Union[int, list[int], Series[int]]], optional
            filter by ``year = x``, by default None
        year_gt : Optional[int], optional
            filter by ``year > x``, by default None
        year_gte : Optional[int], optional
            filter by ``year >= x``, by default None
        year_lt : Optional[int], optional
            filter by ``year < x``, by default None
        year_lte : Optional[int], optional
            filter by ``year <= x``, by default None
        quarter : Optional[Union[int, list[int], Series[int]]], optional
            filter by quarter, by default None
        refinery_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by refineryId, by default None
        owner : Optional[Union[str, list[str], Series[str]]], optional
            filter by Owner/Name, by default None
        capacity_status_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by CapacityStatusId, by default None
        process_unit : Optional[Union[str, list[str], Series[str]]], optional
            filter by ProcessUnit/Name, by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by Refinery/Country/Name, by default None
        region : Optional[Union[str, list[str], Series[str]]], optional
            filter by Refinery/Region/Name, by default None
        filter_exp : Optional[str], optional
            pass-thru ``$filter`` query param to use a handcrafted filter expression, by default None
        skip : int, optional
            pass-thru ``$skip`` query param to skip a certain number of records, by default 0
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame, by default False

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
        >>> ci.WorldRefineryData().get_runs(owner="BP")

        **Using Lists**
        >>> ci.WorldRefineryData().get_runs(year_gte=2020, refinery_id=[18, 357])

        **Using Series**
        >>> df = ci.WorldRefineryData().get_reference_data(type=ci.WorldRefineryData.RefTypes.ProcessUnits)
        >>> ci.WorldRefineryData().get_runs(year=2020, process_unit=df["Name"][:-10], refineryId=1)
        """
        endpoint = "runs"

        filter_params: list[str] = []

        filter_params.append(odata_list_to_filter("Year", year))
        filter_params.append(odata_list_to_filter("Quarter", quarter))
        filter_params.append(odata_list_to_filter("RefineryId", refinery_id))
        filter_params.append(odata_list_to_filter("Owner/Name", owner))
        filter_params.append(
            odata_list_to_filter("CapacityStatusId", capacity_status_id)
        )
        filter_params.append(odata_list_to_filter("ProcessUnit/Name", process_unit))
        filter_params.append(odata_list_to_filter("Refinery/Country/Name", country))
        filter_params.append(odata_list_to_filter("Refinery/Region/Name", region))

        if year_gt:
            filter_params.append(f"year gt {year_gt}")
        if year_gte:
            filter_params.append(f"year ge {year_gte}")
        if year_lt:
            filter_params.append(f"year lt {year_lt}")
        if year_lte:
            filter_params.append(f"year le {year_lte}")

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {
            "$skip": skip,
            "pageSize": page_size,
            "$count": "true",
            "$expand": "*",
        }

        if filter_exp:
            params["$filter"] = filter_exp

        # odata endpoint will not allow '+' character in URL seemingly
        qs = urlencode(params, quote_via=quote)

        return get_data(
            path=f"{self._endpoint}{endpoint}?{qs}",
            params={},
            # params=params,
            paginate=paginate,
            raw=raw,
            df_fn=self._to_df,
            paginate_fn=self._paginate,
        )

    def get_yields(
        self,
        *,
        year: Optional[Union[int, list[int], "Series[int]"]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        quarter: Optional[Union[int, list[int], "Series[int]"]] = None,
        refinery_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        owner: Optional[Union[str, list[str], "Series[str]"]] = None,
        capacity_status_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        process_unit: Optional[Union[str, list[str], "Series[str]"]] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        region: Optional[Union[str, list[str], "Series[str]"]] = None,
        filter_exp: Optional[str] = None,
        skip: int = 0,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Fetch refinery yields by year.

        Parameters
        ----------
        year : Optional[Union[int, list[int], Series[int]]], optional
            filter by ``year = x``, by default None
        year_gt : Optional[int], optional
            filter by ``year > x``, by default None
        year_gte : Optional[int], optional
            filter by ``year >= x``, by default None
        year_lt : Optional[int], optional
            filter by ``year < x``, by default None
        year_lte : Optional[int], optional
            filter by ``year <= x``, by default None
        quarter : Optional[Union[int, list[int], Series[int]]], optional
            filter by quarter, by default None
        refinery_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by refineryId, by default None
        owner : Optional[Union[str, list[str], Series[str]]], optional
            filter by Owner/Name, by default None
        capacity_status_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by CapacityStatusId, by default None
        process_unit : Optional[Union[str, list[str], Series[str]]], optional
            filter by ProcessUnit/Name, by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by Refinery/Country/Name, by default None
        region : Optional[Union[str, list[str], Series[str]]], optional
            filter by Refinery/Region/Name, by default None
        filter_exp : Optional[str], optional
            pass-thru ``$filter`` query param to use a handcrafted filter expression, by default None
        skip : int, optional
            pass-thru ``$skip`` query param to skip a certain number of records, by default 0
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame, by default False

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
        >>> ci.WorldRefineryData().get_yields(refineryId=1)

        **Using Lists**
        >>> ci.WorldRefineryData().get_yields(year=2020, owner=["BP", "S N Repal"])

        **Using Series**
        >>> df = ci.WorldRefineryData().get_reference_data(type=ci.WorldRefineryData.RefTypes.ProcessUnits)
        >>> ci.WorldRefineryData().get_yields(year=2020, process_unit=df["Name"][:-10], refineryId=1)
        """
        endpoint = "yields"

        filter_params: list[str] = []

        filter_params.append(odata_list_to_filter("Year", year))
        filter_params.append(odata_list_to_filter("Quarter", quarter))
        filter_params.append(odata_list_to_filter("RefineryId", refinery_id))
        filter_params.append(odata_list_to_filter("Owner/Name", owner))
        filter_params.append(
            odata_list_to_filter("CapacityStatusId", capacity_status_id)
        )
        filter_params.append(odata_list_to_filter("ProcessUnit/Name", process_unit))
        filter_params.append(odata_list_to_filter("Refinery/Country/Name", country))
        filter_params.append(odata_list_to_filter("Refinery/Region/Name", region))

        if year_gt:
            filter_params.append(f"year gt {year_gt}")
        if year_gte:
            filter_params.append(f"year ge {year_gte}")
        if year_lt:
            filter_params.append(f"year lt {year_lt}")
        if year_lte:
            filter_params.append(f"year le {year_lte}")

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {
            "$skip": skip,
            "pageSize": page_size,
            "$count": "true",
            "$expand": "*",
        }

        if filter_exp:
            params["$filter"] = filter_exp

        # odata endpoint will not allow '+' character in URL seemingly
        qs = urlencode(params, quote_via=quote)

        return get_data(
            path=f"{self._endpoint}{endpoint}?{qs}",
            params={},
            # params=params,
            paginate=paginate,
            raw=raw,
            df_fn=self._to_df,
            paginate_fn=self._paginate,
        )

    @staticmethod
    def _outage_alert_pg_fn(resp: Response) -> Paginator:
        j = resp.json()

        count: int = j["metadata"]["count"]
        page_size: int = j["metadata"]["pageSize"]

        if not page_size:
            return Paginator(False, "page", 0)

        remainder = count % int(page_size)
        quotient = count // int(page_size)
        total_pages = quotient + (1 if remainder > 0 else 0)

        if total_pages <= 1:
            return Paginator(False, "page", total_pages)

        return Paginator(True, "page", total_pages)

    @staticmethod
    def _outage_alerts_to_df(resp: Response) -> DataFrame:
        j = resp.json()

        df = json_normalize(
            j["results"],
            meta=[
                "outageId",
                "refineryId",
                "operatorname",
                "countryName",
                "cityName",
                "latitude",
                "longitude",
                "createdDate",
            ],
            record_path="alerts",
        )
        df["createdDate"] = to_datetime(df["createdDate"], utc=True)
        df["modifiedDate"] = to_datetime(df["modifiedDate"], utc=True)
        df["startDate"] = to_datetime(df["startDate"])
        df["endDate"] = to_datetime(df["endDate"])
        return df

    def get_outage_alerts(
        self,
        *,
        outage_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        created_date: Optional[Union[date, list[date], "Series[date]"]] = None,
        created_date_gt: Optional[date] = None,
        created_date_gte: Optional[date] = None,
        created_date_lt: Optional[date] = None,
        created_date_lte: Optional[date] = None,
        refinery_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        operator: Optional[Union[str, list[str], "Series[str]"]] = None,
        planning_status: Optional[Union[str, list[str], "Series[str]"]] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        process_unit: Optional[Union[str, list[str], "Series[str]"]] = None,
        outage_vol: Optional[Union[float, list[float], "Series[float]"]] = None,
        outage_vol_gt: Optional[float] = None,
        outage_vol_gte: Optional[float] = None,
        outage_vol_lt: Optional[float] = None,
        outage_vol_lte: Optional[float] = None,
        # modified_date: Optional[Union[date, list[date], "Series[date]"]] = None,
        # modified_date_gt: Optional[date] = None,
        # modified_date_gte: Optional[date] = None,
        # modified_date_lt: Optional[date] = None,
        # modified_date_lte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Fetch intra-day refinery outages.

        Parameters
        ----------
        outage_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by outageId, by default None
        created_date : Optional[Union[date, list[date], Series[date]]], optional
            _description_, by default None
        created_date_gt : Optional[date], optional
            _description_, by default None
        created_date_gte : Optional[date], optional
            _description_, by default None
        created_date_lt : Optional[date], optional
            _description_, by default None
        created_date_lte : Optional[date], optional
            _description_, by default None
        refinery_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by refineryId, by default None
        operator : Optional[Union[str, list[str], Series[str]]], optional
            filter by operatorname, by default None
        planning_status : Optional[Union[str, list[str], Series[str]]], optional
            filter by planningStatus, by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by countryName, by default None
        process_unit : Optional[Union[str, list[str], Series[str]]], optional
            filter by processUnitName, by default None
        outage_vol : Optional[Union[float, list[float], Series[float]]], optional
            filter by ``OutageVol_MBD = x`` , by default None
        outage_vol_gt : Optional[float], optional
            filter by ``OutageVol_MBD > x`` , by default None
        outage_vol_gte : Optional[float], optional
            filter by ``OutageVol_MBD >= x`` , by default None
        outage_vol_lt : Optional[float], optional
            filter by ``OutageVol_MBD < x`` , by default None
        outage_vol_lte : Optional[float], optional
            filter by ``OutageVol_MBD <= x`` , by default None
        filter_exp : Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to skip a certain number of records, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame, by default False

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
        >>> ci.WorldRefineryData().get_outage_alerts(refineryId=245)

        **Using Lists**
        >>> ci.WorldRefineryData().get_outage_alerts(process_unit=["Coker", "CDU"])
        """
        endpoint = "outage-alerts"

        filter_params: list[str] = []

        filter_params.append(list_to_filter("outageId", outage_id))
        filter_params.append(list_to_filter("refineryId", refinery_id))
        filter_params.append(list_to_filter("operatorname", operator))
        filter_params.append(list_to_filter("countryName", country))
        filter_params.append(list_to_filter("planningStatus", planning_status))
        filter_params.append(list_to_filter("processUnitName", process_unit))
        filter_params.append(list_to_filter("outageVol_MBD", outage_vol))
        # filter_params.append(list_to_filter("modifiedDate", modified_date))
        filter_params.append(list_to_filter("createdDate", created_date))

        if created_date_gt:
            filter_params.append(f'createdDate > "{created_date_gt}"')
        if created_date_gte:
            filter_params.append(f'createdDate >= "{created_date_gte}"')
        if created_date_lt:
            filter_params.append(f'createdDate < "{created_date_lt}"')
        if created_date_lte:
            filter_params.append(f'createdDate <= "{created_date_lte}"')

        # if modified_date_gt:
        #     filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        # if modified_date_gte:
        #     filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        # if modified_date_lt:
        #     filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        # if modified_date_lte:
        #     filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        if outage_vol_gt:
            filter_params.append(f"OutageVol_MBD > {outage_vol_gt}")
        if outage_vol_gte:
            filter_params.append(f"OutageVol_MBD >= {outage_vol_gte}")
        if outage_vol_lt:
            filter_params.append(f"OutageVol_MBD < {outage_vol_lt}")
        if outage_vol_lte:
            filter_params.append(f"OutageVol_MBD <= {outage_vol_lte}")

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params: dict[str, Union[str, int]] = {
            "page": page,
            "pageSize": page_size,
        }

        if filter_exp:
            params["filter"] = filter_exp

        return get_data(
            path=f"refinery-data/v1.2/{endpoint}",
            params=params,
            paginate=paginate,
            raw=raw,
            df_fn=self._outage_alerts_to_df,
            paginate_fn=self._outage_alert_pg_fn,
        )

    def get_outages(
        self,
        *,
        year: Optional[Union[int, list[int], "Series[int]"]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        date: Optional[Union[date, list[date], "Series[date]"]] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        quarter: Optional[Union[int, list[int], "Series[int]"]] = None,
        refinery_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        owner: Optional[Union[str, list[str], "Series[str]"]] = None,
        process_unit: Optional[Union[str, list[str], "Series[str]"]] = None,
        planning_status: Optional[Union[str, list[str], "Series[str]"]] = None,
        outage_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        outage_vol: Optional[Union[float, list[float], "Series[float]"]] = None,
        outage_vol_gt: Optional[float] = None,
        outage_vol_gte: Optional[float] = None,
        outage_vol_lt: Optional[float] = None,
        outage_vol_lte: Optional[float] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        region: Optional[Union[str, list[str], "Series[str]"]] = None,
        filter_exp: Optional[str] = None,
        skip: int = 0,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Fetch refinery outages.

        Parameters
        ----------
        year : Optional[Union[int, list[int], Series[int]]], optional
            filter by ``year = x`` , by default None
        year_gt : Optional[int], optional
            filter by ``year > x`` , by default None
        year_gte : Optional[int], optional
            filter by ``year >= x`` , by default None
        year_lt : Optional[int], optional
            filter by ``year < x`` , by default None
        year_lte : Optional[int], optional
            filter by ``year <= x`` , by default None
        date : Optional[Union[date, list[date], Series[date]]], optional
            filter by ``date = x`` , by default None
        date_gt : Optional[date], optional
            filter by ``date > x`` , by default None
        date_gte : Optional[date], optional
            filter by ``date >= x`` , by default None
        date_lt : Optional[date], optional
            filter by ``date < x`` , by default None
        date_lte : Optional[date], optional
            filter by ``date <= x`` , by default None
        quarter : Optional[Union[int, list[int], Series[int]]], optional
            filter by quarter, by default None
        refinery_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by refineryId, by default None
        owner : Optional[Union[str, list[str], Series[str]]], optional
            filter by Owner/Name, by default None
        process_unit : Optional[Union[str, list[str], Series[str]]], optional
            filter by ProcessUnit/Name, by default None
        planning_status : Optional[Union[str, list[str], Series[str]]], optional
            filter by PlanningStatus, by default None
        outage_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by outageId, by default None
        outage_vol : Optional[Union[float, list[float], Series[float]]], optional
            filter by ``OutageVol_MBD = x`` , by default None
        outage_vol_gt : Optional[float], optional
            filter by ``OutageVol_MBD > x`` , by default None
        outage_vol_gte : Optional[float], optional
            filter by ``OutageVol_MBD >= x`` , by default None
        outage_vol_lt : Optional[float], optional
            filter by ``OutageVol_MBD < x`` , by default None
        outage_vol_lte : Optional[float], optional
            filter by ``OutageVol_MBD <= x`` , by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by Refinery/Country/Name, by default None
        region : Optional[Union[str, list[str], Series[str]]], optional
            filter by Refinery/Region/Name, by default None
        filter_exp : Optional[str], optional
            pass-thru ``$filter`` query param to use a handcrafted filter expression, by default None
        skip : int, optional
            pass-thru ``$skip`` query param to skip a certain number of records, by default 0
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame, by default False

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
        >>> ci.WorldRefineryData().get_outages(year=2020, quarter=3)

        **Using Lists**
        >>> ci.WorldRefineryData().get_outages(year=2020, planning_status="Unplanned", process_unit=["Coker", "CDU"])

        **Using Series**
        >>> df = ci.WorldRefineryData().get_reference_data(type=ci.WorldRefineryData.RefTypes.Refineries)
        >>> df = df[df['Country.Name'] == "Mexico"]
        >>> ci.WorldRefineryData().get_outages(outage_vol_gte=200, refinery_id=df['Id'])
        """
        endpoint = "outages"

        filter_params: list[str] = []

        filter_params.append(odata_list_to_filter("Year", year))
        filter_params.append(odata_list_to_filter("Date", date))
        filter_params.append(odata_list_to_filter("OutageVol_MBD", outage_vol))
        filter_params.append(odata_list_to_filter("Quarter", quarter))
        filter_params.append(odata_list_to_filter("RefineryId", refinery_id))
        filter_params.append(odata_list_to_filter("Owner/Name", owner))
        filter_params.append(odata_list_to_filter("ProcessUnit/Name", process_unit))
        filter_params.append(odata_list_to_filter("PlanningStatus", planning_status))
        filter_params.append(odata_list_to_filter("OutageId", outage_id))
        filter_params.append(odata_list_to_filter("Refinery/Country/Name", country))
        filter_params.append(odata_list_to_filter("Refinery/Region/Name", region))

        if year_gt:
            filter_params.append(f"year gt {year_gt}")
        if year_gte:
            filter_params.append(f"year ge {year_gte}")
        if year_lt:
            filter_params.append(f"year lt {year_lt}")
        if year_lte:
            filter_params.append(f"year le {year_lte}")

        if date_gt:
            filter_params.append(f"Date gt {date_gt}")
        if date_gte:
            filter_params.append(f"Date ge {date_gte}")
        if date_lt:
            filter_params.append(f"Date lt {date_lt}")
        if date_lte:
            filter_params.append(f"Date le {date_lte}")

        if outage_vol_gt:
            filter_params.append(f"OutageVol_MBD gt {outage_vol_gt}")
        if outage_vol_gte:
            filter_params.append(f"OutageVol_MBD ge {outage_vol_gte}")
        if outage_vol_lt:
            filter_params.append(f"OutageVol_MBD lt {outage_vol_lt}")
        if outage_vol_lte:
            filter_params.append(f"OutageVol_MBD le {outage_vol_lte}")

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {
            "$skip": skip,
            "pageSize": page_size,
            "$count": "true",
            "$expand": "*",
        }

        if filter_exp:
            params["$filter"] = filter_exp

        # odata endpoint will not allow '+' character in URL seemingly
        qs = urlencode(params, quote_via=quote)

        return get_data(
            path=f"{self._endpoint}{endpoint}?{qs}",
            params={},
            # params=params,
            paginate=paginate,
            raw=raw,
            df_fn=self._to_df,
            paginate_fn=self._paginate,
        )

    def get_ownership(
        self,
        *,
        year: Optional[Union[int, list[int], "Series[int]"]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        quarter: Optional[Union[int, list[int], "Series[int]"]] = None,
        refinery_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        owner: Optional[Union[str, list[str], "Series[str]"]] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        region: Optional[Union[str, list[str], "Series[str]"]] = None,
        filter_exp: Optional[str] = None,
        skip: int = 0,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Fetch refinery ownership

        Parameters
        ----------
        year : Optional[Union[int, list[int], Series[int]]], optional
            filter by ``year = x`` , by default None
        year_gt : Optional[int], optional
            filter by ``year > x`` , by default None
        year_gte : Optional[int], optional
            filter by ``year >= x`` , by default None
        year_lt : Optional[int], optional
            filter by ``year < x`` , by default None
        year_lte : Optional[int], optional
            filter by ``year <= x`` , by default None
        quarter : Optional[Union[int, list[int], Series[int]]], optional
            filter by quarter, by default None
        refinery_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by refineryId, by default None
        owner : Optional[Union[str, list[str], Series[str]]], optional
            filter by Owner/Name, by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by Refinery/Country/Name, by default None
        region : Optional[Union[str, list[str], Series[str]]], optional
            filter by Refinery/Region/Name, by default None
        filter_exp : Optional[str], optional
            pass-thru ``$filter`` query param to use a handcrafted filter expression, by default None
        skip : int, optional
            pass-thru ``$skip`` query param to skip a certain number of records, by default 0
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame, by default False

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
        >>> ci.WorldRefineryData().get_ownership(owner="BP")

        **Using Lists**
        >>> ci.WorldRefineryData().get_ownership(year_gte=2017, quarter=[3, 4])

        **Using Series**
        >>> df = ci.WorldRefineryData().get_reference_data(type=ci.WorldRefineryData.RefTypes.Owners)
        >>> df = df[df['Name'].str.contains("Shell")]
        >>> ci.WorldRefineryData().get_ownership(owner=df['Name'])
        """
        endpoint = "ownership"

        filter_params: list[str] = []

        filter_params.append(odata_list_to_filter("Year", year))
        filter_params.append(odata_list_to_filter("Quarter", quarter))
        filter_params.append(odata_list_to_filter("RefineryId", refinery_id))
        filter_params.append(odata_list_to_filter("Owner/Name", owner))
        filter_params.append(odata_list_to_filter("Refinery/Country/Name", country))
        filter_params.append(odata_list_to_filter("Refinery/Region/Name", region))

        if year_gt:
            filter_params.append(f"year gt {year_gt}")
        if year_gte:
            filter_params.append(f"year ge {year_gte}")
        if year_lt:
            filter_params.append(f"year lt {year_lt}")
        if year_lte:
            filter_params.append(f"year le {year_lte}")

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {
            "$skip": skip,
            "pageSize": page_size,
            "$count": "true",
            "$expand": "*",
        }

        if filter_exp:
            params["$filter"] = filter_exp

        # odata endpoint will not allow '+' character in URL seemingly
        qs = urlencode(params, quote_via=quote)

        return get_data(
            path=f"{self._endpoint}{endpoint}?{qs}",
            params={},
            # params=params,
            paginate=paginate,
            raw=raw,
            df_fn=self._to_df,
            paginate_fn=self._paginate,
        )

    def get_margins(
        self,
        *,
        date: Optional[Union[date, list[date], "Series[date]"]] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        margin_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        filter_exp: Optional[str] = None,
        skip: int = 0,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Fetch refinery margins

        Parameters
        ----------
        date : Optional[Union[date, list[date], Series[date]]], optional
            filter by ``date = x`` , by default None
        date_gt : Optional[date], optional
            filter by ``date > x`` , by default None
        date_gte : Optional[date], optional
            filter by ``date >= x`` , by default None
        date_lt : Optional[date], optional
            filter by ``date < x`` , by default None
        date_lte : Optional[date], optional
            filter by ``date <= x`` , by default None
        margin_type : Optional[Union[str, list[str], Series[str]]], optional
            filter by MarginType/Name, by default None
        filter_exp : Optional[str], optional
            pass-thru ``$filter`` query param to use a handcrafted filter expression, by default None
        skip : int, optional
            pass-thru ``$skip`` query param to skip a certain number of records, by default 0
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame, by default False

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
        >>> ci.WorldRefineryData().get_margins(date=date(2023, 2, 17))

        **Using Lists**
        >>> ci.WorldRefineryData().get_margins(date_gte=date(2023, 1, 1), margin_type=['Dated Brent NWE Cracking', 'Dubai Singapore Cracking'])

        **Using Series**
        >>> df = ci.WorldRefineryData().get_reference_data(type=ci.WorldRefineryData.RefTypes.MarginTypes)
        >>> df = df[df['Name'].str.contains("Brent")]
        >>> ci.WorldRefineryData().get_ownership(date_gte=date(2023, 1, 1), margin_type=df['Name'])
        """
        endpoint = "margins"

        filter_params: list[str] = []

        filter_params.append(odata_list_to_filter("Date", date))
        filter_params.append(odata_list_to_filter("MarginType/Name", margin_type))

        if date_gt:
            filter_params.append(f"Date gt {date_gt}")
        if date_gte:
            filter_params.append(f"Date ge {date_gte}")
        if date_lt:
            filter_params.append(f"Date lt {date_lt}")
        if date_lte:
            filter_params.append(f"Date le {date_lte}")

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {
            "$skip": skip,
            "pageSize": page_size,
            "$count": "true",
            "$expand": "*",
        }

        if filter_exp:
            params["$filter"] = filter_exp

        # odata endpoint will not allow '+' character in URL seemingly
        qs = urlencode(params, quote_via=quote)

        return get_data(
            path=f"{self._endpoint}{endpoint}?{qs}",
            params={},
            # params=params,
            paginate=paginate,
            raw=raw,
            df_fn=self._to_df,
            paginate_fn=self._paginate,
        )

    def get_reference_data(
        self, type: RefTypes, raw: bool = False
    ) -> Union[Response, DataFrame]:
        """
        Fetch reference data for the World Refinery Database.

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
        >>> ci.WorldRefineryData().get_reference_data(type=ci.WorldRefineryData.RefTypes.Owners)
        """
        endpoint_path = type.value

        params = {"pageSize": 1000, "$skip": 0, "$expand": "*", "$count": "true"}

        if type == self.RefTypes.PADDs:
            df_fn = self._padd_to_df
        else:
            df_fn = self._to_df

        qs = urlencode(params, quote_via=quote)

        return get_data(
            path=f"{self._endpoint}{endpoint_path}?{qs}",
            params={},
            # params=params,
            paginate=True,
            raw=raw,
            paginate_fn=self._paginate,
            df_fn=df_fn,
        )

    def get_outages_grouped(
        self,
        *,
        start_date: Optional[Union[date, list[date], "Series[date]"]] = None,
        start_date_gt: Optional[date] = None,
        start_date_gte: Optional[date] = None,
        start_date_lt: Optional[date] = None,
        start_date_lte: Optional[date] = None,
        end_date: Optional[Union[date, list[date], "Series[date]"]] = None,
        end_date_gt: Optional[date] = None,
        end_date_gte: Optional[date] = None,
        end_date_lt: Optional[date] = None,
        end_date_lte: Optional[date] = None,
        refinery_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        owner: Optional[Union[str, list[str], "Series[str]"]] = None,
        process_unit: Optional[Union[str, list[str], "Series[str]"]] = None,
        planning_status: Optional[Union[str, list[str], "Series[str]"]] = None,
        outage_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        outage_vol: Optional[Union[float, list[float], "Series[float]"]] = None,
        outage_vol_gt: Optional[float] = None,
        outage_vol_gte: Optional[float] = None,
        outage_vol_lt: Optional[float] = None,
        outage_vol_lte: Optional[float] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        region: Optional[Union[str, list[str], "Series[str]"]] = None,
        filter_exp: Optional[str] = None,
        skip: int = 0,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Transformation of refinery outages endpoint so that each outage-owner is a single row with outage `start_date` and `end_date` as columns

        Parameters
        ----------
        start_date : Optional[Union[date, list[date], Series[date]]], optional
            filter by ``start_date = x`` , by default None
        start_date_gt : Optional[date], optional
            filter by ``start_date > x`` , by default None
        start_date_gte : Optional[date], optional
            filter by ``start_date >= x`` , by default None
        start_date_lt : Optional[date], optional
            filter by ``start_date < x`` , by default None
        start_date_lte : Optional[date], optional
            filter by ``start_date <= x`` , by default None
        end_date : Optional[Union[date, list[date], Series[date]]], optional
            filter by ``end_date = x`` , by default None
        end_date_gt : Optional[date], optional
            filter by ``end_date > x`` , by default None
        end_date_gte : Optional[date], optional
            filter by ``end_date >= x`` , by default None
        end_date_lt : Optional[date], optional
            filter by ``end_date < x`` , by default None
        end_date_lte : Optional[date], optional
            filter by ``end_date <= x`` , by default None
        refinery_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by refineryId, by default None
        owner : Optional[Union[str, list[str], Series[str]]], optional
            filter by Owner/Name, by default None
        process_unit : Optional[Union[str, list[str], Series[str]]], optional
            filter by ProcessUnit/Name, by default None
        planning_status : Optional[Union[str, list[str], Series[str]]], optional
            filter by PlanningStatus, by default None
        outage_id : Optional[Union[int, list[int], Series[int]]], optional
            filter by outageId, by default None
        outage_vol : Optional[Union[float, list[float], Series[float]]], optional
            filter by ``OutageVol_MBD = x`` , by default None
        outage_vol_gt : Optional[float], optional
            filter by ``OutageVol_MBD > x`` , by default None
        outage_vol_gte : Optional[float], optional
            filter by ``OutageVol_MBD >= x`` , by default None
        outage_vol_lt : Optional[float], optional
            filter by ``OutageVol_MBD < x`` , by default None
        outage_vol_lte : Optional[float], optional
            filter by ``OutageVol_MBD <= x`` , by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by Refinery/Country/Name, by default None
        region : Optional[Union[str, list[str], Series[str]]], optional
            filter by Refinery/Region/Name, by default None
        filter_exp : Optional[str], optional
            pass-thru ``$filter`` query param to use a handcrafted filter expression, by default None
        skip : int, optional
            pass-thru ``$skip`` query param to skip a certain number of records, by default 0
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame, by default False

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
        >>> ci.WorldRefineryData().get_outages_grouped(country="United States")

        **Using Lists**
        >>> ci.WorldRefineryData().get_outages_grouped(process_unit=["Coker", "CDU"])

        """
        endpoint = "outages"

        filter_params: list[str] = []

        filter_params.append(odata_list_to_filter("start_date", start_date))
        filter_params.append(odata_list_to_filter("end_date", end_date))
        filter_params.append(odata_list_to_filter("OutageVol_MBD", outage_vol))
        filter_params.append(odata_list_to_filter("RefineryId", refinery_id))
        filter_params.append(odata_list_to_filter("Owner/Name", owner))
        filter_params.append(odata_list_to_filter("ProcessUnit/Name", process_unit))
        filter_params.append(odata_list_to_filter("PlanningStatus", planning_status))
        filter_params.append(odata_list_to_filter("OutageId", outage_id))
        filter_params.append(odata_list_to_filter("Refinery/Country/Name", country))
        filter_params.append(odata_list_to_filter("Refinery/Region/Name", region))

        if start_date_gt:
            filter_params.append(f"start_date gt {start_date_gt}")
        if start_date_gte:
            filter_params.append(f"start_date ge {start_date_gte}")
        if start_date_lt:
            filter_params.append(f"start_date lt {start_date_lt}")
        if start_date_lte:
            filter_params.append(f"start_date le {start_date_lte}")

        if end_date_gt:
            filter_params.append(f"end_date gt {end_date_gt}")
        if end_date_gte:
            filter_params.append(f"end_date ge {end_date_gte}")
        if end_date_lt:
            filter_params.append(f"end_date lt {end_date_lt}")
        if end_date_lte:
            filter_params.append(f"end_date le {end_date_lte}")

        if outage_vol_gt:
            filter_params.append(f"OutageVol_MBD gt {outage_vol_gt}")
        if outage_vol_gte:
            filter_params.append(f"OutageVol_MBD ge {outage_vol_gte}")
        if outage_vol_lt:
            filter_params.append(f"OutageVol_MBD lt {outage_vol_lt}")
        if outage_vol_lte:
            filter_params.append(f"OutageVol_MBD le {outage_vol_lte}")

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {
            "$skip": skip,
            "pageSize": page_size,
            "$count": "true",
        }

        groupby = "OutageId, RefineryId, Owner/Name, Refinery/Country/Name, Refinery/Region/Name, PlanningStatus, ProcessUnit/Name"
        aggregate = "OutageVol_MBD with max as OutageVol_MBD, Date with max as end_date, Date with min as start_date"
        apply_string = f"groupby(({groupby}), aggregate({aggregate}))"
        params["$apply"] = apply_string

        if filter_exp:
            params["$filter"] = filter_exp

        # odata endpoint will not allow '+' character in URL seemingly
        qs = urlencode(params, quote_via=quote)

        return get_data(
            path=f"{self._endpoint}{endpoint}?{qs}",
            params={},
            # params=params,
            paginate=paginate,
            raw=raw,
            df_fn=self._ts_to_df,
            paginate_fn=self._paginate,
        )
