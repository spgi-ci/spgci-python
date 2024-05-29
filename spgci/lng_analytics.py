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
from typing import Union, Optional, List
from requests import Response
from spgci.api_client import get_data, Paginator
from spgci.utilities import list_to_filter, convert_date_to_filter_exp
from pandas import Series, DataFrame, to_datetime  # type: ignore
import pandas as pd
from distutils.version import LooseVersion
from datetime import date, datetime
from enum import Enum


class LNGGlobalAnalytics:
    """
    Lng Tenders Data - Bids, Offers, Trades

    Includes
    --------

    ``get_tenders()`` to fetch LNG Tenders based on tenderStatus, cargo_type, contract_type, contract_option.\n
    ``get_outages()`` to fetch LNG Outages.\n
    ``get_ref_data()`` to fetch LNG Global Analytics Reference Data such as the list of Liquidfaction Trains.\n
    """

    _endpoint = "lng/v1/"
    _outages_endpoint = "outages/lng/v1/"
    _reference_endpoint = "outages/lng/v1/reference/"

    class RefTypes(Enum):
        """LNG Outages Database Reference Data Type"""

        ConfidenceLevel = "confidence-level"
        LiquefactionTrains = "liquefaction-trains"
        LiquefactionProjects = "liquefaction-projects"

    @staticmethod
    def _paginate(resp: Response) -> Paginator:
        j = resp.json()
        total_pages = j["metadata"]["totalPages"]

        if total_pages <= 1:
            return Paginator(False, "page", 1)

        return Paginator(True, "page", total_pages)

    @staticmethod
    def _convert_to_df(resp: Response) -> DataFrame:
        j = resp.json()
        df = DataFrame(j["results"])

        if len(df) > 0:
            df["openingDate"] = to_datetime(df["openingDate"])
            df["closingDate"] = to_datetime(df["closingDate"])
            df["validityDate"] = to_datetime(df["validityDate"])
            df["liftingDeliveryPeriodFrom"] = to_datetime(
                df["liftingDeliveryPeriodFrom"]
            )
            df["liftingDeliveryPeriodTo"] = to_datetime(df["liftingDeliveryPeriodTo"])

        return df

    @staticmethod
    def _paginate_outages(resp: Response) -> Paginator:
        j = resp.json()
        count = j["metadata"]["count"]
        size = j["metadata"]["pageSize"]

        remainder = count % size
        quotient = count // size
        total_pages = quotient + (1 if remainder > 0 else 0)

        if total_pages <= 1:
            return Paginator(False, "page", 1)

        return Paginator(True, "page", total_pages=total_pages)

    @staticmethod
    def _convert_to_df_outages(resp: Response) -> DataFrame:
        j = resp.json()
        df = DataFrame(j["results"])

        if len(df) > 0:
            if "reportDate" in df.columns:
                df["reportDate"] = to_datetime(df["reportDate"])
            if "startDate" in df.columns:
                df["startDate"] = to_datetime(df["startDate"])
            if "endDate" in df.columns:
                df["endDate"] = to_datetime(df["endDate"])

        return df

    @staticmethod
    def _convert_to_df_netbacks(resp: Response) -> DataFrame:
        j = resp.json()
        df = DataFrame(j["results"])

        if "modifiedDate" in df.columns:
            if LooseVersion(pd.__version__) >= LooseVersion("2"):
                df["modifiedDate"] = pd.to_datetime(
                    df["modifiedDate"], format="ISO8601"
                )
            else:
                df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])

        if "date" in df.columns:
            if LooseVersion(pd.__version__) >= LooseVersion("2"):
                df["date"] = pd.to_datetime(df["date"], format="ISO8601")
            else:
                df["date"] = pd.to_datetime(df["date"])

        return df

    def get_netbacks(
        self,
        *,
        date: Optional[Union[list[date], "Series[date]", date]] = None,
        date_gte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_lt: Optional[date] = None,
        export_geography: Optional[Union[list[str], "Series[str]", str]] = None,
        import_geography: Optional[Union[list[str], "Series[str]", str]] = None,
        modified_date: Optional[
            Union[list[datetime], "Series[datetime]", datetime]
        ] = None,
        modified_date_gte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Fetch netbacks.

        Parameters
        ----------
        date : Optional[Union[list[date], Series[date], date]], optional
            filter by date, by default None
        date_gte : Optional[date], optional
            filter by ``date >= x``, by default None
        date_gt : Optional[date], optional
            filter by ``date > x``, by default None
        date_lte : Optional[date], optional
            filter by ``date <= x``, by default None
        date_lt : Optional[date], optional
            filter by ``date < x``, by default None
        export_geography : Optional[Union[list[str], Series[str], str]], optional
            filter by exportGeography, by default None
        import_geography : Optional[Union[list[str], Series[str], str]], optional
            filter by importGeography, by default None
        modified_date: Optional[Union[list[datetime], Series[datetime], datetime]]
            The latest date of modification for the Weather Actual., be default None
        modified_date_gte: Optional[datetime]
            filter by ``modifiedDate >= x`` , by default None
        modified_date_gt: Optional[datetime]
            filter by ``modifiedDate > x`` , by default None
        modified_date_lte: Optional[datetime]
            filter by ``modifiedDate <= x`` , by default None
        modified_date_lt: Optional[datetime]
            filter by ``modifiedDate < x`` , by default None
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
        **Simple**
        >>> ci.LngGlobalAnalytics().get_netbacks()
        """
        endpoint_path = "netbacks"
        filter_params: List[str] = []
        filter_params.append(list_to_filter("importGeography", import_geography))
        filter_params.append(list_to_filter("exportGeography", export_geography))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        filter_params.append(list_to_filter("date", date))

        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt }"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        if date_gt is not None:
            filter_params.append(f'date > "{date_gt }"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"{self._endpoint}{endpoint_path}",
            params=params,
            df_fn=self._convert_to_df_netbacks,
            paginate_fn=self._paginate,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_tenders(
        self,
        *,
        tender_status: Optional[Union[list[str], "Series[str]", str]] = None,
        cargo_type: Optional[Union[list[str], "Series[str]", str]] = None,
        contract_type: Optional[Union[list[str], "Series[str]", str]] = None,
        contract_option: Optional[Union[list[str], "Series[str]", str]] = None,
        country_name: Optional[Union[list[str], "Series[str]", str]] = None,
        issued_by: Optional[Union[list[str], "Series[str]", str]] = None,
        lifting_delivery_period_from: Optional[date] = None,
        lifting_delivery_period_from_lt: Optional[date] = None,
        lifting_delivery_period_from_lte: Optional[date] = None,
        lifting_delivery_period_from_gt: Optional[date] = None,
        lifting_delivery_period_from_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        tender_status : Optional[Union[list[str], Series[str], str]], optional
            filter by tender_status, by default None
        cargo_type : Optional[Union[list[str], Series[str], str]], optional
            filter by cargo_type, by default None
        contract_type : Optional[Union[list[str], Series[str], str]], optional
            filter by contract_type, by default None
        contract_option : Optional[Union[list[str], Series[str], str]], optional
            filter by contract_option, by default None
        country_name : Optional[Union[list[str], Series[str], str]], optional
            filter by country_name, by default None
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame``, by default False
        filter_exp: string
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None

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
        >>> ci.LngTenders().get_tenders()
        """
        endpoint_path = "tenders"
        filter_params: List[str] = []
        filter_params.append(list_to_filter("tenderStatus", tender_status))
        filter_params.append(list_to_filter("cargoType", cargo_type))
        filter_params.append(list_to_filter("contractType", contract_type))
        filter_params.append(list_to_filter("contractOption", contract_option))
        filter_params.append(list_to_filter("countryName", country_name))
        filter_params.append(list_to_filter("issuedBy", issued_by))

        if lifting_delivery_period_from is not None:
            filter_params.append(
                f'liftingDeliveryPeriodFrom = "{lifting_delivery_period_from}"'
            )
        if lifting_delivery_period_from_gt is not None:
            filter_params.append(
                f'liftingDeliveryPeriodFrom > "{lifting_delivery_period_from_gt}"'
            )
        if lifting_delivery_period_from_gte is not None:
            filter_params.append(
                f'liftingDeliveryPeriodFrom >= "{lifting_delivery_period_from_gte}"'
            )
        if lifting_delivery_period_from_lt is not None:
            filter_params.append(
                f'liftingDeliveryPeriodFrom < "{lifting_delivery_period_from_lt}"'
            )
        if lifting_delivery_period_from_lte is not None:
            filter_params.append(
                f'liftingDeliveryPeriodFrom <= "{lifting_delivery_period_from_lte}"'
            )

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"{self._endpoint}{endpoint_path}",
            params=params,
            df_fn=self._convert_to_df,
            paginate_fn=self._paginate,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_outages(
        self,
        *,
        liquefaction_project_name: Optional[
            Union[list[str], "Series[str]", str]
        ] = None,
        commodity_name: Optional[Union[list[str], "Series[str]", str]] = None,
        train_name: Optional[Union[list[str], "Series[str]", str]] = None,
        start_date: Optional[Union[date, list[date], "Series[date]"]] = None,
        start_date_lt: Optional[date] = None,
        start_date_lte: Optional[date] = None,
        start_date_gt: Optional[date] = None,
        start_date_gte: Optional[date] = None,
        end_date: Optional[Union[date, list[date], "Series[date]"]] = None,
        end_date_lt: Optional[date] = None,
        end_date_lte: Optional[date] = None,
        end_date_gt: Optional[date] = None,
        end_date_gte: Optional[date] = None,
        report_date: Optional[Union[date, list[date], "Series[date]"]] = None,
        report_date_lt: Optional[date] = None,
        report_date_lte: Optional[date] = None,
        report_date_gt: Optional[date] = None,
        report_date_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        liquefaction_project_name : Optional[Union[list[str], Series[str], str]], optional
            filter by liquefactionProjectName, by default None
        commodity_name : Optional[Union[list[str], Series[str], str]], optional
            filter by commodity_name, by default None
        train_name : Optional[Union[list[str], Series[str], str]], optional
            filter by train_name, by default None
        start_date : Optional[date], optional
            filter by ``startDate = x`` , by default None
        start_date_lt : Optional[date], optional
            filter by ``startDate < x``, by default None
        start_date_lte : Optional[date], optional
            filter by ``startDate <= x``, by default None
        start_date_gt : Optional[date], optional
            filter by ``startDate > x``, by default None
        start_date_gte : Optional[date], optional
            filter by ``startDate >= x``, by default None
        report_date : Optional[date], optional
            filter by ``reportDate = x`` , by default None
        report_date_lt : Optional[date], optional
            filter by ``reportDate < x``, by default None
        report_date_lte : Optional[date], optional
            filter by ``reportDate <= x``, by default None
        report_date_gt : Optional[date], optional
            filter by ``reportDate > x``, by default None
        report_date_gte : Optional[date], optional
            filter by ``reportDate >= x``, by default None
        end_date : Optional[date], optional
            filter by ``endDate = x`` , by default None
        end_date_lt : Optional[date], optional
            filter by ``endDate < x``, by default None
        end_date_lte : Optional[date], optional
            filter by ``endDate <= x``, by default None
        end_date_gt : Optional[date], optional
            filter by ``endDate > x``, by default None
        end_date_gte : Optional[date], optional
            filter by ``endDate >= x``, by default None
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame``, by default False
        filter_exp: string
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
          page : int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False

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
        >>> ci.LNGGlobalAnalytics().get_lng_data()
        """
        endpoint_path = "data"
        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("liquefactionProjectName", liquefaction_project_name)
        )
        filter_params.append(list_to_filter("commodityName", commodity_name))
        filter_params.append(list_to_filter("trainName", train_name))
        filter_params.append(list_to_filter("startDate", start_date))
        filter_params.append(list_to_filter("endDate", end_date))
        filter_params.append(list_to_filter("reportDate", report_date))

        filter_params = convert_date_to_filter_exp(
            "startDate",
            start_date_gt,
            start_date_gte,
            start_date_lt,
            start_date_lte,
            filter_params,
        )
        filter_params = convert_date_to_filter_exp(
            "endDate",
            end_date_gt,
            end_date_gte,
            end_date_lt,
            end_date_lte,
            filter_params,
        )
        filter_params = convert_date_to_filter_exp(
            "reportDate",
            report_date_gt,
            report_date_gte,
            report_date_lt,
            report_date_lte,
            filter_params,
        )

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"{self._outages_endpoint}{endpoint_path}",
            params=params,
            df_fn=self._convert_to_df_outages,
            paginate_fn=self._paginate_outages,
            raw=raw,
            paginate=paginate,
        )
        return response

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
        >>> ci.LNGGlobalAnalytics().get_reference_data(type=ci.LngOutages.RefTypes.LiquefactionTrains)
        """
        endpoint_path = type.value

        params = {"pageSize": 1000}

        return get_data(
            path=f"{self._reference_endpoint}{endpoint_path}",
            params=params,
            paginate=True,
            raw=raw,
            paginate_fn=self._paginate_outages,
        )
