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
from packaging.version import parse
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
        date_columns = [
            "openingDate",
            "closingDate",
            "validityDate",
            "liftingDeliveryPeriodFrom",
            "liftingDeliveryPeriodTo",
            "month",
            "pointInTimeMonth",
            "modifiedDate",
            "createdDate",
            "ballastEndDate",
            "ballastStartDate",
            "dateArrived",
            "dateLoaded",
            "loadDate",
            "reexportDate",
            "arrivalDate",
            "transshipmentDate",
        ]

        for column in date_columns:
            if column in df.columns:
                if parse(pd.__version__) >= parse("2"):
                    df[column] = pd.to_datetime(
                        df[column], utc=True, format="ISO8601", errors="coerce"
                    )
                else:
                    df[column] = pd.to_datetime(df[column], errors="coerce", utc=True)  # type: ignore

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
            if parse(pd.__version__) >= parse("2"):
                df["modifiedDate"] = pd.to_datetime(
                    df["modifiedDate"], format="ISO8601"
                )
            else:
                df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])

        if "date" in df.columns:
            if parse(pd.__version__) >= parse("2"):
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

    def get_demand_forecast_current(
        self,
        *,
        import_market: Optional[Union[list[str], Series[str], str]] = None,
        month: Optional[date] = None,
        month_lt: Optional[date] = None,
        month_lte: Optional[date] = None,
        month_gt: Optional[date] = None,
        month_gte: Optional[date] = None,
        point_in_time_month: Optional[date] = None,
        point_in_time_month_lt: Optional[date] = None,
        point_in_time_month_lte: Optional[date] = None,
        point_in_time_month_gt: Optional[date] = None,
        point_in_time_month_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides the latest Demand forecast data

        Parameters
        ----------

         import_market: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG is imported., by default None
         month: Optional[date], optional
             A unit of time representing a period of approximately 30 days, by default None
         month_gt: Optional[date], optional
             filter by `month > x`, by default None
         month_gte: Optional[date], optional
             filter by `month >= x`, by default None
         month_lt: Optional[date], optional
             filter by `month < x`, by default None
         month_lte: Optional[date], optional
             filter by `month <= x`, by default None
         point_in_time_month: Optional[date], optional
             A specific moment within a given month, often used for precise data or event references., by default None
         point_in_time_month_gt: Optional[date], optional
             filter by `point_in_time_month > x`, by default None
         point_in_time_month_gte: Optional[date], optional
             filter by `point_in_time_month >= x`, by default None
         point_in_time_month_lt: Optional[date], optional
             filter by `point_in_time_month < x`, by default None
         point_in_time_month_lte: Optional[date], optional
             filter by `point_in_time_month <= x`, by default None
         modified_date: Optional[datetime], optional
             The latest date of modification for the current demand forecast, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("importMarket", import_market))
        filter_params.append(list_to_filter("month", month))
        if month_gt is not None:
            filter_params.append(f'month > "{month_gt}"')
        if month_gte is not None:
            filter_params.append(f'month >= "{month_gte}"')
        if month_lt is not None:
            filter_params.append(f'month < "{month_lt}"')
        if month_lte is not None:
            filter_params.append(f'month <= "{month_lte}"')
        filter_params.append(list_to_filter("pointInTimeMonth", point_in_time_month))
        if point_in_time_month_gt is not None:
            filter_params.append(f'pointInTimeMonth > "{point_in_time_month_gt}"')
        if point_in_time_month_gte is not None:
            filter_params.append(f'pointInTimeMonth >= "{point_in_time_month_gte}"')
        if point_in_time_month_lt is not None:
            filter_params.append(f'pointInTimeMonth < "{point_in_time_month_lt}"')
        if point_in_time_month_lte is not None:
            filter_params.append(f'pointInTimeMonth <= "{point_in_time_month_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/demand-forecast/current",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_supply_forecast_current(
        self,
        *,
        export_project: Optional[Union[list[str], Series[str], str]] = None,
        export_market: Optional[Union[list[str], Series[str], str]] = None,
        month: Optional[date] = None,
        month_lt: Optional[date] = None,
        month_lte: Optional[date] = None,
        month_gt: Optional[date] = None,
        month_gte: Optional[date] = None,
        point_in_time_month: Optional[date] = None,
        point_in_time_month_lt: Optional[date] = None,
        point_in_time_month_lte: Optional[date] = None,
        point_in_time_month_gt: Optional[date] = None,
        point_in_time_month_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides the latest Supply forecast data

        Parameters
        ----------

         export_project: Optional[Union[list[str], Series[str], str]]
             A specific venture or initiative focused on exporting LNG supply  to international markets., by default None
         export_market: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG supply are being sold and shipped from a particular location., by default None
         month: Optional[date], optional
             A unit of time representing a period of approximately 30 days, by default None
         month_gt: Optional[date], optional
             filter by `month > x`, by default None
         month_gte: Optional[date], optional
             filter by `month >= x`, by default None
         month_lt: Optional[date], optional
             filter by `month < x`, by default None
         month_lte: Optional[date], optional
             filter by `month <= x`, by default None
         point_in_time_month: Optional[date], optional
             A specific moment within a given month, often used for precise data or event references., by default None
         point_in_time_month_gt: Optional[date], optional
             filter by `point_in_time_month > x` , by default None
         point_in_time_month_gte: Optional[date], optional
             filter by `point_in_time_month >= x`, by default None
         point_in_time_month_lt: Optional[date], optional
             filter by `point_in_time_month < x`, by default None
         point_in_time_month_lte: Optional[date], optional
             filter by `point_in_time_month <= x`, by default None
         modified_date: Optional[datetime], optional
             The latest date of modification for the current supply forecast, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("exportProject", export_project))
        filter_params.append(list_to_filter("exportMarket", export_market))
        filter_params.append(list_to_filter("month", month))
        if month_gt is not None:
            filter_params.append(f'month > "{month_gt}"')
        if month_gte is not None:
            filter_params.append(f'month >= "{month_gte}"')
        if month_lt is not None:
            filter_params.append(f'month < "{month_lt}"')
        if month_lte is not None:
            filter_params.append(f'month <= "{month_lte}"')
        filter_params.append(list_to_filter("pointInTimeMonth", point_in_time_month))
        if point_in_time_month_gt is not None:
            filter_params.append(f'pointInTimeMonth > "{point_in_time_month_gt}"')
        if point_in_time_month_gte is not None:
            filter_params.append(f'pointInTimeMonth >= "{point_in_time_month_gte}"')
        if point_in_time_month_lt is not None:
            filter_params.append(f'pointInTimeMonth < "{point_in_time_month_lt}"')
        if point_in_time_month_lte is not None:
            filter_params.append(f'pointInTimeMonth <= "{point_in_time_month_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/supply-forecast/current",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_demand_forecast_history(
        self,
        *,
        import_market: Optional[Union[list[str], Series[str], str]] = None,
        month: Optional[date] = None,
        month_lt: Optional[date] = None,
        month_lte: Optional[date] = None,
        month_gt: Optional[date] = None,
        month_gte: Optional[date] = None,
        point_in_time_month: Optional[date] = None,
        point_in_time_month_lt: Optional[date] = None,
        point_in_time_month_lte: Optional[date] = None,
        point_in_time_month_gt: Optional[date] = None,
        point_in_time_month_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides the actual points in time from Jan 2023 Demand forecast data

        Parameters
        ----------

         import_market: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG is imported., by default None
         month: Optional[date], optional
             A unit of time representing a period of approximately 30 days, by default None
         month_gt: Optional[date], optional
             filter by `month > x`, by default None
         month_gte: Optional[date], optional
             filter by `month >= x`, by default None
         month_lt: Optional[date], optional
             filter by `month < x`, by default None
         month_lte: Optional[date], optional
             filter by `month <= x`, by default None
         point_in_time_month: Optional[date], optional
             A specific moment within a given month, often used for precise data or event references., by default None
         point_in_time_month_gt: Optional[date], optional
             filter by `point_in_time_month > x`, by default None
         point_in_time_month_gte: Optional[date], optional
             filter by `point_in_time_month >= x`, by default None
         point_in_time_month_lt: Optional[date], optional
             filter by `point_in_time_month < x`, by default None
         point_in_time_month_lte: Optional[date], optional
             filter by `point_in_time_month <= x`, by default None
         modified_date: Optional[datetime], optional
             The latest date of modification for the historical demand forecast, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("importMarket", import_market))
        filter_params.append(list_to_filter("month", month))
        if month_gt is not None:
            filter_params.append(f'month > "{month_gt}"')
        if month_gte is not None:
            filter_params.append(f'month >= "{month_gte}"')
        if month_lt is not None:
            filter_params.append(f'month < "{month_lt}"')
        if month_lte is not None:
            filter_params.append(f'month <= "{month_lte}"')
        filter_params.append(list_to_filter("pointInTimeMonth", point_in_time_month))
        if point_in_time_month_gt is not None:
            filter_params.append(f'pointInTimeMonth > "{point_in_time_month_gt}"')
        if point_in_time_month_gte is not None:
            filter_params.append(f'pointInTimeMonth >= "{point_in_time_month_gte}"')
        if point_in_time_month_lt is not None:
            filter_params.append(f'pointInTimeMonth < "{point_in_time_month_lt}"')
        if point_in_time_month_lte is not None:
            filter_params.append(f'pointInTimeMonth <= "{point_in_time_month_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/demand-forecast/history",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_supply_forecast_history(
        self,
        *,
        export_project: Optional[Union[list[str], Series[str], str]] = None,
        export_market: Optional[Union[list[str], Series[str], str]] = None,
        month: Optional[date] = None,
        month_lt: Optional[date] = None,
        month_lte: Optional[date] = None,
        month_gt: Optional[date] = None,
        month_gte: Optional[date] = None,
        point_in_time_month: Optional[date] = None,
        point_in_time_month_lt: Optional[date] = None,
        point_in_time_month_lte: Optional[date] = None,
        point_in_time_month_gt: Optional[date] = None,
        point_in_time_month_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides the actual points in time from Jan 2023 Supply forecast data

        Parameters
        ----------

         export_project: Optional[Union[list[str], Series[str], str]]
             A specific venture or initiative focused on exporting LNG supply to international markets., by default None
         export_market: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG supply are being sold and shipped from a particular location., by default None
         month: Optional[date], optional
             A unit of time representing a period of approximately 30 days., by default None
         month_gt: Optional[date], optional
             filter by `month > x`, by default None
         month_gte: Optional[date], optional
             filter by `month >= x`, by default None
         month_lt: Optional[date], optional
             filter by `month < x`, by default None
         month_lte: Optional[date], optional
             filter by `month <= x`, by default None
         point_in_time_month: Optional[date], optional
             A specific moment within a given month, often used for precise data or event references., by default None
         point_in_time_month_gt: Optional[date], optional
             filter by `point_in_time_month > x`, by default None
         point_in_time_month_gte: Optional[date], optional
             filter by `point_in_time_month >= x`, by default None
         point_in_time_month_lt: Optional[date], optional
             filter by `point_in_time_month < x`, by default None
         point_in_time_month_lte: Optional[date], optional
             filter by `point_in_time_month <= x`, by default None
         modified_date: Optional[datetime], optional
             The latest date of modification for the historical supply forecast, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("exportProject", export_project))
        filter_params.append(list_to_filter("exportMarket", export_market))
        filter_params.append(list_to_filter("month", month))
        if month_gt is not None:
            filter_params.append(f'month > "{month_gt}"')
        if month_gte is not None:
            filter_params.append(f'month >= "{month_gte}"')
        if month_lt is not None:
            filter_params.append(f'month < "{month_lt}"')
        if month_lte is not None:
            filter_params.append(f'month <= "{month_lte}"')
        filter_params.append(list_to_filter("pointInTimeMonth", point_in_time_month))
        if point_in_time_month_gt is not None:
            filter_params.append(f'pointInTimeMonth > "{point_in_time_month_gt}"')
        if point_in_time_month_gte is not None:
            filter_params.append(f'pointInTimeMonth >= "{point_in_time_month_gte}"')
        if point_in_time_month_lt is not None:
            filter_params.append(f'pointInTimeMonth < "{point_in_time_month_lt}"')
        if point_in_time_month_lte is not None:
            filter_params.append(f'pointInTimeMonth <= "{point_in_time_month_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/supply-forecast/history",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_reference_data(
        self, type: RefTypes, raw: bool = False
    ) -> Union[Response, DataFrame]:
        """
        Fetch reference data for the Global LNG Analytics

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

    def get_cargo_historical_bilateral_trade_flows(
        self,
        *,
        export_market: Optional[Union[list[str], Series[str], str]] = None,
        import_market: Optional[Union[list[str], Series[str], str]] = None,
        month_arrived: Optional[date] = None,
        month_arrived_lt: Optional[date] = None,
        month_arrived_lte: Optional[date] = None,
        month_arrived_gt: Optional[date] = None,
        month_arrived_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        This API provides historical monthly aggregated volume data showing country to country flows.

        Parameters
        ----------

         export_market: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG supply are being sold and shipped., by default None
         import_market: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG supply are being bought., by default None
         month_arrived: Optional[date], optional
             A unit of time representing a period of approximately 30 days., by default None
         month_arrived_gt: Optional[date], optional
             filter by '' month_arrived > x '', by default None
         month_arrived_gte: Optional[date], optional
             filter by month_arrived, by default None
         month_arrived_lt: Optional[date], optional
             filter by month_arrived, by default None
         month_arrived_lte: Optional[date], optional
             filter by month_arrived, by default None
         modified_date: Optional[datetime], optional
             The latest date that this record was modified., by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("exportMarket", export_market))
        filter_params.append(list_to_filter("importMarket", import_market))
        filter_params.append(list_to_filter("monthArrived", month_arrived))
        if month_arrived_gt is not None:
            filter_params.append(f'monthArrived > "{month_arrived_gt}"')
        if month_arrived_gte is not None:
            filter_params.append(f'monthArrived >= "{month_arrived_gte}"')
        if month_arrived_lt is not None:
            filter_params.append(f'monthArrived < "{month_arrived_lt}"')
        if month_arrived_lte is not None:
            filter_params.append(f'monthArrived <= "{month_arrived_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/cargo/historical-bilateral-trade-flows",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_cargo_trips(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        vessel_name: Optional[Union[list[str], Series[str], str]] = None,
        vessel_imo: Optional[int] = None,
        vessel_imo_lt: Optional[int] = None,
        vessel_imo_lte: Optional[int] = None,
        vessel_imo_gt: Optional[int] = None,
        vessel_imo_gte: Optional[int] = None,
        supply_plant: Optional[Union[list[str], Series[str], str]] = None,
        trade_route: Optional[Union[list[str], Series[str], str]] = None,
        reexport_port: Optional[Union[list[str], Series[str], str]] = None,
        receiving_port: Optional[Union[list[str], Series[str], str]] = None,
        supply_market: Optional[Union[list[str], Series[str], str]] = None,
        receiving_market: Optional[Union[list[str], Series[str], str]] = None,
        laden_ballast: Optional[Union[list[str], Series[str], str]] = None,
        supply_project_parent: Optional[Union[list[str], Series[str], str]] = None,
        is_spot_or_short_term: Optional[Union[list[str], Series[str], str]] = None,
        date_loaded: Optional[datetime] = None,
        date_loaded_lt: Optional[datetime] = None,
        date_loaded_lte: Optional[datetime] = None,
        date_loaded_gt: Optional[datetime] = None,
        date_loaded_gte: Optional[datetime] = None,
        date_arrived: Optional[datetime] = None,
        date_arrived_lt: Optional[datetime] = None,
        date_arrived_lte: Optional[datetime] = None,
        date_arrived_gt: Optional[datetime] = None,
        date_arrived_gte: Optional[datetime] = None,
        final_loaded_cbm: Optional[str] = None,
        final_loaded_cbm_lt: Optional[str] = None,
        final_loaded_cbm_lte: Optional[str] = None,
        final_loaded_cbm_gt: Optional[str] = None,
        final_loaded_cbm_gte: Optional[str] = None,
        final_loaded_mt: Optional[str] = None,
        final_loaded_mt_lt: Optional[str] = None,
        final_loaded_mt_lte: Optional[str] = None,
        final_loaded_mt_gt: Optional[str] = None,
        final_loaded_mt_gte: Optional[str] = None,
        final_loaded_bcf: Optional[str] = None,
        final_loaded_bcf_lt: Optional[str] = None,
        final_loaded_bcf_lte: Optional[str] = None,
        final_loaded_bcf_gt: Optional[str] = None,
        final_loaded_bcf_gte: Optional[str] = None,
        final_unloaded_cbm: Optional[str] = None,
        final_unloaded_cbm_lt: Optional[str] = None,
        final_unloaded_cbm_lte: Optional[str] = None,
        final_unloaded_cbm_gt: Optional[str] = None,
        final_unloaded_cbm_gte: Optional[str] = None,
        final_unloaded_mt: Optional[str] = None,
        final_unloaded_mt_lt: Optional[str] = None,
        final_unloaded_mt_lte: Optional[str] = None,
        final_unloaded_mt_gt: Optional[str] = None,
        final_unloaded_mt_gte: Optional[str] = None,
        final_unloaded_bcf: Optional[str] = None,
        final_unloaded_bcf_lt: Optional[str] = None,
        final_unloaded_bcf_lte: Optional[str] = None,
        final_unloaded_bcf_gt: Optional[str] = None,
        final_unloaded_bcf_gte: Optional[str] = None,
        ballast_start_date: Optional[datetime] = None,
        ballast_start_date_lt: Optional[datetime] = None,
        ballast_start_date_lte: Optional[datetime] = None,
        ballast_start_date_gt: Optional[datetime] = None,
        ballast_start_date_gte: Optional[datetime] = None,
        ballast_start_port: Optional[Union[list[str], Series[str], str]] = None,
        ballast_start_terminal: Optional[Union[list[str], Series[str], str]] = None,
        ballast_start_market: Optional[Union[list[str], Series[str], str]] = None,
        ballast_end_date: Optional[datetime] = None,
        ballast_end_date_lt: Optional[datetime] = None,
        ballast_end_date_lte: Optional[datetime] = None,
        ballast_end_date_gt: Optional[datetime] = None,
        ballast_end_date_gte: Optional[datetime] = None,
        ballast_end_port: Optional[Union[list[str], Series[str], str]] = None,
        ballast_end_market: Optional[Union[list[str], Series[str], str]] = None,
        transshipment_port: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Completed commercial journeys of (LNG) vessel from point A to point B.

        Parameters
        ----------

         id: Optional[int], optional
             Trip ID, by default None
         id_gt: Optional[int], optional
             filter by '' id > x '', by default None
         id_gte: Optional[int], optional
             filter by id, by default None
         id_lt: Optional[int], optional
             filter by id, by default None
         id_lte: Optional[int], optional
             filter by id, by default None
         vessel_name: Optional[Union[list[str], Series[str], str]]
             Vessel name, by default None
         vessel_imo: Optional[int], optional
             Vessel IMO number, by default None
         vessel_imo_gt: Optional[int], optional
             filter by '' vessel_imo > x '', by default None
         vessel_imo_gte: Optional[int], optional
             filter by vessel_imo, by default None
         vessel_imo_lt: Optional[int], optional
             filter by vessel_imo, by default None
         vessel_imo_lte: Optional[int], optional
             filter by vessel_imo, by default None
         supply_plant: Optional[Union[list[str], Series[str], str]]
             Supply plant, by default None
         trade_route: Optional[Union[list[str], Series[str], str]]
             Trade route for the trip, by default None
         reexport_port: Optional[Union[list[str], Series[str], str]]
             Source port if this is a reexport trip, by default None
         receiving_port: Optional[Union[list[str], Series[str], str]]
             Receiving port, by default None
         supply_market: Optional[Union[list[str], Series[str], str]]
             Source market, by default None
         receiving_market: Optional[Union[list[str], Series[str], str]]
             Receiving market, by default None
         laden_ballast: Optional[Union[list[str], Series[str], str]]
             Whether current trip is laden or ballast, by default None
         supply_project_parent: Optional[Union[list[str], Series[str], str]]
             Supply project, by default None
         is_spot_or_short_term: Optional[Union[list[str], Series[str], str]]
             Whether the trip's commercial status is designated as spot or short-term trip or a long-term trip, by default None
         date_loaded: Optional[datetime], optional
             Cargo load date for the trip, by default None
         date_loaded_gt: Optional[datetime], optional
             filter by '' date_loaded > x '', by default None
         date_loaded_gte: Optional[datetime], optional
             filter by date_loaded, by default None
         date_loaded_lt: Optional[datetime], optional
             filter by date_loaded, by default None
         date_loaded_lte: Optional[datetime], optional
             filter by date_loaded, by default None
         date_arrived: Optional[datetime], optional
             Cargo unload date for the trip, by default None
         date_arrived_gt: Optional[datetime], optional
             filter by '' date_arrived > x '', by default None
         date_arrived_gte: Optional[datetime], optional
             filter by date_arrived, by default None
         date_arrived_lt: Optional[datetime], optional
             filter by date_arrived, by default None
         date_arrived_lte: Optional[datetime], optional
             filter by date_arrived, by default None
         final_loaded_cbm: Optional[str], optional
             Final loaded volume in cubic meters including multi port loadings if more than one loading occurs, by default None
         final_loaded_cbm_gt: Optional[str], optional
             filter by '' final_loaded_cbm > x '', by default None
         final_loaded_cbm_gte: Optional[str], optional
             filter by final_loaded_cbm, by default None
         final_loaded_cbm_lt: Optional[str], optional
             filter by final_loaded_cbm, by default None
         final_loaded_cbm_lte: Optional[str], optional
             filter by final_loaded_cbm, by default None
         final_loaded_mt: Optional[str], optional
             Final loaded volume in metric tons including multi port loadings if more than one loading occurs, by default None
         final_loaded_mt_gt: Optional[str], optional
             filter by '' final_loaded_mt > x '', by default None
         final_loaded_mt_gte: Optional[str], optional
             filter by final_loaded_mt, by default None
         final_loaded_mt_lt: Optional[str], optional
             filter by final_loaded_mt, by default None
         final_loaded_mt_lte: Optional[str], optional
             filter by final_loaded_mt, by default None
         final_loaded_bcf: Optional[str], optional
             Final loaded volume in billion cubic feet including multi port loadings if more than one loading occurs, by default None
         final_loaded_bcf_gt: Optional[str], optional
             filter by '' final_loaded_bcf > x '', by default None
         final_loaded_bcf_gte: Optional[str], optional
             filter by final_loaded_bcf, by default None
         final_loaded_bcf_lt: Optional[str], optional
             filter by final_loaded_bcf, by default None
         final_loaded_bcf_lte: Optional[str], optional
             filter by final_loaded_bcf, by default None
         final_unloaded_cbm: Optional[str], optional
             Final unloaded volume in cubic meters including multi port unloadings if more than one unloading occurs, by default None
         final_unloaded_cbm_gt: Optional[str], optional
             filter by '' final_unloaded_cbm > x '', by default None
         final_unloaded_cbm_gte: Optional[str], optional
             filter by final_unloaded_cbm, by default None
         final_unloaded_cbm_lt: Optional[str], optional
             filter by final_unloaded_cbm, by default None
         final_unloaded_cbm_lte: Optional[str], optional
             filter by final_unloaded_cbm, by default None
         final_unloaded_mt: Optional[str], optional
             Final unloaded volume in metric tons including multi port unloadings if more than one unloading occurs, by default None
         final_unloaded_mt_gt: Optional[str], optional
             filter by '' final_unloaded_mt > x '', by default None
         final_unloaded_mt_gte: Optional[str], optional
             filter by final_unloaded_mt, by default None
         final_unloaded_mt_lt: Optional[str], optional
             filter by final_unloaded_mt, by default None
         final_unloaded_mt_lte: Optional[str], optional
             filter by final_unloaded_mt, by default None
         final_unloaded_bcf: Optional[str], optional
             Final unloaded volume in billion cubic feet including multi port unloadings if more than one unloading occurs, by default None
         final_unloaded_bcf_gt: Optional[str], optional
             filter by '' final_unloaded_bcf > x '', by default None
         final_unloaded_bcf_gte: Optional[str], optional
             filter by final_unloaded_bcf, by default None
         final_unloaded_bcf_lt: Optional[str], optional
             filter by final_unloaded_bcf, by default None
         final_unloaded_bcf_lte: Optional[str], optional
             filter by final_unloaded_bcf, by default None
         ballast_start_date: Optional[datetime], optional
             Start date of ballast trip, by default None
         ballast_start_date_gt: Optional[datetime], optional
             filter by '' ballast_start_date > x '', by default None
         ballast_start_date_gte: Optional[datetime], optional
             filter by ballast_start_date, by default None
         ballast_start_date_lt: Optional[datetime], optional
             filter by ballast_start_date, by default None
         ballast_start_date_lte: Optional[datetime], optional
             filter by ballast_start_date, by default None
         ballast_start_port: Optional[Union[list[str], Series[str], str]]
             Start port of ballast trip, by default None
         ballast_start_terminal: Optional[Union[list[str], Series[str], str]]
             Start terminal of ballast trip, by default None
         ballast_start_market: Optional[Union[list[str], Series[str], str]]
             Start market of ballast trip, by default None
         ballast_end_date: Optional[datetime], optional
             End date of ballast trip, by default None
         ballast_end_date_gt: Optional[datetime], optional
             filter by '' ballast_end_date > x '', by default None
         ballast_end_date_gte: Optional[datetime], optional
             filter by ballast_end_date, by default None
         ballast_end_date_lt: Optional[datetime], optional
             filter by ballast_end_date, by default None
         ballast_end_date_lte: Optional[datetime], optional
             filter by ballast_end_date, by default None
         ballast_end_port: Optional[Union[list[str], Series[str], str]]
             End port of ballast trip, by default None
         ballast_end_market: Optional[Union[list[str], Series[str], str]]
             End market of ballast trip, by default None
         transshipment_port: Optional[Union[list[str], Series[str], str]]
             Port where transshipment occurred, by default None
         modified_date: Optional[datetime], optional
             Trip record latest modified date, by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

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
        filter_params.append(list_to_filter("vesselName", vessel_name))
        filter_params.append(list_to_filter("vesselImo", vessel_imo))
        if vessel_imo_gt is not None:
            filter_params.append(f'vesselImo > "{vessel_imo_gt}"')
        if vessel_imo_gte is not None:
            filter_params.append(f'vesselImo >= "{vessel_imo_gte}"')
        if vessel_imo_lt is not None:
            filter_params.append(f'vesselImo < "{vessel_imo_lt}"')
        if vessel_imo_lte is not None:
            filter_params.append(f'vesselImo <= "{vessel_imo_lte}"')
        filter_params.append(list_to_filter("supplyPlant", supply_plant))
        filter_params.append(list_to_filter("tradeRoute", trade_route))
        filter_params.append(list_to_filter("reexportPort", reexport_port))
        filter_params.append(list_to_filter("receivingPort", receiving_port))
        filter_params.append(list_to_filter("supplyMarket", supply_market))
        filter_params.append(list_to_filter("receivingMarket", receiving_market))
        filter_params.append(list_to_filter("ladenBallast", laden_ballast))
        filter_params.append(
            list_to_filter("supplyProjectParent", supply_project_parent)
        )
        filter_params.append(list_to_filter("isSpotOrShortTerm", is_spot_or_short_term))
        filter_params.append(list_to_filter("dateLoaded", date_loaded))
        if date_loaded_gt is not None:
            filter_params.append(f'dateLoaded > "{date_loaded_gt}"')
        if date_loaded_gte is not None:
            filter_params.append(f'dateLoaded >= "{date_loaded_gte}"')
        if date_loaded_lt is not None:
            filter_params.append(f'dateLoaded < "{date_loaded_lt}"')
        if date_loaded_lte is not None:
            filter_params.append(f'dateLoaded <= "{date_loaded_lte}"')
        filter_params.append(list_to_filter("dateArrived", date_arrived))
        if date_arrived_gt is not None:
            filter_params.append(f'dateArrived > "{date_arrived_gt}"')
        if date_arrived_gte is not None:
            filter_params.append(f'dateArrived >= "{date_arrived_gte}"')
        if date_arrived_lt is not None:
            filter_params.append(f'dateArrived < "{date_arrived_lt}"')
        if date_arrived_lte is not None:
            filter_params.append(f'dateArrived <= "{date_arrived_lte}"')
        filter_params.append(list_to_filter("finalLoadedCbm", final_loaded_cbm))
        if final_loaded_cbm_gt is not None:
            filter_params.append(f'finalLoadedCbm > "{final_loaded_cbm_gt}"')
        if final_loaded_cbm_gte is not None:
            filter_params.append(f'finalLoadedCbm >= "{final_loaded_cbm_gte}"')
        if final_loaded_cbm_lt is not None:
            filter_params.append(f'finalLoadedCbm < "{final_loaded_cbm_lt}"')
        if final_loaded_cbm_lte is not None:
            filter_params.append(f'finalLoadedCbm <= "{final_loaded_cbm_lte}"')
        filter_params.append(list_to_filter("finalLoadedMt", final_loaded_mt))
        if final_loaded_mt_gt is not None:
            filter_params.append(f'finalLoadedMt > "{final_loaded_mt_gt}"')
        if final_loaded_mt_gte is not None:
            filter_params.append(f'finalLoadedMt >= "{final_loaded_mt_gte}"')
        if final_loaded_mt_lt is not None:
            filter_params.append(f'finalLoadedMt < "{final_loaded_mt_lt}"')
        if final_loaded_mt_lte is not None:
            filter_params.append(f'finalLoadedMt <= "{final_loaded_mt_lte}"')
        filter_params.append(list_to_filter("finalLoadedBcf", final_loaded_bcf))
        if final_loaded_bcf_gt is not None:
            filter_params.append(f'finalLoadedBcf > "{final_loaded_bcf_gt}"')
        if final_loaded_bcf_gte is not None:
            filter_params.append(f'finalLoadedBcf >= "{final_loaded_bcf_gte}"')
        if final_loaded_bcf_lt is not None:
            filter_params.append(f'finalLoadedBcf < "{final_loaded_bcf_lt}"')
        if final_loaded_bcf_lte is not None:
            filter_params.append(f'finalLoadedBcf <= "{final_loaded_bcf_lte}"')
        filter_params.append(list_to_filter("finalUnloadedCbm", final_unloaded_cbm))
        if final_unloaded_cbm_gt is not None:
            filter_params.append(f'finalUnloadedCbm > "{final_unloaded_cbm_gt}"')
        if final_unloaded_cbm_gte is not None:
            filter_params.append(f'finalUnloadedCbm >= "{final_unloaded_cbm_gte}"')
        if final_unloaded_cbm_lt is not None:
            filter_params.append(f'finalUnloadedCbm < "{final_unloaded_cbm_lt}"')
        if final_unloaded_cbm_lte is not None:
            filter_params.append(f'finalUnloadedCbm <= "{final_unloaded_cbm_lte}"')
        filter_params.append(list_to_filter("finalUnloadedMt", final_unloaded_mt))
        if final_unloaded_mt_gt is not None:
            filter_params.append(f'finalUnloadedMt > "{final_unloaded_mt_gt}"')
        if final_unloaded_mt_gte is not None:
            filter_params.append(f'finalUnloadedMt >= "{final_unloaded_mt_gte}"')
        if final_unloaded_mt_lt is not None:
            filter_params.append(f'finalUnloadedMt < "{final_unloaded_mt_lt}"')
        if final_unloaded_mt_lte is not None:
            filter_params.append(f'finalUnloadedMt <= "{final_unloaded_mt_lte}"')
        filter_params.append(list_to_filter("finalUnloadedBcf", final_unloaded_bcf))
        if final_unloaded_bcf_gt is not None:
            filter_params.append(f'finalUnloadedBcf > "{final_unloaded_bcf_gt}"')
        if final_unloaded_bcf_gte is not None:
            filter_params.append(f'finalUnloadedBcf >= "{final_unloaded_bcf_gte}"')
        if final_unloaded_bcf_lt is not None:
            filter_params.append(f'finalUnloadedBcf < "{final_unloaded_bcf_lt}"')
        if final_unloaded_bcf_lte is not None:
            filter_params.append(f'finalUnloadedBcf <= "{final_unloaded_bcf_lte}"')
        filter_params.append(list_to_filter("ballastStartDate", ballast_start_date))
        if ballast_start_date_gt is not None:
            filter_params.append(f'ballastStartDate > "{ballast_start_date_gt}"')
        if ballast_start_date_gte is not None:
            filter_params.append(f'ballastStartDate >= "{ballast_start_date_gte}"')
        if ballast_start_date_lt is not None:
            filter_params.append(f'ballastStartDate < "{ballast_start_date_lt}"')
        if ballast_start_date_lte is not None:
            filter_params.append(f'ballastStartDate <= "{ballast_start_date_lte}"')
        filter_params.append(list_to_filter("ballastStartPort", ballast_start_port))
        filter_params.append(
            list_to_filter("ballastStartTerminal", ballast_start_terminal)
        )
        filter_params.append(list_to_filter("ballastStartMarket", ballast_start_market))
        filter_params.append(list_to_filter("ballastEndDate", ballast_end_date))
        if ballast_end_date_gt is not None:
            filter_params.append(f'ballastEndDate > "{ballast_end_date_gt}"')
        if ballast_end_date_gte is not None:
            filter_params.append(f'ballastEndDate >= "{ballast_end_date_gte}"')
        if ballast_end_date_lt is not None:
            filter_params.append(f'ballastEndDate < "{ballast_end_date_lt}"')
        if ballast_end_date_lte is not None:
            filter_params.append(f'ballastEndDate <= "{ballast_end_date_lte}"')
        filter_params.append(list_to_filter("ballastEndPort", ballast_end_port))
        filter_params.append(list_to_filter("ballastEndMarket", ballast_end_market))
        filter_params.append(list_to_filter("transshipmentPort", transshipment_port))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/cargo/trips",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_cargo_events_partial_load(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        load_date: Optional[datetime] = None,
        load_date_lt: Optional[datetime] = None,
        load_date_lte: Optional[datetime] = None,
        load_date_gt: Optional[datetime] = None,
        load_date_gte: Optional[datetime] = None,
        supply_plant: Optional[Union[list[str], Series[str], str]] = None,
        supply_market: Optional[Union[list[str], Series[str], str]] = None,
        supply_project_parent: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Occurrences involving the loading of a portion of LNG cargo during a Transportation process.

        Parameters
        ----------

         id: Optional[int], optional
             Event ID, by default None
         id_gt: Optional[int], optional
             filter by '' id > x '', by default None
         id_gte: Optional[int], optional
             filter by id, by default None
         id_lt: Optional[int], optional
             filter by id, by default None
         id_lte: Optional[int], optional
             filter by id, by default None
         load_date: Optional[datetime], optional
             Load date of the partial loading, by default None
         load_date_gt: Optional[datetime], optional
             filter by '' load_date > x '', by default None
         load_date_gte: Optional[datetime], optional
             filter by load_date, by default None
         load_date_lt: Optional[datetime], optional
             filter by load_date, by default None
         load_date_lte: Optional[datetime], optional
             filter by load_date, by default None
         supply_plant: Optional[Union[list[str], Series[str], str]]
             Supply plant of the cargo, by default None
         supply_market: Optional[Union[list[str], Series[str], str]]
             Supply market of the cargo, by default None
         supply_project_parent: Optional[Union[list[str], Series[str], str]]
             Supply project's parent, by default None
         modified_date: Optional[datetime], optional
             Event record latest modified date., by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

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
        filter_params.append(list_to_filter("loadDate", load_date))
        if load_date_gt is not None:
            filter_params.append(f'loadDate > "{load_date_gt}"')
        if load_date_gte is not None:
            filter_params.append(f'loadDate >= "{load_date_gte}"')
        if load_date_lt is not None:
            filter_params.append(f'loadDate < "{load_date_lt}"')
        if load_date_lte is not None:
            filter_params.append(f'loadDate <= "{load_date_lte}"')
        filter_params.append(list_to_filter("supplyPlant", supply_plant))
        filter_params.append(list_to_filter("supplyMarket", supply_market))
        filter_params.append(
            list_to_filter("supplyProjectParent", supply_project_parent)
        )
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/cargo/events/partial-load",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_cargo_events_partial_unload(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        buyer: Optional[Union[list[str], Series[str], str]] = None,
        is_spot_or_short_term: Optional[Union[list[str], Series[str], str]] = None,
        receiving_port: Optional[Union[list[str], Series[str], str]] = None,
        terminal: Optional[Union[list[str], Series[str], str]] = None,
        receiving_market: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Incidents related to the partial unloading of LNG cargo during transportation.

        Parameters
        ----------

         id: Optional[int], optional
             Event ID, by default None
         id_gt: Optional[int], optional
             filter by '' id > x '', by default None
         id_gte: Optional[int], optional
             filter by id, by default None
         id_lt: Optional[int], optional
             filter by id, by default None
         id_lte: Optional[int], optional
             filter by id, by default None
         buyer: Optional[Union[list[str], Series[str], str]]
             Buyer of the trip, by default None
         is_spot_or_short_term: Optional[Union[list[str], Series[str], str]]
             Whether the delivery is a short-term trade or not, by default None
         receiving_port: Optional[Union[list[str], Series[str], str]]
             Receiving port, by default None
         terminal: Optional[Union[list[str], Series[str], str]]
             Receiving terminal, by default None
         receiving_market: Optional[Union[list[str], Series[str], str]]
             Receiving market, by default None
         modified_date: Optional[datetime], optional
             Event record latest modified date., by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

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
        filter_params.append(list_to_filter("buyer", buyer))
        filter_params.append(list_to_filter("isSpotOrShortTerm", is_spot_or_short_term))
        filter_params.append(list_to_filter("receivingPort", receiving_port))
        filter_params.append(list_to_filter("terminal", terminal))
        filter_params.append(list_to_filter("receivingMarket", receiving_market))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/cargo/events/partial-unload",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_cargo_events_partial_reexport(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        reexport_date: Optional[datetime] = None,
        reexport_date_lt: Optional[datetime] = None,
        reexport_date_lte: Optional[datetime] = None,
        reexport_date_gt: Optional[datetime] = None,
        reexport_date_gte: Optional[datetime] = None,
        supply_market: Optional[Union[list[str], Series[str], str]] = None,
        reexport_port: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Instances where a fraction of the initially imported LNG is re-exported during the transportation process.

        Parameters
        ----------

         id: Optional[int], optional
             Event ID, by default None
         id_gt: Optional[int], optional
             filter by '' id > x '', by default None
         id_gte: Optional[int], optional
             filter by id, by default None
         id_lt: Optional[int], optional
             filter by id, by default None
         id_lte: Optional[int], optional
             filter by id, by default None
         reexport_date: Optional[datetime], optional
             Load date of the partial re-exporting, by default None
         reexport_date_gt: Optional[datetime], optional
             filter by '' reexport_date > x '', by default None
         reexport_date_gte: Optional[datetime], optional
             filter by reexport_date, by default None
         reexport_date_lt: Optional[datetime], optional
             filter by reexport_date, by default None
         reexport_date_lte: Optional[datetime], optional
             filter by reexport_date, by default None
         supply_market: Optional[Union[list[str], Series[str], str]]
             Supply market of the re-export of the cargo, by default None
         reexport_port: Optional[Union[list[str], Series[str], str]]
             Source port of the re-export of the cargo, by default None
         modified_date: Optional[datetime], optional
             Event record latest modified date., by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

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
        filter_params.append(list_to_filter("reexportDate", reexport_date))
        if reexport_date_gt is not None:
            filter_params.append(f'reexportDate > "{reexport_date_gt}"')
        if reexport_date_gte is not None:
            filter_params.append(f'reexportDate >= "{reexport_date_gte}"')
        if reexport_date_lt is not None:
            filter_params.append(f'reexportDate < "{reexport_date_lt}"')
        if reexport_date_lte is not None:
            filter_params.append(f'reexportDate <= "{reexport_date_lte}"')
        filter_params.append(list_to_filter("supplyMarket", supply_market))
        filter_params.append(list_to_filter("reexportPort", reexport_port))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/cargo/events/partial-reexport",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_cargo_waterborne_trade(
        self,
        *,
        date_loaded: Optional[date] = None,
        date_loaded_lt: Optional[date] = None,
        date_loaded_lte: Optional[date] = None,
        date_loaded_gt: Optional[date] = None,
        date_loaded_gte: Optional[date] = None,
        date_arrived: Optional[date] = None,
        date_arrived_lt: Optional[date] = None,
        date_arrived_lte: Optional[date] = None,
        date_arrived_gt: Optional[date] = None,
        date_arrived_gte: Optional[date] = None,
        delivery_vessel_name: Optional[Union[list[str], Series[str], str]] = None,
        imo_number: Optional[int] = None,
        imo_number_lt: Optional[int] = None,
        imo_number_lte: Optional[int] = None,
        imo_number_gt: Optional[int] = None,
        imo_number_gte: Optional[int] = None,
        supply_market: Optional[Union[list[str], Series[str], str]] = None,
        supply_plant: Optional[Union[list[str], Series[str], str]] = None,
        supply_project: Optional[Union[list[str], Series[str], str]] = None,
        trade_route: Optional[Union[list[str], Series[str], str]] = None,
        reexport_port: Optional[Union[list[str], Series[str], str]] = None,
        receiving_port: Optional[Union[list[str], Series[str], str]] = None,
        receiving_market: Optional[Union[list[str], Series[str], str]] = None,
        participant1_charterer: Optional[Union[list[str], Series[str], str]] = None,
        participant2_buyer: Optional[Union[list[str], Series[str], str]] = None,
        is_spot_or_short_term: Optional[Union[list[str], Series[str], str]] = None,
        initial_vessel_name: Optional[Union[list[str], Series[str], str]] = None,
        transshipment_port: Optional[Union[list[str], Series[str], str]] = None,
        transshipment_date: Optional[date] = None,
        transshipment_date_lt: Optional[date] = None,
        transshipment_date_lte: Optional[date] = None,
        transshipment_date_gt: Optional[date] = None,
        transshipment_date_gte: Optional[date] = None,
        capacity_or_volume_type: Optional[Union[list[str], Series[str], str]] = None,
        capacity_or_volume_uom: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Table of LNG cargos by load and arrival date as well as numerous volumetric, geographic, and commercial attributes

        Parameters
        ----------

         date_loaded: Optional[date], optional
             The date when the LNG cargo was loaded onto the vessel, by default None
         date_loaded_gt: Optional[date], optional
             filter by '' date_loaded > x '', by default None
         date_loaded_gte: Optional[date], optional
             filter by date_loaded, by default None
         date_loaded_lt: Optional[date], optional
             filter by date_loaded, by default None
         date_loaded_lte: Optional[date], optional
             filter by date_loaded, by default None
         date_arrived: Optional[date], optional
             The date when the vessel arrived at its destination, by default None
         date_arrived_gt: Optional[date], optional
             filter by '' date_arrived > x '', by default None
         date_arrived_gte: Optional[date], optional
             filter by date_arrived, by default None
         date_arrived_lt: Optional[date], optional
             filter by date_arrived, by default None
         date_arrived_lte: Optional[date], optional
             filter by date_arrived, by default None
         delivery_vessel_name: Optional[Union[list[str], Series[str], str]]
             The name of the vessel used for delivery, by default None
         imo_number: Optional[int], optional
             The International Maritime Organization number of the vessel, by default None
         imo_number_gt: Optional[int], optional
             filter by '' imo_number > x '', by default None
         imo_number_gte: Optional[int], optional
             filter by imo_number, by default None
         imo_number_lt: Optional[int], optional
             filter by imo_number, by default None
         imo_number_lte: Optional[int], optional
             filter by imo_number, by default None
         supply_market: Optional[Union[list[str], Series[str], str]]
             The market or country where the LNG was supplied from, by default None
         supply_plant: Optional[Union[list[str], Series[str], str]]
             The LNG plant from which the LNG was supplied, by default None
         supply_project: Optional[Union[list[str], Series[str], str]]
             The LNG project from which the LNG was supplied, by default None
         trade_route: Optional[Union[list[str], Series[str], str]]
             The route taken by the vessel for the trade, by default None
         reexport_port: Optional[Union[list[str], Series[str], str]]
             The port where the LNG was re-exported, if applicable, by default None
         receiving_port: Optional[Union[list[str], Series[str], str]]
             The port where the LNG was received, by default None
         receiving_market: Optional[Union[list[str], Series[str], str]]
             The market or country where the LNG was received, by default None
         participant1_charterer: Optional[Union[list[str], Series[str], str]]
             The charterer of the vessel, by default None
         participant2_buyer: Optional[Union[list[str], Series[str], str]]
             The buyer of the LNG, by default None
         is_spot_or_short_term: Optional[Union[list[str], Series[str], str]]
             Indicates whether the trade was spot or short-term, long-term, or unknown, by default None
         initial_vessel_name: Optional[Union[list[str], Series[str], str]]
             The original name of the vessel before any transshipment, by default None
         transshipment_port: Optional[Union[list[str], Series[str], str]]
             The port where transshipment occurred, if applicable, by default None
         transshipment_date: Optional[date], optional
             The date when transshipment occurred, if applicable, by default None
         transshipment_date_gt: Optional[date], optional
             filter by '' transshipment_date > x '', by default None
         transshipment_date_gte: Optional[date], optional
             filter by transshipment_date, by default None
         transshipment_date_lt: Optional[date], optional
             filter by transshipment_date, by default None
         transshipment_date_lte: Optional[date], optional
             filter by transshipment_date, by default None
         capacity_or_volume_type: Optional[Union[list[str], Series[str], str]]
             Indicates different cargo volume types or the capacity of the vessel for a specific cargo, by default None
         capacity_or_volume_uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure for the cargo volume type or the capacity of the vessel, by default None
         modified_date: Optional[datetime], optional
             The date when the trade record was last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("dateLoaded", date_loaded))
        if date_loaded_gt is not None:
            filter_params.append(f'dateLoaded > "{date_loaded_gt}"')
        if date_loaded_gte is not None:
            filter_params.append(f'dateLoaded >= "{date_loaded_gte}"')
        if date_loaded_lt is not None:
            filter_params.append(f'dateLoaded < "{date_loaded_lt}"')
        if date_loaded_lte is not None:
            filter_params.append(f'dateLoaded <= "{date_loaded_lte}"')
        filter_params.append(list_to_filter("dateArrived", date_arrived))
        if date_arrived_gt is not None:
            filter_params.append(f'dateArrived > "{date_arrived_gt}"')
        if date_arrived_gte is not None:
            filter_params.append(f'dateArrived >= "{date_arrived_gte}"')
        if date_arrived_lt is not None:
            filter_params.append(f'dateArrived < "{date_arrived_lt}"')
        if date_arrived_lte is not None:
            filter_params.append(f'dateArrived <= "{date_arrived_lte}"')
        filter_params.append(list_to_filter("deliveryVesselName", delivery_vessel_name))
        filter_params.append(list_to_filter("imoNumber", imo_number))
        if imo_number_gt is not None:
            filter_params.append(f'imoNumber > "{imo_number_gt}"')
        if imo_number_gte is not None:
            filter_params.append(f'imoNumber >= "{imo_number_gte}"')
        if imo_number_lt is not None:
            filter_params.append(f'imoNumber < "{imo_number_lt}"')
        if imo_number_lte is not None:
            filter_params.append(f'imoNumber <= "{imo_number_lte}"')
        filter_params.append(list_to_filter("supplyMarket", supply_market))
        filter_params.append(list_to_filter("supplyPlant", supply_plant))
        filter_params.append(list_to_filter("supplyProject", supply_project))
        filter_params.append(list_to_filter("tradeRoute", trade_route))
        filter_params.append(list_to_filter("reexportPort", reexport_port))
        filter_params.append(list_to_filter("receivingPort", receiving_port))
        filter_params.append(list_to_filter("receivingMarket", receiving_market))
        filter_params.append(
            list_to_filter("participant1Charterer", participant1_charterer)
        )
        filter_params.append(list_to_filter("participant2Buyer", participant2_buyer))
        filter_params.append(list_to_filter("isSpotOrShortTerm", is_spot_or_short_term))
        filter_params.append(list_to_filter("initialVesselName", initial_vessel_name))
        filter_params.append(list_to_filter("transshipmentPort", transshipment_port))
        filter_params.append(list_to_filter("transshipmentDate", transshipment_date))
        if transshipment_date_gt is not None:
            filter_params.append(f'transshipmentDate > "{transshipment_date_gt}"')
        if transshipment_date_gte is not None:
            filter_params.append(f'transshipmentDate >= "{transshipment_date_gte}"')
        if transshipment_date_lt is not None:
            filter_params.append(f'transshipmentDate < "{transshipment_date_lt}"')
        if transshipment_date_lte is not None:
            filter_params.append(f'transshipmentDate <= "{transshipment_date_lte}"')
        filter_params.append(
            list_to_filter("capacityOrVolumeType", capacity_or_volume_type)
        )
        filter_params.append(
            list_to_filter("capacityOrVolumeUom", capacity_or_volume_uom)
        )
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/cargo/waterborne-trade",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
