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
from typing import Union, Optional, List
from requests import Response
from spgci.api_client import get_data, Paginator
from spgci.utilities import list_to_filter, convert_date_to_filter_exp
from pandas import Series, DataFrame, to_datetime  # type: ignore
import pandas as pd
from packaging.version import parse
from datetime import date, datetime


class LNGGlobalAnalytics:
    """
    Lng Tenders Data - Bids, Offers, Trades

    Includes
    --------

    ``get_tenders()`` to fetch LNG Tenders based on tenderStatus, cargo_type, contract_type, contract_option.\n
    """

    _endpoint = "lng/v1/"

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
            "startDate",
            "endDate",
            "createDate",
            "reportDate",
            "shareholderModifiedDate",
            "shareModifiedDate",
            "ownershipStartDate",
            "announcedStartDate",
            "estimatedStartDate",
            "capexDate",
            "offlineDate",
            "statusModifiedDate",
            "capacityModifiedDate",
            "announcedStartDateModifiedDate",
            "estimatedStartDateModifiedDate",
            "announcedStartDateAtFinalInvestmentDecision",
            "latestAnnouncedFinalInvestmentDecisionDate",
            "estimatedFirstCargoDate",
            "estimatedFinalInvestmentDecisionDate",
            "originalSigningDate",
            "preliminarySigningDate",
            "contractPriceAsOfDate",
            "buyerModifiedDate",
            "announcedStartModifiedDate",
            "lengthModifiedDate",
            "publishedVolumeModifiedDate",
            "estimatedBuildoutModifiedDate",
            "contractStartDate",
            "startModifiedDate",
            "capacityOwnerModifiedDate",
            "typeModifiedDate",
            "announcedStartDateOriginal",
            "datePhaseFirstAnnounced",
            "contractDate",
            "deliveryDate",
            "retiredDate",
            "buildoutMonthEstimated",
            "createdDateEstimated",
            "modifiedDateEstimated",
            "estimatedEndDate",
            "originalSigning",
            "buildoutMonthAnnounced",
            "createdDateAnnounced",
            "modifiedDateAnnounced",
            "period",
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
         page_size: int = 5000,
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
         page_size: int = 5000,
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
         page_size: int = 5000,
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
         page_size: int = 5000,
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
         page_size: int = 5000,
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
         page_size: int = 5000,
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
        page_size: int = 5000,
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
         page_size: int = 5000,
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
         page_size: int = 5000,
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
         page_size: int = 5000,
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
         page_size: int = 5000,
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

    def get_outages(
        self,
        *,
        liquefaction_project_id: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_project_name: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_train_id: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_train_name: Optional[Union[list[str], Series[str], str]] = None,
        alert_id: Optional[Union[list[str], Series[str], str]] = None,
        alert_name: Optional[Union[list[str], Series[str], str]] = None,
        status_id: Optional[Union[list[str], Series[str], str]] = None,
        status_name: Optional[Union[list[str], Series[str], str]] = None,
        confidence_level_id: Optional[Union[list[str], Series[str], str]] = None,
        confidence_level_name: Optional[Union[list[str], Series[str], str]] = None,
        report_date: Optional[datetime] = None,
        report_date_lt: Optional[datetime] = None,
        report_date_lte: Optional[datetime] = None,
        report_date_gt: Optional[datetime] = None,
        report_date_gte: Optional[datetime] = None,
        report_date_comment: Optional[Union[list[str], Series[str], str]] = None,
        start_date: Optional[datetime] = None,
        start_date_lt: Optional[datetime] = None,
        start_date_lte: Optional[datetime] = None,
        start_date_gt: Optional[datetime] = None,
        start_date_gte: Optional[datetime] = None,
        start_date_comment: Optional[Union[list[str], Series[str], str]] = None,
        end_date: Optional[datetime] = None,
        end_date_lt: Optional[datetime] = None,
        end_date_lte: Optional[datetime] = None,
        end_date_gt: Optional[datetime] = None,
        end_date_gte: Optional[datetime] = None,
        end_date_comment: Optional[Union[list[str], Series[str], str]] = None,
        create_date: Optional[datetime] = None,
        create_date_lt: Optional[datetime] = None,
        create_date_lte: Optional[datetime] = None,
        create_date_gt: Optional[datetime] = None,
        create_date_gte: Optional[datetime] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        total_capacity: Optional[float] = None,
        total_capacity_lt: Optional[float] = None,
        total_capacity_lte: Optional[float] = None,
        total_capacity_gt: Optional[float] = None,
        total_capacity_gte: Optional[float] = None,
        available_capacity: Optional[float] = None,
        available_capacity_lt: Optional[float] = None,
        available_capacity_lte: Optional[float] = None,
        available_capacity_gt: Optional[float] = None,
        available_capacity_gte: Optional[float] = None,
        offline_capacity: Optional[float] = None,
        offline_capacity_lt: Optional[float] = None,
        offline_capacity_lte: Optional[float] = None,
        offline_capacity_gt: Optional[float] = None,
        offline_capacity_gte: Optional[float] = None,
        offline_capacity_comment: Optional[Union[list[str], Series[str], str]] = None,
        run_rate: Optional[float] = None,
        run_rate_lt: Optional[float] = None,
        run_rate_lte: Optional[float] = None,
        run_rate_gt: Optional[float] = None,
        run_rate_gte: Optional[float] = None,
        run_loss: Optional[float] = None,
        run_loss_lt: Optional[float] = None,
        run_loss_lte: Optional[float] = None,
        run_loss_gt: Optional[float] = None,
        run_loss_gte: Optional[float] = None,
        unit_of_measure: Optional[Union[list[str], Series[str], str]] = None,
        general_comment: Optional[Union[list[str], Series[str], str]] = None,
        infrastructure_type: Optional[Union[list[str], Series[str], str]] = None,
        commodity_name: Optional[Union[list[str], Series[str], str]] = None,
        outage_id: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        The Liquefaction Outages database stores the listings of incidents that affect
        production at LNG liquefaction plants around the world,
        whether it be for a planned maintenance period or an unplanned outage caused by technical or upstream issues,
        industrial action, inclement weather, or any other event that would take part of or an entire plant offline.
        The database is updated at least twice a week, or at a more frequent ad-hoc basis
        during periods when information on existing and new outages is released.

        Parameters
        ----------

         liquefaction_project_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier for a liquefaction project, often used for tracking and categorizing purposes., by default None
         liquefaction_project_name: Optional[Union[list[str], Series[str], str]]
             The name of the liquefaction project., by default None
         liquefaction_train_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier for a specific train within a liquefaction project, used for differentiation and categorization., by default None
         liquefaction_train_name: Optional[Union[list[str], Series[str], str]]
             The name assigned to a specific train within a liquefaction project., by default None
         alert_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier associated with an alert or notification, serving as a reference point for identifying and managing alerts., by default None
         alert_name: Optional[Union[list[str], Series[str], str]]
             The name or title of the alert, providing a concise description of the outages being reported., by default None
         status_id: Optional[Union[list[str], Series[str], str]]
             An identifier representing the current state or status of the outages in question., by default None
         status_name: Optional[Union[list[str], Series[str], str]]
             The descriptive name of the current status or state of the outages., by default None
         confidence_level_id: Optional[Union[list[str], Series[str], str]]
             An identifier representing the confidence level or degree of certainty associated with the provided outages data., by default None
         confidence_level_name: Optional[Union[list[str], Series[str], str]]
             The descriptive name of the confidence level associated with the data, indicating the reliability or certainty., by default None
         report_date: Optional[datetime], optional
             The date on which the report, alert, or information was generated or recorded., by default None
         report_date_gt: Optional[datetime], optional
             filter by `report_date > x`, by default None
         report_date_gte: Optional[datetime], optional
             filter by `report_date >= x`, by default None
         report_date_lt: Optional[datetime], optional
             filter by `report_date < x`, by default None
         report_date_lte: Optional[datetime], optional
             filter by `report_date <= x`, by default None
         report_date_comment: Optional[Union[list[str], Series[str], str]]
             Additional commentary or explanation related to the report date, providing context or details., by default None
         start_date: Optional[datetime], optional
             The date marking the beginning of the LNG outage., by default None
         start_date_gt: Optional[datetime], optional
             filter by `start_date > x`, by default None
         start_date_gte: Optional[datetime], optional
             filter by `start_date >= x`, by default None
         start_date_lt: Optional[datetime], optional
             filter by `start_date < x`, by default None
         start_date_lte: Optional[datetime], optional
             filter by `start_date <= x`, by default None
         start_date_comment: Optional[Union[list[str], Series[str], str]]
             Extra information or context related to the start date, explaining any relevant details., by default None
         end_date: Optional[datetime], optional
             The date signifying the conclusion or end of a particular LNG outage., by default None
         end_date_gt: Optional[datetime], optional
             filter by `end_date > x` , by default None
         end_date_gte: Optional[datetime], optional
             filter by `end_date >= x`, by default None
         end_date_lt: Optional[datetime], optional
             filter by `end_date < x`, by default None
         end_date_lte: Optional[datetime], optional
             filter by `end_date <= x`, by default None
         end_date_comment: Optional[Union[list[str], Series[str], str]]
             Supplementary notes or details regarding the end date, offering further insight., by default None
         create_date: Optional[datetime], optional
             The initial outage capture date., by default None
         create_date_gt: Optional[datetime], optional
             filter by `create_date > x`, by default None
         create_date_gte: Optional[datetime], optional
             filter by `create_date >= x`, by default None
         create_date_lt: Optional[datetime], optional
             filter by `create_date < x`, by default None
         create_date_lte: Optional[datetime], optional
             filter by `create_date <= x`, by default None
         modified_date: Optional[datetime], optional
             The latest date of modification for the outage., by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         total_capacity: Optional[float], optional
             The overall capacity or quantity associated with a certain measurement or parameter., by default None
         total_capacity_gt: Optional[float], optional
             filter by `total_capacity > x`, by default None
         total_capacity_gte: Optional[float], optional
             filter by `total_capacity >= x`, by default None
         total_capacity_lt: Optional[float], optional
             filter by `total_capacity < x`, by default None
         total_capacity_lte: Optional[float], optional
             filter by `total_capacity <= x`, by default None
         available_capacity: Optional[float], optional
             Available capacity is calculated by subtracting offline capacity from total capacity., by default None
         available_capacity_gt: Optional[float], optional
             filter by `available_capacity > x` , by default None
         available_capacity_gte: Optional[float], optional
             filter by `available_capacity >= x`, by default None
         available_capacity_lt: Optional[float], optional
             filter by `available_capacity < x`, by default None
         available_capacity_lte: Optional[float], optional
             filter by `available_capacity <= x`, by default None
         offline_capacity: Optional[float], optional
             The portion of total capacity that has been impacted by an outage., by default None
         offline_capacity_gt: Optional[float], optional
             filter by `offline_capacity > x`, by default None
         offline_capacity_gte: Optional[float], optional
             filter by `offline_capacity >= x`, by default None
         offline_capacity_lt: Optional[float], optional
             filter by `offline_capacity < x`, by default None
         offline_capacity_lte: Optional[float], optional
             filter by `offline_capacity <= x`, by default None
         offline_capacity_comment: Optional[Union[list[str], Series[str], str]]
             Comments regarding the impacted capacity value. , by default None
         run_rate: Optional[float], optional
             The rate or speed at which an outage is occurring or has occurred., by default None
         run_rate_gt: Optional[float], optional
             filter by `run_rate > x`, by default None
         run_rate_gte: Optional[float], optional
             filter by `run_rate >= x`, by default None
         run_rate_lt: Optional[float], optional
             filter by `run_rate < x`, by default None
         run_rate_lte: Optional[float], optional
             filter by `run_rate <= x`, by default None
         run_loss: Optional[float], optional
             The reduction or loss in efficiency due to an outage., by default None
         run_loss_gt: Optional[float], optional
             filter by `run_loss > x`, by default None
         run_loss_gte: Optional[float], optional
             filter by `run_loss >= x`, by default None
         run_loss_lt: Optional[float], optional
             filter by `run_loss < x`, by default None
         run_loss_lte: Optional[float], optional
             filter by `run_loss <= x`, by default None
         unit_of_measure: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used to quantify the values provided in the data., by default None
         general_comment: Optional[Union[list[str], Series[str], str]]
             A general comment or note providing additional information, context, or insights related to an outage., by default None
         infrastructure_type: Optional[Union[list[str], Series[str], str]]
             Indicates the type or category of infrastructure associated with the outage., by default None
         commodity_name: Optional[Union[list[str], Series[str], str]]
             The name of the specific commodity (e.g Liquefied Natural Gas, LNG)., by default None
         outage_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier assigned to a particular outage event, used for tracking and referencing purposes., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """
        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("liquefactionProjectId", liquefaction_project_id)
        )
        filter_params.append(
            list_to_filter("liquefactionProjectName", liquefaction_project_name)
        )
        filter_params.append(
            list_to_filter("liquefactionTrainId", liquefaction_train_id)
        )
        filter_params.append(
            list_to_filter("liquefactionTrainName", liquefaction_train_name)
        )
        filter_params.append(list_to_filter("alertId", alert_id))
        filter_params.append(list_to_filter("alertName", alert_name))
        filter_params.append(list_to_filter("statusId", status_id))
        filter_params.append(list_to_filter("statusName", status_name))
        filter_params.append(list_to_filter("confidenceLevelId", confidence_level_id))
        filter_params.append(
            list_to_filter("confidenceLevelName", confidence_level_name)
        )
        filter_params.append(list_to_filter("reportDate", report_date))
        if report_date_gt is not None:
            filter_params.append(f'reportDate > "{report_date_gt}"')
        if report_date_gte is not None:
            filter_params.append(f'reportDate >= "{report_date_gte}"')
        if report_date_lt is not None:
            filter_params.append(f'reportDate < "{report_date_lt}"')
        if report_date_lte is not None:
            filter_params.append(f'reportDate <= "{report_date_lte}"')
        filter_params.append(list_to_filter("reportDateComment", report_date_comment))
        filter_params.append(list_to_filter("startDate", start_date))
        if start_date_gt is not None:
            filter_params.append(f'startDate > "{start_date_gt}"')
        if start_date_gte is not None:
            filter_params.append(f'startDate >= "{start_date_gte}"')
        if start_date_lt is not None:
            filter_params.append(f'startDate < "{start_date_lt}"')
        if start_date_lte is not None:
            filter_params.append(f'startDate <= "{start_date_lte}"')
        filter_params.append(list_to_filter("startDateComment", start_date_comment))
        filter_params.append(list_to_filter("endDate", end_date))
        if end_date_gt is not None:
            filter_params.append(f'endDate > "{end_date_gt}"')
        if end_date_gte is not None:
            filter_params.append(f'endDate >= "{end_date_gte}"')
        if end_date_lt is not None:
            filter_params.append(f'endDate < "{end_date_lt}"')
        if end_date_lte is not None:
            filter_params.append(f'endDate <= "{end_date_lte}"')
        filter_params.append(list_to_filter("endDateComment", end_date_comment))
        filter_params.append(list_to_filter("createDate", create_date))
        if create_date_gt is not None:
            filter_params.append(f'createDate > "{create_date_gt}"')
        if create_date_gte is not None:
            filter_params.append(f'createDate >= "{create_date_gte}"')
        if create_date_lt is not None:
            filter_params.append(f'createDate < "{create_date_lt}"')
        if create_date_lte is not None:
            filter_params.append(f'createDate <= "{create_date_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')
        filter_params.append(list_to_filter("totalCapacity", total_capacity))
        if total_capacity_gt is not None:
            filter_params.append(f'totalCapacity > "{total_capacity_gt}"')
        if total_capacity_gte is not None:
            filter_params.append(f'totalCapacity >= "{total_capacity_gte}"')
        if total_capacity_lt is not None:
            filter_params.append(f'totalCapacity < "{total_capacity_lt}"')
        if total_capacity_lte is not None:
            filter_params.append(f'totalCapacity <= "{total_capacity_lte}"')
        filter_params.append(list_to_filter("availableCapacity", available_capacity))
        if available_capacity_gt is not None:
            filter_params.append(f'availableCapacity > "{available_capacity_gt}"')
        if available_capacity_gte is not None:
            filter_params.append(f'availableCapacity >= "{available_capacity_gte}"')
        if available_capacity_lt is not None:
            filter_params.append(f'availableCapacity < "{available_capacity_lt}"')
        if available_capacity_lte is not None:
            filter_params.append(f'availableCapacity <= "{available_capacity_lte}"')
        filter_params.append(list_to_filter("offlineCapacity", offline_capacity))
        if offline_capacity_gt is not None:
            filter_params.append(f'offlineCapacity > "{offline_capacity_gt}"')
        if offline_capacity_gte is not None:
            filter_params.append(f'offlineCapacity >= "{offline_capacity_gte}"')
        if offline_capacity_lt is not None:
            filter_params.append(f'offlineCapacity < "{offline_capacity_lt}"')
        if offline_capacity_lte is not None:
            filter_params.append(f'offlineCapacity <= "{offline_capacity_lte}"')
        filter_params.append(
            list_to_filter("offlineCapacityComment", offline_capacity_comment)
        )
        filter_params.append(list_to_filter("runRate", run_rate))
        if run_rate_gt is not None:
            filter_params.append(f'runRate > "{run_rate_gt}"')
        if run_rate_gte is not None:
            filter_params.append(f'runRate >= "{run_rate_gte}"')
        if run_rate_lt is not None:
            filter_params.append(f'runRate < "{run_rate_lt}"')
        if run_rate_lte is not None:
            filter_params.append(f'runRate <= "{run_rate_lte}"')
        filter_params.append(list_to_filter("runLoss", run_loss))
        if run_loss_gt is not None:
            filter_params.append(f'runLoss > "{run_loss_gt}"')
        if run_loss_gte is not None:
            filter_params.append(f'runLoss >= "{run_loss_gte}"')
        if run_loss_lt is not None:
            filter_params.append(f'runLoss < "{run_loss_lt}"')
        if run_loss_lte is not None:
            filter_params.append(f'runLoss <= "{run_loss_lte}"')
        filter_params.append(list_to_filter("unitOfMeasure", unit_of_measure))
        filter_params.append(list_to_filter("generalComment", general_comment))
        filter_params.append(list_to_filter("infrastructureType", infrastructure_type))
        filter_params.append(list_to_filter("commodityName", commodity_name))
        filter_params.append(list_to_filter("outageId", outage_id))

        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        response = get_data(
            path=f"/lng/v1/outages/",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_liquefaction_economics(
        self,
        *,
        economic_group: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_project: Optional[Union[list[str], Series[str], str]] = None,
        supply_market: Optional[Union[list[str], Series[str], str]] = None,
        start_year: Optional[int] = None,
        start_year_lt: Optional[int] = None,
        start_year_lte: Optional[int] = None,
        start_year_gt: Optional[int] = None,
        start_year_gte: Optional[int] = None,
        capital_recovered: Optional[Union[list[str], Series[str], str]] = None,
        midstream_discount_rate: Optional[Union[list[str], Series[str], str]] = None,
        upstream_discount_rate: Optional[Union[list[str], Series[str], str]] = None,
        upstream_pricing_scenario: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_capacity: Optional[float] = None,
        liquefaction_capacity_lt: Optional[float] = None,
        liquefaction_capacity_lte: Optional[float] = None,
        liquefaction_capacity_gt: Optional[float] = None,
        liquefaction_capacity_gte: Optional[float] = None,
        economics_category: Optional[Union[list[str], Series[str], str]] = None,
        economics_category_uom: Optional[Union[list[str], Series[str], str]] = None,
        economics_category_currency: Optional[
            Union[list[str], Series[str], str]
        ] = None,
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
        Table of project breakeven costs for upstream, pipeline, and liquefaction segments.
        Costs are estimated for various rates of returns and oil price scenarios

        Parameters
        ----------

         economic_group: Optional[Union[list[str], Series[str], str]]
             Classification of the project based on economic characteristics, by default None
         liquefaction_project: Optional[Union[list[str], Series[str], str]]
             Name of the specific liquefaction project, by default None
         supply_market: Optional[Union[list[str], Series[str], str]]
             The market from which the feedstock is sourced, by default None
         start_year: Optional[int], optional
             Year of project start-up, by default None
         start_year_gt: Optional[int], optional
             filter by `start_year > x`, by default None
         start_year_gte: Optional[int], optional
             filter by `start_year >= x`, by default None
         start_year_lt: Optional[int], optional
             filter by `start_year < x`, by default None
         start_year_lte: Optional[int], optional
             filter by `start_year <= x`, by default None
         capital_recovered: Optional[Union[list[str], Series[str], str]]
             Yes or no whether all capital is recovered, by default None
         midstream_discount_rate: Optional[Union[list[str], Series[str], str]]
             Discount rate applied to midstream cash flows, by default None
         upstream_discount_rate: Optional[Union[list[str], Series[str], str]]
             Discount rate applied to upstream cash flows, by default None
         upstream_pricing_scenario: Optional[Union[list[str], Series[str], str]]
             The name of the category defining the upstream modeling scenario, by default None
         liquefaction_capacity: Optional[float], optional
             Annual liquefaction capacity, by default None
         liquefaction_capacity_gt: Optional[float], optional
             filter by `liquefaction_capacity > x`, by default None
         liquefaction_capacity_gte: Optional[float], optional
             filter by `liquefaction_capacity >= x`, by default None
         liquefaction_capacity_lt: Optional[float], optional
             filter by `liquefaction_capacity < x`, by default None
         liquefaction_capacity_lte: Optional[float], optional
             filter by `liquefaction_capacity <= x`, by default None
         economics_category: Optional[Union[list[str], Series[str], str]]
             The name of the cost component in liquefaction project economic analysis, by default None
         economics_category_uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the cost for the specific component of liquefaction project economic analysis, by default None
         economics_category_currency: Optional[Union[list[str], Series[str], str]]
             Currency of the cost for the specific component of liquefaction project economic analysis, by default None
         modified_date: Optional[datetime], optional
             Liquefaction Economics record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("economicGroup", economic_group))
        filter_params.append(
            list_to_filter("liquefactionProject", liquefaction_project)
        )
        filter_params.append(list_to_filter("supplyMarket", supply_market))
        filter_params.append(list_to_filter("startYear", start_year))
        if start_year_gt is not None:
            filter_params.append(f'startYear > "{start_year_gt}"')
        if start_year_gte is not None:
            filter_params.append(f'startYear >= "{start_year_gte}"')
        if start_year_lt is not None:
            filter_params.append(f'startYear < "{start_year_lt}"')
        if start_year_lte is not None:
            filter_params.append(f'startYear <= "{start_year_lte}"')
        filter_params.append(list_to_filter("capitalRecovered", capital_recovered))
        filter_params.append(
            list_to_filter("midstreamDiscountRate", midstream_discount_rate)
        )
        filter_params.append(
            list_to_filter("upstreamDiscountRate", upstream_discount_rate)
        )
        filter_params.append(
            list_to_filter("upstreamPricingScenario", upstream_pricing_scenario)
        )
        filter_params.append(
            list_to_filter("liquefactionCapacity", liquefaction_capacity)
        )
        if liquefaction_capacity_gt is not None:
            filter_params.append(f'liquefactionCapacity > "{liquefaction_capacity_gt}"')
        if liquefaction_capacity_gte is not None:
            filter_params.append(
                f'liquefactionCapacity >= "{liquefaction_capacity_gte}"'
            )
        if liquefaction_capacity_lt is not None:
            filter_params.append(f'liquefactionCapacity < "{liquefaction_capacity_lt}"')
        if liquefaction_capacity_lte is not None:
            filter_params.append(
                f'liquefactionCapacity <= "{liquefaction_capacity_lte}"'
            )
        filter_params.append(list_to_filter("economicsCategory", economics_category))
        filter_params.append(
            list_to_filter("economicsCategoryUom", economics_category_uom)
        )
        filter_params.append(
            list_to_filter("economicsCategoryCurrency", economics_category_currency)
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
            path=f"/lng/v1/analytics/assets-contracts/liquefaction-economics",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_country_coasts(
        self,
        *,
        country_coast: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        basin: Optional[Union[list[str], Series[str], str]] = None,
        region_export: Optional[Union[list[str], Series[str], str]] = None,
        region_import: Optional[Union[list[str], Series[str], str]] = None,
        region_cross_basin_import: Optional[Union[list[str], Series[str], str]] = None,
        region_general: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides information on the countries and coasts as well as their geographic regions. This is used for classification purposes

        Parameters
        ----------

         country_coast: Optional[Union[list[str], Series[str], str]]
             Specific coast and country identity, by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name of the country associated with the coast, by default None
         basin: Optional[Union[list[str], Series[str], str]]
             The geographic basin where the country coast is located, by default None
         region_export: Optional[Union[list[str], Series[str], str]]
             Regional classification for export country coasts, by default None
         region_import: Optional[Union[list[str], Series[str], str]]
             Regional classification for import country coasts, by default None
         region_cross_basin_import: Optional[Union[list[str], Series[str], str]]
             Cross-basin trade regional classification for different country coasts, by default None
         region_general: Optional[Union[list[str], Series[str], str]]
             General regional classification for country coasts, by default None
         modified_date: Optional[datetime], optional
             Country coasts record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("countryCoast", country_coast))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("basin", basin))
        filter_params.append(list_to_filter("regionExport", region_export))
        filter_params.append(list_to_filter("regionImport", region_import))
        filter_params.append(
            list_to_filter("regionCrossBasinImport", region_cross_basin_import)
        )
        filter_params.append(list_to_filter("regionGeneral", region_general))
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
            path=f"/lng/v1/analytics/assets-contracts/country-coasts",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_current_liquefaction(
        self,
        *,
        supply_project: Optional[Union[list[str], Series[str], str]] = None,
        export_country: Optional[Union[list[str], Series[str], str]] = None,
        export_basin: Optional[Union[list[str], Series[str], str]] = None,
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
        Table of regional classifications for liquefaction projects.
        This is a support table that can be used to create relationships between other tables

        Parameters
        ----------

         supply_project: Optional[Union[list[str], Series[str], str]]
             Name of the LNG supply project, by default None
         export_country: Optional[Union[list[str], Series[str], str]]
             Country where the supply project is located, by default None
         export_basin: Optional[Union[list[str], Series[str], str]]
             The geographic basin where the supply project is located, by default None
         modified_date: Optional[datetime], optional
             Current liuquefaction record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("supplyProject", supply_project))
        filter_params.append(list_to_filter("exportCountry", export_country))
        filter_params.append(list_to_filter("exportBasin", export_basin))
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
            path=f"/lng/v1/analytics/assets-contracts/current-liquefaction",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_current_regasification(
        self,
        *,
        import_port: Optional[Union[list[str], Series[str], str]] = None,
        import_country: Optional[Union[list[str], Series[str], str]] = None,
        import_basin: Optional[Union[list[str], Series[str], str]] = None,
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
        Table of regional classifications for regasification phases. This is a support table that can be used to create relationships between other tables

        Parameters
        ----------

         import_port: Optional[Union[list[str], Series[str], str]]
             Name of existing (or soon-to-be-existing) regasification terminal, by default None
         import_country: Optional[Union[list[str], Series[str], str]]
             Country where the regasification port is located, by default None
         import_basin: Optional[Union[list[str], Series[str], str]]
             Geological basin associated with the import location, by default None
         modified_date: Optional[datetime], optional
             Current regaisification record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("importPort", import_port))
        filter_params.append(list_to_filter("importCountry", import_country))
        filter_params.append(list_to_filter("importBasin", import_basin))
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
            path=f"/lng/v1/analytics/assets-contracts/current-regasification",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_liquefaction_projects(
        self,
        *,
        liquefaction_project: Optional[Union[list[str], Series[str], str]] = None,
        country_coast: Optional[Union[list[str], Series[str], str]] = None,
        supply_market: Optional[Union[list[str], Series[str], str]] = None,
        supply_basin: Optional[Union[list[str], Series[str], str]] = None,
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
        List of liquefaction projects with their IDs and associated country coasts

        Parameters
        ----------

         liquefaction_project: Optional[Union[list[str], Series[str], str]]
             Name of the liquefaction project, by default None
         country_coast: Optional[Union[list[str], Series[str], str]]
             Country and coast identity where the project is located, by default None
         supply_market: Optional[Union[list[str], Series[str], str]]
             Market where the liquefaction project is located, by default None
         supply_basin: Optional[Union[list[str], Series[str], str]]
             Geographic basin where the liquefaction project is located, by default None
         modified_date: Optional[datetime], optional
             Liquefication projects record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("liquefactionProject", liquefaction_project)
        )
        filter_params.append(list_to_filter("countryCoast", country_coast))
        filter_params.append(list_to_filter("supplyMarket", supply_market))
        filter_params.append(list_to_filter("supplyBasin", supply_basin))
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
            path=f"/lng/v1/analytics/assets-contracts/liquefaction-projects",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_liquefaction_train_ownership(
        self,
        *,
        liquefaction_train: Optional[Union[list[str], Series[str], str]] = None,
        shareholder: Optional[Union[list[str], Series[str], str]] = None,
        share: Optional[float] = None,
        share_lt: Optional[float] = None,
        share_lte: Optional[float] = None,
        share_gt: Optional[float] = None,
        share_gte: Optional[float] = None,
        created_date: Optional[datetime] = None,
        created_date_lt: Optional[datetime] = None,
        created_date_lte: Optional[datetime] = None,
        created_date_gt: Optional[datetime] = None,
        created_date_gte: Optional[datetime] = None,
        shareholder_modified_date: Optional[datetime] = None,
        shareholder_modified_date_lt: Optional[datetime] = None,
        shareholder_modified_date_lte: Optional[datetime] = None,
        shareholder_modified_date_gt: Optional[datetime] = None,
        shareholder_modified_date_gte: Optional[datetime] = None,
        share_modified_date: Optional[datetime] = None,
        share_modified_date_lt: Optional[datetime] = None,
        share_modified_date_lte: Optional[datetime] = None,
        share_modified_date_gt: Optional[datetime] = None,
        share_modified_date_gte: Optional[datetime] = None,
        ownership_start_date: Optional[datetime] = None,
        ownership_start_date_lt: Optional[datetime] = None,
        ownership_start_date_lte: Optional[datetime] = None,
        ownership_start_date_gt: Optional[datetime] = None,
        ownership_start_date_gte: Optional[datetime] = None,
        ownership_end_date: Optional[datetime] = None,
        ownership_end_date_lt: Optional[datetime] = None,
        ownership_end_date_lte: Optional[datetime] = None,
        ownership_end_date_gt: Optional[datetime] = None,
        ownership_end_date_gte: Optional[datetime] = None,
        current_owner: Optional[Union[list[str], Series[str], str]] = None,
        country_coast: Optional[Union[list[str], Series[str], str]] = None,
        supply_market: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_project: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides information on the ownership of liquefaction trains over time

        Parameters
        ----------

         liquefaction_train: Optional[Union[list[str], Series[str], str]]
             Name of the liquefaction train, by default None
         shareholder: Optional[Union[list[str], Series[str], str]]
             Company holding ownership in the liquefaction train, by default None
         share: Optional[float], optional
             Percentage of ownership held by the shareholder, by default None
         share_gt: Optional[float], optional
             filter by `share > x`, by default None
         share_gte: Optional[float], optional
             filter by `share >= x`, by default None
         share_lt: Optional[float], optional
             filter by `share < x`, by default None
         share_lte: Optional[float], optional
             filter by `share <= x`, by default None
         created_date: Optional[datetime], optional
             Date when the ownership record was created, by default None
         created_date_gt: Optional[datetime], optional
             filter by `created_date > x`, by default None
         created_date_gte: Optional[datetime], optional
             filter by `created_date >= x`, by default None
         created_date_lt: Optional[datetime], optional
             filter by `created_date < x`, by default None
         created_date_lte: Optional[datetime], optional
             filter by `created_date <= x`, by default None
         shareholder_modified_date: Optional[datetime], optional
             Date of last modification to shareholder information, by default None
         shareholder_modified_date_gt: Optional[datetime], optional
             filter by `shareholder_modified_date > x`, by default None
         shareholder_modified_date_gte: Optional[datetime], optional
             filter by `shareholder_modified_date >= x`, by default None
         shareholder_modified_date_lt: Optional[datetime], optional
             filter by `shareholder_modified_date < x`, by default None
         shareholder_modified_date_lte: Optional[datetime], optional
             filter by `shareholder_modified_date <= x`, by default None
         share_modified_date: Optional[datetime], optional
             Date of last change to the ownership share, by default None
         share_modified_date_gt: Optional[datetime], optional
             filter by `share_modified_date > x`, by default None
         share_modified_date_gte: Optional[datetime], optional
             filter by `share_modified_date >= x`, by default None
         share_modified_date_lt: Optional[datetime], optional
             filter by `share_modified_date < x`, by default None
         share_modified_date_lte: Optional[datetime], optional
             filter by `share_modified_date <= x`, by default None
         ownership_start_date: Optional[datetime], optional
             Date when the ownership began, by default None
         ownership_start_date_gt: Optional[datetime], optional
             filter by `ownership_start_date > x`, by default None
         ownership_start_date_gte: Optional[datetime], optional
             filter by `ownership_start_date >= x`, by default None
         ownership_start_date_lt: Optional[datetime], optional
             filter by `ownership_start_date < x`, by default None
         ownership_start_date_lte: Optional[datetime], optional
             filter by `ownership_start_date <= x`, by default None
         ownership_end_date: Optional[datetime], optional
             Date when the ownership ended, if applicable, by default None
         ownership_end_date_gt: Optional[datetime], optional
             filter by `ownership_end_date > x`, by default None
         ownership_end_date_gte: Optional[datetime], optional
             filter by `ownership_end_date >= x`, by default None
         ownership_end_date_lt: Optional[datetime], optional
             filter by `ownership_end_date < x`, by default None
         ownership_end_date_lte: Optional[datetime], optional
             filter by `ownership_end_date <= x`, by default None
         current_owner: Optional[Union[list[str], Series[str], str]]
             Indicates if the shareholder is a current owner, by default None
         country_coast: Optional[Union[list[str], Series[str], str]]
             Country and coast identity associated with the liquefaction train, by default None
         supply_market: Optional[Union[list[str], Series[str], str]]
             Market where the liquefaction train is located, by default None
         liquefaction_project: Optional[Union[list[str], Series[str], str]]
             The project to which the train belongs, by default None
         modified_date: Optional[datetime], optional
             Liquefaction train ownership record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("liquefactionTrain", liquefaction_train))
        filter_params.append(list_to_filter("shareholder", shareholder))
        filter_params.append(list_to_filter("share", share))
        if share_gt is not None:
            filter_params.append(f'share > "{share_gt}"')
        if share_gte is not None:
            filter_params.append(f'share >= "{share_gte}"')
        if share_lt is not None:
            filter_params.append(f'share < "{share_lt}"')
        if share_lte is not None:
            filter_params.append(f'share <= "{share_lte}"')
        filter_params.append(list_to_filter("createdDate", created_date))
        if created_date_gt is not None:
            filter_params.append(f'createdDate > "{created_date_gt}"')
        if created_date_gte is not None:
            filter_params.append(f'createdDate >= "{created_date_gte}"')
        if created_date_lt is not None:
            filter_params.append(f'createdDate < "{created_date_lt}"')
        if created_date_lte is not None:
            filter_params.append(f'createdDate <= "{created_date_lte}"')
        filter_params.append(
            list_to_filter("shareholderModifiedDate", shareholder_modified_date)
        )
        if shareholder_modified_date_gt is not None:
            filter_params.append(
                f'shareholderModifiedDate > "{shareholder_modified_date_gt}"'
            )
        if shareholder_modified_date_gte is not None:
            filter_params.append(
                f'shareholderModifiedDate >= "{shareholder_modified_date_gte}"'
            )
        if shareholder_modified_date_lt is not None:
            filter_params.append(
                f'shareholderModifiedDate < "{shareholder_modified_date_lt}"'
            )
        if shareholder_modified_date_lte is not None:
            filter_params.append(
                f'shareholderModifiedDate <= "{shareholder_modified_date_lte}"'
            )
        filter_params.append(list_to_filter("shareModifiedDate", share_modified_date))
        if share_modified_date_gt is not None:
            filter_params.append(f'shareModifiedDate > "{share_modified_date_gt}"')
        if share_modified_date_gte is not None:
            filter_params.append(f'shareModifiedDate >= "{share_modified_date_gte}"')
        if share_modified_date_lt is not None:
            filter_params.append(f'shareModifiedDate < "{share_modified_date_lt}"')
        if share_modified_date_lte is not None:
            filter_params.append(f'shareModifiedDate <= "{share_modified_date_lte}"')
        filter_params.append(list_to_filter("ownershipStartDate", ownership_start_date))
        if ownership_start_date_gt is not None:
            filter_params.append(f'ownershipStartDate > "{ownership_start_date_gt}"')
        if ownership_start_date_gte is not None:
            filter_params.append(f'ownershipStartDate >= "{ownership_start_date_gte}"')
        if ownership_start_date_lt is not None:
            filter_params.append(f'ownershipStartDate < "{ownership_start_date_lt}"')
        if ownership_start_date_lte is not None:
            filter_params.append(f'ownershipStartDate <= "{ownership_start_date_lte}"')
        filter_params.append(list_to_filter("ownershipEndDate", ownership_end_date))
        if ownership_end_date_gt is not None:
            filter_params.append(f'ownershipEndDate > "{ownership_end_date_gt}"')
        if ownership_end_date_gte is not None:
            filter_params.append(f'ownershipEndDate >= "{ownership_end_date_gte}"')
        if ownership_end_date_lt is not None:
            filter_params.append(f'ownershipEndDate < "{ownership_end_date_lt}"')
        if ownership_end_date_lte is not None:
            filter_params.append(f'ownershipEndDate <= "{ownership_end_date_lte}"')
        filter_params.append(list_to_filter("currentOwner", current_owner))
        filter_params.append(list_to_filter("countryCoast", country_coast))
        filter_params.append(list_to_filter("supplyMarket", supply_market))
        filter_params.append(
            list_to_filter("liquefactionProject", liquefaction_project)
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
            path=f"/lng/v1/analytics/assets-contracts/liquefaction-train-ownership",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_liquefaction_trains(
        self,
        *,
        liquefaction_train: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_project: Optional[Union[list[str], Series[str], str]] = None,
        train_status: Optional[Union[list[str], Series[str], str]] = None,
        announced_start_date: Optional[datetime] = None,
        announced_start_date_lt: Optional[datetime] = None,
        announced_start_date_lte: Optional[datetime] = None,
        announced_start_date_gt: Optional[datetime] = None,
        announced_start_date_gte: Optional[datetime] = None,
        estimated_start_date: Optional[datetime] = None,
        estimated_start_date_lt: Optional[datetime] = None,
        estimated_start_date_lte: Optional[datetime] = None,
        estimated_start_date_gt: Optional[datetime] = None,
        estimated_start_date_gte: Optional[datetime] = None,
        offline_date: Optional[datetime] = None,
        offline_date_lt: Optional[datetime] = None,
        offline_date_lte: Optional[datetime] = None,
        offline_date_gt: Optional[datetime] = None,
        offline_date_gte: Optional[datetime] = None,
        green_brownfield: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_technology: Optional[Union[list[str], Series[str], str]] = None,
        created_date: Optional[datetime] = None,
        created_date_lt: Optional[datetime] = None,
        created_date_lte: Optional[datetime] = None,
        created_date_gt: Optional[datetime] = None,
        created_date_gte: Optional[datetime] = None,
        status_modified_date: Optional[datetime] = None,
        status_modified_date_lt: Optional[datetime] = None,
        status_modified_date_lte: Optional[datetime] = None,
        status_modified_date_gt: Optional[datetime] = None,
        status_modified_date_gte: Optional[datetime] = None,
        capacity_modified_date: Optional[datetime] = None,
        capacity_modified_date_lt: Optional[datetime] = None,
        capacity_modified_date_lte: Optional[datetime] = None,
        capacity_modified_date_gt: Optional[datetime] = None,
        capacity_modified_date_gte: Optional[datetime] = None,
        announced_start_date_modified_date: Optional[datetime] = None,
        announced_start_date_modified_date_lt: Optional[datetime] = None,
        announced_start_date_modified_date_lte: Optional[datetime] = None,
        announced_start_date_modified_date_gt: Optional[datetime] = None,
        announced_start_date_modified_date_gte: Optional[datetime] = None,
        estimated_start_date_modified_date: Optional[datetime] = None,
        estimated_start_date_modified_date_lt: Optional[datetime] = None,
        estimated_start_date_modified_date_lte: Optional[datetime] = None,
        estimated_start_date_modified_date_gt: Optional[datetime] = None,
        estimated_start_date_modified_date_gte: Optional[datetime] = None,
        announced_start_date_at_final_investment_decision: Optional[datetime] = None,
        announced_start_date_at_final_investment_decision_lt: Optional[datetime] = None,
        announced_start_date_at_final_investment_decision_lte: Optional[
            datetime
        ] = None,
        announced_start_date_at_final_investment_decision_gt: Optional[datetime] = None,
        announced_start_date_at_final_investment_decision_gte: Optional[
            datetime
        ] = None,
        latest_announced_final_investment_decision_date: Optional[datetime] = None,
        latest_announced_final_investment_decision_date_lt: Optional[datetime] = None,
        latest_announced_final_investment_decision_date_lte: Optional[datetime] = None,
        latest_announced_final_investment_decision_date_gt: Optional[datetime] = None,
        latest_announced_final_investment_decision_date_gte: Optional[datetime] = None,
        estimated_first_cargo_date: Optional[datetime] = None,
        estimated_first_cargo_date_lt: Optional[datetime] = None,
        estimated_first_cargo_date_lte: Optional[datetime] = None,
        estimated_first_cargo_date_gt: Optional[datetime] = None,
        estimated_first_cargo_date_gte: Optional[datetime] = None,
        risk_factor_feedstock_availability: Optional[int] = None,
        risk_factor_feedstock_availability_lt: Optional[int] = None,
        risk_factor_feedstock_availability_lte: Optional[int] = None,
        risk_factor_feedstock_availability_gt: Optional[int] = None,
        risk_factor_feedstock_availability_gte: Optional[int] = None,
        risk_factor_politics_and_geopolitics: Optional[int] = None,
        risk_factor_politics_and_geopolitics_lt: Optional[int] = None,
        risk_factor_politics_and_geopolitics_lte: Optional[int] = None,
        risk_factor_politics_and_geopolitics_gt: Optional[int] = None,
        risk_factor_politics_and_geopolitics_gte: Optional[int] = None,
        risk_factor_environmental_regulation: Optional[int] = None,
        risk_factor_environmental_regulation_lt: Optional[int] = None,
        risk_factor_environmental_regulation_lte: Optional[int] = None,
        risk_factor_environmental_regulation_gt: Optional[int] = None,
        risk_factor_environmental_regulation_gte: Optional[int] = None,
        risk_factor_domestic_gas_needs: Optional[int] = None,
        risk_factor_domestic_gas_needs_lt: Optional[int] = None,
        risk_factor_domestic_gas_needs_lte: Optional[int] = None,
        risk_factor_domestic_gas_needs_gt: Optional[int] = None,
        risk_factor_domestic_gas_needs_gte: Optional[int] = None,
        risk_factor_partner_priorities: Optional[int] = None,
        risk_factor_partner_priorities_lt: Optional[int] = None,
        risk_factor_partner_priorities_lte: Optional[int] = None,
        risk_factor_partner_priorities_gt: Optional[int] = None,
        risk_factor_partner_priorities_gte: Optional[int] = None,
        risk_factor_project_economics: Optional[int] = None,
        risk_factor_project_economics_lt: Optional[int] = None,
        risk_factor_project_economics_lte: Optional[int] = None,
        risk_factor_project_economics_gt: Optional[int] = None,
        risk_factor_project_economics_gte: Optional[int] = None,
        risk_factor_ability_to_execute: Optional[int] = None,
        risk_factor_ability_to_execute_lt: Optional[int] = None,
        risk_factor_ability_to_execute_lte: Optional[int] = None,
        risk_factor_ability_to_execute_gt: Optional[int] = None,
        risk_factor_ability_to_execute_gte: Optional[int] = None,
        risk_factor_contracts: Optional[int] = None,
        risk_factor_contracts_lt: Optional[int] = None,
        risk_factor_contracts_lte: Optional[int] = None,
        risk_factor_contracts_gt: Optional[int] = None,
        risk_factor_contracts_gte: Optional[int] = None,
        risk_factor_overall: Optional[int] = None,
        risk_factor_overall_lt: Optional[int] = None,
        risk_factor_overall_lte: Optional[int] = None,
        risk_factor_overall_gt: Optional[int] = None,
        risk_factor_overall_gte: Optional[int] = None,
        estimated_final_investment_decision_date: Optional[datetime] = None,
        estimated_final_investment_decision_date_lt: Optional[datetime] = None,
        estimated_final_investment_decision_date_lte: Optional[datetime] = None,
        estimated_final_investment_decision_date_gt: Optional[datetime] = None,
        estimated_final_investment_decision_date_gte: Optional[datetime] = None,
        number_of_trains: Optional[int] = None,
        number_of_trains_lt: Optional[int] = None,
        number_of_trains_lte: Optional[int] = None,
        number_of_trains_gt: Optional[int] = None,
        number_of_trains_gte: Optional[int] = None,
        flng_charterer: Optional[Union[list[str], Series[str], str]] = None,
        project_type: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_train_feature: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_train_feature_uom: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        liquefaction_train_feature_currency: Optional[
            Union[list[str], Series[str], str]
        ] = None,
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
        List of all existing, under construction, and future liquefaction projects as well as their attributes

        Parameters
        ----------

         liquefaction_train: Optional[Union[list[str], Series[str], str]]
             Name of the individual liquefaction unit, by default None
         liquefaction_project: Optional[Union[list[str], Series[str], str]]
             Associated liquefaction project, by default None
         train_status: Optional[Union[list[str], Series[str], str]]
             Current operational status of the train, by default None
         announced_start_date: Optional[datetime], optional
             Publicly declared start date, by default None
         announced_start_date_gt: Optional[datetime], optional
             filter by `announced_start_date > x`, by default None
         announced_start_date_gte: Optional[datetime], optional
             filter by `announced_start_date >= x`, by default None
         announced_start_date_lt: Optional[datetime], optional
             filter by `announced_start_date < x`, by default None
         announced_start_date_lte: Optional[datetime], optional
             filter by `announced_start_date <= x`, by default None
         estimated_start_date: Optional[datetime], optional
             Our projected date for commercial operations to begin, by default None
         estimated_start_date_gt: Optional[datetime], optional
             filter by `estimated_start_date > x`, by default None
         estimated_start_date_gte: Optional[datetime], optional
             filter by `estimated_start_date >= x`, by default None
         estimated_start_date_lt: Optional[datetime], optional
             filter by `estimated_start_date < x`, by default None
         estimated_start_date_lte: Optional[datetime], optional
             filter by `estimated_start_date <= x`, by default None
         offline_date: Optional[datetime], optional
             Date when the train went offline, if applicable, by default None
         offline_date_gt: Optional[datetime], optional
             filter by `offline_date > x`, by default None
         offline_date_gte: Optional[datetime], optional
             filter by `offline_date >= x`, by default None
         offline_date_lt: Optional[datetime], optional
             filter by `offline_date < x`, by default None
         offline_date_lte: Optional[datetime], optional
             filter by `offline_date <= x`, by default None
         green_brownfield: Optional[Union[list[str], Series[str], str]]
             Classification as a new (greenfield) or upgraded (brownfield) project, by default None
         liquefaction_technology: Optional[Union[list[str], Series[str], str]]
             Technology used for liquefaction, by default None
         created_date: Optional[datetime], optional
             Date when the train record was created, by default None
         created_date_gt: Optional[datetime], optional
             filter by `created_date > x`, by default None
         created_date_gte: Optional[datetime], optional
             filter by `created_date >= x`, by default None
         created_date_lt: Optional[datetime], optional
             filter by `created_date < x`, by default None
         created_date_lte: Optional[datetime], optional
             filter by `created_date <= x`, by default None
         status_modified_date: Optional[datetime], optional
             Date when the train's status was last updated, by default None
         status_modified_date_gt: Optional[datetime], optional
             filter by `status_modified_date > x`, by default None
         status_modified_date_gte: Optional[datetime], optional
             filter by `status_modified_date >= x`, by default None
         status_modified_date_lt: Optional[datetime], optional
             filter by `status_modified_date < x`, by default None
         status_modified_date_lte: Optional[datetime], optional
             filter by `status_modified_date <= x`, by default None
         capacity_modified_date: Optional[datetime], optional
             Date when the train's capacity information was last updated, by default None
         capacity_modified_date_gt: Optional[datetime], optional
             filter by `capacity_modified_date > x`, by default None
         capacity_modified_date_gte: Optional[datetime], optional
             filter by `capacity_modified_date >= x`, by default None
         capacity_modified_date_lt: Optional[datetime], optional
             filter by `capacity_modified_date < x`, by default None
         capacity_modified_date_lte: Optional[datetime], optional
             filter by `capacity_modified_date <= x`, by default None
         announced_start_date_modified_date: Optional[datetime], optional
             Date when the announced start date was last updated, by default None
         announced_start_date_modified_date_gt: Optional[datetime], optional
             filter by `announced_start_date_modified_date > x`, by default None
         announced_start_date_modified_date_gte: Optional[datetime], optional
             filter by `announced_start_date_modified_date >= x`, by default None
         announced_start_date_modified_date_lt: Optional[datetime], optional
             filter by `announced_start_date_modified_date < x`, by default None
         announced_start_date_modified_date_lte: Optional[datetime], optional
             filter by `announced_start_date_modified_date <= x`, by default None
         estimated_start_date_modified_date: Optional[datetime], optional
             Date when the estimated start date was last updated, by default None
         estimated_start_date_modified_date_gt: Optional[datetime], optional
             filter by `estimated_start_date_modified_date > x`, by default None
         estimated_start_date_modified_date_gte: Optional[datetime], optional
             filter by `estimated_start_date_modified_date >= x`, by default None
         estimated_start_date_modified_date_lt: Optional[datetime], optional
             filter by `estimated_start_date_modified_date < x`, by default None
         estimated_start_date_modified_date_lte: Optional[datetime], optional
             filter by `estimated_start_date_modified_date <= x`, by default None
         announced_start_date_at_final_investment_decision: Optional[datetime], optional
             Start date announced at the time of the final investment decision, by default None
         announced_start_date_at_final_investment_decision_gt: Optional[datetime], optional
             filter by `announced_start_date_at_final_investment_decision > x`, by default None
         announced_start_date_at_final_investment_decision_gte: Optional[datetime], optional
             filter by `announced_start_date_at_final_investment_decision >= x`, by default None
         announced_start_date_at_final_investment_decision_lt: Optional[datetime], optional
             filter by `announced_start_date_at_final_investment_decision < x`, by default None
         announced_start_date_at_final_investment_decision_lte: Optional[datetime], optional
             filter by `announced_start_date_at_final_investment_decision <= x`, by default None
         latest_announced_final_investment_decision_date: Optional[datetime], optional
             Most recent final investment decision date, by default None
         latest_announced_final_investment_decision_date_gt: Optional[datetime], optional
             filter by `latest_announced_final_investment_decision_date > x`, by default None
         latest_announced_final_investment_decision_date_gte: Optional[datetime], optional
             filter by `latest_announced_final_investment_decision_date >= x`, by default None
         latest_announced_final_investment_decision_date_lt: Optional[datetime], optional
             filter by `latest_announced_final_investment_decision_date < x`, by default None
         latest_announced_final_investment_decision_date_lte: Optional[datetime], optional
             filter by `latest_announced_final_investment_decision_date <= x`, by default None
         estimated_first_cargo_date: Optional[datetime], optional
             Projected date for the first shipment of LNG, by default None
         estimated_first_cargo_date_gt: Optional[datetime], optional
             filter by `estimated_first_cargo_date > x`, by default None
         estimated_first_cargo_date_gte: Optional[datetime], optional
             filter by `estimated_first_cargo_date >= x`, by default None
         estimated_first_cargo_date_lt: Optional[datetime], optional
             filter by `estimated_first_cargo_date < x`, by default None
         estimated_first_cargo_date_lte: Optional[datetime], optional
             filter by `estimated_first_cargo_date <= x`, by default None
         risk_factor_feedstock_availability: Optional[int], optional
             Various risks associated with feedstock availability, by default None
         risk_factor_feedstock_availability_gt: Optional[int], optional
             filter by `risk_factor_feedstock_availability > x`, by default None
         risk_factor_feedstock_availability_gte: Optional[int], optional
             filter by `risk_factor_feedstock_availability >= x`, by default None
         risk_factor_feedstock_availability_lt: Optional[int], optional
             filter by `risk_factor_feedstock_availability < x`, by default None
         risk_factor_feedstock_availability_lte: Optional[int], optional
             filter by `risk_factor_feedstock_availability <= x`, by default None
         risk_factor_politics_and_geopolitics: Optional[int], optional
             Various risks associated with politics, by default None
         risk_factor_politics_and_geopolitics_gt: Optional[int], optional
             filter by `risk_factor_politics_and_geopolitics > x`, by default None
         risk_factor_politics_and_geopolitics_gte: Optional[int], optional
             filter by `risk_factor_politics_and_geopolitics >= x`, by default None
         risk_factor_politics_and_geopolitics_lt: Optional[int], optional
             filter by `risk_factor_politics_and_geopolitics < x`, by default None
         risk_factor_politics_and_geopolitics_lte: Optional[int], optional
             filter by `risk_factor_politics_and_geopolitics <= x`, by default None
         risk_factor_environmental_regulation: Optional[int], optional
             Various risks associated with environmental regulation, by default None
         risk_factor_environmental_regulation_gt: Optional[int], optional
             filter by `risk_factor_environmental_regulation > x`, by default None
         risk_factor_environmental_regulation_gte: Optional[int], optional
             filter by `risk_factor_environmental_regulation >= x`, by default None
         risk_factor_environmental_regulation_lt: Optional[int], optional
             filter by `risk_factor_environmental_regulation < x`, by default None
         risk_factor_environmental_regulation_lte: Optional[int], optional
             filter by `risk_factor_environmental_regulation <= x`, by default None
         risk_factor_domestic_gas_needs: Optional[int], optional
             Various risks associated with domestic gas needs, by default None
         risk_factor_domestic_gas_needs_gt: Optional[int], optional
             filter by `risk_factor_domestic_gas_needs > x`, by default None
         risk_factor_domestic_gas_needs_gte: Optional[int], optional
             filter by `risk_factor_domestic_gas_needs >= x`, by default None
         risk_factor_domestic_gas_needs_lt: Optional[int], optional
             filter by `risk_factor_domestic_gas_needs < x`, by default None
         risk_factor_domestic_gas_needs_lte: Optional[int], optional
             filter by `risk_factor_domestic_gas_needs <= x`, by default None
         risk_factor_partner_priorities: Optional[int], optional
             Various risks associated with partner priorities, by default None
         risk_factor_partner_priorities_gt: Optional[int], optional
             filter by `risk_factor_partner_priorities > x`, by default None
         risk_factor_partner_priorities_gte: Optional[int], optional
             filter by `risk_factor_partner_priorities >= x`, by default None
         risk_factor_partner_priorities_lt: Optional[int], optional
             filter by `risk_factor_partner_priorities < x`, by default None
         risk_factor_partner_priorities_lte: Optional[int], optional
             filter by `risk_factor_partner_priorities <= x`, by default None
         risk_factor_project_economics: Optional[int], optional
             Various risks associated with project economics, by default None
         risk_factor_project_economics_gt: Optional[int], optional
             filter by `risk_factor_project_economics > x`, by default None
         risk_factor_project_economics_gte: Optional[int], optional
             filter by `risk_factor_project_economics >= x`, by default None
         risk_factor_project_economics_lt: Optional[int], optional
             filter by `risk_factor_project_economics < x`, by default None
         risk_factor_project_economics_lte: Optional[int], optional
             filter by `risk_factor_project_economics <= x`, by default None
         risk_factor_ability_to_execute: Optional[int], optional
             Various risks associated with execution ability, by default None
         risk_factor_ability_to_execute_gt: Optional[int], optional
             filter by `risk_factor_ability_to_execute > x`, by default None
         risk_factor_ability_to_execute_gte: Optional[int], optional
             filter by `risk_factor_ability_to_execute >= x`, by default None
         risk_factor_ability_to_execute_lt: Optional[int], optional
             filter by `risk_factor_ability_to_execute < x`, by default None
         risk_factor_ability_to_execute_lte: Optional[int], optional
             filter by `risk_factor_ability_to_execute <= x`, by default None
         risk_factor_contracts: Optional[int], optional
             Various risks associated with contracts, by default None
         risk_factor_contracts_gt: Optional[int], optional
             filter by `risk_factor_contracts > x`, by default None
         risk_factor_contracts_gte: Optional[int], optional
             filter by `risk_factor_contracts >= x`, by default None
         risk_factor_contracts_lt: Optional[int], optional
             filter by `risk_factor_contracts < x`, by default None
         risk_factor_contracts_lte: Optional[int], optional
             filter by `risk_factor_contracts <= x`, by default None
         risk_factor_overall: Optional[int], optional
             Various risks associated with overall project risk, by default None
         risk_factor_overall_gt: Optional[int], optional
             filter by `risk_factor_overall > x`, by default None
         risk_factor_overall_gte: Optional[int], optional
             filter by `risk_factor_overall >= x`, by default None
         risk_factor_overall_lt: Optional[int], optional
             filter by `risk_factor_overall < x`, by default None
         risk_factor_overall_lte: Optional[int], optional
             filter by `risk_factor_overall <= x`, by default None
         estimated_final_investment_decision_date: Optional[datetime], optional
             Projected date for the final investment decision, by default None
         estimated_final_investment_decision_date_gt: Optional[datetime], optional
             filter by `estimated_final_investment_decision_date > x`, by default None
         estimated_final_investment_decision_date_gte: Optional[datetime], optional
             filter by `estimated_final_investment_decision_date >= x`, by default None
         estimated_final_investment_decision_date_lt: Optional[datetime], optional
             filter by `estimated_final_investment_decision_date < x`, by default None
         estimated_final_investment_decision_date_lte: Optional[datetime], optional
             filter by `estimated_final_investment_decision_date <= x`, by default None
         number_of_trains: Optional[int], optional
             Total number of liquefaction trains for each record, by default None
         number_of_trains_gt: Optional[int], optional
             filter by `number_of_trains > x`, by default None
         number_of_trains_gte: Optional[int], optional
             filter by `number_of_trains >= x`, by default None
         number_of_trains_lt: Optional[int], optional
             filter by `number_of_trains < x`, by default None
         number_of_trains_lte: Optional[int], optional
             filter by `number_of_trains <= x`, by default None
         flng_charterer: Optional[Union[list[str], Series[str], str]]
             Entity chartering any floating LNG facilities, by default None
         project_type: Optional[Union[list[str], Series[str], str]]
             Classification of the project type (e.g., onshore, offshore, floating), by default None
         liquefaction_train_feature: Optional[Union[list[str], Series[str], str]]
             Types of features of liquefaction trains ranging from capacity to storage to capital expenditure (capex) to other facility-specific characteristics, by default None
         liquefaction_train_feature_uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the corresponding liquefaction train feature, by default None
         liquefaction_train_feature_currency: Optional[Union[list[str], Series[str], str]]
             Currency of the corresponding liquefaction train feature, by default None
         modified_date: Optional[datetime], optional
             Liquefaction Trains record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("liquefactionTrain", liquefaction_train))
        filter_params.append(
            list_to_filter("liquefactionProject", liquefaction_project)
        )
        filter_params.append(list_to_filter("trainStatus", train_status))
        filter_params.append(list_to_filter("announcedStartDate", announced_start_date))
        if announced_start_date_gt is not None:
            filter_params.append(f'announcedStartDate > "{announced_start_date_gt}"')
        if announced_start_date_gte is not None:
            filter_params.append(f'announcedStartDate >= "{announced_start_date_gte}"')
        if announced_start_date_lt is not None:
            filter_params.append(f'announcedStartDate < "{announced_start_date_lt}"')
        if announced_start_date_lte is not None:
            filter_params.append(f'announcedStartDate <= "{announced_start_date_lte}"')
        filter_params.append(list_to_filter("estimatedStartDate", estimated_start_date))
        if estimated_start_date_gt is not None:
            filter_params.append(f'estimatedStartDate > "{estimated_start_date_gt}"')
        if estimated_start_date_gte is not None:
            filter_params.append(f'estimatedStartDate >= "{estimated_start_date_gte}"')
        if estimated_start_date_lt is not None:
            filter_params.append(f'estimatedStartDate < "{estimated_start_date_lt}"')
        if estimated_start_date_lte is not None:
            filter_params.append(f'estimatedStartDate <= "{estimated_start_date_lte}"')
        filter_params.append(list_to_filter("offlineDate", offline_date))
        if offline_date_gt is not None:
            filter_params.append(f'offlineDate > "{offline_date_gt}"')
        if offline_date_gte is not None:
            filter_params.append(f'offlineDate >= "{offline_date_gte}"')
        if offline_date_lt is not None:
            filter_params.append(f'offlineDate < "{offline_date_lt}"')
        if offline_date_lte is not None:
            filter_params.append(f'offlineDate <= "{offline_date_lte}"')
        filter_params.append(list_to_filter("greenBrownfield", green_brownfield))
        filter_params.append(
            list_to_filter("liquefactionTechnology", liquefaction_technology)
        )
        filter_params.append(list_to_filter("createdDate", created_date))
        if created_date_gt is not None:
            filter_params.append(f'createdDate > "{created_date_gt}"')
        if created_date_gte is not None:
            filter_params.append(f'createdDate >= "{created_date_gte}"')
        if created_date_lt is not None:
            filter_params.append(f'createdDate < "{created_date_lt}"')
        if created_date_lte is not None:
            filter_params.append(f'createdDate <= "{created_date_lte}"')
        filter_params.append(list_to_filter("statusModifiedDate", status_modified_date))
        if status_modified_date_gt is not None:
            filter_params.append(f'statusModifiedDate > "{status_modified_date_gt}"')
        if status_modified_date_gte is not None:
            filter_params.append(f'statusModifiedDate >= "{status_modified_date_gte}"')
        if status_modified_date_lt is not None:
            filter_params.append(f'statusModifiedDate < "{status_modified_date_lt}"')
        if status_modified_date_lte is not None:
            filter_params.append(f'statusModifiedDate <= "{status_modified_date_lte}"')
        filter_params.append(
            list_to_filter("capacityModifiedDate", capacity_modified_date)
        )
        if capacity_modified_date_gt is not None:
            filter_params.append(
                f'capacityModifiedDate > "{capacity_modified_date_gt}"'
            )
        if capacity_modified_date_gte is not None:
            filter_params.append(
                f'capacityModifiedDate >= "{capacity_modified_date_gte}"'
            )
        if capacity_modified_date_lt is not None:
            filter_params.append(
                f'capacityModifiedDate < "{capacity_modified_date_lt}"'
            )
        if capacity_modified_date_lte is not None:
            filter_params.append(
                f'capacityModifiedDate <= "{capacity_modified_date_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "announcedStartDateModifiedDate", announced_start_date_modified_date
            )
        )
        if announced_start_date_modified_date_gt is not None:
            filter_params.append(
                f'announcedStartDateModifiedDate > "{announced_start_date_modified_date_gt}"'
            )
        if announced_start_date_modified_date_gte is not None:
            filter_params.append(
                f'announcedStartDateModifiedDate >= "{announced_start_date_modified_date_gte}"'
            )
        if announced_start_date_modified_date_lt is not None:
            filter_params.append(
                f'announcedStartDateModifiedDate < "{announced_start_date_modified_date_lt}"'
            )
        if announced_start_date_modified_date_lte is not None:
            filter_params.append(
                f'announcedStartDateModifiedDate <= "{announced_start_date_modified_date_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "estimatedStartDateModifiedDate", estimated_start_date_modified_date
            )
        )
        if estimated_start_date_modified_date_gt is not None:
            filter_params.append(
                f'estimatedStartDateModifiedDate > "{estimated_start_date_modified_date_gt}"'
            )
        if estimated_start_date_modified_date_gte is not None:
            filter_params.append(
                f'estimatedStartDateModifiedDate >= "{estimated_start_date_modified_date_gte}"'
            )
        if estimated_start_date_modified_date_lt is not None:
            filter_params.append(
                f'estimatedStartDateModifiedDate < "{estimated_start_date_modified_date_lt}"'
            )
        if estimated_start_date_modified_date_lte is not None:
            filter_params.append(
                f'estimatedStartDateModifiedDate <= "{estimated_start_date_modified_date_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "announcedStartDateAtFinalInvestmentDecision",
                announced_start_date_at_final_investment_decision,
            )
        )
        if announced_start_date_at_final_investment_decision_gt is not None:
            filter_params.append(
                f'announcedStartDateAtFinalInvestmentDecision > "{announced_start_date_at_final_investment_decision_gt}"'
            )
        if announced_start_date_at_final_investment_decision_gte is not None:
            filter_params.append(
                f'announcedStartDateAtFinalInvestmentDecision >= "{announced_start_date_at_final_investment_decision_gte}"'
            )
        if announced_start_date_at_final_investment_decision_lt is not None:
            filter_params.append(
                f'announcedStartDateAtFinalInvestmentDecision < "{announced_start_date_at_final_investment_decision_lt}"'
            )
        if announced_start_date_at_final_investment_decision_lte is not None:
            filter_params.append(
                f'announcedStartDateAtFinalInvestmentDecision <= "{announced_start_date_at_final_investment_decision_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "latestAnnouncedFinalInvestmentDecisionDate",
                latest_announced_final_investment_decision_date,
            )
        )
        if latest_announced_final_investment_decision_date_gt is not None:
            filter_params.append(
                f'latestAnnouncedFinalInvestmentDecisionDate > "{latest_announced_final_investment_decision_date_gt}"'
            )
        if latest_announced_final_investment_decision_date_gte is not None:
            filter_params.append(
                f'latestAnnouncedFinalInvestmentDecisionDate >= "{latest_announced_final_investment_decision_date_gte}"'
            )
        if latest_announced_final_investment_decision_date_lt is not None:
            filter_params.append(
                f'latestAnnouncedFinalInvestmentDecisionDate < "{latest_announced_final_investment_decision_date_lt}"'
            )
        if latest_announced_final_investment_decision_date_lte is not None:
            filter_params.append(
                f'latestAnnouncedFinalInvestmentDecisionDate <= "{latest_announced_final_investment_decision_date_lte}"'
            )
        filter_params.append(
            list_to_filter("estimatedFirstCargoDate", estimated_first_cargo_date)
        )
        if estimated_first_cargo_date_gt is not None:
            filter_params.append(
                f'estimatedFirstCargoDate > "{estimated_first_cargo_date_gt}"'
            )
        if estimated_first_cargo_date_gte is not None:
            filter_params.append(
                f'estimatedFirstCargoDate >= "{estimated_first_cargo_date_gte}"'
            )
        if estimated_first_cargo_date_lt is not None:
            filter_params.append(
                f'estimatedFirstCargoDate < "{estimated_first_cargo_date_lt}"'
            )
        if estimated_first_cargo_date_lte is not None:
            filter_params.append(
                f'estimatedFirstCargoDate <= "{estimated_first_cargo_date_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "riskFactorFeedstockAvailability", risk_factor_feedstock_availability
            )
        )
        if risk_factor_feedstock_availability_gt is not None:
            filter_params.append(
                f'riskFactorFeedstockAvailability > "{risk_factor_feedstock_availability_gt}"'
            )
        if risk_factor_feedstock_availability_gte is not None:
            filter_params.append(
                f'riskFactorFeedstockAvailability >= "{risk_factor_feedstock_availability_gte}"'
            )
        if risk_factor_feedstock_availability_lt is not None:
            filter_params.append(
                f'riskFactorFeedstockAvailability < "{risk_factor_feedstock_availability_lt}"'
            )
        if risk_factor_feedstock_availability_lte is not None:
            filter_params.append(
                f'riskFactorFeedstockAvailability <= "{risk_factor_feedstock_availability_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "riskFactorPoliticsAndGeopolitics", risk_factor_politics_and_geopolitics
            )
        )
        if risk_factor_politics_and_geopolitics_gt is not None:
            filter_params.append(
                f'riskFactorPoliticsAndGeopolitics > "{risk_factor_politics_and_geopolitics_gt}"'
            )
        if risk_factor_politics_and_geopolitics_gte is not None:
            filter_params.append(
                f'riskFactorPoliticsAndGeopolitics >= "{risk_factor_politics_and_geopolitics_gte}"'
            )
        if risk_factor_politics_and_geopolitics_lt is not None:
            filter_params.append(
                f'riskFactorPoliticsAndGeopolitics < "{risk_factor_politics_and_geopolitics_lt}"'
            )
        if risk_factor_politics_and_geopolitics_lte is not None:
            filter_params.append(
                f'riskFactorPoliticsAndGeopolitics <= "{risk_factor_politics_and_geopolitics_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "riskFactorEnvironmentalRegulation",
                risk_factor_environmental_regulation,
            )
        )
        if risk_factor_environmental_regulation_gt is not None:
            filter_params.append(
                f'riskFactorEnvironmentalRegulation > "{risk_factor_environmental_regulation_gt}"'
            )
        if risk_factor_environmental_regulation_gte is not None:
            filter_params.append(
                f'riskFactorEnvironmentalRegulation >= "{risk_factor_environmental_regulation_gte}"'
            )
        if risk_factor_environmental_regulation_lt is not None:
            filter_params.append(
                f'riskFactorEnvironmentalRegulation < "{risk_factor_environmental_regulation_lt}"'
            )
        if risk_factor_environmental_regulation_lte is not None:
            filter_params.append(
                f'riskFactorEnvironmentalRegulation <= "{risk_factor_environmental_regulation_lte}"'
            )
        filter_params.append(
            list_to_filter("riskFactorDomesticGasNeeds", risk_factor_domestic_gas_needs)
        )
        if risk_factor_domestic_gas_needs_gt is not None:
            filter_params.append(
                f'riskFactorDomesticGasNeeds > "{risk_factor_domestic_gas_needs_gt}"'
            )
        if risk_factor_domestic_gas_needs_gte is not None:
            filter_params.append(
                f'riskFactorDomesticGasNeeds >= "{risk_factor_domestic_gas_needs_gte}"'
            )
        if risk_factor_domestic_gas_needs_lt is not None:
            filter_params.append(
                f'riskFactorDomesticGasNeeds < "{risk_factor_domestic_gas_needs_lt}"'
            )
        if risk_factor_domestic_gas_needs_lte is not None:
            filter_params.append(
                f'riskFactorDomesticGasNeeds <= "{risk_factor_domestic_gas_needs_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "riskFactorPartnerPriorities", risk_factor_partner_priorities
            )
        )
        if risk_factor_partner_priorities_gt is not None:
            filter_params.append(
                f'riskFactorPartnerPriorities > "{risk_factor_partner_priorities_gt}"'
            )
        if risk_factor_partner_priorities_gte is not None:
            filter_params.append(
                f'riskFactorPartnerPriorities >= "{risk_factor_partner_priorities_gte}"'
            )
        if risk_factor_partner_priorities_lt is not None:
            filter_params.append(
                f'riskFactorPartnerPriorities < "{risk_factor_partner_priorities_lt}"'
            )
        if risk_factor_partner_priorities_lte is not None:
            filter_params.append(
                f'riskFactorPartnerPriorities <= "{risk_factor_partner_priorities_lte}"'
            )
        filter_params.append(
            list_to_filter("riskFactorProjectEconomics", risk_factor_project_economics)
        )
        if risk_factor_project_economics_gt is not None:
            filter_params.append(
                f'riskFactorProjectEconomics > "{risk_factor_project_economics_gt}"'
            )
        if risk_factor_project_economics_gte is not None:
            filter_params.append(
                f'riskFactorProjectEconomics >= "{risk_factor_project_economics_gte}"'
            )
        if risk_factor_project_economics_lt is not None:
            filter_params.append(
                f'riskFactorProjectEconomics < "{risk_factor_project_economics_lt}"'
            )
        if risk_factor_project_economics_lte is not None:
            filter_params.append(
                f'riskFactorProjectEconomics <= "{risk_factor_project_economics_lte}"'
            )
        filter_params.append(
            list_to_filter("riskFactorAbilityToExecute", risk_factor_ability_to_execute)
        )
        if risk_factor_ability_to_execute_gt is not None:
            filter_params.append(
                f'riskFactorAbilityToExecute > "{risk_factor_ability_to_execute_gt}"'
            )
        if risk_factor_ability_to_execute_gte is not None:
            filter_params.append(
                f'riskFactorAbilityToExecute >= "{risk_factor_ability_to_execute_gte}"'
            )
        if risk_factor_ability_to_execute_lt is not None:
            filter_params.append(
                f'riskFactorAbilityToExecute < "{risk_factor_ability_to_execute_lt}"'
            )
        if risk_factor_ability_to_execute_lte is not None:
            filter_params.append(
                f'riskFactorAbilityToExecute <= "{risk_factor_ability_to_execute_lte}"'
            )
        filter_params.append(
            list_to_filter("riskFactorContracts", risk_factor_contracts)
        )
        if risk_factor_contracts_gt is not None:
            filter_params.append(f'riskFactorContracts > "{risk_factor_contracts_gt}"')
        if risk_factor_contracts_gte is not None:
            filter_params.append(
                f'riskFactorContracts >= "{risk_factor_contracts_gte}"'
            )
        if risk_factor_contracts_lt is not None:
            filter_params.append(f'riskFactorContracts < "{risk_factor_contracts_lt}"')
        if risk_factor_contracts_lte is not None:
            filter_params.append(
                f'riskFactorContracts <= "{risk_factor_contracts_lte}"'
            )
        filter_params.append(list_to_filter("riskFactorOverall", risk_factor_overall))
        if risk_factor_overall_gt is not None:
            filter_params.append(f'riskFactorOverall > "{risk_factor_overall_gt}"')
        if risk_factor_overall_gte is not None:
            filter_params.append(f'riskFactorOverall >= "{risk_factor_overall_gte}"')
        if risk_factor_overall_lt is not None:
            filter_params.append(f'riskFactorOverall < "{risk_factor_overall_lt}"')
        if risk_factor_overall_lte is not None:
            filter_params.append(f'riskFactorOverall <= "{risk_factor_overall_lte}"')
        filter_params.append(
            list_to_filter(
                "estimatedFinalInvestmentDecisionDate",
                estimated_final_investment_decision_date,
            )
        )
        if estimated_final_investment_decision_date_gt is not None:
            filter_params.append(
                f'estimatedFinalInvestmentDecisionDate > "{estimated_final_investment_decision_date_gt}"'
            )
        if estimated_final_investment_decision_date_gte is not None:
            filter_params.append(
                f'estimatedFinalInvestmentDecisionDate >= "{estimated_final_investment_decision_date_gte}"'
            )
        if estimated_final_investment_decision_date_lt is not None:
            filter_params.append(
                f'estimatedFinalInvestmentDecisionDate < "{estimated_final_investment_decision_date_lt}"'
            )
        if estimated_final_investment_decision_date_lte is not None:
            filter_params.append(
                f'estimatedFinalInvestmentDecisionDate <= "{estimated_final_investment_decision_date_lte}"'
            )
        filter_params.append(list_to_filter("numberOfTrains", number_of_trains))
        if number_of_trains_gt is not None:
            filter_params.append(f'numberOfTrains > "{number_of_trains_gt}"')
        if number_of_trains_gte is not None:
            filter_params.append(f'numberOfTrains >= "{number_of_trains_gte}"')
        if number_of_trains_lt is not None:
            filter_params.append(f'numberOfTrains < "{number_of_trains_lt}"')
        if number_of_trains_lte is not None:
            filter_params.append(f'numberOfTrains <= "{number_of_trains_lte}"')
        filter_params.append(list_to_filter("flngCharterer", flng_charterer))
        filter_params.append(list_to_filter("projectType", project_type))
        filter_params.append(
            list_to_filter("liquefactionTrainFeature", liquefaction_train_feature)
        )
        filter_params.append(
            list_to_filter(
                "liquefactionTrainFeatureUom", liquefaction_train_feature_uom
            )
        )
        filter_params.append(
            list_to_filter(
                "liquefactionTrainFeatureCurrency", liquefaction_train_feature_currency
            )
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
            path=f"/lng/v1/analytics/assets-contracts/liquefaction-trains",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_offtake_contracts(
        self,
        *,
        liquefaction_project: Optional[Union[list[str], Series[str], str]] = None,
        contract_group: Optional[Union[list[str], Series[str], str]] = None,
        exporter: Optional[Union[list[str], Series[str], str]] = None,
        buyer: Optional[Union[list[str], Series[str], str]] = None,
        assumed_destination: Optional[Union[list[str], Series[str], str]] = None,
        original_signing_date: Optional[datetime] = None,
        original_signing_date_lt: Optional[datetime] = None,
        original_signing_date_lte: Optional[datetime] = None,
        original_signing_date_gt: Optional[datetime] = None,
        original_signing_date_gte: Optional[datetime] = None,
        latest_contract_revision_date: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        announced_start_date: Optional[datetime] = None,
        announced_start_date_lt: Optional[datetime] = None,
        announced_start_date_lte: Optional[datetime] = None,
        announced_start_date_gt: Optional[datetime] = None,
        announced_start_date_gte: Optional[datetime] = None,
        preliminary_signing_date: Optional[datetime] = None,
        preliminary_signing_date_lt: Optional[datetime] = None,
        preliminary_signing_date_lte: Optional[datetime] = None,
        preliminary_signing_date_gt: Optional[datetime] = None,
        preliminary_signing_date_gte: Optional[datetime] = None,
        length_years: Optional[float] = None,
        length_years_lt: Optional[float] = None,
        length_years_lte: Optional[float] = None,
        length_years_gt: Optional[float] = None,
        length_years_gte: Optional[float] = None,
        contract_model_type: Optional[Union[list[str], Series[str], str]] = None,
        percentage_of_train: Optional[float] = None,
        percentage_of_train_lt: Optional[float] = None,
        percentage_of_train_lte: Optional[float] = None,
        percentage_of_train_gt: Optional[float] = None,
        percentage_of_train_gte: Optional[float] = None,
        destination_flexibility: Optional[Union[list[str], Series[str], str]] = None,
        contract_status: Optional[Union[list[str], Series[str], str]] = None,
        shipping_terms: Optional[Union[list[str], Series[str], str]] = None,
        contract_price_linkage_type: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        contract_price_slope: Optional[float] = None,
        contract_price_slope_lt: Optional[float] = None,
        contract_price_slope_lte: Optional[float] = None,
        contract_price_slope_gt: Optional[float] = None,
        contract_price_slope_gte: Optional[float] = None,
        contract_price_linkage: Optional[Union[list[str], Series[str], str]] = None,
        fid_enabling: Optional[Union[list[str], Series[str], str]] = None,
        green_or_brownfield: Optional[Union[list[str], Series[str], str]] = None,
        created_date: Optional[datetime] = None,
        created_date_lt: Optional[datetime] = None,
        created_date_lte: Optional[datetime] = None,
        created_date_gt: Optional[datetime] = None,
        created_date_gte: Optional[datetime] = None,
        buyer_modified_date: Optional[datetime] = None,
        buyer_modified_date_lt: Optional[datetime] = None,
        buyer_modified_date_lte: Optional[datetime] = None,
        buyer_modified_date_gt: Optional[datetime] = None,
        buyer_modified_date_gte: Optional[datetime] = None,
        announced_start_modified_date: Optional[datetime] = None,
        announced_start_modified_date_lt: Optional[datetime] = None,
        announced_start_modified_date_lte: Optional[datetime] = None,
        announced_start_modified_date_gt: Optional[datetime] = None,
        announced_start_modified_date_gte: Optional[datetime] = None,
        length_modified_date: Optional[datetime] = None,
        length_modified_date_lt: Optional[datetime] = None,
        length_modified_date_lte: Optional[datetime] = None,
        length_modified_date_gt: Optional[datetime] = None,
        length_modified_date_gte: Optional[datetime] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        published_volume_modified_date: Optional[datetime] = None,
        published_volume_modified_date_lt: Optional[datetime] = None,
        published_volume_modified_date_lte: Optional[datetime] = None,
        published_volume_modified_date_gt: Optional[datetime] = None,
        published_volume_modified_date_gte: Optional[datetime] = None,
        estimated_buildout_modified_date: Optional[datetime] = None,
        estimated_buildout_modified_date_lt: Optional[datetime] = None,
        estimated_buildout_modified_date_lte: Optional[datetime] = None,
        estimated_buildout_modified_date_gt: Optional[datetime] = None,
        estimated_buildout_modified_date_gte: Optional[datetime] = None,
        contract_volume_type: Optional[Union[list[str], Series[str], str]] = None,
        contract_volume_uom: Optional[Union[list[str], Series[str], str]] = None,
        supply_market: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        List of all announced and observed offtake contract relationships for liquefaction projects and company portfolios as well as their attributes

        Parameters
        ----------

         liquefaction_project: Optional[Union[list[str], Series[str], str]]
             Name of the associated liquefaction project, by default None
         contract_group: Optional[Union[list[str], Series[str], str]]
             Group or consortium involved in the contract, by default None
         exporter: Optional[Union[list[str], Series[str], str]]
             Entity responsible for exporting the LNG, by default None
         buyer: Optional[Union[list[str], Series[str], str]]
             Entity purchasing the LNG, by default None
         assumed_destination: Optional[Union[list[str], Series[str], str]]
             Expected delivery location for the LNG, by default None
         original_signing_date: Optional[datetime], optional
             Date when the contract was first signed, by default None
         original_signing_date_gt: Optional[datetime], optional
             filter by `original_signing_date > x`, by default None
         original_signing_date_gte: Optional[datetime], optional
             filter by `original_signing_date >= x`, by default None
         original_signing_date_lt: Optional[datetime], optional
             filter by `original_signing_date < x`, by default None
         original_signing_date_lte: Optional[datetime], optional
             filter by `original_signing_date <= x`, by default None
         latest_contract_revision_date: Optional[Union[list[str], Series[str], str]]
             Date of the most recent contract amendment, by default None
         announced_start_date: Optional[datetime], optional
             Publicly declared start date of the contract, by default None
         announced_start_date_gt: Optional[datetime], optional
             filter by `announced_start_date > x`, by default None
         announced_start_date_gte: Optional[datetime], optional
             filter by `announced_start_date >= x`, by default None
         announced_start_date_lt: Optional[datetime], optional
             filter by `announced_start_date < x`, by default None
         announced_start_date_lte: Optional[datetime], optional
             filter by `announced_start_date <= x`, by default None
         preliminary_signing_date: Optional[datetime], optional
             Date when the contract was preliminarily signed, by default None
         preliminary_signing_date_gt: Optional[datetime], optional
             filter by `preliminary_signing_date > x`, by default None
         preliminary_signing_date_gte: Optional[datetime], optional
             filter by `preliminary_signing_date >= x`, by default None
         preliminary_signing_date_lt: Optional[datetime], optional
             filter by `preliminary_signing_date < x`, by default None
         preliminary_signing_date_lte: Optional[datetime], optional
             filter by `preliminary_signing_date <= x`, by default None
         length_years: Optional[float], optional
             Duration of the contract in years, by default None
         length_years_gt: Optional[float], optional
             filter by `length_years > x`, by default None
         length_years_gte: Optional[float], optional
             filter by `length_years >= x`, by default None
         length_years_lt: Optional[float], optional
             filter by `length_years < x`, by default None
         length_years_lte: Optional[float], optional
             filter by `length_years <= x`, by default None
         contract_model_type: Optional[Union[list[str], Series[str], str]]
             Type of contract model used, by default None
         percentage_of_train: Optional[float], optional
             Share of the liquefaction train's capacity allocated to the contract, by default None
         percentage_of_train_gt: Optional[float], optional
             filter by `percentage_of_train > x`, by default None
         percentage_of_train_gte: Optional[float], optional
             filter by `percentage_of_train >= x`, by default None
         percentage_of_train_lt: Optional[float], optional
             filter by `percentage_of_train < x`, by default None
         percentage_of_train_lte: Optional[float], optional
             filter by `percentage_of_train <= x`, by default None
         destination_flexibility: Optional[Union[list[str], Series[str], str]]
             Designation if the offtake contract is destination-fixed or is flexible, by default None
         contract_status: Optional[Union[list[str], Series[str], str]]
             Current status of the contract, by default None
         shipping_terms: Optional[Union[list[str], Series[str], str]]
             Terms related to the transportation of LNG, by default None
         contract_price_linkage_type: Optional[Union[list[str], Series[str], str]]
             Identifies the general commodity that the pricing formula is applied to, by default None
         contract_price_slope: Optional[float], optional
             Slope of the price formula in the contract, by default None
         contract_price_slope_gt: Optional[float], optional
             filter by `contract_price_slope > x`, by default None
         contract_price_slope_gte: Optional[float], optional
             filter by `contract_price_slope >= x`, by default None
         contract_price_slope_lt: Optional[float], optional
             filter by `contract_price_slope < x`, by default None
         contract_price_slope_lte: Optional[float], optional
             filter by `contract_price_slope <= x`, by default None
         contract_price_linkage: Optional[Union[list[str], Series[str], str]]
             Identifies the specific price marker that the pricing formula is applied to, by default None
         fid_enabling: Optional[Union[list[str], Series[str], str]]
             Indicates if the contract enables the final investment decision, by default None
         green_or_brownfield: Optional[Union[list[str], Series[str], str]]
             Indicates if the project is a new development (greenfield) or an upgrade (brownfield), by default None
         created_date: Optional[datetime], optional
             Date when the contract record was created, by default None
         created_date_gt: Optional[datetime], optional
             filter by `created_date > x`, by default None
         created_date_gte: Optional[datetime], optional
             filter by `created_date >= x`, by default None
         created_date_lt: Optional[datetime], optional
             filter by `created_date < x`, by default None
         created_date_lte: Optional[datetime], optional
             filter by `created_date <= x`, by default None
         buyer_modified_date: Optional[datetime], optional
             Date when the buyer information was last updated, by default None
         buyer_modified_date_gt: Optional[datetime], optional
             filter by `buyer_modified_date > x`, by default None
         buyer_modified_date_gte: Optional[datetime], optional
             filter by `buyer_modified_date >= x`, by default None
         buyer_modified_date_lt: Optional[datetime], optional
             filter by `buyer_modified_date < x`, by default None
         buyer_modified_date_lte: Optional[datetime], optional
             filter by `buyer_modified_date <= x`, by default None
         announced_start_modified_date: Optional[datetime], optional
             Date when the announced start date was last updated, by default None
         announced_start_modified_date_gt: Optional[datetime], optional
             filter by `announced_start_modified_date > x`, by default None
         announced_start_modified_date_gte: Optional[datetime], optional
             filter by `announced_start_modified_date >= x`, by default None
         announced_start_modified_date_lt: Optional[datetime], optional
             filter by `announced_start_modified_date < x`, by default None
         announced_start_modified_date_lte: Optional[datetime], optional
             filter by `announced_start_modified_date <= x`, by default None
         length_modified_date: Optional[datetime], optional
             Date when the contract length was last updated, by default None
         length_modified_date_gt: Optional[datetime], optional
             filter by `length_modified_date > x`, by default None
         length_modified_date_gte: Optional[datetime], optional
             filter by `length_modified_date >= x`, by default None
         length_modified_date_lt: Optional[datetime], optional
             filter by `length_modified_date < x`, by default None
         length_modified_date_lte: Optional[datetime], optional
             filter by `length_modified_date <= x`, by default None
         modified_date: Optional[datetime], optional
             Date when the contract was last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         published_volume_modified_date: Optional[datetime], optional
             Date when the published volume was last updated, by default None
         published_volume_modified_date_gt: Optional[datetime], optional
             filter by `published_volume_modified_date > x`, by default None
         published_volume_modified_date_gte: Optional[datetime], optional
             filter by `published_volume_modified_date >= x`, by default None
         published_volume_modified_date_lt: Optional[datetime], optional
             filter by `published_volume_modified_date < x`, by default None
         published_volume_modified_date_lte: Optional[datetime], optional
             filter by `published_volume_modified_date <= x`, by default None
         estimated_buildout_modified_date: Optional[datetime], optional
             Date when the estimated buildout was last updated, by default None
         estimated_buildout_modified_date_gt: Optional[datetime], optional
             filter by `estimated_buildout_modified_date > x`, by default None
         estimated_buildout_modified_date_gte: Optional[datetime], optional
             filter by `estimated_buildout_modified_date >= x`, by default None
         estimated_buildout_modified_date_lt: Optional[datetime], optional
             filter by `estimated_buildout_modified_date < x`, by default None
         estimated_buildout_modified_date_lte: Optional[datetime], optional
             filter by `estimated_buildout_modified_date <= x`, by default None
         contract_volume_type: Optional[Union[list[str], Series[str], str]]
             Type of contract volume information, by default None
         contract_volume_uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the contract volume, by default None
         supply_market: Optional[Union[list[str], Series[str], str]]
             Market supplying the LNG for the contract, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("liquefactionProject", liquefaction_project)
        )
        filter_params.append(list_to_filter("contractGroup", contract_group))
        filter_params.append(list_to_filter("exporter", exporter))
        filter_params.append(list_to_filter("buyer", buyer))
        filter_params.append(list_to_filter("assumedDestination", assumed_destination))
        filter_params.append(
            list_to_filter("originalSigningDate", original_signing_date)
        )
        if original_signing_date_gt is not None:
            filter_params.append(f'originalSigningDate > "{original_signing_date_gt}"')
        if original_signing_date_gte is not None:
            filter_params.append(
                f'originalSigningDate >= "{original_signing_date_gte}"'
            )
        if original_signing_date_lt is not None:
            filter_params.append(f'originalSigningDate < "{original_signing_date_lt}"')
        if original_signing_date_lte is not None:
            filter_params.append(
                f'originalSigningDate <= "{original_signing_date_lte}"'
            )
        filter_params.append(
            list_to_filter("latestContractRevisionDate", latest_contract_revision_date)
        )
        filter_params.append(list_to_filter("announcedStartDate", announced_start_date))
        if announced_start_date_gt is not None:
            filter_params.append(f'announcedStartDate > "{announced_start_date_gt}"')
        if announced_start_date_gte is not None:
            filter_params.append(f'announcedStartDate >= "{announced_start_date_gte}"')
        if announced_start_date_lt is not None:
            filter_params.append(f'announcedStartDate < "{announced_start_date_lt}"')
        if announced_start_date_lte is not None:
            filter_params.append(f'announcedStartDate <= "{announced_start_date_lte}"')
        filter_params.append(
            list_to_filter("preliminarySigningDate", preliminary_signing_date)
        )
        if preliminary_signing_date_gt is not None:
            filter_params.append(
                f'preliminarySigningDate > "{preliminary_signing_date_gt}"'
            )
        if preliminary_signing_date_gte is not None:
            filter_params.append(
                f'preliminarySigningDate >= "{preliminary_signing_date_gte}"'
            )
        if preliminary_signing_date_lt is not None:
            filter_params.append(
                f'preliminarySigningDate < "{preliminary_signing_date_lt}"'
            )
        if preliminary_signing_date_lte is not None:
            filter_params.append(
                f'preliminarySigningDate <= "{preliminary_signing_date_lte}"'
            )
        filter_params.append(list_to_filter("lengthYears", length_years))
        if length_years_gt is not None:
            filter_params.append(f'lengthYears > "{length_years_gt}"')
        if length_years_gte is not None:
            filter_params.append(f'lengthYears >= "{length_years_gte}"')
        if length_years_lt is not None:
            filter_params.append(f'lengthYears < "{length_years_lt}"')
        if length_years_lte is not None:
            filter_params.append(f'lengthYears <= "{length_years_lte}"')
        filter_params.append(list_to_filter("contractModelType", contract_model_type))
        filter_params.append(list_to_filter("percentageOfTrain", percentage_of_train))
        if percentage_of_train_gt is not None:
            filter_params.append(f'percentageOfTrain > "{percentage_of_train_gt}"')
        if percentage_of_train_gte is not None:
            filter_params.append(f'percentageOfTrain >= "{percentage_of_train_gte}"')
        if percentage_of_train_lt is not None:
            filter_params.append(f'percentageOfTrain < "{percentage_of_train_lt}"')
        if percentage_of_train_lte is not None:
            filter_params.append(f'percentageOfTrain <= "{percentage_of_train_lte}"')
        filter_params.append(
            list_to_filter("destinationFlexibility", destination_flexibility)
        )
        filter_params.append(list_to_filter("contractStatus", contract_status))
        filter_params.append(list_to_filter("shippingTerms", shipping_terms))
        filter_params.append(
            list_to_filter("contractPriceLinkageType", contract_price_linkage_type)
        )
        filter_params.append(list_to_filter("contractPriceSlope", contract_price_slope))
        if contract_price_slope_gt is not None:
            filter_params.append(f'contractPriceSlope > "{contract_price_slope_gt}"')
        if contract_price_slope_gte is not None:
            filter_params.append(f'contractPriceSlope >= "{contract_price_slope_gte}"')
        if contract_price_slope_lt is not None:
            filter_params.append(f'contractPriceSlope < "{contract_price_slope_lt}"')
        if contract_price_slope_lte is not None:
            filter_params.append(f'contractPriceSlope <= "{contract_price_slope_lte}"')
        filter_params.append(
            list_to_filter("contractPriceLinkage", contract_price_linkage)
        )
        filter_params.append(list_to_filter("fidEnabling", fid_enabling))
        filter_params.append(list_to_filter("greenOrBrownfield", green_or_brownfield))
        filter_params.append(list_to_filter("createdDate", created_date))
        if created_date_gt is not None:
            filter_params.append(f'createdDate > "{created_date_gt}"')
        if created_date_gte is not None:
            filter_params.append(f'createdDate >= "{created_date_gte}"')
        if created_date_lt is not None:
            filter_params.append(f'createdDate < "{created_date_lt}"')
        if created_date_lte is not None:
            filter_params.append(f'createdDate <= "{created_date_lte}"')
        filter_params.append(list_to_filter("buyerModifiedDate", buyer_modified_date))
        if buyer_modified_date_gt is not None:
            filter_params.append(f'buyerModifiedDate > "{buyer_modified_date_gt}"')
        if buyer_modified_date_gte is not None:
            filter_params.append(f'buyerModifiedDate >= "{buyer_modified_date_gte}"')
        if buyer_modified_date_lt is not None:
            filter_params.append(f'buyerModifiedDate < "{buyer_modified_date_lt}"')
        if buyer_modified_date_lte is not None:
            filter_params.append(f'buyerModifiedDate <= "{buyer_modified_date_lte}"')
        filter_params.append(
            list_to_filter("announcedStartModifiedDate", announced_start_modified_date)
        )
        if announced_start_modified_date_gt is not None:
            filter_params.append(
                f'announcedStartModifiedDate > "{announced_start_modified_date_gt}"'
            )
        if announced_start_modified_date_gte is not None:
            filter_params.append(
                f'announcedStartModifiedDate >= "{announced_start_modified_date_gte}"'
            )
        if announced_start_modified_date_lt is not None:
            filter_params.append(
                f'announcedStartModifiedDate < "{announced_start_modified_date_lt}"'
            )
        if announced_start_modified_date_lte is not None:
            filter_params.append(
                f'announcedStartModifiedDate <= "{announced_start_modified_date_lte}"'
            )
        filter_params.append(list_to_filter("lengthModifiedDate", length_modified_date))
        if length_modified_date_gt is not None:
            filter_params.append(f'lengthModifiedDate > "{length_modified_date_gt}"')
        if length_modified_date_gte is not None:
            filter_params.append(f'lengthModifiedDate >= "{length_modified_date_gte}"')
        if length_modified_date_lt is not None:
            filter_params.append(f'lengthModifiedDate < "{length_modified_date_lt}"')
        if length_modified_date_lte is not None:
            filter_params.append(f'lengthModifiedDate <= "{length_modified_date_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')
        filter_params.append(
            list_to_filter(
                "publishedVolumeModifiedDate", published_volume_modified_date
            )
        )
        if published_volume_modified_date_gt is not None:
            filter_params.append(
                f'publishedVolumeModifiedDate > "{published_volume_modified_date_gt}"'
            )
        if published_volume_modified_date_gte is not None:
            filter_params.append(
                f'publishedVolumeModifiedDate >= "{published_volume_modified_date_gte}"'
            )
        if published_volume_modified_date_lt is not None:
            filter_params.append(
                f'publishedVolumeModifiedDate < "{published_volume_modified_date_lt}"'
            )
        if published_volume_modified_date_lte is not None:
            filter_params.append(
                f'publishedVolumeModifiedDate <= "{published_volume_modified_date_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "estimatedBuildoutModifiedDate", estimated_buildout_modified_date
            )
        )
        if estimated_buildout_modified_date_gt is not None:
            filter_params.append(
                f'estimatedBuildoutModifiedDate > "{estimated_buildout_modified_date_gt}"'
            )
        if estimated_buildout_modified_date_gte is not None:
            filter_params.append(
                f'estimatedBuildoutModifiedDate >= "{estimated_buildout_modified_date_gte}"'
            )
        if estimated_buildout_modified_date_lt is not None:
            filter_params.append(
                f'estimatedBuildoutModifiedDate < "{estimated_buildout_modified_date_lt}"'
            )
        if estimated_buildout_modified_date_lte is not None:
            filter_params.append(
                f'estimatedBuildoutModifiedDate <= "{estimated_buildout_modified_date_lte}"'
            )
        filter_params.append(list_to_filter("contractVolumeType", contract_volume_type))
        filter_params.append(list_to_filter("contractVolumeUom", contract_volume_uom))
        filter_params.append(list_to_filter("supplyMarket", supply_market))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/offtake-contracts",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_regasification_contracts(
        self,
        *,
        regasification_phase: Optional[Union[list[str], Series[str], str]] = None,
        regasification_project: Optional[Union[list[str], Series[str], str]] = None,
        capacity_owner: Optional[Union[list[str], Series[str], str]] = None,
        contract_type: Optional[Union[list[str], Series[str], str]] = None,
        contract_length_years: Optional[float] = None,
        contract_length_years_lt: Optional[float] = None,
        contract_length_years_lte: Optional[float] = None,
        contract_length_years_gt: Optional[float] = None,
        contract_length_years_gte: Optional[float] = None,
        contract_start_date: Optional[datetime] = None,
        contract_start_date_lt: Optional[datetime] = None,
        contract_start_date_lte: Optional[datetime] = None,
        contract_start_date_gt: Optional[datetime] = None,
        contract_start_date_gte: Optional[datetime] = None,
        created_date: Optional[datetime] = None,
        created_date_lt: Optional[datetime] = None,
        created_date_lte: Optional[datetime] = None,
        created_date_gt: Optional[datetime] = None,
        created_date_gte: Optional[datetime] = None,
        capacity_modified_date: Optional[datetime] = None,
        capacity_modified_date_lt: Optional[datetime] = None,
        capacity_modified_date_lte: Optional[datetime] = None,
        capacity_modified_date_gt: Optional[datetime] = None,
        capacity_modified_date_gte: Optional[datetime] = None,
        start_modified_date: Optional[datetime] = None,
        start_modified_date_lt: Optional[datetime] = None,
        start_modified_date_lte: Optional[datetime] = None,
        start_modified_date_gt: Optional[datetime] = None,
        start_modified_date_gte: Optional[datetime] = None,
        capacity_owner_modified_date: Optional[datetime] = None,
        capacity_owner_modified_date_lt: Optional[datetime] = None,
        capacity_owner_modified_date_lte: Optional[datetime] = None,
        capacity_owner_modified_date_gt: Optional[datetime] = None,
        capacity_owner_modified_date_gte: Optional[datetime] = None,
        type_modified_date: Optional[datetime] = None,
        type_modified_date_lt: Optional[datetime] = None,
        type_modified_date_lte: Optional[datetime] = None,
        type_modified_date_gt: Optional[datetime] = None,
        type_modified_date_gte: Optional[datetime] = None,
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
        List of all announced and observed capacity contract relationships for regasification phases as well as their attributes

        Parameters
        ----------

         regasification_phase: Optional[Union[list[str], Series[str], str]]
             Regasification phase associated with the contract, by default None
         regasification_project: Optional[Union[list[str], Series[str], str]]
             Name of the regasification project, by default None
         capacity_owner: Optional[Union[list[str], Series[str], str]]
             Entity or company that owns the regasification capacity, by default None
         contract_type: Optional[Union[list[str], Series[str], str]]
             Type of regasification contract, by default None
         contract_length_years: Optional[float], optional
             Length of the contract in years, by default None
         contract_length_years_gt: Optional[float], optional
             filter by `contract_length_years > x`, by default None
         contract_length_years_gte: Optional[float], optional
             filter by `contract_length_years >= x`, by default None
         contract_length_years_lt: Optional[float], optional
             filter by `contract_length_years < x`, by default None
         contract_length_years_lte: Optional[float], optional
             filter by `contract_length_years <= x`, by default None
         contract_start_date: Optional[datetime], optional
             Start date of the contract, by default None
         contract_start_date_gt: Optional[datetime], optional
             filter by `contract_start_date > x`, by default None
         contract_start_date_gte: Optional[datetime], optional
             filter by `contract_start_date >= x`, by default None
         contract_start_date_lt: Optional[datetime], optional
             filter by `contract_start_date < x`, by default None
         contract_start_date_lte: Optional[datetime], optional
             filter by `contract_start_date <= x`, by default None
         created_date: Optional[datetime], optional
             Date when the contract was created, by default None
         created_date_gt: Optional[datetime], optional
             filter by `created_date > x`, by default None
         created_date_gte: Optional[datetime], optional
             filter by `created_date >= x`, by default None
         created_date_lt: Optional[datetime], optional
             filter by `created_date < x`, by default None
         created_date_lte: Optional[datetime], optional
             filter by `created_date <= x`, by default None
         capacity_modified_date: Optional[datetime], optional
             Date when the capacity rights were modified, by default None
         capacity_modified_date_gt: Optional[datetime], optional
             filter by `capacity_modified_date > x`, by default None
         capacity_modified_date_gte: Optional[datetime], optional
             filter by `capacity_modified_date >= x`, by default None
         capacity_modified_date_lt: Optional[datetime], optional
             filter by `capacity_modified_date < x`, by default None
         capacity_modified_date_lte: Optional[datetime], optional
             filter by `capacity_modified_date <= x`, by default None
         start_modified_date: Optional[datetime], optional
             Date when the contract start date was modified, by default None
         start_modified_date_gt: Optional[datetime], optional
             filter by `start_modified_date > x`, by default None
         start_modified_date_gte: Optional[datetime], optional
             filter by `start_modified_date >= x`, by default None
         start_modified_date_lt: Optional[datetime], optional
             filter by `start_modified_date < x`, by default None
         start_modified_date_lte: Optional[datetime], optional
             filter by `start_modified_date <= x`, by default None
         capacity_owner_modified_date: Optional[datetime], optional
             Date when the capacity owner was modified, by default None
         capacity_owner_modified_date_gt: Optional[datetime], optional
             filter by `capacity_owner_modified_date > x`, by default None
         capacity_owner_modified_date_gte: Optional[datetime], optional
             filter by `capacity_owner_modified_date >= x`, by default None
         capacity_owner_modified_date_lt: Optional[datetime], optional
             filter by `capacity_owner_modified_date < x`, by default None
         capacity_owner_modified_date_lte: Optional[datetime], optional
             filter by `capacity_owner_modified_date <= x`, by default None
         type_modified_date: Optional[datetime], optional
             Date when the contract type was modified, by default None
         type_modified_date_gt: Optional[datetime], optional
             filter by `type_modified_date > x`, by default None
         type_modified_date_gte: Optional[datetime], optional
             filter by `type_modified_date >= x`, by default None
         type_modified_date_lt: Optional[datetime], optional
             filter by `type_modified_date < x`, by default None
         type_modified_date_lte: Optional[datetime], optional
             filter by `type_modified_date <= x`, by default None
         modified_date: Optional[datetime], optional
             Regasification contracts record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("regasificationPhase", regasification_phase)
        )
        filter_params.append(
            list_to_filter("regasificationProject", regasification_project)
        )
        filter_params.append(list_to_filter("capacityOwner", capacity_owner))
        filter_params.append(list_to_filter("contractType", contract_type))
        filter_params.append(
            list_to_filter("contractLengthYears", contract_length_years)
        )
        if contract_length_years_gt is not None:
            filter_params.append(f'contractLengthYears > "{contract_length_years_gt}"')
        if contract_length_years_gte is not None:
            filter_params.append(
                f'contractLengthYears >= "{contract_length_years_gte}"'
            )
        if contract_length_years_lt is not None:
            filter_params.append(f'contractLengthYears < "{contract_length_years_lt}"')
        if contract_length_years_lte is not None:
            filter_params.append(
                f'contractLengthYears <= "{contract_length_years_lte}"'
            )
        filter_params.append(list_to_filter("contractStartDate", contract_start_date))
        if contract_start_date_gt is not None:
            filter_params.append(f'contractStartDate > "{contract_start_date_gt}"')
        if contract_start_date_gte is not None:
            filter_params.append(f'contractStartDate >= "{contract_start_date_gte}"')
        if contract_start_date_lt is not None:
            filter_params.append(f'contractStartDate < "{contract_start_date_lt}"')
        if contract_start_date_lte is not None:
            filter_params.append(f'contractStartDate <= "{contract_start_date_lte}"')
        filter_params.append(list_to_filter("createdDate", created_date))
        if created_date_gt is not None:
            filter_params.append(f'createdDate > "{created_date_gt}"')
        if created_date_gte is not None:
            filter_params.append(f'createdDate >= "{created_date_gte}"')
        if created_date_lt is not None:
            filter_params.append(f'createdDate < "{created_date_lt}"')
        if created_date_lte is not None:
            filter_params.append(f'createdDate <= "{created_date_lte}"')
        filter_params.append(
            list_to_filter("capacityModifiedDate", capacity_modified_date)
        )
        if capacity_modified_date_gt is not None:
            filter_params.append(
                f'capacityModifiedDate > "{capacity_modified_date_gt}"'
            )
        if capacity_modified_date_gte is not None:
            filter_params.append(
                f'capacityModifiedDate >= "{capacity_modified_date_gte}"'
            )
        if capacity_modified_date_lt is not None:
            filter_params.append(
                f'capacityModifiedDate < "{capacity_modified_date_lt}"'
            )
        if capacity_modified_date_lte is not None:
            filter_params.append(
                f'capacityModifiedDate <= "{capacity_modified_date_lte}"'
            )
        filter_params.append(list_to_filter("startModifiedDate", start_modified_date))
        if start_modified_date_gt is not None:
            filter_params.append(f'startModifiedDate > "{start_modified_date_gt}"')
        if start_modified_date_gte is not None:
            filter_params.append(f'startModifiedDate >= "{start_modified_date_gte}"')
        if start_modified_date_lt is not None:
            filter_params.append(f'startModifiedDate < "{start_modified_date_lt}"')
        if start_modified_date_lte is not None:
            filter_params.append(f'startModifiedDate <= "{start_modified_date_lte}"')
        filter_params.append(
            list_to_filter("capacityOwnerModifiedDate", capacity_owner_modified_date)
        )
        if capacity_owner_modified_date_gt is not None:
            filter_params.append(
                f'capacityOwnerModifiedDate > "{capacity_owner_modified_date_gt}"'
            )
        if capacity_owner_modified_date_gte is not None:
            filter_params.append(
                f'capacityOwnerModifiedDate >= "{capacity_owner_modified_date_gte}"'
            )
        if capacity_owner_modified_date_lt is not None:
            filter_params.append(
                f'capacityOwnerModifiedDate < "{capacity_owner_modified_date_lt}"'
            )
        if capacity_owner_modified_date_lte is not None:
            filter_params.append(
                f'capacityOwnerModifiedDate <= "{capacity_owner_modified_date_lte}"'
            )
        filter_params.append(list_to_filter("typeModifiedDate", type_modified_date))
        if type_modified_date_gt is not None:
            filter_params.append(f'typeModifiedDate > "{type_modified_date_gt}"')
        if type_modified_date_gte is not None:
            filter_params.append(f'typeModifiedDate >= "{type_modified_date_gte}"')
        if type_modified_date_lt is not None:
            filter_params.append(f'typeModifiedDate < "{type_modified_date_lt}"')
        if type_modified_date_lte is not None:
            filter_params.append(f'typeModifiedDate <= "{type_modified_date_lte}"')
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
            path=f"/lng/v1/analytics/assets-contracts/regasification-contracts",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_regasification_phase_ownership(
        self,
        *,
        regasification_phase: Optional[Union[list[str], Series[str], str]] = None,
        shareholder: Optional[Union[list[str], Series[str], str]] = None,
        created_date: Optional[datetime] = None,
        created_date_lt: Optional[datetime] = None,
        created_date_lte: Optional[datetime] = None,
        created_date_gt: Optional[datetime] = None,
        created_date_gte: Optional[datetime] = None,
        shareholder_modified_date: Optional[datetime] = None,
        shareholder_modified_date_lt: Optional[datetime] = None,
        shareholder_modified_date_lte: Optional[datetime] = None,
        shareholder_modified_date_gt: Optional[datetime] = None,
        shareholder_modified_date_gte: Optional[datetime] = None,
        share_modified_date: Optional[datetime] = None,
        share_modified_date_lt: Optional[datetime] = None,
        share_modified_date_lte: Optional[datetime] = None,
        share_modified_date_gt: Optional[datetime] = None,
        share_modified_date_gte: Optional[datetime] = None,
        ownership_start_date: Optional[datetime] = None,
        ownership_start_date_lt: Optional[datetime] = None,
        ownership_start_date_lte: Optional[datetime] = None,
        ownership_start_date_gt: Optional[datetime] = None,
        ownership_start_date_gte: Optional[datetime] = None,
        ownership_end_date: Optional[datetime] = None,
        ownership_end_date_lt: Optional[datetime] = None,
        ownership_end_date_lte: Optional[datetime] = None,
        ownership_end_date_gt: Optional[datetime] = None,
        ownership_end_date_gte: Optional[datetime] = None,
        current_owner: Optional[Union[list[str], Series[str], str]] = None,
        country_coast: Optional[Union[list[str], Series[str], str]] = None,
        import_market: Optional[Union[list[str], Series[str], str]] = None,
        regasification_project: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides information on the ownership of regasification phases over time

        Parameters
        ----------

         regasification_phase: Optional[Union[list[str], Series[str], str]]
             Phase of the regasification project, by default None
         shareholder: Optional[Union[list[str], Series[str], str]]
             Entity or company that holds ownership in the regasification phase, by default None
         created_date: Optional[datetime], optional
             Date when the ownership record was created, by default None
         created_date_gt: Optional[datetime], optional
             filter by `created_date > x`, by default None
         created_date_gte: Optional[datetime], optional
             filter by `created_date >= x`, by default None
         created_date_lt: Optional[datetime], optional
             filter by `created_date < x`, by default None
         created_date_lte: Optional[datetime], optional
             filter by `created_date <= x`, by default None
         shareholder_modified_date: Optional[datetime], optional
             Date when the shareholder information was modified, by default None
         shareholder_modified_date_gt: Optional[datetime], optional
             filter by `shareholder_modified_date > x`, by default None
         shareholder_modified_date_gte: Optional[datetime], optional
             filter by `shareholder_modified_date >= x`, by default None
         shareholder_modified_date_lt: Optional[datetime], optional
             filter by `shareholder_modified_date < x`, by default None
         shareholder_modified_date_lte: Optional[datetime], optional
             filter by `shareholder_modified_date <= x`, by default None
         share_modified_date: Optional[datetime], optional
             Date when the ownership share was modified, by default None
         share_modified_date_gt: Optional[datetime], optional
             filter by `share_modified_date > x`, by default None
         share_modified_date_gte: Optional[datetime], optional
             filter by `share_modified_date >= x`, by default None
         share_modified_date_lt: Optional[datetime], optional
             filter by `share_modified_date < x`, by default None
         share_modified_date_lte: Optional[datetime], optional
             filter by `share_modified_date <= x`, by default None
         ownership_start_date: Optional[datetime], optional
             Start date of the ownership, by default None
         ownership_start_date_gt: Optional[datetime], optional
             filter by `ownership_start_date > x`, by default None
         ownership_start_date_gte: Optional[datetime], optional
             filter by `ownership_start_date >= x`, by default None
         ownership_start_date_lt: Optional[datetime], optional
             filter by `ownership_start_date < x`, by default None
         ownership_start_date_lte: Optional[datetime], optional
             filter by `ownership_start_date <= x`, by default None
         ownership_end_date: Optional[datetime], optional
             End date of the ownership, by default None
         ownership_end_date_gt: Optional[datetime], optional
             filter by `ownership_end_date > x`, by default None
         ownership_end_date_gte: Optional[datetime], optional
             filter by `ownership_end_date >= x`, by default None
         ownership_end_date_lt: Optional[datetime], optional
             filter by `ownership_end_date < x`, by default None
         ownership_end_date_lte: Optional[datetime], optional
             filter by `ownership_end_date <= x`, by default None
         current_owner: Optional[Union[list[str], Series[str], str]]
             Current owner of the regasification phase, by default None
         country_coast: Optional[Union[list[str], Series[str], str]]
             Country coast associated with the regasification phase, by default None
         import_market: Optional[Union[list[str], Series[str], str]]
             Market where the regasification project is located, by default None
         regasification_project: Optional[Union[list[str], Series[str], str]]
             Name of the regasification project associated with the phase, by default None
         modified_date: Optional[datetime], optional
             Regasificaion phase ownership record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("regasificationPhase", regasification_phase)
        )
        filter_params.append(list_to_filter("shareholder", shareholder))
        filter_params.append(list_to_filter("createdDate", created_date))
        if created_date_gt is not None:
            filter_params.append(f'createdDate > "{created_date_gt}"')
        if created_date_gte is not None:
            filter_params.append(f'createdDate >= "{created_date_gte}"')
        if created_date_lt is not None:
            filter_params.append(f'createdDate < "{created_date_lt}"')
        if created_date_lte is not None:
            filter_params.append(f'createdDate <= "{created_date_lte}"')
        filter_params.append(
            list_to_filter("shareholderModifiedDate", shareholder_modified_date)
        )
        if shareholder_modified_date_gt is not None:
            filter_params.append(
                f'shareholderModifiedDate > "{shareholder_modified_date_gt}"'
            )
        if shareholder_modified_date_gte is not None:
            filter_params.append(
                f'shareholderModifiedDate >= "{shareholder_modified_date_gte}"'
            )
        if shareholder_modified_date_lt is not None:
            filter_params.append(
                f'shareholderModifiedDate < "{shareholder_modified_date_lt}"'
            )
        if shareholder_modified_date_lte is not None:
            filter_params.append(
                f'shareholderModifiedDate <= "{shareholder_modified_date_lte}"'
            )
        filter_params.append(list_to_filter("shareModifiedDate", share_modified_date))
        if share_modified_date_gt is not None:
            filter_params.append(f'shareModifiedDate > "{share_modified_date_gt}"')
        if share_modified_date_gte is not None:
            filter_params.append(f'shareModifiedDate >= "{share_modified_date_gte}"')
        if share_modified_date_lt is not None:
            filter_params.append(f'shareModifiedDate < "{share_modified_date_lt}"')
        if share_modified_date_lte is not None:
            filter_params.append(f'shareModifiedDate <= "{share_modified_date_lte}"')
        filter_params.append(list_to_filter("ownershipStartDate", ownership_start_date))
        if ownership_start_date_gt is not None:
            filter_params.append(f'ownershipStartDate > "{ownership_start_date_gt}"')
        if ownership_start_date_gte is not None:
            filter_params.append(f'ownershipStartDate >= "{ownership_start_date_gte}"')
        if ownership_start_date_lt is not None:
            filter_params.append(f'ownershipStartDate < "{ownership_start_date_lt}"')
        if ownership_start_date_lte is not None:
            filter_params.append(f'ownershipStartDate <= "{ownership_start_date_lte}"')
        filter_params.append(list_to_filter("ownershipEndDate", ownership_end_date))
        if ownership_end_date_gt is not None:
            filter_params.append(f'ownershipEndDate > "{ownership_end_date_gt}"')
        if ownership_end_date_gte is not None:
            filter_params.append(f'ownershipEndDate >= "{ownership_end_date_gte}"')
        if ownership_end_date_lt is not None:
            filter_params.append(f'ownershipEndDate < "{ownership_end_date_lt}"')
        if ownership_end_date_lte is not None:
            filter_params.append(f'ownershipEndDate <= "{ownership_end_date_lte}"')
        filter_params.append(list_to_filter("currentOwner", current_owner))
        filter_params.append(list_to_filter("countryCoast", country_coast))
        filter_params.append(list_to_filter("importMarket", import_market))
        filter_params.append(
            list_to_filter("regasificationProject", regasification_project)
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
            path=f"/lng/v1/analytics/assets-contracts/regasification-phase-ownership",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_regasification_phases(
        self,
        *,
        regasification_phase: Optional[Union[list[str], Series[str], str]] = None,
        regasification_project: Optional[Union[list[str], Series[str], str]] = None,
        phase_status: Optional[Union[list[str], Series[str], str]] = None,
        announced_start_date: Optional[datetime] = None,
        announced_start_date_lt: Optional[datetime] = None,
        announced_start_date_lte: Optional[datetime] = None,
        announced_start_date_gt: Optional[datetime] = None,
        announced_start_date_gte: Optional[datetime] = None,
        estimated_start_date: Optional[datetime] = None,
        estimated_start_date_lt: Optional[datetime] = None,
        estimated_start_date_lte: Optional[datetime] = None,
        estimated_start_date_gt: Optional[datetime] = None,
        estimated_start_date_gte: Optional[datetime] = None,
        feed_contractor: Optional[Union[list[str], Series[str], str]] = None,
        epc_contractor: Optional[Union[list[str], Series[str], str]] = None,
        terminal_type: Optional[Union[list[str], Series[str], str]] = None,
        able_to_reload: Optional[int] = None,
        able_to_reload_lt: Optional[int] = None,
        able_to_reload_lte: Optional[int] = None,
        able_to_reload_gt: Optional[int] = None,
        able_to_reload_gte: Optional[int] = None,
        created_date: Optional[datetime] = None,
        created_date_lt: Optional[datetime] = None,
        created_date_lte: Optional[datetime] = None,
        created_date_gt: Optional[datetime] = None,
        created_date_gte: Optional[datetime] = None,
        status_modified_date: Optional[datetime] = None,
        status_modified_date_lt: Optional[datetime] = None,
        status_modified_date_lte: Optional[datetime] = None,
        status_modified_date_gt: Optional[datetime] = None,
        status_modified_date_gte: Optional[datetime] = None,
        capacity_modified_date: Optional[datetime] = None,
        capacity_modified_date_lt: Optional[datetime] = None,
        capacity_modified_date_lte: Optional[datetime] = None,
        capacity_modified_date_gt: Optional[datetime] = None,
        capacity_modified_date_gte: Optional[datetime] = None,
        announced_start_date_modified_date: Optional[datetime] = None,
        announced_start_date_modified_date_lt: Optional[datetime] = None,
        announced_start_date_modified_date_lte: Optional[datetime] = None,
        announced_start_date_modified_date_gt: Optional[datetime] = None,
        announced_start_date_modified_date_gte: Optional[datetime] = None,
        estimated_start_date_modified_date: Optional[datetime] = None,
        estimated_start_date_modified_date_lt: Optional[datetime] = None,
        estimated_start_date_modified_date_lte: Optional[datetime] = None,
        estimated_start_date_modified_date_gt: Optional[datetime] = None,
        estimated_start_date_modified_date_gte: Optional[datetime] = None,
        date_phase_first_announced: Optional[datetime] = None,
        date_phase_first_announced_lt: Optional[datetime] = None,
        date_phase_first_announced_lte: Optional[datetime] = None,
        date_phase_first_announced_gt: Optional[datetime] = None,
        date_phase_first_announced_gte: Optional[datetime] = None,
        small_scale: Optional[int] = None,
        small_scale_lt: Optional[int] = None,
        small_scale_lte: Optional[int] = None,
        small_scale_gt: Optional[int] = None,
        small_scale_gte: Optional[int] = None,
        regasification_phase_feature: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        regasification_phase_feature_uom: Optional[
            Union[list[str], Series[str], str]
        ] = None,
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
        Provides information on LNG regasification phases

        Parameters
        ----------

         regasification_phase: Optional[Union[list[str], Series[str], str]]
             Name of the regasification phase, by default None
         regasification_project: Optional[Union[list[str], Series[str], str]]
             Name of the regasification project associated with the phase, by default None
         phase_status: Optional[Union[list[str], Series[str], str]]
             Status of the regasification phase, by default None
         announced_start_date: Optional[datetime], optional
             Latest publically announced start date of the regasification phase, by default None
         announced_start_date_gt: Optional[datetime], optional
             filter by `announced_start_date > x`, by default None
         announced_start_date_gte: Optional[datetime], optional
             filter by `announced_start_date >= x`, by default None
         announced_start_date_lt: Optional[datetime], optional
             filter by `announced_start_date < x`, by default None
         announced_start_date_lte: Optional[datetime], optional
             filter by `announced_start_date <= x`, by default None
         estimated_start_date: Optional[datetime], optional
             Our estimated start date of the regasification phase, by default None
         estimated_start_date_gt: Optional[datetime], optional
             filter by `estimated_start_date > x`, by default None
         estimated_start_date_gte: Optional[datetime], optional
             filter by `estimated_start_date >= x`, by default None
         estimated_start_date_lt: Optional[datetime], optional
             filter by `estimated_start_date < x`, by default None
         estimated_start_date_lte: Optional[datetime], optional
             filter by `estimated_start_date <= x`, by default None
         feed_contractor: Optional[Union[list[str], Series[str], str]]
             Contractor responsible for the front-end engineering and design of the regasification phase, by default None
         epc_contractor: Optional[Union[list[str], Series[str], str]]
             Contractor responsible for the engineering, procurement, and construction of the regasification phase, by default None
         terminal_type: Optional[Union[list[str], Series[str], str]]
             Type of regasification terminal, by default None
         able_to_reload: Optional[int], optional
             Indicates whether the regasification phase is capable of reloading LNG onto ships, by default None
         able_to_reload_gt: Optional[int], optional
             filter by `able_to_reload > x`, by default None
         able_to_reload_gte: Optional[int], optional
             filter by `able_to_reload >= x`, by default None
         able_to_reload_lt: Optional[int], optional
             filter by `able_to_reload < x`, by default None
         able_to_reload_lte: Optional[int], optional
             filter by `able_to_reload <= x`, by default None
         created_date: Optional[datetime], optional
             Date when the regasification phase record was created, by default None
         created_date_gt: Optional[datetime], optional
             filter by `created_date > x`, by default None
         created_date_gte: Optional[datetime], optional
             filter by `created_date >= x`, by default None
         created_date_lt: Optional[datetime], optional
             filter by `created_date < x`, by default None
         created_date_lte: Optional[datetime], optional
             filter by `created_date <= x`, by default None
         status_modified_date: Optional[datetime], optional
             Date when the phase status was last modified, by default None
         status_modified_date_gt: Optional[datetime], optional
             filter by `status_modified_date > x`, by default None
         status_modified_date_gte: Optional[datetime], optional
             filter by `status_modified_date >= x`, by default None
         status_modified_date_lt: Optional[datetime], optional
             filter by `status_modified_date < x`, by default None
         status_modified_date_lte: Optional[datetime], optional
             filter by `status_modified_date <= x`, by default None
         capacity_modified_date: Optional[datetime], optional
             Date when the capacity of the regasification phase was last modified, by default None
         capacity_modified_date_gt: Optional[datetime], optional
             filter by `capacity_modified_date > x`, by default None
         capacity_modified_date_gte: Optional[datetime], optional
             filter by `capacity_modified_date >= x`, by default None
         capacity_modified_date_lt: Optional[datetime], optional
             filter by `capacity_modified_date < x`, by default None
         capacity_modified_date_lte: Optional[datetime], optional
             filter by `capacity_modified_date <= x`, by default None
         announced_start_date_modified_date: Optional[datetime], optional
             Date when the announced start date of the regasification phase was last modified, by default None
         announced_start_date_modified_date_gt: Optional[datetime], optional
             filter by `announced_start_date_modified_date > x`, by default None
         announced_start_date_modified_date_gte: Optional[datetime], optional
             filter by `announced_start_date_modified_date >= x`, by default None
         announced_start_date_modified_date_lt: Optional[datetime], optional
             filter by `announced_start_date_modified_date < x`, by default None
         announced_start_date_modified_date_lte: Optional[datetime], optional
             filter by `announced_start_date_modified_date <= x`, by default None
         estimated_start_date_modified_date: Optional[datetime], optional
             Date when the estimated start date of the regasification phase was last modified, by default None
         estimated_start_date_modified_date_gt: Optional[datetime], optional
             filter by `estimated_start_date_modified_date > x`, by default None
         estimated_start_date_modified_date_gte: Optional[datetime], optional
             filter by `estimated_start_date_modified_date >= x`, by default None
         estimated_start_date_modified_date_lt: Optional[datetime], optional
             filter by `estimated_start_date_modified_date < x`, by default None
         estimated_start_date_modified_date_lte: Optional[datetime], optional
             filter by `estimated_start_date_modified_date <= x`, by default None
         date_phase_first_announced: Optional[datetime], optional
             Date when the regasification phase was first announced, by default None
         date_phase_first_announced_gt: Optional[datetime], optional
             filter by `date_phase_first_announced > x`, by default None
         date_phase_first_announced_gte: Optional[datetime], optional
             filter by `date_phase_first_announced >= x`, by default None
         date_phase_first_announced_lt: Optional[datetime], optional
             filter by `date_phase_first_announced < x`, by default None
         date_phase_first_announced_lte: Optional[datetime], optional
             filter by `date_phase_first_announced <= x`, by default None
         small_scale: Optional[int], optional
             Indicates whether the regasification phase is a small-scale project, by default None
         small_scale_gt: Optional[int], optional
             filter by `small_scale > x`, by default None
         small_scale_gte: Optional[int], optional
             filter by `small_scale >= x`, by default None
         small_scale_lt: Optional[int], optional
             filter by `small_scale < x`, by default None
         small_scale_lte: Optional[int], optional
             filter by `small_scale <= x`, by default None
         regasification_phase_feature: Optional[Union[list[str], Series[str], str]]
             Types of features of regasification phases ranging from capacity to storage and other facility-specific characteristics, by default None
         regasification_phase_feature_uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the corresponding regasification phase feature, by default None
         modified_date: Optional[datetime], optional
             Regasification phases record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("regasificationPhase", regasification_phase)
        )
        filter_params.append(
            list_to_filter("regasificationProject", regasification_project)
        )
        filter_params.append(list_to_filter("phaseStatus", phase_status))
        filter_params.append(list_to_filter("announcedStartDate", announced_start_date))
        if announced_start_date_gt is not None:
            filter_params.append(f'announcedStartDate > "{announced_start_date_gt}"')
        if announced_start_date_gte is not None:
            filter_params.append(f'announcedStartDate >= "{announced_start_date_gte}"')
        if announced_start_date_lt is not None:
            filter_params.append(f'announcedStartDate < "{announced_start_date_lt}"')
        if announced_start_date_lte is not None:
            filter_params.append(f'announcedStartDate <= "{announced_start_date_lte}"')
        filter_params.append(list_to_filter("estimatedStartDate", estimated_start_date))
        if estimated_start_date_gt is not None:
            filter_params.append(f'estimatedStartDate > "{estimated_start_date_gt}"')
        if estimated_start_date_gte is not None:
            filter_params.append(f'estimatedStartDate >= "{estimated_start_date_gte}"')
        if estimated_start_date_lt is not None:
            filter_params.append(f'estimatedStartDate < "{estimated_start_date_lt}"')
        if estimated_start_date_lte is not None:
            filter_params.append(f'estimatedStartDate <= "{estimated_start_date_lte}"')
        filter_params.append(list_to_filter("feedContractor", feed_contractor))
        filter_params.append(list_to_filter("epcContractor", epc_contractor))
        filter_params.append(list_to_filter("terminalType", terminal_type))
        filter_params.append(list_to_filter("ableToReload", able_to_reload))
        if able_to_reload_gt is not None:
            filter_params.append(f'ableToReload > "{able_to_reload_gt}"')
        if able_to_reload_gte is not None:
            filter_params.append(f'ableToReload >= "{able_to_reload_gte}"')
        if able_to_reload_lt is not None:
            filter_params.append(f'ableToReload < "{able_to_reload_lt}"')
        if able_to_reload_lte is not None:
            filter_params.append(f'ableToReload <= "{able_to_reload_lte}"')
        filter_params.append(list_to_filter("createdDate", created_date))
        if created_date_gt is not None:
            filter_params.append(f'createdDate > "{created_date_gt}"')
        if created_date_gte is not None:
            filter_params.append(f'createdDate >= "{created_date_gte}"')
        if created_date_lt is not None:
            filter_params.append(f'createdDate < "{created_date_lt}"')
        if created_date_lte is not None:
            filter_params.append(f'createdDate <= "{created_date_lte}"')
        filter_params.append(list_to_filter("statusModifiedDate", status_modified_date))
        if status_modified_date_gt is not None:
            filter_params.append(f'statusModifiedDate > "{status_modified_date_gt}"')
        if status_modified_date_gte is not None:
            filter_params.append(f'statusModifiedDate >= "{status_modified_date_gte}"')
        if status_modified_date_lt is not None:
            filter_params.append(f'statusModifiedDate < "{status_modified_date_lt}"')
        if status_modified_date_lte is not None:
            filter_params.append(f'statusModifiedDate <= "{status_modified_date_lte}"')
        filter_params.append(
            list_to_filter("capacityModifiedDate", capacity_modified_date)
        )
        if capacity_modified_date_gt is not None:
            filter_params.append(
                f'capacityModifiedDate > "{capacity_modified_date_gt}"'
            )
        if capacity_modified_date_gte is not None:
            filter_params.append(
                f'capacityModifiedDate >= "{capacity_modified_date_gte}"'
            )
        if capacity_modified_date_lt is not None:
            filter_params.append(
                f'capacityModifiedDate < "{capacity_modified_date_lt}"'
            )
        if capacity_modified_date_lte is not None:
            filter_params.append(
                f'capacityModifiedDate <= "{capacity_modified_date_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "announcedStartDateModifiedDate", announced_start_date_modified_date
            )
        )
        if announced_start_date_modified_date_gt is not None:
            filter_params.append(
                f'announcedStartDateModifiedDate > "{announced_start_date_modified_date_gt}"'
            )
        if announced_start_date_modified_date_gte is not None:
            filter_params.append(
                f'announcedStartDateModifiedDate >= "{announced_start_date_modified_date_gte}"'
            )
        if announced_start_date_modified_date_lt is not None:
            filter_params.append(
                f'announcedStartDateModifiedDate < "{announced_start_date_modified_date_lt}"'
            )
        if announced_start_date_modified_date_lte is not None:
            filter_params.append(
                f'announcedStartDateModifiedDate <= "{announced_start_date_modified_date_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "estimatedStartDateModifiedDate", estimated_start_date_modified_date
            )
        )
        if estimated_start_date_modified_date_gt is not None:
            filter_params.append(
                f'estimatedStartDateModifiedDate > "{estimated_start_date_modified_date_gt}"'
            )
        if estimated_start_date_modified_date_gte is not None:
            filter_params.append(
                f'estimatedStartDateModifiedDate >= "{estimated_start_date_modified_date_gte}"'
            )
        if estimated_start_date_modified_date_lt is not None:
            filter_params.append(
                f'estimatedStartDateModifiedDate < "{estimated_start_date_modified_date_lt}"'
            )
        if estimated_start_date_modified_date_lte is not None:
            filter_params.append(
                f'estimatedStartDateModifiedDate <= "{estimated_start_date_modified_date_lte}"'
            )
        filter_params.append(
            list_to_filter("datePhaseFirstAnnounced", date_phase_first_announced)
        )
        if date_phase_first_announced_gt is not None:
            filter_params.append(
                f'datePhaseFirstAnnounced > "{date_phase_first_announced_gt}"'
            )
        if date_phase_first_announced_gte is not None:
            filter_params.append(
                f'datePhaseFirstAnnounced >= "{date_phase_first_announced_gte}"'
            )
        if date_phase_first_announced_lt is not None:
            filter_params.append(
                f'datePhaseFirstAnnounced < "{date_phase_first_announced_lt}"'
            )
        if date_phase_first_announced_lte is not None:
            filter_params.append(
                f'datePhaseFirstAnnounced <= "{date_phase_first_announced_lte}"'
            )
        filter_params.append(list_to_filter("smallScale", small_scale))
        if small_scale_gt is not None:
            filter_params.append(f'smallScale > "{small_scale_gt}"')
        if small_scale_gte is not None:
            filter_params.append(f'smallScale >= "{small_scale_gte}"')
        if small_scale_lt is not None:
            filter_params.append(f'smallScale < "{small_scale_lt}"')
        if small_scale_lte is not None:
            filter_params.append(f'smallScale <= "{small_scale_lte}"')
        filter_params.append(
            list_to_filter("regasificationPhaseFeature", regasification_phase_feature)
        )
        filter_params.append(
            list_to_filter(
                "regasificationPhaseFeatureUom", regasification_phase_feature_uom
            )
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
            path=f"/lng/v1/analytics/assets-contracts/regasification-phases",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_regasification_projects(
        self,
        *,
        regasification_project: Optional[Union[list[str], Series[str], str]] = None,
        country_coast: Optional[Union[list[str], Series[str], str]] = None,
        import_market: Optional[Union[list[str], Series[str], str]] = None,
        import_region: Optional[Union[list[str], Series[str], str]] = None,
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
        List of regasification projects with their IDs and their associated country coasts

        Parameters
        ----------

         regasification_project: Optional[Union[list[str], Series[str], str]]
             Name of regasification project, by default None
         country_coast: Optional[Union[list[str], Series[str], str]]
             Country coast where the regasification project is located, by default None
         import_market: Optional[Union[list[str], Series[str], str]]
             Market where the regasification project is located, by default None
         import_region: Optional[Union[list[str], Series[str], str]]
             Region where the regasification project is located, by default None
         modified_date: Optional[datetime], optional
             Regasification projects record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("regasificationProject", regasification_project)
        )
        filter_params.append(list_to_filter("countryCoast", country_coast))
        filter_params.append(list_to_filter("importMarket", import_market))
        filter_params.append(list_to_filter("importRegion", import_region))
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
            path=f"/lng/v1/analytics/assets-contracts/regasification-projects",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_vessel(
        self,
        *,
        imo_number: Optional[int] = None,
        imo_number_lt: Optional[int] = None,
        imo_number_lte: Optional[int] = None,
        imo_number_gt: Optional[int] = None,
        imo_number_gte: Optional[int] = None,
        vessel_name: Optional[Union[list[str], Series[str], str]] = None,
        propulsion_system: Optional[Union[list[str], Series[str], str]] = None,
        vessel_type: Optional[Union[list[str], Series[str], str]] = None,
        charterer: Optional[Union[list[str], Series[str], str]] = None,
        operator: Optional[Union[list[str], Series[str], str]] = None,
        shipowner: Optional[Union[list[str], Series[str], str]] = None,
        shipowner2: Optional[Union[list[str], Series[str], str]] = None,
        shipbuilder: Optional[Union[list[str], Series[str], str]] = None,
        country_of_build: Optional[Union[list[str], Series[str], str]] = None,
        flag: Optional[Union[list[str], Series[str], str]] = None,
        cargo_containment_system: Optional[Union[list[str], Series[str], str]] = None,
        vessel_status: Optional[Union[list[str], Series[str], str]] = None,
        name_currently_in_use: Optional[Union[list[str], Series[str], str]] = None,
        contract_date: Optional[date] = None,
        contract_date_lt: Optional[date] = None,
        contract_date_lte: Optional[date] = None,
        contract_date_gt: Optional[date] = None,
        contract_date_gte: Optional[date] = None,
        delivery_date: Optional[date] = None,
        delivery_date_lt: Optional[date] = None,
        delivery_date_lte: Optional[date] = None,
        delivery_date_gt: Optional[date] = None,
        delivery_date_gte: Optional[date] = None,
        vessel_feature: Optional[Union[list[str], Series[str], str]] = None,
        vessel_feature_uom: Optional[Union[list[str], Series[str], str]] = None,
        vessel_feature_currency: Optional[Union[list[str], Series[str], str]] = None,
        created_date: Optional[datetime] = None,
        created_date_lt: Optional[datetime] = None,
        created_date_lte: Optional[datetime] = None,
        created_date_gt: Optional[datetime] = None,
        created_date_gte: Optional[datetime] = None,
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
        List of all retired, existing, and future vessels as well as their attributes

        Parameters
        ----------

         imo_number: Optional[int], optional
             International Maritime Organization (IMO) number assigned to the vessel, by default None
         imo_number_gt: Optional[int], optional
             filter by `imo_number > x`, by default None
         imo_number_gte: Optional[int], optional
             filter by `imo_number >= x`, by default None
         imo_number_lt: Optional[int], optional
             filter by `imo_number < x`, by default None
         imo_number_lte: Optional[int], optional
             filter by `imo_number <= x`, by default None
         vessel_name: Optional[Union[list[str], Series[str], str]]
             Name of the LNG vessel, by default None
         propulsion_system: Optional[Union[list[str], Series[str], str]]
             Propulsion type of vessel, by default None
         vessel_type: Optional[Union[list[str], Series[str], str]]
             Type or classification of the vessel, by default None
         charterer: Optional[Union[list[str], Series[str], str]]
             Entity or company that charters or leases the vessel, by default None
         operator: Optional[Union[list[str], Series[str], str]]
             Entity or company responsible for operating the vessel, by default None
         shipowner: Optional[Union[list[str], Series[str], str]]
             Primary owner of the vessel, by default None
         shipowner2: Optional[Union[list[str], Series[str], str]]
             Additional owners of the vessel, if applicable, by default None
         shipbuilder: Optional[Union[list[str], Series[str], str]]
             Shipyard or company that constructed the vessel, by default None
         country_of_build: Optional[Union[list[str], Series[str], str]]
             Country where the vessel was built, by default None
         flag: Optional[Union[list[str], Series[str], str]]
             Flag state or country under which the vessel is registered, by default None
         cargo_containment_system: Optional[Union[list[str], Series[str], str]]
             System used for containing the LNG cargo on the vessel, by default None
         vessel_status: Optional[Union[list[str], Series[str], str]]
             Current status of the vessel, by default None
         name_currently_in_use: Optional[Union[list[str], Series[str], str]]
             Yes or no if the identified name is currently in use, by default None
         contract_date: Optional[date], optional
             Date when the vessel contract was signed, by default None
         contract_date_gt: Optional[date], optional
             filter by `contract_date > x`, by default None
         contract_date_gte: Optional[date], optional
             filter by `contract_date >= x`, by default None
         contract_date_lt: Optional[date], optional
             filter by `contract_date < x`, by default None
         contract_date_lte: Optional[date], optional
             filter by `contract_date <= x`, by default None
         delivery_date: Optional[date], optional
             Date when the vessel was delivered, by default None
         delivery_date_gt: Optional[date], optional
             filter by `delivery_date > x`, by default None
         delivery_date_gte: Optional[date], optional
             filter by `delivery_date >= x`, by default None
         delivery_date_lt: Optional[date], optional
             filter by `delivery_date < x`, by default None
         delivery_date_lte: Optional[date], optional
             filter by `delivery_date <= x`, by default None
         vessel_feature: Optional[Union[list[str], Series[str], str]]
             Types of features of vessels ranging from capacity to cost to other facility-specific characteristics, by default None
         vessel_feature_uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the corresponding vessel feature, by default None
         vessel_feature_currency: Optional[Union[list[str], Series[str], str]]
             Currency of the corresponding vessel feature, by default None
         created_date: Optional[datetime], optional
             Date when the vessel record was created, by default None
         created_date_gt: Optional[datetime], optional
             filter by `created_date > x`, by default None
         created_date_gte: Optional[datetime], optional
             filter by `created_date >= x`, by default None
         created_date_lt: Optional[datetime], optional
             filter by `created_date < x`, by default None
         created_date_lte: Optional[datetime], optional
             filter by `created_date <= x`, by default None
         modified_date: Optional[datetime], optional
             Vessel record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("imoNumber", imo_number))
        if imo_number_gt is not None:
            filter_params.append(f'imoNumber > "{imo_number_gt}"')
        if imo_number_gte is not None:
            filter_params.append(f'imoNumber >= "{imo_number_gte}"')
        if imo_number_lt is not None:
            filter_params.append(f'imoNumber < "{imo_number_lt}"')
        if imo_number_lte is not None:
            filter_params.append(f'imoNumber <= "{imo_number_lte}"')
        filter_params.append(list_to_filter("vesselName", vessel_name))
        filter_params.append(list_to_filter("propulsionSystem", propulsion_system))
        filter_params.append(list_to_filter("vesselType", vessel_type))
        filter_params.append(list_to_filter("charterer", charterer))
        filter_params.append(list_to_filter("operator", operator))
        filter_params.append(list_to_filter("shipowner", shipowner))
        filter_params.append(list_to_filter("shipowner2", shipowner2))
        filter_params.append(list_to_filter("shipbuilder", shipbuilder))
        filter_params.append(list_to_filter("countryOfBuild", country_of_build))
        filter_params.append(list_to_filter("flag", flag))
        filter_params.append(
            list_to_filter("cargoContainmentSystem", cargo_containment_system)
        )
        filter_params.append(list_to_filter("vesselStatus", vessel_status))
        filter_params.append(
            list_to_filter("nameCurrentlyInUse", name_currently_in_use)
        )
        filter_params.append(list_to_filter("contractDate", contract_date))
        if contract_date_gt is not None:
            filter_params.append(f'contractDate > "{contract_date_gt}"')
        if contract_date_gte is not None:
            filter_params.append(f'contractDate >= "{contract_date_gte}"')
        if contract_date_lt is not None:
            filter_params.append(f'contractDate < "{contract_date_lt}"')
        if contract_date_lte is not None:
            filter_params.append(f'contractDate <= "{contract_date_lte}"')
        filter_params.append(list_to_filter("deliveryDate", delivery_date))
        if delivery_date_gt is not None:
            filter_params.append(f'deliveryDate > "{delivery_date_gt}"')
        if delivery_date_gte is not None:
            filter_params.append(f'deliveryDate >= "{delivery_date_gte}"')
        if delivery_date_lt is not None:
            filter_params.append(f'deliveryDate < "{delivery_date_lt}"')
        if delivery_date_lte is not None:
            filter_params.append(f'deliveryDate <= "{delivery_date_lte}"')
        filter_params.append(list_to_filter("vesselFeature", vessel_feature))
        filter_params.append(list_to_filter("vesselFeatureUom", vessel_feature_uom))
        filter_params.append(
            list_to_filter("vesselFeatureCurrency", vessel_feature_currency)
        )
        filter_params.append(list_to_filter("createdDate", created_date))
        if created_date_gt is not None:
            filter_params.append(f'createdDate > "{created_date_gt}"')
        if created_date_gte is not None:
            filter_params.append(f'createdDate >= "{created_date_gte}"')
        if created_date_lt is not None:
            filter_params.append(f'createdDate < "{created_date_lt}"')
        if created_date_lte is not None:
            filter_params.append(f'createdDate <= "{created_date_lte}"')
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
            path=f"/lng/v1/analytics/assets-contracts/vessel",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_monthly_estimated_buildout_offtake_contracts(
        self,
        *,
        buildout_month_estimated: Optional[datetime] = None,
        buildout_month_estimated_lt: Optional[datetime] = None,
        buildout_month_estimated_lte: Optional[datetime] = None,
        buildout_month_estimated_gt: Optional[datetime] = None,
        buildout_month_estimated_gte: Optional[datetime] = None,
        created_date_estimated: Optional[datetime] = None,
        created_date_estimated_lt: Optional[datetime] = None,
        created_date_estimated_lte: Optional[datetime] = None,
        created_date_estimated_gt: Optional[datetime] = None,
        created_date_estimated_gte: Optional[datetime] = None,
        modified_date_estimated: Optional[datetime] = None,
        modified_date_estimated_lt: Optional[datetime] = None,
        modified_date_estimated_lte: Optional[datetime] = None,
        modified_date_estimated_gt: Optional[datetime] = None,
        modified_date_estimated_gte: Optional[datetime] = None,
        buyer: Optional[Union[list[str], Series[str], str]] = None,
        exporter: Optional[Union[list[str], Series[str], str]] = None,
        contract_group: Optional[Union[list[str], Series[str], str]] = None,
        supply_market: Optional[Union[list[str], Series[str], str]] = None,
        assumed_destination: Optional[Union[list[str], Series[str], str]] = None,
        contract_type: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_project: Optional[Union[list[str], Series[str], str]] = None,
        shipping_terms: Optional[Union[list[str], Series[str], str]] = None,
        destination_flexibility: Optional[Union[list[str], Series[str], str]] = None,
        estimated_start_date: Optional[datetime] = None,
        estimated_start_date_lt: Optional[datetime] = None,
        estimated_start_date_lte: Optional[datetime] = None,
        estimated_start_date_gt: Optional[datetime] = None,
        estimated_start_date_gte: Optional[datetime] = None,
        estimated_end_date: Optional[datetime] = None,
        estimated_end_date_lt: Optional[datetime] = None,
        estimated_end_date_lte: Optional[datetime] = None,
        estimated_end_date_gt: Optional[datetime] = None,
        estimated_end_date_gte: Optional[datetime] = None,
        length_years: Optional[float] = None,
        length_years_lt: Optional[float] = None,
        length_years_lte: Optional[float] = None,
        length_years_gt: Optional[float] = None,
        length_years_gte: Optional[float] = None,
        original_signing: Optional[datetime] = None,
        original_signing_lt: Optional[datetime] = None,
        original_signing_lte: Optional[datetime] = None,
        original_signing_gt: Optional[datetime] = None,
        original_signing_gte: Optional[datetime] = None,
        annual_contract_volume: Optional[float] = None,
        annual_contract_volume_lt: Optional[float] = None,
        annual_contract_volume_lte: Optional[float] = None,
        annual_contract_volume_gt: Optional[float] = None,
        annual_contract_volume_gte: Optional[float] = None,
        annual_contract_volume_uom: Optional[Union[list[str], Series[str], str]] = None,
        initial_contract_volume: Optional[float] = None,
        initial_contract_volume_lt: Optional[float] = None,
        initial_contract_volume_lte: Optional[float] = None,
        initial_contract_volume_gt: Optional[float] = None,
        initial_contract_volume_gte: Optional[float] = None,
        initial_contract_volume_uom: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        pricing_linkage: Optional[Union[list[str], Series[str], str]] = None,
        specific_price_link: Optional[Union[list[str], Series[str], str]] = None,
        fid_enabling: Optional[Union[list[str], Series[str], str]] = None,
        green_or_brownfield: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides monthly estimated buildout information on LNG offtake contracts

        Parameters
        ----------

         buildout_month_estimated: Optional[datetime], optional
             Month for which the estimated buildout information is provided, by default None
         buildout_month_estimated_gt: Optional[datetime], optional
             filter by `buildout_month_estimated > x`, by default None
         buildout_month_estimated_gte: Optional[datetime], optional
             filter by `buildout_month_estimated >= x`, by default None
         buildout_month_estimated_lt: Optional[datetime], optional
             filter by `buildout_month_estimated < x`, by default None
         buildout_month_estimated_lte: Optional[datetime], optional
             filter by `buildout_month_estimated <= x`, by default None
         created_date_estimated: Optional[datetime], optional
             Date when the estimated buildout information was created, by default None
         created_date_estimated_gt: Optional[datetime], optional
             filter by `created_date_estimated > x`, by default None
         created_date_estimated_gte: Optional[datetime], optional
             filter by `created_date_estimated >= x`, by default None
         created_date_estimated_lt: Optional[datetime], optional
             filter by `created_date_estimated < x`, by default None
         created_date_estimated_lte: Optional[datetime], optional
             filter by `created_date_estimated <= x`, by default None
         modified_date_estimated: Optional[datetime], optional
             Date when the estimated buildout information was last modified, by default None
         modified_date_estimated_gt: Optional[datetime], optional
             filter by `modified_date_estimated > x`, by default None
         modified_date_estimated_gte: Optional[datetime], optional
             filter by `modified_date_estimated >= x`, by default None
         modified_date_estimated_lt: Optional[datetime], optional
             filter by `modified_date_estimated < x`, by default None
         modified_date_estimated_lte: Optional[datetime], optional
             filter by `modified_date_estimated <= x`, by default None
         buyer: Optional[Union[list[str], Series[str], str]]
             Entity or company purchasing the LNG, by default None
         exporter: Optional[Union[list[str], Series[str], str]]
             Entity or company exporting the LNG, by default None
         contract_group: Optional[Union[list[str], Series[str], str]]
             Group or category to which the contract belongs, by default None
         supply_market: Optional[Union[list[str], Series[str], str]]
             Market where the offtake contract is located, by default None
         assumed_destination: Optional[Union[list[str], Series[str], str]]
             Assumed destination for the LNG, by default None
         contract_type: Optional[Union[list[str], Series[str], str]]
             The status of the capacity contract, by default None
         liquefaction_project: Optional[Union[list[str], Series[str], str]]
             Name or title of the liquefaction project associated with the contract, by default None
         shipping_terms: Optional[Union[list[str], Series[str], str]]
             Terms and conditions related to the shipping of the LNG, by default None
         destination_flexibility: Optional[Union[list[str], Series[str], str]]
             Designation if the offtake contract is destination-fixed or is flexible, by default None
         estimated_start_date: Optional[datetime], optional
             Estimated start date for the offtake contract, by default None
         estimated_start_date_gt: Optional[datetime], optional
             filter by `estimated_start_date > x`, by default None
         estimated_start_date_gte: Optional[datetime], optional
             filter by `estimated_start_date >= x`, by default None
         estimated_start_date_lt: Optional[datetime], optional
             filter by `estimated_start_date < x`, by default None
         estimated_start_date_lte: Optional[datetime], optional
             filter by `estimated_start_date <= x`, by default None
         estimated_end_date: Optional[datetime], optional
             Estimated end date for the offtake contract, by default None
         estimated_end_date_gt: Optional[datetime], optional
             filter by `estimated_end_date > x`, by default None
         estimated_end_date_gte: Optional[datetime], optional
             filter by `estimated_end_date >= x`, by default None
         estimated_end_date_lt: Optional[datetime], optional
             filter by `estimated_end_date < x`, by default None
         estimated_end_date_lte: Optional[datetime], optional
             filter by `estimated_end_date <= x`, by default None
         length_years: Optional[float], optional
             Duration of the contract in years, by default None
         length_years_gt: Optional[float], optional
             filter by `length_years > x`, by default None
         length_years_gte: Optional[float], optional
             filter by `length_years >= x`, by default None
         length_years_lt: Optional[float], optional
             filter by `length_years < x`, by default None
         length_years_lte: Optional[float], optional
             filter by `length_years <= x`, by default None
         original_signing: Optional[datetime], optional
             Date when the contract was originally signed, by default None
         original_signing_gt: Optional[datetime], optional
             filter by `original_signing > x`, by default None
         original_signing_gte: Optional[datetime], optional
             filter by `original_signing >= x`, by default None
         original_signing_lt: Optional[datetime], optional
             filter by `original_signing < x`, by default None
         original_signing_lte: Optional[datetime], optional
             filter by `original_signing <= x`, by default None
         annual_contract_volume: Optional[float], optional
             Numeric values of the annual contract quantity for the given offtake contract, by default None
         annual_contract_volume_gt: Optional[float], optional
             filter by `annual_contract_volume > x`, by default None
         annual_contract_volume_gte: Optional[float], optional
             filter by `annual_contract_volume >= x`, by default None
         annual_contract_volume_lt: Optional[float], optional
             filter by `annual_contract_volume < x`, by default None
         annual_contract_volume_lte: Optional[float], optional
             filter by `annual_contract_volume <= x`, by default None
         annual_contract_volume_uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the annual contract quantity for the given offtake contract, by default None
         initial_contract_volume: Optional[float], optional
             Numeric values of the initial contract quantity for the given offtake contract, by default None
         initial_contract_volume_gt: Optional[float], optional
             filter by `initial_contract_volume > x`, by default None
         initial_contract_volume_gte: Optional[float], optional
             filter by `initial_contract_volume >= x`, by default None
         initial_contract_volume_lt: Optional[float], optional
             filter by `initial_contract_volume < x`, by default None
         initial_contract_volume_lte: Optional[float], optional
             filter by `initial_contract_volume <= x`, by default None
         initial_contract_volume_uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the initial contract quantity for the given offtake contract, by default None
         pricing_linkage: Optional[Union[list[str], Series[str], str]]
             Determining contract pricing, by default None
         specific_price_link: Optional[Union[list[str], Series[str], str]]
             Prices formula or reference for contract pricing calculation, by default None
         fid_enabling: Optional[Union[list[str], Series[str], str]]
             Category denoting the timing of when the contract was signed relative to the project's FID milestone, by default None
         green_or_brownfield: Optional[Union[list[str], Series[str], str]]
             Whether the contract is associated with a greenfield or brownfield facility or portfolio, by default None
         modified_date: Optional[datetime], optional
             Date when the offtake contract was last modified, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("buildoutMonthEstimated", buildout_month_estimated)
        )
        if buildout_month_estimated_gt is not None:
            filter_params.append(
                f'buildoutMonthEstimated > "{buildout_month_estimated_gt}"'
            )
        if buildout_month_estimated_gte is not None:
            filter_params.append(
                f'buildoutMonthEstimated >= "{buildout_month_estimated_gte}"'
            )
        if buildout_month_estimated_lt is not None:
            filter_params.append(
                f'buildoutMonthEstimated < "{buildout_month_estimated_lt}"'
            )
        if buildout_month_estimated_lte is not None:
            filter_params.append(
                f'buildoutMonthEstimated <= "{buildout_month_estimated_lte}"'
            )
        filter_params.append(
            list_to_filter("createdDateEstimated", created_date_estimated)
        )
        if created_date_estimated_gt is not None:
            filter_params.append(
                f'createdDateEstimated > "{created_date_estimated_gt}"'
            )
        if created_date_estimated_gte is not None:
            filter_params.append(
                f'createdDateEstimated >= "{created_date_estimated_gte}"'
            )
        if created_date_estimated_lt is not None:
            filter_params.append(
                f'createdDateEstimated < "{created_date_estimated_lt}"'
            )
        if created_date_estimated_lte is not None:
            filter_params.append(
                f'createdDateEstimated <= "{created_date_estimated_lte}"'
            )
        filter_params.append(
            list_to_filter("modifiedDateEstimated", modified_date_estimated)
        )
        if modified_date_estimated_gt is not None:
            filter_params.append(
                f'modifiedDateEstimated > "{modified_date_estimated_gt}"'
            )
        if modified_date_estimated_gte is not None:
            filter_params.append(
                f'modifiedDateEstimated >= "{modified_date_estimated_gte}"'
            )
        if modified_date_estimated_lt is not None:
            filter_params.append(
                f'modifiedDateEstimated < "{modified_date_estimated_lt}"'
            )
        if modified_date_estimated_lte is not None:
            filter_params.append(
                f'modifiedDateEstimated <= "{modified_date_estimated_lte}"'
            )
        filter_params.append(list_to_filter("buyer", buyer))
        filter_params.append(list_to_filter("exporter", exporter))
        filter_params.append(list_to_filter("contractGroup", contract_group))
        filter_params.append(list_to_filter("supplyMarket", supply_market))
        filter_params.append(list_to_filter("assumedDestination", assumed_destination))
        filter_params.append(list_to_filter("contractType", contract_type))
        filter_params.append(
            list_to_filter("liquefactionProject", liquefaction_project)
        )
        filter_params.append(list_to_filter("shippingTerms", shipping_terms))
        filter_params.append(
            list_to_filter("destinationFlexibility", destination_flexibility)
        )
        filter_params.append(list_to_filter("estimatedStartDate", estimated_start_date))
        if estimated_start_date_gt is not None:
            filter_params.append(f'estimatedStartDate > "{estimated_start_date_gt}"')
        if estimated_start_date_gte is not None:
            filter_params.append(f'estimatedStartDate >= "{estimated_start_date_gte}"')
        if estimated_start_date_lt is not None:
            filter_params.append(f'estimatedStartDate < "{estimated_start_date_lt}"')
        if estimated_start_date_lte is not None:
            filter_params.append(f'estimatedStartDate <= "{estimated_start_date_lte}"')
        filter_params.append(list_to_filter("estimatedEndDate", estimated_end_date))
        if estimated_end_date_gt is not None:
            filter_params.append(f'estimatedEndDate > "{estimated_end_date_gt}"')
        if estimated_end_date_gte is not None:
            filter_params.append(f'estimatedEndDate >= "{estimated_end_date_gte}"')
        if estimated_end_date_lt is not None:
            filter_params.append(f'estimatedEndDate < "{estimated_end_date_lt}"')
        if estimated_end_date_lte is not None:
            filter_params.append(f'estimatedEndDate <= "{estimated_end_date_lte}"')
        filter_params.append(list_to_filter("lengthYears", length_years))
        if length_years_gt is not None:
            filter_params.append(f'lengthYears > "{length_years_gt}"')
        if length_years_gte is not None:
            filter_params.append(f'lengthYears >= "{length_years_gte}"')
        if length_years_lt is not None:
            filter_params.append(f'lengthYears < "{length_years_lt}"')
        if length_years_lte is not None:
            filter_params.append(f'lengthYears <= "{length_years_lte}"')
        filter_params.append(list_to_filter("originalSigning", original_signing))
        if original_signing_gt is not None:
            filter_params.append(f'originalSigning > "{original_signing_gt}"')
        if original_signing_gte is not None:
            filter_params.append(f'originalSigning >= "{original_signing_gte}"')
        if original_signing_lt is not None:
            filter_params.append(f'originalSigning < "{original_signing_lt}"')
        if original_signing_lte is not None:
            filter_params.append(f'originalSigning <= "{original_signing_lte}"')
        filter_params.append(
            list_to_filter("annualContractVolume", annual_contract_volume)
        )
        if annual_contract_volume_gt is not None:
            filter_params.append(
                f'annualContractVolume > "{annual_contract_volume_gt}"'
            )
        if annual_contract_volume_gte is not None:
            filter_params.append(
                f'annualContractVolume >= "{annual_contract_volume_gte}"'
            )
        if annual_contract_volume_lt is not None:
            filter_params.append(
                f'annualContractVolume < "{annual_contract_volume_lt}"'
            )
        if annual_contract_volume_lte is not None:
            filter_params.append(
                f'annualContractVolume <= "{annual_contract_volume_lte}"'
            )
        filter_params.append(
            list_to_filter("annualContractVolumeUom", annual_contract_volume_uom)
        )
        filter_params.append(
            list_to_filter("initialContractVolume", initial_contract_volume)
        )
        if initial_contract_volume_gt is not None:
            filter_params.append(
                f'initialContractVolume > "{initial_contract_volume_gt}"'
            )
        if initial_contract_volume_gte is not None:
            filter_params.append(
                f'initialContractVolume >= "{initial_contract_volume_gte}"'
            )
        if initial_contract_volume_lt is not None:
            filter_params.append(
                f'initialContractVolume < "{initial_contract_volume_lt}"'
            )
        if initial_contract_volume_lte is not None:
            filter_params.append(
                f'initialContractVolume <= "{initial_contract_volume_lte}"'
            )
        filter_params.append(
            list_to_filter("initialContractVolumeUom", initial_contract_volume_uom)
        )
        filter_params.append(list_to_filter("pricingLinkage", pricing_linkage))
        filter_params.append(list_to_filter("specificPriceLink", specific_price_link))
        filter_params.append(list_to_filter("fidEnabling", fid_enabling))
        filter_params.append(list_to_filter("greenOrBrownfield", green_or_brownfield))
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
            path=f"/lng/v1/analytics/assets-contracts/monthly-estimated-buildout/offtake-contracts",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_monthly_estimated_buildout_liquefaction_capacity(
        self,
        *,
        buildout_month_estimated: Optional[datetime] = None,
        buildout_month_estimated_lt: Optional[datetime] = None,
        buildout_month_estimated_lte: Optional[datetime] = None,
        buildout_month_estimated_gt: Optional[datetime] = None,
        buildout_month_estimated_gte: Optional[datetime] = None,
        created_date_estimated: Optional[datetime] = None,
        created_date_estimated_lt: Optional[datetime] = None,
        created_date_estimated_lte: Optional[datetime] = None,
        created_date_estimated_gt: Optional[datetime] = None,
        created_date_estimated_gte: Optional[datetime] = None,
        modified_date_estimated: Optional[datetime] = None,
        modified_date_estimated_lt: Optional[datetime] = None,
        modified_date_estimated_lte: Optional[datetime] = None,
        modified_date_estimated_gt: Optional[datetime] = None,
        modified_date_estimated_gte: Optional[datetime] = None,
        buildout_month_announced: Optional[datetime] = None,
        buildout_month_announced_lt: Optional[datetime] = None,
        buildout_month_announced_lte: Optional[datetime] = None,
        buildout_month_announced_gt: Optional[datetime] = None,
        buildout_month_announced_gte: Optional[datetime] = None,
        created_date_announced: Optional[datetime] = None,
        created_date_announced_lt: Optional[datetime] = None,
        created_date_announced_lte: Optional[datetime] = None,
        created_date_announced_gt: Optional[datetime] = None,
        created_date_announced_gte: Optional[datetime] = None,
        modified_date_announced: Optional[datetime] = None,
        modified_date_announced_lt: Optional[datetime] = None,
        modified_date_announced_lte: Optional[datetime] = None,
        modified_date_announced_gt: Optional[datetime] = None,
        modified_date_announced_gte: Optional[datetime] = None,
        liquefaction_train: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_project: Optional[Union[list[str], Series[str], str]] = None,
        initial_capacity: Optional[float] = None,
        initial_capacity_lt: Optional[float] = None,
        initial_capacity_lte: Optional[float] = None,
        initial_capacity_gt: Optional[float] = None,
        initial_capacity_gte: Optional[float] = None,
        initial_capacity_uom: Optional[Union[list[str], Series[str], str]] = None,
        estimated_start_date: Optional[datetime] = None,
        estimated_start_date_lt: Optional[datetime] = None,
        estimated_start_date_lte: Optional[datetime] = None,
        estimated_start_date_gt: Optional[datetime] = None,
        estimated_start_date_gte: Optional[datetime] = None,
        announced_start_date: Optional[datetime] = None,
        announced_start_date_lt: Optional[datetime] = None,
        announced_start_date_lte: Optional[datetime] = None,
        announced_start_date_gt: Optional[datetime] = None,
        announced_start_date_gte: Optional[datetime] = None,
        train_status: Optional[Union[list[str], Series[str], str]] = None,
        green_brownfield: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_technology: Optional[Union[list[str], Series[str], str]] = None,
        supply_market: Optional[Union[list[str], Series[str], str]] = None,
        train_operator: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides monthly announced and estimated capacity buildouts for liquefaction trains and their attributes

        Parameters
        ----------

         buildout_month_estimated: Optional[datetime], optional
             Month for which the estimated buildout information is provided, by default None
         buildout_month_estimated_gt: Optional[datetime], optional
             filter by `buildout_month_estimated > x`, by default None
         buildout_month_estimated_gte: Optional[datetime], optional
             filter by `buildout_month_estimated >= x`, by default None
         buildout_month_estimated_lt: Optional[datetime], optional
             filter by `buildout_month_estimated < x`, by default None
         buildout_month_estimated_lte: Optional[datetime], optional
             filter by `buildout_month_estimated <= x`, by default None
         created_date_estimated: Optional[datetime], optional
             Date when the estimated buildout information was created, by default None
         created_date_estimated_gt: Optional[datetime], optional
             filter by `created_date_estimated > x`, by default None
         created_date_estimated_gte: Optional[datetime], optional
             filter by `created_date_estimated >= x`, by default None
         created_date_estimated_lt: Optional[datetime], optional
             filter by `created_date_estimated < x`, by default None
         created_date_estimated_lte: Optional[datetime], optional
             filter by `created_date_estimated <= x`, by default None
         modified_date_estimated: Optional[datetime], optional
             Date when the estimated buildout information was last modified, by default None
         modified_date_estimated_gt: Optional[datetime], optional
             filter by `modified_date_estimated > x`, by default None
         modified_date_estimated_gte: Optional[datetime], optional
             filter by `modified_date_estimated >= x`, by default None
         modified_date_estimated_lt: Optional[datetime], optional
             filter by `modified_date_estimated < x`, by default None
         modified_date_estimated_lte: Optional[datetime], optional
             filter by `modified_date_estimated <= x`, by default None
         buildout_month_announced: Optional[datetime], optional
             Month for which the announced buildout information is provided, by default None
         buildout_month_announced_gt: Optional[datetime], optional
             filter by `buildout_month_announced > x`, by default None
         buildout_month_announced_gte: Optional[datetime], optional
             filter by `buildout_month_announced >= x`, by default None
         buildout_month_announced_lt: Optional[datetime], optional
             filter by `buildout_month_announced < x`, by default None
         buildout_month_announced_lte: Optional[datetime], optional
             filter by `buildout_month_announced <= x`, by default None
         created_date_announced: Optional[datetime], optional
             Date when the announced buildout information was created, by default None
         created_date_announced_gt: Optional[datetime], optional
             filter by `created_date_announced > x`, by default None
         created_date_announced_gte: Optional[datetime], optional
             filter by `created_date_announced >= x`, by default None
         created_date_announced_lt: Optional[datetime], optional
             filter by `created_date_announced < x`, by default None
         created_date_announced_lte: Optional[datetime], optional
             filter by `created_date_announced <= x`, by default None
         modified_date_announced: Optional[datetime], optional
             Date when the announced buildout information was last modified, by default None
         modified_date_announced_gt: Optional[datetime], optional
             filter by `modified_date_announced > x`, by default None
         modified_date_announced_gte: Optional[datetime], optional
             filter by `modified_date_announced >= x`, by default None
         modified_date_announced_lt: Optional[datetime], optional
             filter by `modified_date_announced < x`, by default None
         modified_date_announced_lte: Optional[datetime], optional
             filter by `modified_date_announced <= x`, by default None
         liquefaction_train: Optional[Union[list[str], Series[str], str]]
             Name or identifier of the liquefaction train associated with the buildout information, by default None
         liquefaction_project: Optional[Union[list[str], Series[str], str]]
             Name or title of the liquefaction project associated with the buildout information, by default None
         initial_capacity: Optional[float], optional
             Numeric values of the initial capacity of the liquefaction train, by default None
         initial_capacity_gt: Optional[float], optional
             filter by `initial_capacity > x`, by default None
         initial_capacity_gte: Optional[float], optional
             filter by `initial_capacity >= x`, by default None
         initial_capacity_lt: Optional[float], optional
             filter by `initial_capacity < x`, by default None
         initial_capacity_lte: Optional[float], optional
             filter by `initial_capacity <= x`, by default None
         initial_capacity_uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the initial capacity of the liquefaction train, by default None
         estimated_start_date: Optional[datetime], optional
             Our estimated start date for the liquefaction train, by default None
         estimated_start_date_gt: Optional[datetime], optional
             filter by `estimated_start_date > x`, by default None
         estimated_start_date_gte: Optional[datetime], optional
             filter by `estimated_start_date >= x`, by default None
         estimated_start_date_lt: Optional[datetime], optional
             filter by `estimated_start_date < x`, by default None
         estimated_start_date_lte: Optional[datetime], optional
             filter by `estimated_start_date <= x`, by default None
         announced_start_date: Optional[datetime], optional
             Announced start date for the liquefaction train, by default None
         announced_start_date_gt: Optional[datetime], optional
             filter by `announced_start_date > x`, by default None
         announced_start_date_gte: Optional[datetime], optional
             filter by `announced_start_date >= x`, by default None
         announced_start_date_lt: Optional[datetime], optional
             filter by `announced_start_date < x`, by default None
         announced_start_date_lte: Optional[datetime], optional
             filter by `announced_start_date <= x`, by default None
         train_status: Optional[Union[list[str], Series[str], str]]
             Status of the liquefaction train, by default None
         green_brownfield: Optional[Union[list[str], Series[str], str]]
             Indicates whether the liquefaction project is greenfield or brownfield, by default None
         liquefaction_technology: Optional[Union[list[str], Series[str], str]]
             Technology used for liquefaction, by default None
         supply_market: Optional[Union[list[str], Series[str], str]]
             Market where the liquefaction train is located, by default None
         train_operator: Optional[Union[list[str], Series[str], str]]
             Entity or company operating the liquefaction train, by default None
         modified_date: Optional[datetime], optional
             Liquefaction capacity monthly estimated buildout record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("buildoutMonthEstimated", buildout_month_estimated)
        )
        if buildout_month_estimated_gt is not None:
            filter_params.append(
                f'buildoutMonthEstimated > "{buildout_month_estimated_gt}"'
            )
        if buildout_month_estimated_gte is not None:
            filter_params.append(
                f'buildoutMonthEstimated >= "{buildout_month_estimated_gte}"'
            )
        if buildout_month_estimated_lt is not None:
            filter_params.append(
                f'buildoutMonthEstimated < "{buildout_month_estimated_lt}"'
            )
        if buildout_month_estimated_lte is not None:
            filter_params.append(
                f'buildoutMonthEstimated <= "{buildout_month_estimated_lte}"'
            )
        filter_params.append(
            list_to_filter("createdDateEstimated", created_date_estimated)
        )
        if created_date_estimated_gt is not None:
            filter_params.append(
                f'createdDateEstimated > "{created_date_estimated_gt}"'
            )
        if created_date_estimated_gte is not None:
            filter_params.append(
                f'createdDateEstimated >= "{created_date_estimated_gte}"'
            )
        if created_date_estimated_lt is not None:
            filter_params.append(
                f'createdDateEstimated < "{created_date_estimated_lt}"'
            )
        if created_date_estimated_lte is not None:
            filter_params.append(
                f'createdDateEstimated <= "{created_date_estimated_lte}"'
            )
        filter_params.append(
            list_to_filter("modifiedDateEstimated", modified_date_estimated)
        )
        if modified_date_estimated_gt is not None:
            filter_params.append(
                f'modifiedDateEstimated > "{modified_date_estimated_gt}"'
            )
        if modified_date_estimated_gte is not None:
            filter_params.append(
                f'modifiedDateEstimated >= "{modified_date_estimated_gte}"'
            )
        if modified_date_estimated_lt is not None:
            filter_params.append(
                f'modifiedDateEstimated < "{modified_date_estimated_lt}"'
            )
        if modified_date_estimated_lte is not None:
            filter_params.append(
                f'modifiedDateEstimated <= "{modified_date_estimated_lte}"'
            )
        filter_params.append(
            list_to_filter("buildoutMonthAnnounced", buildout_month_announced)
        )
        if buildout_month_announced_gt is not None:
            filter_params.append(
                f'buildoutMonthAnnounced > "{buildout_month_announced_gt}"'
            )
        if buildout_month_announced_gte is not None:
            filter_params.append(
                f'buildoutMonthAnnounced >= "{buildout_month_announced_gte}"'
            )
        if buildout_month_announced_lt is not None:
            filter_params.append(
                f'buildoutMonthAnnounced < "{buildout_month_announced_lt}"'
            )
        if buildout_month_announced_lte is not None:
            filter_params.append(
                f'buildoutMonthAnnounced <= "{buildout_month_announced_lte}"'
            )
        filter_params.append(
            list_to_filter("createdDateAnnounced", created_date_announced)
        )
        if created_date_announced_gt is not None:
            filter_params.append(
                f'createdDateAnnounced > "{created_date_announced_gt}"'
            )
        if created_date_announced_gte is not None:
            filter_params.append(
                f'createdDateAnnounced >= "{created_date_announced_gte}"'
            )
        if created_date_announced_lt is not None:
            filter_params.append(
                f'createdDateAnnounced < "{created_date_announced_lt}"'
            )
        if created_date_announced_lte is not None:
            filter_params.append(
                f'createdDateAnnounced <= "{created_date_announced_lte}"'
            )
        filter_params.append(
            list_to_filter("modifiedDateAnnounced", modified_date_announced)
        )
        if modified_date_announced_gt is not None:
            filter_params.append(
                f'modifiedDateAnnounced > "{modified_date_announced_gt}"'
            )
        if modified_date_announced_gte is not None:
            filter_params.append(
                f'modifiedDateAnnounced >= "{modified_date_announced_gte}"'
            )
        if modified_date_announced_lt is not None:
            filter_params.append(
                f'modifiedDateAnnounced < "{modified_date_announced_lt}"'
            )
        if modified_date_announced_lte is not None:
            filter_params.append(
                f'modifiedDateAnnounced <= "{modified_date_announced_lte}"'
            )
        filter_params.append(list_to_filter("liquefactionTrain", liquefaction_train))
        filter_params.append(
            list_to_filter("liquefactionProject", liquefaction_project)
        )
        filter_params.append(list_to_filter("initialCapacity", initial_capacity))
        if initial_capacity_gt is not None:
            filter_params.append(f'initialCapacity > "{initial_capacity_gt}"')
        if initial_capacity_gte is not None:
            filter_params.append(f'initialCapacity >= "{initial_capacity_gte}"')
        if initial_capacity_lt is not None:
            filter_params.append(f'initialCapacity < "{initial_capacity_lt}"')
        if initial_capacity_lte is not None:
            filter_params.append(f'initialCapacity <= "{initial_capacity_lte}"')
        filter_params.append(list_to_filter("initialCapacityUom", initial_capacity_uom))
        filter_params.append(list_to_filter("estimatedStartDate", estimated_start_date))
        if estimated_start_date_gt is not None:
            filter_params.append(f'estimatedStartDate > "{estimated_start_date_gt}"')
        if estimated_start_date_gte is not None:
            filter_params.append(f'estimatedStartDate >= "{estimated_start_date_gte}"')
        if estimated_start_date_lt is not None:
            filter_params.append(f'estimatedStartDate < "{estimated_start_date_lt}"')
        if estimated_start_date_lte is not None:
            filter_params.append(f'estimatedStartDate <= "{estimated_start_date_lte}"')
        filter_params.append(list_to_filter("announcedStartDate", announced_start_date))
        if announced_start_date_gt is not None:
            filter_params.append(f'announcedStartDate > "{announced_start_date_gt}"')
        if announced_start_date_gte is not None:
            filter_params.append(f'announcedStartDate >= "{announced_start_date_gte}"')
        if announced_start_date_lt is not None:
            filter_params.append(f'announcedStartDate < "{announced_start_date_lt}"')
        if announced_start_date_lte is not None:
            filter_params.append(f'announcedStartDate <= "{announced_start_date_lte}"')
        filter_params.append(list_to_filter("trainStatus", train_status))
        filter_params.append(list_to_filter("greenBrownfield", green_brownfield))
        filter_params.append(
            list_to_filter("liquefactionTechnology", liquefaction_technology)
        )
        filter_params.append(list_to_filter("supplyMarket", supply_market))
        filter_params.append(list_to_filter("trainOperator", train_operator))
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
            path=f"/lng/v1/analytics/assets-contracts/monthly-estimated-buildout/liquefaction-capacity",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_monthly_estimated_buildout_regasification_contracts(
        self,
        *,
        buildout_month_estimated: Optional[datetime] = None,
        buildout_month_estimated_lt: Optional[datetime] = None,
        buildout_month_estimated_lte: Optional[datetime] = None,
        buildout_month_estimated_gt: Optional[datetime] = None,
        buildout_month_estimated_gte: Optional[datetime] = None,
        created_date_estimated: Optional[datetime] = None,
        created_date_estimated_lt: Optional[datetime] = None,
        created_date_estimated_lte: Optional[datetime] = None,
        created_date_estimated_gt: Optional[datetime] = None,
        created_date_estimated_gte: Optional[datetime] = None,
        modified_date_estimated: Optional[datetime] = None,
        modified_date_estimated_lt: Optional[datetime] = None,
        modified_date_estimated_lte: Optional[datetime] = None,
        modified_date_estimated_gt: Optional[datetime] = None,
        modified_date_estimated_gte: Optional[datetime] = None,
        buildout_month_announced: Optional[datetime] = None,
        buildout_month_announced_lt: Optional[datetime] = None,
        buildout_month_announced_lte: Optional[datetime] = None,
        buildout_month_announced_gt: Optional[datetime] = None,
        buildout_month_announced_gte: Optional[datetime] = None,
        created_date_announced: Optional[datetime] = None,
        created_date_announced_lt: Optional[datetime] = None,
        created_date_announced_lte: Optional[datetime] = None,
        created_date_announced_gt: Optional[datetime] = None,
        created_date_announced_gte: Optional[datetime] = None,
        modified_date_announced: Optional[datetime] = None,
        modified_date_announced_lt: Optional[datetime] = None,
        modified_date_announced_lte: Optional[datetime] = None,
        modified_date_announced_gt: Optional[datetime] = None,
        modified_date_announced_gte: Optional[datetime] = None,
        regasification_phase: Optional[Union[list[str], Series[str], str]] = None,
        regasification_terminal: Optional[Union[list[str], Series[str], str]] = None,
        market: Optional[Union[list[str], Series[str], str]] = None,
        terminal_type: Optional[Union[list[str], Series[str], str]] = None,
        contract_type: Optional[Union[list[str], Series[str], str]] = None,
        capacity_owner: Optional[Union[list[str], Series[str], str]] = None,
        contract_volume: Optional[float] = None,
        contract_volume_lt: Optional[float] = None,
        contract_volume_lte: Optional[float] = None,
        contract_volume_gt: Optional[float] = None,
        contract_volume_gte: Optional[float] = None,
        contract_volume_uom: Optional[Union[list[str], Series[str], str]] = None,
        phase_capacity: Optional[float] = None,
        phase_capacity_lt: Optional[float] = None,
        phase_capacity_lte: Optional[float] = None,
        phase_capacity_gt: Optional[float] = None,
        phase_capacity_gte: Optional[float] = None,
        phase_capacity_uom: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides monthly announced and estimated offtake contract buildouts for liquefaction projects and their attributes

        Parameters
        ----------

         buildout_month_estimated: Optional[datetime], optional
             Month the buildout is estimated to start, by default None
         buildout_month_estimated_gt: Optional[datetime], optional
             filter by `buildout_month_estimated > x`, by default None
         buildout_month_estimated_gte: Optional[datetime], optional
             filter by `buildout_month_estimated >= x`, by default None
         buildout_month_estimated_lt: Optional[datetime], optional
             filter by `buildout_month_estimated < x`, by default None
         buildout_month_estimated_lte: Optional[datetime], optional
             filter by `buildout_month_estimated <= x`, by default None
         created_date_estimated: Optional[datetime], optional
             Date when the estimated buildout information was created, by default None
         created_date_estimated_gt: Optional[datetime], optional
             filter by `created_date_estimated > x`, by default None
         created_date_estimated_gte: Optional[datetime], optional
             filter by `created_date_estimated >= x`, by default None
         created_date_estimated_lt: Optional[datetime], optional
             filter by `created_date_estimated < x`, by default None
         created_date_estimated_lte: Optional[datetime], optional
             filter by `created_date_estimated <= x`, by default None
         modified_date_estimated: Optional[datetime], optional
             Date when the estimated buildout information was last modified, by default None
         modified_date_estimated_gt: Optional[datetime], optional
             filter by `modified_date_estimated > x`, by default None
         modified_date_estimated_gte: Optional[datetime], optional
             filter by `modified_date_estimated >= x`, by default None
         modified_date_estimated_lt: Optional[datetime], optional
             filter by `modified_date_estimated < x`, by default None
         modified_date_estimated_lte: Optional[datetime], optional
             filter by `modified_date_estimated <= x`, by default None
         buildout_month_announced: Optional[datetime], optional
             Month for which the announced buildout information is provided, by default None
         buildout_month_announced_gt: Optional[datetime], optional
             filter by `buildout_month_announced > x`, by default None
         buildout_month_announced_gte: Optional[datetime], optional
             filter by `buildout_month_announced >= x`, by default None
         buildout_month_announced_lt: Optional[datetime], optional
             filter by `buildout_month_announced < x`, by default None
         buildout_month_announced_lte: Optional[datetime], optional
             filter by `buildout_month_announced <= x`, by default None
         created_date_announced: Optional[datetime], optional
             Date when the announced buildout information was created, by default None
         created_date_announced_gt: Optional[datetime], optional
             filter by `created_date_announced > x`, by default None
         created_date_announced_gte: Optional[datetime], optional
             filter by `created_date_announced >= x`, by default None
         created_date_announced_lt: Optional[datetime], optional
             filter by `created_date_announced < x`, by default None
         created_date_announced_lte: Optional[datetime], optional
             filter by `created_date_announced <= x`, by default None
         modified_date_announced: Optional[datetime], optional
             Date when the announced buildout information was last modified, by default None
         modified_date_announced_gt: Optional[datetime], optional
             filter by `modified_date_announced > x`, by default None
         modified_date_announced_gte: Optional[datetime], optional
             filter by `modified_date_announced >= x`, by default None
         modified_date_announced_lt: Optional[datetime], optional
             filter by `modified_date_announced < x`, by default None
         modified_date_announced_lte: Optional[datetime], optional
             filter by `modified_date_announced <= x`, by default None
         regasification_phase: Optional[Union[list[str], Series[str], str]]
             Name of regasification phase the contract is associated with, by default None
         regasification_terminal: Optional[Union[list[str], Series[str], str]]
             Name of regasification terminal the contract is associated with, by default None
         market: Optional[Union[list[str], Series[str], str]]
             Market where the regasification contract is located, by default None
         terminal_type: Optional[Union[list[str], Series[str], str]]
             Offshore or onshore terminal, by default None
         contract_type: Optional[Union[list[str], Series[str], str]]
             The status of the capcity contract, by default None
         capacity_owner: Optional[Union[list[str], Series[str], str]]
             Company of joint venture name that owns the capacity contract, by default None
         contract_volume: Optional[float], optional
             Numeric values of the contract quantity for the given regasification contract, by default None
         contract_volume_gt: Optional[float], optional
             filter by `contract_volume > x`, by default None
         contract_volume_gte: Optional[float], optional
             filter by `contract_volume >= x`, by default None
         contract_volume_lt: Optional[float], optional
             filter by `contract_volume < x`, by default None
         contract_volume_lte: Optional[float], optional
             filter by `contract_volume <= x`, by default None
         contract_volume_uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the contract quantity for the given regasification contract, by default None
         phase_capacity: Optional[float], optional
             Numeric values of the regasification phase capacity for the corresponding regasification contract, by default None
         phase_capacity_gt: Optional[float], optional
             filter by `phase_capacity > x`, by default None
         phase_capacity_gte: Optional[float], optional
             filter by `phase_capacity >= x`, by default None
         phase_capacity_lt: Optional[float], optional
             filter by `phase_capacity < x`, by default None
         phase_capacity_lte: Optional[float], optional
             filter by `phase_capacity <= x`, by default None
         phase_capacity_uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the regasification phase capacity for the corresponding regasification contract, by default None
         modified_date: Optional[datetime], optional
             Date when the regasification contract was last modified, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("buildoutMonthEstimated", buildout_month_estimated)
        )
        if buildout_month_estimated_gt is not None:
            filter_params.append(
                f'buildoutMonthEstimated > "{buildout_month_estimated_gt}"'
            )
        if buildout_month_estimated_gte is not None:
            filter_params.append(
                f'buildoutMonthEstimated >= "{buildout_month_estimated_gte}"'
            )
        if buildout_month_estimated_lt is not None:
            filter_params.append(
                f'buildoutMonthEstimated < "{buildout_month_estimated_lt}"'
            )
        if buildout_month_estimated_lte is not None:
            filter_params.append(
                f'buildoutMonthEstimated <= "{buildout_month_estimated_lte}"'
            )
        filter_params.append(
            list_to_filter("createdDateEstimated", created_date_estimated)
        )
        if created_date_estimated_gt is not None:
            filter_params.append(
                f'createdDateEstimated > "{created_date_estimated_gt}"'
            )
        if created_date_estimated_gte is not None:
            filter_params.append(
                f'createdDateEstimated >= "{created_date_estimated_gte}"'
            )
        if created_date_estimated_lt is not None:
            filter_params.append(
                f'createdDateEstimated < "{created_date_estimated_lt}"'
            )
        if created_date_estimated_lte is not None:
            filter_params.append(
                f'createdDateEstimated <= "{created_date_estimated_lte}"'
            )
        filter_params.append(
            list_to_filter("modifiedDateEstimated", modified_date_estimated)
        )
        if modified_date_estimated_gt is not None:
            filter_params.append(
                f'modifiedDateEstimated > "{modified_date_estimated_gt}"'
            )
        if modified_date_estimated_gte is not None:
            filter_params.append(
                f'modifiedDateEstimated >= "{modified_date_estimated_gte}"'
            )
        if modified_date_estimated_lt is not None:
            filter_params.append(
                f'modifiedDateEstimated < "{modified_date_estimated_lt}"'
            )
        if modified_date_estimated_lte is not None:
            filter_params.append(
                f'modifiedDateEstimated <= "{modified_date_estimated_lte}"'
            )
        filter_params.append(
            list_to_filter("buildoutMonthAnnounced", buildout_month_announced)
        )
        if buildout_month_announced_gt is not None:
            filter_params.append(
                f'buildoutMonthAnnounced > "{buildout_month_announced_gt}"'
            )
        if buildout_month_announced_gte is not None:
            filter_params.append(
                f'buildoutMonthAnnounced >= "{buildout_month_announced_gte}"'
            )
        if buildout_month_announced_lt is not None:
            filter_params.append(
                f'buildoutMonthAnnounced < "{buildout_month_announced_lt}"'
            )
        if buildout_month_announced_lte is not None:
            filter_params.append(
                f'buildoutMonthAnnounced <= "{buildout_month_announced_lte}"'
            )
        filter_params.append(
            list_to_filter("createdDateAnnounced", created_date_announced)
        )
        if created_date_announced_gt is not None:
            filter_params.append(
                f'createdDateAnnounced > "{created_date_announced_gt}"'
            )
        if created_date_announced_gte is not None:
            filter_params.append(
                f'createdDateAnnounced >= "{created_date_announced_gte}"'
            )
        if created_date_announced_lt is not None:
            filter_params.append(
                f'createdDateAnnounced < "{created_date_announced_lt}"'
            )
        if created_date_announced_lte is not None:
            filter_params.append(
                f'createdDateAnnounced <= "{created_date_announced_lte}"'
            )
        filter_params.append(
            list_to_filter("modifiedDateAnnounced", modified_date_announced)
        )
        if modified_date_announced_gt is not None:
            filter_params.append(
                f'modifiedDateAnnounced > "{modified_date_announced_gt}"'
            )
        if modified_date_announced_gte is not None:
            filter_params.append(
                f'modifiedDateAnnounced >= "{modified_date_announced_gte}"'
            )
        if modified_date_announced_lt is not None:
            filter_params.append(
                f'modifiedDateAnnounced < "{modified_date_announced_lt}"'
            )
        if modified_date_announced_lte is not None:
            filter_params.append(
                f'modifiedDateAnnounced <= "{modified_date_announced_lte}"'
            )
        filter_params.append(
            list_to_filter("regasificationPhase", regasification_phase)
        )
        filter_params.append(
            list_to_filter("regasificationTerminal", regasification_terminal)
        )
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("terminalType", terminal_type))
        filter_params.append(list_to_filter("contractType", contract_type))
        filter_params.append(list_to_filter("capacityOwner", capacity_owner))
        filter_params.append(list_to_filter("contractVolume", contract_volume))
        if contract_volume_gt is not None:
            filter_params.append(f'contractVolume > "{contract_volume_gt}"')
        if contract_volume_gte is not None:
            filter_params.append(f'contractVolume >= "{contract_volume_gte}"')
        if contract_volume_lt is not None:
            filter_params.append(f'contractVolume < "{contract_volume_lt}"')
        if contract_volume_lte is not None:
            filter_params.append(f'contractVolume <= "{contract_volume_lte}"')
        filter_params.append(list_to_filter("contractVolumeUom", contract_volume_uom))
        filter_params.append(list_to_filter("phaseCapacity", phase_capacity))
        if phase_capacity_gt is not None:
            filter_params.append(f'phaseCapacity > "{phase_capacity_gt}"')
        if phase_capacity_gte is not None:
            filter_params.append(f'phaseCapacity >= "{phase_capacity_gte}"')
        if phase_capacity_lt is not None:
            filter_params.append(f'phaseCapacity < "{phase_capacity_lt}"')
        if phase_capacity_lte is not None:
            filter_params.append(f'phaseCapacity <= "{phase_capacity_lte}"')
        filter_params.append(list_to_filter("phaseCapacityUom", phase_capacity_uom))
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
            path=f"/lng/v1/analytics/assets-contracts/monthly-estimated-buildout/regasification-contracts",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_monthly_estimated_buildout_regasification_capacity(
        self,
        *,
        buildout_month_estimated: Optional[datetime] = None,
        buildout_month_estimated_lt: Optional[datetime] = None,
        buildout_month_estimated_lte: Optional[datetime] = None,
        buildout_month_estimated_gt: Optional[datetime] = None,
        buildout_month_estimated_gte: Optional[datetime] = None,
        created_date_estimated: Optional[datetime] = None,
        created_date_estimated_lt: Optional[datetime] = None,
        created_date_estimated_lte: Optional[datetime] = None,
        created_date_estimated_gt: Optional[datetime] = None,
        created_date_estimated_gte: Optional[datetime] = None,
        modified_date_estimated: Optional[datetime] = None,
        modified_date_estimated_lt: Optional[datetime] = None,
        modified_date_estimated_lte: Optional[datetime] = None,
        modified_date_estimated_gt: Optional[datetime] = None,
        modified_date_estimated_gte: Optional[datetime] = None,
        buildout_month_announced: Optional[datetime] = None,
        buildout_month_announced_lt: Optional[datetime] = None,
        buildout_month_announced_lte: Optional[datetime] = None,
        buildout_month_announced_gt: Optional[datetime] = None,
        buildout_month_announced_gte: Optional[datetime] = None,
        created_date_announced: Optional[datetime] = None,
        created_date_announced_lt: Optional[datetime] = None,
        created_date_announced_lte: Optional[datetime] = None,
        created_date_announced_gt: Optional[datetime] = None,
        created_date_announced_gte: Optional[datetime] = None,
        modified_date_announced: Optional[datetime] = None,
        modified_date_announced_lt: Optional[datetime] = None,
        modified_date_announced_lte: Optional[datetime] = None,
        modified_date_announced_gt: Optional[datetime] = None,
        modified_date_announced_gte: Optional[datetime] = None,
        regasification_phase: Optional[Union[list[str], Series[str], str]] = None,
        regasification_project: Optional[Union[list[str], Series[str], str]] = None,
        market: Optional[Union[list[str], Series[str], str]] = None,
        phase_status: Optional[Union[list[str], Series[str], str]] = None,
        estimated_start_date: Optional[datetime] = None,
        estimated_start_date_lt: Optional[datetime] = None,
        estimated_start_date_lte: Optional[datetime] = None,
        estimated_start_date_gt: Optional[datetime] = None,
        estimated_start_date_gte: Optional[datetime] = None,
        announced_start_date: Optional[datetime] = None,
        announced_start_date_lt: Optional[datetime] = None,
        announced_start_date_lte: Optional[datetime] = None,
        announced_start_date_gt: Optional[datetime] = None,
        announced_start_date_gte: Optional[datetime] = None,
        initial_capacity: Optional[float] = None,
        initial_capacity_lt: Optional[float] = None,
        initial_capacity_lte: Optional[float] = None,
        initial_capacity_gt: Optional[float] = None,
        initial_capacity_gte: Optional[float] = None,
        initial_capacity_uom: Optional[Union[list[str], Series[str], str]] = None,
        terminal_type_name: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides monthly announced and estimated capacity buildouts for regasification phases and their attributes

        Parameters
        ----------

         buildout_month_estimated: Optional[datetime], optional
             Month in which the estimated buildout of regasification capacity is expected to occur, by default None
         buildout_month_estimated_gt: Optional[datetime], optional
             filter by `buildout_month_estimated > x`, by default None
         buildout_month_estimated_gte: Optional[datetime], optional
             filter by `buildout_month_estimated >= x`, by default None
         buildout_month_estimated_lt: Optional[datetime], optional
             filter by `buildout_month_estimated < x`, by default None
         buildout_month_estimated_lte: Optional[datetime], optional
             filter by `buildout_month_estimated <= x`, by default None
         created_date_estimated: Optional[datetime], optional
             Date when the estimated buildout information was created, by default None
         created_date_estimated_gt: Optional[datetime], optional
             filter by `created_date_estimated > x`, by default None
         created_date_estimated_gte: Optional[datetime], optional
             filter by `created_date_estimated >= x`, by default None
         created_date_estimated_lt: Optional[datetime], optional
             filter by `created_date_estimated < x`, by default None
         created_date_estimated_lte: Optional[datetime], optional
             filter by `created_date_estimated <= x`, by default None
         modified_date_estimated: Optional[datetime], optional
             Date when the estimated buildout information was last modified, by default None
         modified_date_estimated_gt: Optional[datetime], optional
             filter by `modified_date_estimated > x`, by default None
         modified_date_estimated_gte: Optional[datetime], optional
             filter by `modified_date_estimated >= x`, by default None
         modified_date_estimated_lt: Optional[datetime], optional
             filter by `modified_date_estimated < x`, by default None
         modified_date_estimated_lte: Optional[datetime], optional
             filter by `modified_date_estimated <= x`, by default None
         buildout_month_announced: Optional[datetime], optional
             Month for which the announced buildout information is provided, by default None
         buildout_month_announced_gt: Optional[datetime], optional
             filter by `buildout_month_announced > x`, by default None
         buildout_month_announced_gte: Optional[datetime], optional
             filter by `buildout_month_announced >= x`, by default None
         buildout_month_announced_lt: Optional[datetime], optional
             filter by `buildout_month_announced < x`, by default None
         buildout_month_announced_lte: Optional[datetime], optional
             filter by `buildout_month_announced <= x`, by default None
         created_date_announced: Optional[datetime], optional
             Date when the announced buildout was created, by default None
         created_date_announced_gt: Optional[datetime], optional
             filter by `created_date_announced > x`, by default None
         created_date_announced_gte: Optional[datetime], optional
             filter by `created_date_announced >= x`, by default None
         created_date_announced_lt: Optional[datetime], optional
             filter by `created_date_announced < x`, by default None
         created_date_announced_lte: Optional[datetime], optional
             filter by `created_date_announced <= x`, by default None
         modified_date_announced: Optional[datetime], optional
             Date when the announced buildout was last modified, by default None
         modified_date_announced_gt: Optional[datetime], optional
             filter by `modified_date_announced > x`, by default None
         modified_date_announced_gte: Optional[datetime], optional
             filter by `modified_date_announced >= x`, by default None
         modified_date_announced_lt: Optional[datetime], optional
             filter by `modified_date_announced < x`, by default None
         modified_date_announced_lte: Optional[datetime], optional
             filter by `modified_date_announced <= x`, by default None
         regasification_phase: Optional[Union[list[str], Series[str], str]]
             Name of the regasification phase, by default None
         regasification_project: Optional[Union[list[str], Series[str], str]]
             Name of the regasification project associated with the phase, by default None
         market: Optional[Union[list[str], Series[str], str]]
             Market where the regasification phase is located, by default None
         phase_status: Optional[Union[list[str], Series[str], str]]
             Status of the regasification phase, by default None
         estimated_start_date: Optional[datetime], optional
             Our estimated start date for the regasification phase, by default None
         estimated_start_date_gt: Optional[datetime], optional
             filter by `estimated_start_date > x`, by default None
         estimated_start_date_gte: Optional[datetime], optional
             filter by `estimated_start_date >= x`, by default None
         estimated_start_date_lt: Optional[datetime], optional
             filter by `estimated_start_date < x`, by default None
         estimated_start_date_lte: Optional[datetime], optional
             filter by `estimated_start_date <= x`, by default None
         announced_start_date: Optional[datetime], optional
             Announced start date for the regasification phase, by default None
         announced_start_date_gt: Optional[datetime], optional
             filter by `announced_start_date > x`, by default None
         announced_start_date_gte: Optional[datetime], optional
             filter by `announced_start_date >= x`, by default None
         announced_start_date_lt: Optional[datetime], optional
             filter by `announced_start_date < x`, by default None
         announced_start_date_lte: Optional[datetime], optional
             filter by `announced_start_date <= x`, by default None
         initial_capacity: Optional[float], optional
             Numeric values of the initial capacity of the regasification phase, by default None
         initial_capacity_gt: Optional[float], optional
             filter by `initial_capacity > x`, by default None
         initial_capacity_gte: Optional[float], optional
             filter by `initial_capacity >= x`, by default None
         initial_capacity_lt: Optional[float], optional
             filter by `initial_capacity < x`, by default None
         initial_capacity_lte: Optional[float], optional
             filter by `initial_capacity <= x`, by default None
         initial_capacity_uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the initial capacity of the regasification phase, by default None
         terminal_type_name: Optional[Union[list[str], Series[str], str]]
             Type of regasification terminal, by default None
         modified_date: Optional[datetime], optional
             Regasification capacity monthly estimated buildout record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("buildoutMonthEstimated", buildout_month_estimated)
        )
        if buildout_month_estimated_gt is not None:
            filter_params.append(
                f'buildoutMonthEstimated > "{buildout_month_estimated_gt}"'
            )
        if buildout_month_estimated_gte is not None:
            filter_params.append(
                f'buildoutMonthEstimated >= "{buildout_month_estimated_gte}"'
            )
        if buildout_month_estimated_lt is not None:
            filter_params.append(
                f'buildoutMonthEstimated < "{buildout_month_estimated_lt}"'
            )
        if buildout_month_estimated_lte is not None:
            filter_params.append(
                f'buildoutMonthEstimated <= "{buildout_month_estimated_lte}"'
            )
        filter_params.append(
            list_to_filter("createdDateEstimated", created_date_estimated)
        )
        if created_date_estimated_gt is not None:
            filter_params.append(
                f'createdDateEstimated > "{created_date_estimated_gt}"'
            )
        if created_date_estimated_gte is not None:
            filter_params.append(
                f'createdDateEstimated >= "{created_date_estimated_gte}"'
            )
        if created_date_estimated_lt is not None:
            filter_params.append(
                f'createdDateEstimated < "{created_date_estimated_lt}"'
            )
        if created_date_estimated_lte is not None:
            filter_params.append(
                f'createdDateEstimated <= "{created_date_estimated_lte}"'
            )
        filter_params.append(
            list_to_filter("modifiedDateEstimated", modified_date_estimated)
        )
        if modified_date_estimated_gt is not None:
            filter_params.append(
                f'modifiedDateEstimated > "{modified_date_estimated_gt}"'
            )
        if modified_date_estimated_gte is not None:
            filter_params.append(
                f'modifiedDateEstimated >= "{modified_date_estimated_gte}"'
            )
        if modified_date_estimated_lt is not None:
            filter_params.append(
                f'modifiedDateEstimated < "{modified_date_estimated_lt}"'
            )
        if modified_date_estimated_lte is not None:
            filter_params.append(
                f'modifiedDateEstimated <= "{modified_date_estimated_lte}"'
            )
        filter_params.append(
            list_to_filter("buildoutMonthAnnounced", buildout_month_announced)
        )
        if buildout_month_announced_gt is not None:
            filter_params.append(
                f'buildoutMonthAnnounced > "{buildout_month_announced_gt}"'
            )
        if buildout_month_announced_gte is not None:
            filter_params.append(
                f'buildoutMonthAnnounced >= "{buildout_month_announced_gte}"'
            )
        if buildout_month_announced_lt is not None:
            filter_params.append(
                f'buildoutMonthAnnounced < "{buildout_month_announced_lt}"'
            )
        if buildout_month_announced_lte is not None:
            filter_params.append(
                f'buildoutMonthAnnounced <= "{buildout_month_announced_lte}"'
            )
        filter_params.append(
            list_to_filter("createdDateAnnounced", created_date_announced)
        )
        if created_date_announced_gt is not None:
            filter_params.append(
                f'createdDateAnnounced > "{created_date_announced_gt}"'
            )
        if created_date_announced_gte is not None:
            filter_params.append(
                f'createdDateAnnounced >= "{created_date_announced_gte}"'
            )
        if created_date_announced_lt is not None:
            filter_params.append(
                f'createdDateAnnounced < "{created_date_announced_lt}"'
            )
        if created_date_announced_lte is not None:
            filter_params.append(
                f'createdDateAnnounced <= "{created_date_announced_lte}"'
            )
        filter_params.append(
            list_to_filter("modifiedDateAnnounced", modified_date_announced)
        )
        if modified_date_announced_gt is not None:
            filter_params.append(
                f'modifiedDateAnnounced > "{modified_date_announced_gt}"'
            )
        if modified_date_announced_gte is not None:
            filter_params.append(
                f'modifiedDateAnnounced >= "{modified_date_announced_gte}"'
            )
        if modified_date_announced_lt is not None:
            filter_params.append(
                f'modifiedDateAnnounced < "{modified_date_announced_lt}"'
            )
        if modified_date_announced_lte is not None:
            filter_params.append(
                f'modifiedDateAnnounced <= "{modified_date_announced_lte}"'
            )
        filter_params.append(
            list_to_filter("regasificationPhase", regasification_phase)
        )
        filter_params.append(
            list_to_filter("regasificationProject", regasification_project)
        )
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("phaseStatus", phase_status))
        filter_params.append(list_to_filter("estimatedStartDate", estimated_start_date))
        if estimated_start_date_gt is not None:
            filter_params.append(f'estimatedStartDate > "{estimated_start_date_gt}"')
        if estimated_start_date_gte is not None:
            filter_params.append(f'estimatedStartDate >= "{estimated_start_date_gte}"')
        if estimated_start_date_lt is not None:
            filter_params.append(f'estimatedStartDate < "{estimated_start_date_lt}"')
        if estimated_start_date_lte is not None:
            filter_params.append(f'estimatedStartDate <= "{estimated_start_date_lte}"')
        filter_params.append(list_to_filter("announcedStartDate", announced_start_date))
        if announced_start_date_gt is not None:
            filter_params.append(f'announcedStartDate > "{announced_start_date_gt}"')
        if announced_start_date_gte is not None:
            filter_params.append(f'announcedStartDate >= "{announced_start_date_gte}"')
        if announced_start_date_lt is not None:
            filter_params.append(f'announcedStartDate < "{announced_start_date_lt}"')
        if announced_start_date_lte is not None:
            filter_params.append(f'announcedStartDate <= "{announced_start_date_lte}"')
        filter_params.append(list_to_filter("initialCapacity", initial_capacity))
        if initial_capacity_gt is not None:
            filter_params.append(f'initialCapacity > "{initial_capacity_gt}"')
        if initial_capacity_gte is not None:
            filter_params.append(f'initialCapacity >= "{initial_capacity_gte}"')
        if initial_capacity_lt is not None:
            filter_params.append(f'initialCapacity < "{initial_capacity_lt}"')
        if initial_capacity_lte is not None:
            filter_params.append(f'initialCapacity <= "{initial_capacity_lte}"')
        filter_params.append(list_to_filter("initialCapacityUom", initial_capacity_uom))
        filter_params.append(list_to_filter("terminalTypeName", terminal_type_name))
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
            path=f"/lng/v1/analytics/assets-contracts/monthly-estimated-buildout/regasification-capacity",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assets_contracts_feedstock(
        self,
        *,
        economic_group: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_project: Optional[Union[list[str], Series[str], str]] = None,
        feedstock_asset: Optional[Union[list[str], Series[str], str]] = None,
        supply_market: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        production_type: Optional[Union[list[str], Series[str], str]] = None,
        production_uom: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides information on LNG feedstock production over time

        Parameters
        ----------

         economic_group: Optional[Union[list[str], Series[str], str]]
             Classification of the project based on economic characteristics, by default None
         liquefaction_project: Optional[Union[list[str], Series[str], str]]
             Name of the specific liquefaction project, by default None
         feedstock_asset: Optional[Union[list[str], Series[str], str]]
             The specific feedstock asset used in the liquefaction process, by default None
         supply_market: Optional[Union[list[str], Series[str], str]]
             The market from which the feedstock is sourced, by default None
         year: Optional[int], optional
             The year to which the data corresponds, by default None
         year_gt: Optional[int], optional
             filter by `year > x`, by default None
         year_gte: Optional[int], optional
             filter by `year >= x`, by default None
         year_lt: Optional[int], optional
             filter by `year < x`, by default None
         year_lte: Optional[int], optional
             filter by `year <= x`, by default None
         production_type: Optional[Union[list[str], Series[str], str]]
             The category of production. This includes different commodities as well as the capacity of feedstock gas into the liquefaction project, by default None
         production_uom: Optional[Union[list[str], Series[str], str]]
             The unit of measure of the production rate for a given commidity or capacity rate of the liquefaction project, by default None
         modified_date: Optional[datetime], optional
             Feedstock record latest modified date, by default None
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
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("economicGroup", economic_group))
        filter_params.append(
            list_to_filter("liquefactionProject", liquefaction_project)
        )
        filter_params.append(list_to_filter("feedstockAsset", feedstock_asset))
        filter_params.append(list_to_filter("supplyMarket", supply_market))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("productionType", production_type))
        filter_params.append(list_to_filter("productionUom", production_uom))
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
            path=f"/lng/v1/analytics/assets-contracts/feedstock",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_liquefaction_projects(
        self,
        *,
        liquefaction_project_id: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_project_name: Optional[Union[list[str], Series[str], str]] = None,
        country_coast_name: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Allows the user to access list of LNG Liquefaction Projects

        Parameters
        ----------

         liquefaction_project_id: Optional[Union[list[str], Series[str], str]]
             Unique LNG Liquefaction Project Id, by default None
         liquefaction_project_name: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction Project Name, by default None
         country_coast_name: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction country coast name, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("liquefactionProjectId", liquefaction_project_id)
        )
        filter_params.append(
            list_to_filter("liquefactionProjectName", liquefaction_project_name)
        )
        filter_params.append(list_to_filter("countryCoastName", country_coast_name))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/outages/liquefaction-projects",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_liquefaction_trains(
        self,
        *,
        liquefaction_train_id: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_train_name: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_project_id: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_project_name: Optional[Union[list[str], Series[str], str]] = None,
        initial_capacity: Optional[str] = None,
        initial_capacity_lt: Optional[str] = None,
        initial_capacity_lte: Optional[str] = None,
        initial_capacity_gt: Optional[str] = None,
        initial_capacity_gte: Optional[str] = None,
        status: Optional[Union[list[str], Series[str], str]] = None,
        start_date: Optional[datetime] = None,
        start_date_lt: Optional[datetime] = None,
        start_date_lte: Optional[datetime] = None,
        start_date_gt: Optional[datetime] = None,
        start_date_gte: Optional[datetime] = None,
        is_green_or_brown_field: Optional[Union[list[str], Series[str], str]] = None,
        liquefaction_technology_type: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Allows the user to access list of LNG Liquefaction Trains

        Parameters
        ----------

         liquefaction_train_id: Optional[Union[list[str], Series[str], str]]
             Unique LNG Liquefaction Train Id, by default None
         liquefaction_train_name: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction Train Name, by default None
         liquefaction_project_id: Optional[Union[list[str], Series[str], str]]
             Unique LNG Liquefaction Project Id, by default None
         liquefaction_project_name: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction Project Name, by default None
         initial_capacity: Optional[str], optional
             LNG Liquefaction initial capacity of the train, by default None
         initial_capacity_gt: Optional[str], optional
             filter by `initial_capacity > x`, by default None
         initial_capacity_gte: Optional[str], optional
             filter by `initial_capacity >= x`, by default None
         initial_capacity_lt: Optional[str], optional
             filter by `initial_capacity < x`, by default None
         initial_capacity_lte: Optional[str], optional
             filter by `initial_capacity <= x`, by default None
         status: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction status of the train, by default None
         start_date: Optional[datetime], optional
             LNG Liquefaction start date, by default None
         start_date_gt: Optional[datetime], optional
             filter by `start_date > x`, by default None
         start_date_gte: Optional[datetime], optional
             filter by `start_date >= x`, by default None
         start_date_lt: Optional[datetime], optional
             filter by `start_date < x`, by default None
         start_date_lte: Optional[datetime], optional
             filter by `start_date <= x`, by default None
         is_green_or_brown_field: Optional[Union[list[str], Series[str], str]]
             Is green or brown Field, by default None
         liquefaction_technology_type: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction technology type, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(
            list_to_filter("liquefactionTrainId", liquefaction_train_id)
        )
        filter_params.append(
            list_to_filter("liquefactionTrainName", liquefaction_train_name)
        )
        filter_params.append(
            list_to_filter("liquefactionProjectId", liquefaction_project_id)
        )
        filter_params.append(
            list_to_filter("liquefactionProjectName", liquefaction_project_name)
        )
        filter_params.append(list_to_filter("initialCapacity", initial_capacity))
        if initial_capacity_gt is not None:
            filter_params.append(f'initialCapacity > "{initial_capacity_gt}"')
        if initial_capacity_gte is not None:
            filter_params.append(f'initialCapacity >= "{initial_capacity_gte}"')
        if initial_capacity_lt is not None:
            filter_params.append(f'initialCapacity < "{initial_capacity_lt}"')
        if initial_capacity_lte is not None:
            filter_params.append(f'initialCapacity <= "{initial_capacity_lte}"')
        filter_params.append(list_to_filter("status", status))
        filter_params.append(list_to_filter("startDate", start_date))
        if start_date_gt is not None:
            filter_params.append(f'startDate > "{start_date_gt}"')
        if start_date_gte is not None:
            filter_params.append(f'startDate >= "{start_date_gte}"')
        if start_date_lt is not None:
            filter_params.append(f'startDate < "{start_date_lt}"')
        if start_date_lte is not None:
            filter_params.append(f'startDate <= "{start_date_lte}"')
        filter_params.append(
            list_to_filter("isGreenOrBrownField", is_green_or_brown_field)
        )
        filter_params.append(
            list_to_filter("liquefactionTechnologyType", liquefaction_technology_type)
        )

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/outages/liquefaction-trains",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_gas_balances(
        self,
        *,
        source: Optional[Union[list[str], Series[str], str]] = None,
        market: Optional[Union[list[str], Series[str], str]] = None,
        period_type: Optional[Union[list[str], Series[str], str]] = None,
        period: Optional[date] = None,
        period_lt: Optional[date] = None,
        period_lte: Optional[date] = None,
        period_gt: Optional[date] = None,
        period_gte: Optional[date] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides natural gas supply-demand levels by market over time. If available, this dataset includes natural gas import and export data.

        Parameters
        ----------

         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., by default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., by default None
         period_type: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., by default None
         period: Optional[date], optional
             The date that the data refers to. The period’s date will be defined by the Period Type., by default None
         period_gt: Optional[date], optional
             filter by `period > x`, by default None
         period_gte: Optional[date], optional
             filter by `period >= x`, by default None
         period_lt: Optional[date], optional
             filter by `period < x`, by default None
         period_lte: Optional[date], optional
             filter by `period <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., by default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., by default None
         modified_date: Optional[datetime], optional
             Gas Balances record latest modified date., by default None
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
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", period_type))
        filter_params.append(list_to_filter("period", period))
        if period_gt is not None:
            filter_params.append(f'period > "{period_gt}"')
        if period_gte is not None:
            filter_params.append(f'period >= "{period_gte}"')
        if period_lt is not None:
            filter_params.append(f'period < "{period_lt}"')
        if period_lte is not None:
            filter_params.append(f'period <= "{period_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
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
            path=f"/lng/v1/market/gas-balances",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_gas_demand(
        self,
        *,
        source: Optional[Union[list[str], Series[str], str]] = None,
        market: Optional[Union[list[str], Series[str], str]] = None,
        period_type: Optional[Union[list[str], Series[str], str]] = None,
        period: Optional[date] = None,
        period_lt: Optional[date] = None,
        period_lte: Optional[date] = None,
        period_gt: Optional[date] = None,
        period_gte: Optional[date] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides natural gas demand levels by market over time. If available, the dataset includes demand by sector.

        Parameters
        ----------

         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., by default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., by default None
         period_type: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., by default None
         period: Optional[date], optional
             The date that the data refers to. The period’s date will be defined by the Period Type., by default None
         period_gt: Optional[date], optional
             filter by `period > x`, by default None
         period_gte: Optional[date], optional
             filter by `period >= x`, by default None
         period_lt: Optional[date], optional
             filter by `period < x`, by default None
         period_lte: Optional[date], optional
             filter by `period <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., by default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., by default None
         modified_date: Optional[datetime], optional
             Gas Demand record latest modified date., by default None
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
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", period_type))
        filter_params.append(list_to_filter("period", period))
        if period_gt is not None:
            filter_params.append(f'period > "{period_gt}"')
        if period_gte is not None:
            filter_params.append(f'period >= "{period_gte}"')
        if period_lt is not None:
            filter_params.append(f'period < "{period_lt}"')
        if period_lte is not None:
            filter_params.append(f'period <= "{period_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
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
            path=f"/lng/v1/market/gas-demand",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_gas_reserves(
        self,
        *,
        source: Optional[Union[list[str], Series[str], str]] = None,
        market: Optional[Union[list[str], Series[str], str]] = None,
        period_type: Optional[Union[list[str], Series[str], str]] = None,
        period: Optional[date] = None,
        period_lt: Optional[date] = None,
        period_lte: Optional[date] = None,
        period_gt: Optional[date] = None,
        period_gte: Optional[date] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides natural gas supply reserves levels.

        Parameters
        ----------

         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., by default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., by default None
         period_type: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., by default None
         period: Optional[date], optional
             The date that the data refers to. The period’s date will be defined by the Period Type., by default None
         period_gt: Optional[date], optional
             filter by `period > x`, by default None
         period_gte: Optional[date], optional
             filter by `period >= x`, by default None
         period_lt: Optional[date], optional
             filter by `period < x`, by default None
         period_lte: Optional[date], optional
             filter by `period <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., by default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., by default None
         modified_date: Optional[datetime], optional
             Gas Reserves record latest modified date., by default None
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
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", period_type))
        filter_params.append(list_to_filter("period", period))
        if period_gt is not None:
            filter_params.append(f'period > "{period_gt}"')
        if period_gte is not None:
            filter_params.append(f'period >= "{period_gte}"')
        if period_lt is not None:
            filter_params.append(f'period < "{period_lt}"')
        if period_lte is not None:
            filter_params.append(f'period <= "{period_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
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
            path=f"/lng/v1/market/gas-reserves",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_gas_sales(
        self,
        *,
        source: Optional[Union[list[str], Series[str], str]] = None,
        market: Optional[Union[list[str], Series[str], str]] = None,
        period_type: Optional[Union[list[str], Series[str], str]] = None,
        period: Optional[date] = None,
        period_lt: Optional[date] = None,
        period_lte: Optional[date] = None,
        period_gt: Optional[date] = None,
        period_gte: Optional[date] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides natural gas sales levels in markets over time. If available, the dataset includes sales by sector or by supplier type.

        Parameters
        ----------

         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., by default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., by default None
         period_type: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., by default None
         period: Optional[date], optional
             The date that the data refers to. The period’s date will be defined by the Period Type., by default None
         period_gt: Optional[date], optional
             filter by `period > x`, by default None
         period_gte: Optional[date], optional
             filter by `period >= x`, by default None
         period_lt: Optional[date], optional
             filter by `period < x`, by default None
         period_lte: Optional[date], optional
             filter by `period <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., by default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., by default None
         modified_date: Optional[datetime], optional
             Gas Sales record latest modified date., by default None
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
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", period_type))
        filter_params.append(list_to_filter("period", period))
        if period_gt is not None:
            filter_params.append(f'period > "{period_gt}"')
        if period_gte is not None:
            filter_params.append(f'period >= "{period_gte}"')
        if period_lt is not None:
            filter_params.append(f'period < "{period_lt}"')
        if period_lte is not None:
            filter_params.append(f'period <= "{period_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
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
            path=f"/lng/v1/market/gas-sales",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_power_capacity(
        self,
        *,
        source: Optional[Union[list[str], Series[str], str]] = None,
        market: Optional[Union[list[str], Series[str], str]] = None,
        period_type: Optional[Union[list[str], Series[str], str]] = None,
        period: Optional[date] = None,
        period_lt: Optional[date] = None,
        period_lte: Optional[date] = None,
        period_gt: Optional[date] = None,
        period_gte: Optional[date] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides power capacity levels in markets over time. If available, the dataset includes capacity by fuel type or by sector.

        Parameters
        ----------

         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., by default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., by default None
         period_type: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., by default None
         period: Optional[date], optional
             The date that the data refers to. The period’s date will be defined by the Period Type., by default None
         period_gt: Optional[date], optional
             filter by `period > x`, by default None
         period_gte: Optional[date], optional
             filter by `period >= x`, by default None
         period_lt: Optional[date], optional
             filter by `period < x`, by default None
         period_lte: Optional[date], optional
             filter by `period <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., by default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., by default None
         modified_date: Optional[datetime], optional
             Power Capacity record latest modified date., by default None
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
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", period_type))
        filter_params.append(list_to_filter("period", period))
        if period_gt is not None:
            filter_params.append(f'period > "{period_gt}"')
        if period_gte is not None:
            filter_params.append(f'period >= "{period_gte}"')
        if period_lt is not None:
            filter_params.append(f'period < "{period_lt}"')
        if period_lte is not None:
            filter_params.append(f'period <= "{period_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
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
            path=f"/lng/v1/market/power-capacity",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_power_generation(
        self,
        *,
        source: Optional[Union[list[str], Series[str], str]] = None,
        market: Optional[Union[list[str], Series[str], str]] = None,
        period_type: Optional[Union[list[str], Series[str], str]] = None,
        period: Optional[date] = None,
        period_lt: Optional[date] = None,
        period_lte: Optional[date] = None,
        period_gt: Optional[date] = None,
        period_gte: Optional[date] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides power generation levels in markets over time. If available, the dataset includes generation by fuel type or by sector.

        Parameters
        ----------

         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., by default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., by default None
         period_type: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., by default None
         period: Optional[date], optional
             The date that the data refers to. The period’s date will be defined by the Period Type., by default None
         period_gt: Optional[date], optional
             filter by `period > x`, by default None
         period_gte: Optional[date], optional
             filter by `period >= x`, by default None
         period_lt: Optional[date], optional
             filter by `period < x`, by default None
         period_lte: Optional[date], optional
             filter by `period <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., by default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., by default None
         modified_date: Optional[datetime], optional
             Power Generation record latest modified date., by default None
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
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", period_type))
        filter_params.append(list_to_filter("period", period))
        if period_gt is not None:
            filter_params.append(f'period > "{period_gt}"')
        if period_gte is not None:
            filter_params.append(f'period >= "{period_gte}"')
        if period_lt is not None:
            filter_params.append(f'period < "{period_lt}"')
        if period_lte is not None:
            filter_params.append(f'period <= "{period_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
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
            path=f"/lng/v1/market/power-generation",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_storage(
        self,
        *,
        source: Optional[Union[list[str], Series[str], str]] = None,
        market: Optional[Union[list[str], Series[str], str]] = None,
        period_type: Optional[Union[list[str], Series[str], str]] = None,
        period: Optional[date] = None,
        period_lt: Optional[date] = None,
        period_lte: Optional[date] = None,
        period_gt: Optional[date] = None,
        period_gte: Optional[date] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides natural gas storage levels in markets.

        Parameters
        ----------

         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., by default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., by default None
         period_type: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., by default None
         period: Optional[date], optional
             The date that the data refers to. The period’s date will be defined by the Period Type., by default None
         period_gt: Optional[date], optional
             filter by `period > x`, by default None
         period_gte: Optional[date], optional
             filter by `period >= x`, by default None
         period_lt: Optional[date], optional
             filter by `period < x`, by default None
         period_lte: Optional[date], optional
             filter by `period <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., by default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., by default None
         modified_date: Optional[datetime], optional
             Storage record latest modified date., by default None
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
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", period_type))
        filter_params.append(list_to_filter("period", period))
        if period_gt is not None:
            filter_params.append(f'period > "{period_gt}"')
        if period_gte is not None:
            filter_params.append(f'period >= "{period_gte}"')
        if period_lt is not None:
            filter_params.append(f'period < "{period_lt}"')
        if period_lte is not None:
            filter_params.append(f'period <= "{period_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
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
            path=f"/lng/v1/market/storage",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_prices(
        self,
        *,
        source: Optional[Union[list[str], Series[str], str]] = None,
        market: Optional[Union[list[str], Series[str], str]] = None,
        period_type: Optional[Union[list[str], Series[str], str]] = None,
        period: Optional[date] = None,
        period_lt: Optional[date] = None,
        period_lte: Optional[date] = None,
        period_gt: Optional[date] = None,
        period_gte: Optional[date] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides natural gas price levels in markets over time. If available, the dataset includes prices by sector or by import/export type.

        Parameters
        ----------

         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., by default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., by default None
         period_type: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., by default None
         period: Optional[date], optional
             The date that the data refers to. The period’s date will be defined by the Period Type., by default None
         period_gt: Optional[date], optional
             filter by `period > x`, by default None
         period_gte: Optional[date], optional
             filter by `period >= x`, by default None
         period_lt: Optional[date], optional
             filter by `period < x`, by default None
         period_lte: Optional[date], optional
             filter by `period <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., by default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., by default None
         modified_date: Optional[datetime], optional
             Prices record latest modified date., by default None
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
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", period_type))
        filter_params.append(list_to_filter("period", period))
        if period_gt is not None:
            filter_params.append(f'period > "{period_gt}"')
        if period_gte is not None:
            filter_params.append(f'period >= "{period_gte}"')
        if period_lt is not None:
            filter_params.append(f'period < "{period_lt}"')
        if period_lte is not None:
            filter_params.append(f'period <= "{period_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
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
            path=f"/lng/v1/market/prices",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_pipeline(
        self,
        *,
        source: Optional[Union[list[str], Series[str], str]] = None,
        market: Optional[Union[list[str], Series[str], str]] = None,
        period_type: Optional[Union[list[str], Series[str], str]] = None,
        period: Optional[date] = None,
        period_lt: Optional[date] = None,
        period_lte: Optional[date] = None,
        period_gt: Optional[date] = None,
        period_gte: Optional[date] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides natural gas pipeline information by market.

        Parameters
        ----------

         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., by default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., by default None
         period_type: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., by default None
         period: Optional[date], optional
             The date that the data refers to. The period’s date will be defined by the Period Type., by default None
         period_gt: Optional[date], optional
             filter by `period > x`, by default None
         period_gte: Optional[date], optional
             filter by `period >= x`, by default None
         period_lt: Optional[date], optional
             filter by `period < x`, by default None
         period_lte: Optional[date], optional
             filter by `period <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., by default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., by default None
         modified_date: Optional[datetime], optional
             Pipelines record latest modified date., by default None
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
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", period_type))
        filter_params.append(list_to_filter("period", period))
        if period_gt is not None:
            filter_params.append(f'period > "{period_gt}"')
        if period_gte is not None:
            filter_params.append(f'period >= "{period_gte}"')
        if period_lt is not None:
            filter_params.append(f'period < "{period_lt}"')
        if period_lte is not None:
            filter_params.append(f'period <= "{period_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
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
            path=f"/lng/v1/market/pipeline",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_fuel_use(
        self,
        *,
        source: Optional[Union[list[str], Series[str], str]] = None,
        market: Optional[Union[list[str], Series[str], str]] = None,
        period_type: Optional[Union[list[str], Series[str], str]] = None,
        period: Optional[date] = None,
        period_lt: Optional[date] = None,
        period_lte: Optional[date] = None,
        period_gt: Optional[date] = None,
        period_gte: Optional[date] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides energy balances by market and by fuel over time.

        Parameters
        ----------

         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., by default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., by default None
         period_type: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., by default None
         period: Optional[date], optional
             The date that the data refers to. The period’s date will be defined by the Period Type., by default None
         period_gt: Optional[date], optional
             filter by `period > x`, by default None
         period_gte: Optional[date], optional
             filter by `period >= x`, by default None
         period_lt: Optional[date], optional
             filter by `period < x`, by default None
         period_lte: Optional[date], optional
             filter by `period <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., by default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., by default None
         modified_date: Optional[datetime], optional
             Fuel Use record latest modified date., by default None
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
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", period_type))
        filter_params.append(list_to_filter("period", period))
        if period_gt is not None:
            filter_params.append(f'period > "{period_gt}"')
        if period_gte is not None:
            filter_params.append(f'period >= "{period_gte}"')
        if period_lt is not None:
            filter_params.append(f'period < "{period_lt}"')
        if period_lte is not None:
            filter_params.append(f'period <= "{period_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
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
            path=f"/lng/v1/market/fuel-use",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_price_charter_rate_forecast(
        self,
        *,
        month: Optional[date] = None,
        month_lt: Optional[date] = None,
        month_lte: Optional[date] = None,
        month_gt: Optional[date] = None,
        month_gte: Optional[date] = None,
        point_in_time_date: Optional[date] = None,
        point_in_time_date_lt: Optional[date] = None,
        point_in_time_date_lte: Optional[date] = None,
        point_in_time_date_gt: Optional[date] = None,
        point_in_time_date_gte: Optional[date] = None,
        charter_rate_type: Optional[Union[list[str], Series[str], str]] = None,
        charter_rate_currency: Optional[Union[list[str], Series[str], str]] = None,
        charter_rate_frequency: Optional[Union[list[str], Series[str], str]] = None,
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
        This dataset provides a forecast for LNG shipping charter rates for key charter markets

        Parameters
        ----------

         month: Optional[date], optional
             The month in which the forecast is applied to, by default None
         month_gt: Optional[date], optional
             filter by `month > x`, by default None
         month_gte: Optional[date], optional
             filter by `month >= x`, by default None
         month_lt: Optional[date], optional
             filter by `month < x`, by default None
         month_lte: Optional[date], optional
             filter by `month <= x`, by default None
         point_in_time_date: Optional[date], optional
             The date when the forecast was published. This can be used as a reference date to compare forecasts at different points in time., by default None
         point_in_time_date_gt: Optional[date], optional
             filter by `point_in_time_date > x`, by default None
         point_in_time_date_gte: Optional[date], optional
             filter by `point_in_time_date >= x`, by default None
         point_in_time_date_lt: Optional[date], optional
             filter by `point_in_time_date < x`, by default None
         point_in_time_date_lte: Optional[date], optional
             filter by `point_in_time_date <= x`, by default None
         charter_rate_type: Optional[Union[list[str], Series[str], str]]
             The specific charter rate type for each forecast, by default None
         charter_rate_currency: Optional[Union[list[str], Series[str], str]]
              The currency of the corresponding charter rate, by default None
         charter_rate_frequency: Optional[Union[list[str], Series[str], str]]
              The frequency of the value for the charter rate, by default None
         modified_date: Optional[datetime], optional
             Forecast annual prices record latest modified date, by default None
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
        filter_params.append(list_to_filter("month", month))
        if month_gt is not None:
            filter_params.append(f'month > "{month_gt}"')
        if month_gte is not None:
            filter_params.append(f'month >= "{month_gte}"')
        if month_lt is not None:
            filter_params.append(f'month < "{month_lt}"')
        if month_lte is not None:
            filter_params.append(f'month <= "{month_lte}"')
        filter_params.append(list_to_filter("pointInTimeDate", point_in_time_date))
        if point_in_time_date_gt is not None:
            filter_params.append(f'pointInTimeDate > "{point_in_time_date_gt}"')
        if point_in_time_date_gte is not None:
            filter_params.append(f'pointInTimeDate >= "{point_in_time_date_gte}"')
        if point_in_time_date_lt is not None:
            filter_params.append(f'pointInTimeDate < "{point_in_time_date_lt}"')
        if point_in_time_date_lte is not None:
            filter_params.append(f'pointInTimeDate <= "{point_in_time_date_lte}"')
        filter_params.append(list_to_filter("charterRateType", charter_rate_type))
        filter_params.append(
            list_to_filter("charterRateCurrency", charter_rate_currency)
        )
        filter_params.append(
            list_to_filter("charterRateFrequency", charter_rate_frequency)
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
            path=f"/lng/v1/analytics/price/charter-rate-forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_price_annual_forecast(
        self,
        *,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        price_marker_name: Optional[Union[list[str], Series[str], str]] = None,
        price_marker_uom: Optional[Union[list[str], Series[str], str]] = None,
        price_marker_currency: Optional[Union[list[str], Series[str], str]] = None,
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
        Price forecast for select gas and LNG price marker. Annual figures in US dollar per million British thermal units

        Parameters
        ----------

         year: Optional[int], optional
             The date for which the price forecast is provided, by default None
         year_gt: Optional[int], optional
             filter by `year > x`, by default None
         year_gte: Optional[int], optional
             filter by `year >= x`, by default None
         year_lt: Optional[int], optional
             filter by `year < x`, by default None
         year_lte: Optional[int], optional
             filter by `year <= x`, by default None
         price_marker_name: Optional[Union[list[str], Series[str], str]]
             The name of the price marker, by default None
         price_marker_uom: Optional[Union[list[str], Series[str], str]]
             The unit of measure for a given price for the indicated time period, by default None
         price_marker_currency: Optional[Union[list[str], Series[str], str]]
             The currency for a given price for the indicated time period, by default None
         modified_date: Optional[datetime], optional
             Forecast annual prices record latest modified date, by default None
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
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("priceMarkerName", price_marker_name))
        filter_params.append(list_to_filter("priceMarkerUom", price_marker_uom))
        filter_params.append(
            list_to_filter("priceMarkerCurrency", price_marker_currency)
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
            path=f"/lng/v1/analytics/price/annual-forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_price_monthly_forecast(
        self,
        *,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        price_marker_name: Optional[Union[list[str], Series[str], str]] = None,
        price_marker_uom: Optional[Union[list[str], Series[str], str]] = None,
        price_marker_currency: Optional[Union[list[str], Series[str], str]] = None,
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
        Price forecast for select gas and LNG price markers for next few years. Monthly figures in US dollar per million British thermal units

        Parameters
        ----------

         date: Optional[date], optional
             The date for which the price forecast is provided, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         price_marker_name: Optional[Union[list[str], Series[str], str]]
             The name of the price marker, by default None
         price_marker_uom: Optional[Union[list[str], Series[str], str]]
             The unit of measure for a given price for the indicated time period, by default None
         price_marker_currency: Optional[Union[list[str], Series[str], str]]
             The currency for a given price for the indicated time period, by default None
         modified_date: Optional[datetime], optional
             Forecast monthly prices record latest modified date, by default None
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
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("priceMarkerName", price_marker_name))
        filter_params.append(list_to_filter("priceMarkerUom", price_marker_uom))
        filter_params.append(
            list_to_filter("priceMarkerCurrency", price_marker_currency)
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
            path=f"/lng/v1/analytics/price/monthly-forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_price_historical_bilateral_custom(
        self,
        *,
        month: Optional[date] = None,
        month_lt: Optional[date] = None,
        month_lte: Optional[date] = None,
        month_gt: Optional[date] = None,
        month_gte: Optional[date] = None,
        import_market: Optional[Union[list[str], Series[str], str]] = None,
        supply_source: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
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
        Historical bilateral prices of select markets and their LNG supply sources. The data is primarily sourced from customs reporting agencies

        Parameters
        ----------

         month: Optional[date], optional
             The month for which the price data is recorded, by default None
         month_gt: Optional[date], optional
             filter by `month > x`, by default None
         month_gte: Optional[date], optional
             filter by `month >= x`, by default None
         month_lt: Optional[date], optional
             filter by `month < x`, by default None
         month_lte: Optional[date], optional
             filter by `month <= x`, by default None
         import_market: Optional[Union[list[str], Series[str], str]]
             The market or country where the LNG is being imported, by default None
         supply_source: Optional[Union[list[str], Series[str], str]]
             The source or country from which the LNG is being supplied, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measure for a given price for the indicated time period, by default None
         currency: Optional[Union[list[str], Series[str], str]]
             The currency for a given price for the indicated time period, by default None
         modified_date: Optional[datetime], optional
             Historical bilateral customs prices record latest modified date, by default None
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
        filter_params.append(list_to_filter("month", month))
        if month_gt is not None:
            filter_params.append(f'month > "{month_gt}"')
        if month_gte is not None:
            filter_params.append(f'month >= "{month_gte}"')
        if month_lt is not None:
            filter_params.append(f'month < "{month_lt}"')
        if month_lte is not None:
            filter_params.append(f'month <= "{month_lte}"')
        filter_params.append(list_to_filter("importMarket", import_market))
        filter_params.append(list_to_filter("supplySource", supply_source))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("currency", currency))
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
            path=f"/lng/v1/analytics/price/historical/bilateral-custom",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_price_historical_monthly(
        self,
        *,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        price_marker_name: Optional[Union[list[str], Series[str], str]] = None,
        price_marker_uom: Optional[Union[list[str], Series[str], str]] = None,
        price_marker_currency: Optional[Union[list[str], Series[str], str]] = None,
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
        Historical prices for several gas and LNG price markers. Monthly figures in US dollar per million British thermal units

        Parameters
        ----------

         date: Optional[date], optional
             The date for which the price data is recorded, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         price_marker_name: Optional[Union[list[str], Series[str], str]]
             The name of the price marker, by default None
         price_marker_uom: Optional[Union[list[str], Series[str], str]]
             The unit of measure for a given price for the indicated time period, by default None
         price_marker_currency: Optional[Union[list[str], Series[str], str]]
             The currency for a given price for the indicated time period, by default None
         modified_date: Optional[datetime], optional
             Historical monthly prices record latest modified date, by default None
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
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("priceMarkerName", price_marker_name))
        filter_params.append(list_to_filter("priceMarkerUom", price_marker_uom))
        filter_params.append(
            list_to_filter("priceMarkerCurrency", price_marker_currency)
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
            path=f"/lng/v1/analytics/price/historical/monthly",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    

    def get_demand_forecast_short_term_current(
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
        Provides the latest short-term demand forecast data

        Parameters
        ----------

         import_market: Optional[Union[list[str], Series[str], str]]
             Market where the LNG is delivered to, by default None
         month: Optional[date], optional
             Calendar month of the forecasted volume, by default None
         month_gt: Optional[date], optional
             filter by `month > x`, by default None
         month_gte: Optional[date], optional
             filter by `month >= x`, by default None
         month_lt: Optional[date], optional
             filter by `month < x`, by default None
         month_lte: Optional[date], optional
             filter by `month <= x`, by default None
         point_in_time_month: Optional[date], optional
             The month in which the forecast was originally published, by default None
         point_in_time_month_gt: Optional[date], optional
             filter by `point_in_time_month > x`, by default None
         point_in_time_month_gte: Optional[date], optional
             filter by `point_in_time_month >= x`, by default None
         point_in_time_month_lt: Optional[date], optional
             filter by `point_in_time_month < x`, by default None
         point_in_time_month_lte: Optional[date], optional
             filter by `point_in_time_month <= x`, by default None
         modified_date: Optional[datetime], optional
             The latest modified date of the forecasted value, by default None
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
         page_size: int = 5000,
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
            path=f"/lng/v1/demand-forecast/short-term/current",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_supply_forecast_short_term_current(
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
        Provides the latest short-term supply forecast data

        Parameters
        ----------

         export_project: Optional[Union[list[str], Series[str], str]]
             Associated liquefaction project, by default None
         export_market: Optional[Union[list[str], Series[str], str]]
             Market where the liquefaction project is located, by default None
         month: Optional[date], optional
             Calendar month of the forecasted volume, by default None
         month_gt: Optional[date], optional
             filter by `month > x`, by default None
         month_gte: Optional[date], optional
             filter by `month >= x`, by default None
         month_lt: Optional[date], optional
             filter by `month < x`, by default None
         month_lte: Optional[date], optional
             filter by `month <= x`, by default None
         point_in_time_month: Optional[date], optional
             The month in which the forecast was originally published, by default None
         point_in_time_month_gt: Optional[date], optional
             filter by `point_in_time_month > x`, by default None
         point_in_time_month_gte: Optional[date], optional
             filter by `point_in_time_month >= x`, by default None
         point_in_time_month_lt: Optional[date], optional
             filter by `point_in_time_month < x`, by default None
         point_in_time_month_lte: Optional[date], optional
             filter by `point_in_time_month <= x`, by default None
         modified_date: Optional[datetime], optional
             The latest modified date of the forecasted value, by default None
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
         page_size: int = 5000,
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
            path=f"/lng/v1/supply-forecast/short-term/current",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_demand_forecast_short_term_history(
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
        Provides the historical points in time of the short-term demand forecast from January 2021 onwards

        Parameters
        ----------

         import_market: Optional[Union[list[str], Series[str], str]]
             Market where the LNG is delivered to, by default None
         month: Optional[date], optional
             Calendar month of the forecasted volume, by default None
         month_gt: Optional[date], optional
             filter by `month > x`, by default None
         month_gte: Optional[date], optional
             filter by `month >= x`, by default None
         month_lt: Optional[date], optional
             filter by `month < x`, by default None
         month_lte: Optional[date], optional
             filter by `month <= x`, by default None
         point_in_time_month: Optional[date], optional
             The month in which the forecast was originally published, by default None
         point_in_time_month_gt: Optional[date], optional
             filter by `point_in_time_month > x`, by default None
         point_in_time_month_gte: Optional[date], optional
             filter by `point_in_time_month >= x`, by default None
         point_in_time_month_lt: Optional[date], optional
             filter by `point_in_time_month < x`, by default None
         point_in_time_month_lte: Optional[date], optional
             filter by `point_in_time_month <= x`, by default None
         modified_date: Optional[datetime], optional
             The latest modified date of the forecasted value, by default None
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
         page_size: int = 5000,
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
            path=f"/lng/v1/demand-forecast/short-term/history",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_supply_forecast_short_term_history(
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
        Provides the historical points in time of the short-term supply forecast from January 2021 onwards

        Parameters
        ----------

         export_project: Optional[Union[list[str], Series[str], str]]
             Associated liquefaction project, by default None
         export_market: Optional[Union[list[str], Series[str], str]]
             Market where the liquefaction project is located, by default None
         month: Optional[date], optional
             Calendar month of the forecasted volume, by default None
         month_gt: Optional[date], optional
             filter by `month > x`, by default None
         month_gte: Optional[date], optional
             filter by `month >= x`, by default None
         month_lt: Optional[date], optional
             filter by `month < x`, by default None
         month_lte: Optional[date], optional
             filter by `month <= x`, by default None
         point_in_time_month: Optional[date], optional
             The month in which the forecast was originally published, by default None
         point_in_time_month_gt: Optional[date], optional
             filter by `point_in_time_month > x`, by default None
         point_in_time_month_gte: Optional[date], optional
             filter by `point_in_time_month >= x`, by default None
         point_in_time_month_lt: Optional[date], optional
             filter by `point_in_time_month < x`, by default None
         point_in_time_month_lte: Optional[date], optional
             filter by `point_in_time_month <= x`, by default None
         modified_date: Optional[datetime], optional
             The latest modified date of the forecasted value, by default None
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
         page_size: int = 5000,
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
            path=f"/lng/v1/supply-forecast/short-term/history",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    
    def get_events_bunkering(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        bunkering_start_date: Optional[datetime] = None,
        bunkering_start_date_lt: Optional[datetime] = None,
        bunkering_start_date_lte: Optional[datetime] = None,
        bunkering_start_date_gt: Optional[datetime] = None,
        bunkering_start_date_gte: Optional[datetime] = None,
        bunkering_location: Optional[Union[list[str], Series[str], str]] = None,
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
        The events record the date and location a vessel has engaged in bunkering (or re-fueling activity).

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
         bunkering_start_date: Optional[datetime], optional
             The start date of bunkering operations, by default None
         bunkering_start_date_gt: Optional[datetime], optional
             filter by '' bunkering_start_date > x '', by default None
         bunkering_start_date_gte: Optional[datetime], optional
             filter by bunkering_start_date, by default None
         bunkering_start_date_lt: Optional[datetime], optional
             filter by bunkering_start_date, by default None
         bunkering_start_date_lte: Optional[datetime], optional
             filter by bunkering_start_date, by default None
         bunkering_location: Optional[Union[list[str], Series[str], str]]
             The specific name of the location, by default None
         modified_date: Optional[datetime], optional
             Event record latest modified date, by default None
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
         page_size: int = 5000,
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
        filter_params.append(list_to_filter("bunkeringStartDate", bunkering_start_date))
        if bunkering_start_date_gt is not None:
            filter_params.append(f'bunkeringStartDate > "{bunkering_start_date_gt}"')
        if bunkering_start_date_gte is not None:
            filter_params.append(f'bunkeringStartDate >= "{bunkering_start_date_gte}"')
        if bunkering_start_date_lt is not None:
            filter_params.append(f'bunkeringStartDate < "{bunkering_start_date_lt}"')
        if bunkering_start_date_lte is not None:
            filter_params.append(f'bunkeringStartDate <= "{bunkering_start_date_lte}"')
        filter_params.append(list_to_filter("bunkeringLocation", bunkering_location))
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
            path=f"/lng/v1/cargo-premium/events/bunkering",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_events_trade_route(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        route_transit_date: Optional[datetime] = None,
        route_transit_date_lt: Optional[datetime] = None,
        route_transit_date_lte: Optional[datetime] = None,
        route_transit_date_gt: Optional[datetime] = None,
        route_transit_date_gte: Optional[datetime] = None,
        trade_route: Optional[Union[list[str], Series[str], str]] = None,
        vessel_direction: Optional[Union[list[str], Series[str], str]] = None,
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
        The events record the date, location, and duration a vessel has crossed through a major trade route.

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
         route_transit_date: Optional[datetime], optional
             The date the vessel transited the trade route, by default None
         route_transit_date_gt: Optional[datetime], optional
             filter by '' route_transit_date > x '', by default None
         route_transit_date_gte: Optional[datetime], optional
             filter by route_transit_date, by default None
         route_transit_date_lt: Optional[datetime], optional
             filter by route_transit_date, by default None
         route_transit_date_lte: Optional[datetime], optional
             filter by route_transit_date, by default None
         trade_route: Optional[Union[list[str], Series[str], str]]
             The trade route which the vessel passed through, by default None
         vessel_direction: Optional[Union[list[str], Series[str], str]]
             The general direction the vessel passed through the trade (specific to some routes), by default None
         modified_date: Optional[datetime], optional
             Event record latest modified date, by default None
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
         page_size: int = 5000,
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
        filter_params.append(list_to_filter("routeTransitDate", route_transit_date))
        if route_transit_date_gt is not None:
            filter_params.append(f'routeTransitDate > "{route_transit_date_gt}"')
        if route_transit_date_gte is not None:
            filter_params.append(f'routeTransitDate >= "{route_transit_date_gte}"')
        if route_transit_date_lt is not None:
            filter_params.append(f'routeTransitDate < "{route_transit_date_lt}"')
        if route_transit_date_lte is not None:
            filter_params.append(f'routeTransitDate <= "{route_transit_date_lte}"')
        filter_params.append(list_to_filter("tradeRoute", trade_route))
        filter_params.append(list_to_filter("vesselDirection", vessel_direction))
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
            path=f"/lng/v1/cargo-premium/events/trade-route",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_events_journey_point(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        arrival_date: Optional[datetime] = None,
        arrival_date_lt: Optional[datetime] = None,
        arrival_date_lte: Optional[datetime] = None,
        arrival_date_gt: Optional[datetime] = None,
        arrival_date_gte: Optional[datetime] = None,
        location: Optional[Union[list[str], Series[str], str]] = None,
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
        The events record the date and location a vessel has crossed through an identified point along its journey.

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
         arrival_date: Optional[datetime], optional
             The date the vessel passed through journey point, by default None
         arrival_date_gt: Optional[datetime], optional
             filter by '' arrival_date > x '', by default None
         arrival_date_gte: Optional[datetime], optional
             filter by arrival_date, by default None
         arrival_date_lt: Optional[datetime], optional
             filter by arrival_date, by default None
         arrival_date_lte: Optional[datetime], optional
             filter by arrival_date, by default None
         location: Optional[Union[list[str], Series[str], str]]
             The specific name of the location, by default None
         modified_date: Optional[datetime], optional
             Event record latest modified date, by default None
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
         page_size: int = 5000,
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
        filter_params.append(list_to_filter("arrivalDate", arrival_date))
        if arrival_date_gt is not None:
            filter_params.append(f'arrivalDate > "{arrival_date_gt}"')
        if arrival_date_gte is not None:
            filter_params.append(f'arrivalDate >= "{arrival_date_gte}"')
        if arrival_date_lt is not None:
            filter_params.append(f'arrivalDate < "{arrival_date_lt}"')
        if arrival_date_lte is not None:
            filter_params.append(f'arrivalDate <= "{arrival_date_lte}"')
        filter_params.append(list_to_filter("location", location))
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
            path=f"/lng/v1/cargo-premium/events/journey-point",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_events_idling(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        idling_start_date: Optional[datetime] = None,
        idling_start_date_lt: Optional[datetime] = None,
        idling_start_date_lte: Optional[datetime] = None,
        idling_start_date_gt: Optional[datetime] = None,
        idling_start_date_gte: Optional[datetime] = None,
        idling_location: Optional[Union[list[str], Series[str], str]] = None,
        idling_finish_date: Optional[datetime] = None,
        idling_finish_date_lt: Optional[datetime] = None,
        idling_finish_date_lte: Optional[datetime] = None,
        idling_finish_date_gt: Optional[datetime] = None,
        idling_finish_date_gte: Optional[datetime] = None,
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
        The events record the date and location a vessel has been idled or in a relatively localized area for an extended period of time.

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
         idling_start_date: Optional[datetime], optional
             The start date of the idling activity, by default None
         idling_start_date_gt: Optional[datetime], optional
             filter by '' idling_start_date > x '', by default None
         idling_start_date_gte: Optional[datetime], optional
             filter by idling_start_date, by default None
         idling_start_date_lt: Optional[datetime], optional
             filter by idling_start_date, by default None
         idling_start_date_lte: Optional[datetime], optional
             filter by idling_start_date, by default None
         idling_location: Optional[Union[list[str], Series[str], str]]
             The specific name of the location, by default None
         idling_finish_date: Optional[datetime], optional
             The event date of the idling activity, by default None
         idling_finish_date_gt: Optional[datetime], optional
             filter by '' idling_finish_date > x '', by default None
         idling_finish_date_gte: Optional[datetime], optional
             filter by idling_finish_date, by default None
         idling_finish_date_lt: Optional[datetime], optional
             filter by idling_finish_date, by default None
         idling_finish_date_lte: Optional[datetime], optional
             filter by idling_finish_date, by default None
         modified_date: Optional[datetime], optional
             Event record latest modified date, by default None
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
         page_size: int = 5000,
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
        filter_params.append(list_to_filter("idlingStartDate", idling_start_date))
        if idling_start_date_gt is not None:
            filter_params.append(f'idlingStartDate > "{idling_start_date_gt}"')
        if idling_start_date_gte is not None:
            filter_params.append(f'idlingStartDate >= "{idling_start_date_gte}"')
        if idling_start_date_lt is not None:
            filter_params.append(f'idlingStartDate < "{idling_start_date_lt}"')
        if idling_start_date_lte is not None:
            filter_params.append(f'idlingStartDate <= "{idling_start_date_lte}"')
        filter_params.append(list_to_filter("idlingLocation", idling_location))
        filter_params.append(list_to_filter("idlingFinishDate", idling_finish_date))
        if idling_finish_date_gt is not None:
            filter_params.append(f'idlingFinishDate > "{idling_finish_date_gt}"')
        if idling_finish_date_gte is not None:
            filter_params.append(f'idlingFinishDate >= "{idling_finish_date_gte}"')
        if idling_finish_date_lt is not None:
            filter_params.append(f'idlingFinishDate < "{idling_finish_date_lt}"')
        if idling_finish_date_lte is not None:
            filter_params.append(f'idlingFinishDate <= "{idling_finish_date_lte}"')
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
            path=f"/lng/v1/cargo-premium/events/idling",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_events_diversion(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        diversion_date: Optional[datetime] = None,
        diversion_date_lt: Optional[datetime] = None,
        diversion_date_lte: Optional[datetime] = None,
        diversion_date_gt: Optional[datetime] = None,
        diversion_date_gte: Optional[datetime] = None,
        diversion_location: Optional[Union[list[str], Series[str], str]] = None,
        destination_before: Optional[Union[list[str], Series[str], str]] = None,
        destination_after: Optional[Union[list[str], Series[str], str]] = None,
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
        The events record the date and location a vessel has changed course during its journey. It also provides important information about the initial destination and the new destination.

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
         diversion_date: Optional[datetime], optional
             The date a diversion occurred, by default None
         diversion_date_gt: Optional[datetime], optional
             filter by '' diversion_date > x '', by default None
         diversion_date_gte: Optional[datetime], optional
             filter by diversion_date, by default None
         diversion_date_lt: Optional[datetime], optional
             filter by diversion_date, by default None
         diversion_date_lte: Optional[datetime], optional
             filter by diversion_date, by default None
         diversion_location: Optional[Union[list[str], Series[str], str]]
             The specific name of the location, by default None
         destination_before: Optional[Union[list[str], Series[str], str]]
             The specific name of the initial destination, by default None
         destination_after: Optional[Union[list[str], Series[str], str]]
             The specific name of the new destination, by default None
         modified_date: Optional[datetime], optional
             Event record latest modified date, by default None
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
         page_size: int = 5000,
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
        filter_params.append(list_to_filter("diversionDate", diversion_date))
        if diversion_date_gt is not None:
            filter_params.append(f'diversionDate > "{diversion_date_gt}"')
        if diversion_date_gte is not None:
            filter_params.append(f'diversionDate >= "{diversion_date_gte}"')
        if diversion_date_lt is not None:
            filter_params.append(f'diversionDate < "{diversion_date_lt}"')
        if diversion_date_lte is not None:
            filter_params.append(f'diversionDate <= "{diversion_date_lte}"')
        filter_params.append(list_to_filter("diversionLocation", diversion_location))
        filter_params.append(list_to_filter("destinationBefore", destination_before))
        filter_params.append(list_to_filter("destinationAfter", destination_after))
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
            path=f"/lng/v1/cargo-premium/events/diversion",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
