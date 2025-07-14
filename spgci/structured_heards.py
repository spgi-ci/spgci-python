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
from datetime import date


class StructuredHeards:
    """
    StructuredHeards Data

    Includes
    --------

    ``get_heards()`` to fetch structured heards.\n
    ``get_markets()`` to fetch market and attributes associated with it.\n

    """

    _endpoint = "structured-heards/v1/"

    @staticmethod
    def _paginate(resp: Response) -> Paginator:
        j = resp.json()
        total_pages = j["metadata"]["total_pages"]

        if total_pages <= 1:
            return Paginator(False, "page", 1)

        return Paginator(True, "page", total_pages)

    @staticmethod
    def _convert_to_df(resp: Response) -> DataFrame:
        j = resp.json()
        df = DataFrame(j["results"])

        if len(df) > 0:
            if "updatedDate" in df.columns:
                df["updatedDate"] = to_datetime(df["updatedDate"])
            if "rtpTimestamp" in df.columns:
                df["rtpTimestamp"] = to_datetime(df["rtpTimestamp"])
        return df

    @staticmethod
    def _paginate_outages(resp: Response) -> Paginator:
        j = resp.json()
        count = j["metadata"]["count"]
        size = j["metadata"]["pagesize"]

        remainder = count % size
        quotient = count // size
        total_pages = quotient + (1 if remainder > 0 else 0)

        if total_pages <= 1:
            return Paginator(False, "page", 1)

        return Paginator(True, "page", total_pages=total_pages)

    def get_heards(
        self,
        market: Union[list[str], "Series[str]", str],
        *,
        geography: Optional[Union[list[str], "Series[str]", str]] = None,
        commodity: Optional[Union[list[str], "Series[str]", str]] = None,
        location: Optional[Union[list[str], "Series[str]", str]] = None,
        heard_type: Optional[Union[list[str], "Series[str]", str]] = None,
        updated_date_lt: Optional[date] = None,
        updated_date_lte: Optional[date] = None,
        updated_date_gt: Optional[date] = None,
        updated_date_gte: Optional[date] = None,
        rtp_timestamp_lt: Optional[date] = None,
        rtp_timestamp_lte: Optional[date] = None,
        rtp_timestamp_gt: Optional[date] = None,
        rtp_timestamp_gte: Optional[date] = None,
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
        market : Optional[Union[list[str], Series[str], str]]
            filter by market
        geography : Optional[Union[list[str], Series[str], str]], optional
            filter by geography, by default None
        commodity : Optional[Union[list[str], Series[str], str]], optional
            filter by commodity, by default None
        location : Optional[Union[list[str], Series[str], str]], optional
            filter by location, by default None
        heard_type : Optional[Union[list[str], Series[str], str]], optional
            filter by heard_type, by default None
        updated_date_lt: Optional[date], optional
            filter by ``updatedDate < x``, by default None
        updated_date_lte : Optional[date], optional
            filter by ``updatedDate <= x``, by default None
        updated_date_gt : Optional[date], optional
            filter by ``updatedDate > x``, by default None
        updated_date_gte : Optional[date], optional
            filter by ``updatedDate >= x``, by default None
        rtp_timestamp_lt : Optional[date], optional
            filter by ``rtpTimestamp < x``, by default None
        rtp_timestamp_lte : Optional[date], optional
            filter by ``rtpTimestamp <= x``, by default None
        rtp_timestamp_gt : Optional[date], optional
            filter by ``rtpTimestamp > x``, by default None
        rtp_timestamp_gte : Optional[date], optional
            filter by ``rtpTimestamp >= x``, by default None
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
        >>> ci.StructuredHeards().get_heards(market="Americas crude oil")
        """
        endpoint_path = "data"
        filter_params: List[str] = []
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("location", location))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("heard_type", heard_type))

        filter_params = convert_date_to_filter_exp(
            "updatedDate",
            updated_date_gt,
            updated_date_gte,
            updated_date_lt,
            updated_date_lte,
            filter_params,
        )
        filter_params = convert_date_to_filter_exp(
            "rtpTimeStamp",
            rtp_timestamp_gt,
            rtp_timestamp_gte,
            rtp_timestamp_lt,
            rtp_timestamp_lte,
            filter_params,
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

    def get_markets(
        self,
        *,
        market: Optional[Union[list[str], "Series[str]", str]] = None,
        attributes: Optional[Union[list[str], "Series[str]", str]] = None,
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
        market : Optional[Union[list[str], Series[str], str]], optional
            filter by commodity_name, by default None
        attributes : Optional[Union[list[str], Series[str], str]], optional
            filter by train_name, by default None
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
        >>> ci.StructuredHeards().get_markets()
        """
        endpoint_path = "markets"
        filter_params: List[str] = []
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("attributes", attributes))

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
            paginate_fn=self._paginate_outages,
            raw=raw,
            paginate=paginate,
        )
        return response
