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
from spgci.api_client import get_data, Paginator
from typing import List, Union, Optional
from requests import Response
from spgci.utilities import list_to_filter
from pandas import Series, DataFrame, to_datetime  # type: ignore
from datetime import date, datetime
from enum import Enum


class EWindowMarketData:
    """
    EWindow Market Data - Bids, Offers, Trades

    Includes
    --------
    ``OrderState`` enum for order states.\n
    ``OrderType`` enum for order types.\n
    ``get_products()`` to see the list of Products and their latest order date.\n
    ``get_markets()`` to see the list of Markets and their latest order date.\n
    ``get_botes()`` to get Bids, Offers, Trades.\n

    """

    _path = "tradedata/v3/"

    class OrderState(Enum):
        """Order State"""

        Active = "active"
        Consummated = "consummated"
        Inactive = "inactive"
        Withdrawn = "withdrawn"

    class OrderType(Enum):
        """Order Type"""

        Bid = "bid"
        Offer = "offer"
        RFBid = "rf bid"
        RFOffer = "rf offer"

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
            df["order_begin"] = to_datetime(df["order_begin"])
            df["order_end"] = to_datetime(df["order_end"])
            df["order_date"] = to_datetime(df["order_date"])
            df["order_time"] = to_datetime(df["order_time"])
            df["deal_begin"] = to_datetime(df["deal_begin"])
            df["deal_end"] = to_datetime(df["deal_end"])

        return df

    @staticmethod
    def _convert_agg_to_df(resp: Response) -> DataFrame:
        j = resp.json()
        df = DataFrame(j["aggResultValue"])

        if len(df) > 0:
            df["max(order_date)"] = to_datetime(df["max(order_date)"])

        return df

    def get_markets(self, raw: bool = False) -> Union[DataFrame, Response]:
        """
        Fetch the list of Markets.

        Parameters
        ----------
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
        >>> ci.EWindowMarketData().get_markets()
        """
        params = {
            "groupBy": "market",
            "field": "max(order_date)",
            "pageSize": 1000,
            "sort": "order_time:desc",
        }

        return get_data(
            path=f"{self._path}ewindowdata",
            raw=raw,
            params=params,
            paginate=True,
            paginate_fn=self._paginate,
            df_fn=self._convert_agg_to_df,
        )

    def get_products(self, raw: bool = False) -> Union[DataFrame, Response]:
        """
        Fetch the list of Products.

        Parameters
        ----------
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
        >>> ci.EWindowMarketData().get_products()
        """
        params = {
            "groupBy": "product, market",
            "field": "max(order_date)",
            "pageSize": 1000,
            "sort": "order_time:desc",
        }

        return get_data(
            path=f"{self._path}ewindowdata",
            raw=raw,
            params=params,
            paginate=True,
            paginate_fn=self._paginate,
            df_fn=self._convert_agg_to_df,
        )

    def get_botes(
        self,
        *,
        market: Optional[Union[list[str], "Series[str]", str]] = None,
        product: Optional[Union[list[str], "Series[str]", str]] = None,
        hub: Optional[Union[list[str], "Series[str]", str]] = None,
        strip: Optional[Union[list[str], "Series[str]", str]] = None,
        buyer: Optional[Union[list[str], "Series[str]", str]] = None,
        seller: Optional[Union[list[str], "Series[str]", str]] = None,
        order_type: Optional[
            Union[list[str], list[OrderType], "Series[str]", str, OrderType]
        ] = None,
        order_state: Optional[
            Union[list[str], list[OrderState], "Series[str]", str, OrderState]
        ] = None,
        order_id: Optional[Union[list[int], "Series[int]", int]] = None,
        order_time: Optional[Union[datetime, date]] = None,
        order_time_lt: Optional[Union[datetime, date]] = None,
        order_time_lte: Optional[Union[datetime, date]] = None,
        order_time_gt: Optional[Union[datetime, date]] = None,
        order_time_gte: Optional[Union[datetime, date]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Fetch BOTes (Bids, Offers, Trades) from the EWindow MarketData API.

        See ``get_products()`` to search for products.\n
        See ``get_markets()`` to search for markets.\n

        Parameters
        ----------
        market : Optional[Union[list[str], Series[str], str]], optional
            filter by market, by default None
        product : Optional[Union[list[str], Series[str], str]], optional
            filter by product, by default None
        hub : Optional[Union[list[str], Series[str], str]], optional
            filter by hub, by default None
        strip : Optional[Union[list[str], Series[str], str]], optional
            filter by strip, by default None
        buyer : Optional[Union[list[str], Series[str], str]], optional
            filter by buyer, by default None
        seller : Optional[Union[list[str], Series[str], str]], optional
            filter by seller, by default None
        order_type : Optional[Union[list[str], list[OrderType], Series[str], str, OrderType]], optional
            filter by order type, by default None
        order_state : Optional[ Union[list[str], list[OrderState], Series[str], str, OrderState] ], optional
            filter by order state, by default None
        order_id : Optional[Union[list[int], Series[int], int]], optional
            filter by order id, by default None
        order_time : Optional[Union[datetime, date]], optional
            filter by ``order_time = x``, by default None
        order_time_lt : Optional[Union[datetime, date]], optional
            filter by ``order_time < x``, by default None
        order_time_lte : Optional[Union[datetime, date]], optional
            filter by ``order_time <= x``, by default None
        order_time_gt : Optional[Union[datetime, date]], optional
            filter by ``order_time > x``, by default None
        order_time_gte : Optional[Union[datetime, date]], optional
            filter by ``order_time >= x``, by default None
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
        >>> today = date.today()
        >>> ci.EWindowMarketData().get_botes(market=["EU BFOE", "US MidWest"], order_time_gte=today)

        **Date Range**
        >>> d1 = date(2023, 2, 1)
        >>> d2 = date(2023, 2, 3)
        >>> ci.EWindowMarketData().get_botes(market=["EU BFOE", "US MidWest"], order_time_gt=d1, order_time_lt=d2)
        """
        endpoint_path = "ewindowdata"

        filter_params: List[str] = []

        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("product", product))
        filter_params.append(list_to_filter("hub", hub))
        filter_params.append(list_to_filter("strip", strip))
        filter_params.append(list_to_filter("buyer", buyer))
        filter_params.append(list_to_filter("seller", seller))
        filter_params.append(list_to_filter("order_type", order_type))
        filter_params.append(list_to_filter("order_id", order_id))
        filter_params.append(list_to_filter("order_state", order_state))

        if order_time:
            filter_params.append(f'order_time: "{order_time}"')
        if order_time_gt:
            filter_params.append(f'order_time > "{order_time_gt}"')
        if order_time_gte:
            filter_params.append(f'order_time >= "{order_time_gte}"')
        if order_time_lt:
            filter_params.append(f'order_time < "{order_time_lt}"')
        if order_time_lte:
            filter_params.append(f'order_time <= "{order_time_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"{self._path}{endpoint_path}",
            params=params,
            raw=raw,
            df_fn=self._convert_to_df,
            paginate_fn=self._paginate,
            paginate=paginate,
        )

        return response
