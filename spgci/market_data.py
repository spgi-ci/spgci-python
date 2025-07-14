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
from spgci.utilities import list_to_filter
from spgci.api_client import get_data, Paginator, _nop_paginate
from typing import List, Optional, Union
import pandas as pd
from pandas import Series
from requests import Response
from datetime import date
from enum import Enum


class MarketData:
    """
    Platts Symbols and Assessments.

    Includes
    --------
    ``ContractType`` enum for contract types.\n
    ``AssessmentFrequency`` enum for assessment frequencies.\n
    ``get_assessments_by_symbol_current`` get current assessments for a list of symbols.
    ``get_assessments_by_symbol_current`` get current assessments for a list of symbols.
    ``get_assessments_by_mdc_current`` get current assessments for all symbols in an MDC (Market Data Category).
    ``get_assessments_by_mdc_historical`` get historical assessments for all symbols in an MDC (Market Data Category).

    """

    _path = "market-data/v3/value/"
    _ref_path = "market-data/reference-data/v3/search"
    _mdd_fields = "deltaPrice, deltaPercent, pValue, pDate"

    class ContractType(Enum):
        """Contract Type"""

        Spot = "spot"
        Forward = "forward"
        Future = "future"
        Swap = "swap"
        Strip = "strip"
        CFD = "cfd"
        Index = "index"
        OfficialSellingPrice = "official selling price"
        Yield = "yield"
        Contract = "contract"
        ESS = "ess"
        Prompt = "prompt"
        Statistic = "statistic"
        EFP = "efp"
        Netback = "netback"
        EFS = "efs"
        Rack = "rack"

    class AssessmentFrequency(Enum):
        """Asessment Frequency"""

        Intraday = "Intraday"
        Daily = "Daily (7 day)"
        DailyWeekday = "Daily (weekday)"
        DailyBidweekOnly = "Daily (bidweek only)"
        SemiWeekly = "Semi-weekly"
        Weekly = "Weekly"
        SemiMonthly = "Semi-monthly"
        Monthly = "Monthly"
        EveryOtherMonth = "Every other month"
        Quarterly = "Quarterly"
        SemiAnnual = "Semi-annual"
        Yearly = "Yearly"

    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
        j = resp.json()
        df = pd.json_normalize(j["results"], record_path=["data"], meta="symbol")  # type: ignore

        if len(df) > 0:
            df.columns = df.columns.str.replace("change.", "", regex=True)  # type: ignore

        if "assessDate" in df.columns:
            df["assessDate"] = pd.to_datetime(df["assessDate"])  # type: ignore
        if "modDate" in df.columns:
            df["modDate"] = pd.to_datetime(df["modDate"])  # type: ignore
        if "pDate" in df.columns:
            df["pDate"] = pd.to_datetime(df["pDate"])  # type: ignore

        return df

    @staticmethod
    def _paginate(resp: Response) -> Paginator:
        j = resp.json()
        total_pages = j["metadata"]["totalPages"]

        if total_pages <= 1:
            return Paginator(False, "page", 1)

        return Paginator(True, "page", total_pages=total_pages)

    @staticmethod
    def _search_to_df(resp: Response) -> pd.DataFrame:
        j = resp.json()
        df = pd.DataFrame(j["results"])

        if len(df) > 0:
            cols = ["symbol", "description"]
            df: pd.DataFrame = df[cols + [x for x in df.columns if x not in cols]]  # type: ignore

        return df

    @staticmethod
    def _ref_paginate(resp: Response) -> Paginator:
        j = resp.json()
        total_pages = j["metadata"]["total_pages"]

        if total_pages <= 1:
            return Paginator(False, "page", 1)

        return Paginator(True, "page", total_pages=total_pages)

    def get_assessments_by_symbol_current(
        self,
        *,
        symbol: Optional[Union[list[str], "Series[str]", str]] = None,
        bate: Optional[Union[list[str], "Series[str]", str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 10000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[pd.DataFrame, Response]:
        """
        Fetch Current Assessments by Symbol from the Market Data API.

        See ``get_symbols()`` to search for symbol codes.\n
        See ``get_assessments_by_symbol_historical()`` to include historical assessments as well.\n

        Parameters
        ----------
        symbol : Optional[Union[list[str], Series[str], str]], optional
            filter by symbol, by default None
        bate : Optional[Union[list[str], Series[str], str]], optional
            filter by bate, by default None
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
        >>> ci.MarketData().get_assessments_symbol_current(symbol="PCAAS00")

        **Multiple Symbols and Bates**
        >>> ci.MarketData().get_assessments_symbol_current(symbol=["PCAAS00", "PCAAT00"], bate=["c", "h"])
        """
        filter_params: List[str] = []

        filter_params.append(list_to_filter("symbol", symbol))
        filter_params.append(list_to_filter("bate", bate))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        endpoint_path = "current/symbol"
        params = {
            "filter": filter_exp,
            "page": page,
            "pageSize": page_size,
            "field": self._mdd_fields,
        }

        return get_data(
            path=f"{self._path}{endpoint_path}",
            df_fn=self._convert_to_df,
            paginate_fn=self._paginate,
            params=params,
            raw=raw,
            paginate=paginate,
        )

    def get_assessments_by_symbol_historical(
        self,
        *,
        symbol: Optional[Union[list[str], "Series[str]", str]] = None,
        bate: Optional[Union[list[str], "Series[str]", str]] = None,
        assess_date: Optional[date] = None,
        assess_date_lt: Optional[date] = None,
        assess_date_lte: Optional[date] = None,
        assess_date_gt: Optional[date] = None,
        assess_date_gte: Optional[date] = None,
        modified_date: Optional[date] = None,
        modified_date_lt: Optional[date] = None,
        modified_date_lte: Optional[date] = None,
        modified_date_gt: Optional[date] = None,
        modified_date_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 10000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[pd.DataFrame, Response]:
        """
        Fetch Historical Assessments by Symbol from the Market Data API.

        See ``get_symbols()`` to search for symbol codes.\n
        See ``get_assessments_by_symbol_current()`` for the latest assessments only.\n

        Parameters
        ----------
        symbol : Optional[Union[list[str], Series[str], str]], optional
            filter by symbol, by default None
        bate : Optional[Union[list[str], Series[str], str]], optional
            filter by bate, by default None
        assess_date : Optional[date], optional
            filter by ``assessDate = x`` , by default None
        assess_date_lt : Optional[date], optional
            filter by ``assessDate < x``, by default None
        assess_date_lte : Optional[date], optional
            filter by ``assessDate <= x``, by default None
        assess_date_gt : Optional[date], optional
            filter by ``assessDate > x``, by default None
        assess_date_gte : Optional[date], optional
            filter by ``assessDate >= x``, by default None
        modified_date : Optional[date], optional
            filter by ``modDate = x`` , by default None
        modified_date_lt: Optional[date], optional
            filter by ``modDate < x``, by default None
        modified_date_lte : Optional[date], optional
            filter by ``modDate <= x``, by default None
        modified_date_gt : Optional[date], optional
            filter by ``modDate > x``, by default None
        modified_date_gte : Optional[date], optional
            filter by ``modDate >= x``, by default None
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
        >>> ci.MarketData().get_assessments_symbol_historical(symbol="PCAAS00")

        **Multiple Symbols and Bates**
        >>> ci.MarketData().get_assessments_symbol_historical(symbol=["PCAAS00", "PCAAT00"], bate=["c", "h"])

        **Date Range**
        >>> d1 = date(2023, 1, 1)
        >>> d2 = date(2023, 2, 1)
        >>> ci.MarketData().get_assessments_symbol_historical(symbol=["PCAAS00", "PCAAT00"], assess_date_gte=d1, assess_date_lte=d2])
        """
        filter_params: List[str] = []

        filter_params.append(list_to_filter("symbol", symbol))
        filter_params.append(list_to_filter("bate", bate))

        if assess_date != None:
            filter_params.append(f'assessDate: "{assess_date}"')
        if assess_date_gt != None:
            filter_params.append(f'assessDate > "{assess_date_gt}"')
        if assess_date_gte != None:
            filter_params.append(f'assessDate >= "{assess_date_gte}"')
        if assess_date_lt != None:
            filter_params.append(f'assessDate < "{assess_date_lt}"')
        if assess_date_lte != None:
            filter_params.append(f'assessDate <= "{assess_date_lte}"')

        if modified_date != None:
            filter_params.append(f'modDate: "{modified_date}"')
        if modified_date_gt != None:
            filter_params.append(f'modDate > "{modified_date_gt}"')
        if modified_date_gte != None:
            filter_params.append(f'modDate >= "{modified_date_gte}"')
        if modified_date_lt != None:
            filter_params.append(f'modDate < "{modified_date_lt}"')
        if modified_date_lte != None:
            filter_params.append(f'modDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        endpoint_path = "history/symbol"
        params = {"filter": filter_exp, "page": page, "pageSize": page_size}
        return get_data(
            path=f"{self._path}{endpoint_path}",
            df_fn=self._convert_to_df,
            paginate_fn=self._paginate,
            params=params,
            paginate=paginate,
            raw=raw,
        )

    def get_assessments_by_mdc_current(
        self,
        *,
        mdc: str,
        bate: Optional[Union[list[str], "Series[str]", str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 10000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[pd.DataFrame, Response]:
        """
        Fetch Current Assessments by MDC from the MarketData API.

        See ``get_mdcs()`` for a list of Market Data Categories.\n
        See ``get_assessments_by_mdc_historical()`` to include historical assessments as well.\n

        Parameters
        ----------
        mdc : str
            filter by Market Data Category
        bate : Optional[Union[list[str], Series[str], str], optional
            filter by bate, by default None
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
        >>> ci.MarketData().get_assessments_by_mdc_current(mdc="ET")

        **Include bate**
        >>> ci.MarketData().get_assessments_by_mdc_current(mdc="ET", bate=['c', 'u'])

        **Turn off auto pagination**
        >>> ci.MarketData().get_assessments_by_mdc_current(mdc="ET", paginate=false)
        """
        filter_params: List[str] = []
        filter_params.append(f'MDC: "{mdc}"')
        filter_params.append(list_to_filter("bate", bate))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        endpoint_path = "current/mdc"
        params = {
            "filter": filter_exp,
            "page": page,
            "pageSize": page_size,
            "field": self._mdd_fields,
        }
        return get_data(
            path=f"{self._path}{endpoint_path}",
            df_fn=self._convert_to_df,
            paginate_fn=self._paginate,
            params=params,
            paginate=paginate,
            raw=raw,
        )

    def get_assessments_by_mdc_historical(
        self,
        *,
        mdc: str,
        assess_date: Optional[date] = None,
        assess_date_lt: Optional[date] = None,
        assess_date_lte: Optional[date] = None,
        assess_date_gt: Optional[date] = None,
        assess_date_gte: Optional[date] = None,
        modified_date: Optional[date] = None,
        modified_date_lt: Optional[date] = None,
        modified_date_lte: Optional[date] = None,
        modified_date_gt: Optional[date] = None,
        modified_date_gte: Optional[date] = None,
        bate: Optional[Union[list[str], "Series[str]", str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 10000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[pd.DataFrame, Response]:
        """
        Fetch Historical Assessments by MDC from the MarketData API.

        See ``get_mdcs()`` for a list of Market Data Categories.\n
        See ``get_assessments_by_mdc_current()`` for the latest assessments only.\n

        Parameters
        ----------
        mdc : str
            filter by Market Data Category
        assess_date : Optional[date], optional
            filter by ``assessDate = x`` , by default None
        assess_date_lt : Optional[date], optional
            filter by ``assessDate < x``, by default None
        assess_date_lte : Optional[date], optional
            filter by ``assessDate <= x``, by default None
        assess_date_gt : Optional[date], optional
            filter by ``assessDate > x``, by default None
        assess_date_gte : Optional[date], optional
            filter by ``assessDate >= x``, by default None
        modified_date : Optional[date], optional
            filter by ``modDate = x`` , by default None
        modified_date_lt: Optional[date], optional
            filter by ``modDate < x``, by default None
        modified_date_lte : Optional[date], optional
            filter by ``modDate <= x``, by default None
        modified_date_gt : Optional[date], optional
            filter by ``modDate > x``, by default None
        modified_date_gte : Optional[date], optional
            filter by ``modDate >= x``, by default None
        bate : Union[list[str], Series[str], str], optional
            filter by bate, by default []
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
        >>> ci.MarketData().get_assessments_by_mdc_historical(mdc="ET", assess_date=date(2023,2,1))

        **Date Range**
        >>> d1 = date(2023,1,1)
        >>> d2 = date(2023,2,1)
        >>> ci.MarketData().get_assessments_by_mdc_historical(mdc="ET", assess_date_gte=d1, assess_date_lte=d2)
        """
        filter_params: List[str] = []
        filter_params.append(f'MDC: "{mdc}"')
        filter_params.append(list_to_filter("bate", bate))

        if assess_date != None:
            filter_params.append(f'assessDate: "{assess_date}"')
        if assess_date_gt != None:
            filter_params.append(f'assessDate > "{assess_date_gt}"')
        if assess_date_gte != None:
            filter_params.append(f'assessDate >= "{assess_date_gte}"')
        if assess_date_lt != None:
            filter_params.append(f'assessDate < "{assess_date_lt}"')
        if assess_date_lte != None:
            filter_params.append(f'assessDate <= "{assess_date_lte}"')

        if modified_date != None:
            filter_params.append(f'modDate: "{modified_date}"')
        if modified_date_gt != None:
            filter_params.append(f'modDate > "{modified_date_gt}"')
        if modified_date_gte != None:
            filter_params.append(f'modDate >= "{modified_date_gte}"')
        if modified_date_lt != None:
            filter_params.append(f'modDate < "{modified_date_lt}"')
        if modified_date_lte != None:
            filter_params.append(f'modDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        endpoint_path = "history/mdc"
        params = {"filter": filter_exp, "page": page, "pageSize": page_size}
        return get_data(
            path=f"{self._path}{endpoint_path}",
            df_fn=self._convert_to_df,
            paginate_fn=self._paginate,
            params=params,
            paginate=paginate,
            raw=raw,
        )

    def get_symbols(
        self,
        *,
        q: Optional[str] = None,
        commodity: Optional[Union[list[str], "Series[str]", str]] = None,
        contract_type: Optional[
            Union[list[str], list[ContractType], "Series[str]", str, ContractType]
        ] = None,
        currency: Optional[Union[list[str], "Series[str]", str]] = None,
        uom: Optional[Union[list[str], "Series[str]", str]] = None,
        symbol: Optional[Union[list[str], "Series[str]", str]] = None,
        delivery_region_basis: Optional[Union[list[str], "Series[str]", str]] = None,
        curve_code: Optional[Union[list[str], "Series[str]", str]] = None,
        mdc: Optional[Union[list[str], "Series[str]", str]] = None,
        assessment_frequency: Optional[
            Union[
                list[str],
                list[AssessmentFrequency],
                "Series[str]",
                str,
                AssessmentFrequency,
            ]
        ] = None,
        modified_date: Optional[date] = None,
        modified_date_lt: Optional[date] = None,
        modified_date_lte: Optional[date] = None,
        modified_date_gt: Optional[date] = None,
        modified_date_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[pd.DataFrame, Response]:
        """
        Fetch Symbols from the MarketData API.

        Parameters
        ----------
        q : Optional[str], optional
            filter across fields using free text search, by default None
        commodity : Optional[Union[list[str], Series[str], str]], optional
            filter by commodity, by default None
        contract_type : Optional[Union[list[str], list[ContractType], Series[str], str, ContractType] ], optional
            filter by contract type, by default None
        currency : Optional[Union[list[str], Series[str], str]], optional
            filter by currency, by default None
        uom : Optional[Union[list[str], Series[str], str]], optional
            filter by unit of measure, by default None
        symbol : Optional[Union[list[str], Series[str], str]], optional
            filter by symbol, by default None
        delivery_region_basis : Optional[Union[list[str], Series[str], str]], optional
            filter by delivery region basis, by default None
        curve_code : Optional[Union[list[str], Series[str], str]], optional
            filter by curve code, by default None
        mdc : Optional[Union[list[str], Series[str], str]], optional
            filter by Market Data Category, by default None
        assessment_frequency: Optional[Union[list[str], list[AssessmentFrequency], Series[str], str, AssessmentFrequency]], optional
            filter by Assessment Frequency, by default None
        modified_date : Optional[date], optional
            filter by ``modified_date = x`` , by default None
        modified_date_lt: Optional[date], optional
            filter by ``modified_date < x``, by default None
        modified_date_lte : Optional[date], optional
            filter by ``modified_date <= x``, by default None
        modified_date_gt : Optional[date], optional
            filter by ``modified_date > x``, by default None
        modified_date_gte : Optional[date], optional
            filter by ``modified_date >= x``, by default None
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
        **Free text search**
        >>> ci.MarketData().get_symbols(q="Brent")

        **Using String**
        >>> ci.MarketData().get_symbols(contract_type="Forward")

        **Using List**
        >>> ci.MarketData().get_symbols(currency=["USD", "EUR"])

        **Using Enum**
        >>> ci.MarketData().get_symbols(currency=["USD", "EUR"], contract_type=[ci.MarketData.ContractType.Forward, ci.MarketData.ContractType.Spot])
        """
        filter_params: List[str] = []

        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("contract_type", contract_type))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(
            list_to_filter("delivery_region_basis", delivery_region_basis)
        )
        filter_params.append(list_to_filter("curve_code", curve_code))
        filter_params.append(list_to_filter("symbol", symbol))
        filter_params.append(list_to_filter("mdc", mdc))
        filter_params.append(
            list_to_filter("assessment_frequency", assessment_frequency)
        )
        if modified_date != None:
            filter_params.append(f'modified_date: "{modified_date}"')
        if modified_date_gt != None:
            filter_params.append(f'modified_date > "{modified_date_gt}"')
        if modified_date_gte != None:
            filter_params.append(f'modified_date >= "{modified_date_gte}"')
        if modified_date_lt != None:
            filter_params.append(f'modified_date < "{modified_date_lt}"')
        if modified_date_lte != None:
            filter_params.append(f'modified_date <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {
            "q": q,
            "filter": filter_exp,
            "page": page,
            "pageSize": page_size,
        }
        return get_data(
            path=f"{self._ref_path}",
            df_fn=self._search_to_df,
            paginate_fn=self._ref_paginate,
            params=params,
            paginate=paginate,
            raw=raw,
        )

    def get_mdcs(
        self, *, subscribed_only: bool = True, raw: bool = False
    ) -> Union[pd.DataFrame, Response]:
        """
        Fetch the list of Market Data Categories (MDC)

        Parameters
        ----------
        subscribed_only : bool, optional
            return only MDC which you have access to, by default True
        raw : bool, optional
            return a ``requests.Response`` object instead of a ``DataFrame``, by default False

        Returns
        -------
        Union[pd.DataFrame, Response]
            DataFrame
                DataFrame of the ``response.json()``
            Response
                Raw ``requests.Response`` object
        """
        params = {"subscribed_only": subscribed_only}

        return get_data(
            path="market-data/reference-data/v3/mdc",
            params=params,
            raw=raw,
            paginate_fn=_nop_paginate,
        )
