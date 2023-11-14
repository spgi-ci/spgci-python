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

from spgci.api_client import Paginator, get_data
from typing import List, Union, Optional
from datetime import date
from requests import Response
from spgci.utilities import list_to_filter
from pandas import Series, DataFrame, to_datetime  # type: ignore
from enum import Enum


class ForwardCurves:
    """
    Platts Forward Curves.

    Includes
    --------
    ``MatFrequency`` enum of maturity frequencies.\n
    ``CurveType`` enum of curve types.\n
    ``ContractType`` enum of contract types.\n
    ``get_curves()`` to get curve data, such as description, currency, maturity frequencies.\n
    ``get_assessments()`` to get assessment data for the symbols in a curve on a particular date.\n

    """

    _path = "market-data/forward-curve/v3/"
    _ref_path = "market-data/reference-data/v3/forward-curve/search"

    class MatFrequency(Enum):
        """Derivative Maturity Frequency"""

        Hour = "hour"
        Week = "week"
        Month = "month"
        Quarter = "quarter"
        Season = "season"
        Year = "year"
        GasYear = "gas year"

    class CurveType(Enum):
        """Curve Type"""

        Relative = "relative forward curve"
        Absolute = "absolute forward curve"

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

        # make date fields the correct datatype

        if "assessDate" in df.columns:
            df["assessDate"] = to_datetime(df["assessDate"])
        if "roll_date" in df.columns:
            df["roll_date"] = to_datetime(df["roll_date"])
        if "expiry_date" in df.columns:
            df["expiry_date"] = to_datetime(df["expiry_date"])

        return df

    @staticmethod
    def _ref_paginate(resp: Response) -> Paginator:
        j = resp.json()
        total_pages = j["metadata"]["total_pages"]

        if total_pages <= 1:
            return Paginator(False, "page", 1)

        return Paginator(True, "page", total_pages)

    @staticmethod
    def _ref_to_df(resp: Response) -> DataFrame:
        j = resp.json()
        df = DataFrame(j["results"])

        return df

    def get_assessments(
        self,
        *,
        curve_code: Optional[Union["list[str]", "Series[str]", str]] = None,
        derivative_maturity_frequency: Optional[
            Union["list[str]", "list[MatFrequency]", "Series[str]", str, MatFrequency]
        ] = None,
        assess_date: Optional[date] = None,
        assess_date_gt: Optional[date] = None,
        assess_date_gte: Optional[date] = None,
        assess_date_lt: Optional[date] = None,
        assess_date_lte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 10000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Fetch Forward Curves Metadata from the Market Data API.

        See ``get_curves()`` to search for curve codes.\n

        Parameters
        ----------
        curve_code : Optional[Union[list[str], Series[str], str]], optional
            fitler by curve code, by default None
        derivative_maturity_frequency : Optional[ Union[list[str], list[MatFrequency], Series[str], str, MatFrequency] ], optional
            fitler by maturity frequency, by default None
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
        **Get Latest Curve**
        >>> ci.ForwardCurves().get_assessments(curve_code="CN003")

        **Get Multiple Curves**
        >>> ci.ForwardCurves().get_assessments(curve_code=["CN003", "CN002"])

        **Get Monthly Contracts**
        >>> ci.ForwardCurves().get_assessments(curve_code=["CN003", "CN002"], derivative_maturity_frequency="Month")

        **Get Assessments for a Curve on a particular date**
        >>> ci.ForwardCurves().get_assessments(curve_code=["CN003"], assess_date=date(2023,2,13))
        """
        endpoint_path = "curve-codes"

        filter_params: List[str] = []

        filter_params.append(list_to_filter("curve_code", curve_code))
        filter_params.append(
            list_to_filter(
                "derivative_maturity_frequency", derivative_maturity_frequency
            )
        )

        if assess_date:
            filter_params.append(f'assessDate: "{assess_date}"')
        if assess_date_gt:
            filter_params.append(f'assessDate > "{assess_date_gt}"')
        if assess_date_gte:
            filter_params.append(f'assessDate >= "{assess_date_gte}"')
        if assess_date_lt:
            filter_params.append(f'assessDate < "{assess_date_lt}"')
        if assess_date_lte:
            filter_params.append(f'assessDate <= "{assess_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {
            "filter": filter_exp,
            "page": page,
            "pageSize": page_size,
        }

        response = get_data(
            path=f"{self._path}{endpoint_path}",
            params=params,
            raw=raw,
            paginate=paginate,
            df_fn=self._convert_to_df,
            paginate_fn=self._paginate,
        )

        return response

    def get_curves(
        self,
        *,
        q: Optional[str] = None,
        commodity: Optional[Union["list[str]", "Series[str]", str]] = None,
        contract_type: Optional[
            Union["list[str]", "list[ContractType]", "Series[str]", str, ContractType]
        ] = None,
        currency: Optional[Union["list[str]", "Series[str]", str]] = None,
        uom: Optional[Union["list[str]", "Series[str]", str]] = None,
        delivery_region: Optional[Union["list[str]", "Series[str]", str]] = None,
        curve_code: Optional[Union["list[str]", "Series[str]", str]] = None,
        curve_type: Optional[
            Union["list[str]", "list[CurveType]", "Series[str]", str, CurveType]
        ] = None,
        mdc: Optional[Union["list[str]", "Series[str]", str]] = None,
        derivative_maturity_frequency: Optional[
            Union["list[str]", "list[MatFrequency]", "Series[str]", str, MatFrequency]
        ] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
        subscribed_only: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Fetch Forward Curves by Curve Code from the Market Data API.

        See ``get_assessments()`` to get assessments for each symbol in a curve.\n

         Parameters
         ----------
         q : Optional[str], optional
             filter across fields using free text search, by default None
         commodity : Optional[Union[list[str], Series[str], str]], optional
             filter by commodity, by default None
         contract_type : Optional[ Union[list[str], list[ContractType], Series[str], str, ContractType] ], optional
             filter by contract type, by default None
         currency : Optional[Union[list[str], Series[str], str]], optional
             filter by currency, by default None
         uom : Optional[Union[list[str], Series[str], str]], optional
             filter by unit of measure, by default None
         delivery_region : Optional[Union[list[str], Series[str], str]], optional
             filter by delivery_region, by default None
         curve_code : Optional[Union[list[str], Series[str], str]], optional
             filter by curve code, by default None
         curve_type : Optional[Union[list[str], list[CurveType], Series[str], str, CurveType]], optional
             filter by curve type, by default None
         mdc : Optional[Union[list[str], Series[str], str]], optional
             fitler by Market Data Category, by default None
         derivative_maturity_frequency : Optional[ Union[list[str], list[MatFrequency], Series[str], str, MatFrequency] ], optional
             filter by maturity frequency, by default None
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
         subscribed_only : bool, optional
             return only curves which you have access to , by default False

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
         >>> ci.ForwardCurves().get_curves(q="Brent")

         **Find Benzene Curves**
         >>> ci.ForwardCurves().get_curves(commodity="Benzene")

         **Find Monthly Relative Curves by MDC**
         >>> ci.ForwardCurves().get_curves(curve_type=ci.ForwardCurves.CurveType.Relative, mdc=["DR"], derivative_maturity_frequency='Month')
        """
        filter_params: List[str] = []

        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("contract_type", contract_type))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("delivery_region", delivery_region))
        filter_params.append(
            list_to_filter(
                "derivative_maturity_frequency", derivative_maturity_frequency
            )
        )
        filter_params.append(list_to_filter("curve_code", curve_code))
        filter_params.append(list_to_filter("curve_type", curve_type))
        filter_params.append(list_to_filter("mdc", mdc))

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
            "subscribed_only": subscribed_only,
        }
        return get_data(
            path=f"{self._ref_path}",
            df_fn=self._ref_to_df,
            paginate_fn=self._ref_paginate,
            params=params,
            paginate=paginate,
            raw=raw,
        )
