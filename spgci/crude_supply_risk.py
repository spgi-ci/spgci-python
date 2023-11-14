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
import pandas as pd
from typing import Union, Optional, List
from spgci.api_client import get_data, Paginator
from spgci.utilities import list_to_filter
from datetime import date
from typing_extensions import Literal


class CrudeAnalytics:
    """
    Short-term supply-side price risk index. This index captures the price influence of changes in oil supply in the 0-30 day outlook period.
    Factors are assessed on the country level: A positive score indicates a bullish supply impact on price, whereas a negative score indicates a bearish impact.
    The absolute value of the score is an indicator of the magnitude of the impact.
    The global price risk index is an aggregate of country-level risk scores, weighted by the country’s share of global crude and condensate capacity.

    Includes
    --------
    ``get_country_scores()`` get oil supply risk country scores.
    ``get_country_total_scores()`` get aggregated country scores.

    """

    _path = "crude-analytics/v1/oil-supply-risk/"

    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
        j = resp.json()
        df = pd.json_normalize(j["results"])  # type: ignore

        if "scoring_date" in df.columns:
            df["scoring_date"] = pd.to_datetime(df["scoring_date"])  # type: ignore

        return df

    @staticmethod
    def _paginate(resp: Response) -> Paginator:
        j = resp.json()
        count = j["metaData"]["count"]
        size = j["metaData"]["pageSize"]

        remainder = count % size
        quotient = count // size
        total_pages = quotient + (1 if remainder > 0 else 0)

        if total_pages <= 1:
            return Paginator(False, "page", 1)

        return Paginator(True, "page", total_pages=total_pages)

    def get_country_scores(
        self,
        *,
        scoring_date: Optional[Union[date, list[date], "Series[date]"]] = None,
        scoring_date_gt: Optional[date] = None,
        scoring_date_gte: Optional[date] = None,
        scoring_date_lt: Optional[date] = None,
        scoring_date_lte: Optional[date] = None,
        status: Optional[Literal["Current", "Historical"]] = None,
        region: Optional[Union[str, list[str], "Series[str]"]] = None,
        country: Optional[Union[str, list[str], "Series[str]"]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Analytics data for North America Natural Gas Pipelines .

        Parameters
        ----------

        scoring_date : Optional[Union[date, list[date], Series[date]]], optional
            filter by scoring_date, by default None
        scoring_date_gt : Optional[date], optional
            filter by ``scoring_date > x``, by default None
        scoring_date_gte : Optional[date], optional
            filter by ``scoring_date >= x``, by default None
        scoring_date_lt : Optional[date], optional
            filter by ``scoring_date < x``, by default None
        scoring_date_lte : Optional[date], optional
            filter by ``scoring_date <= x``, by default None
        status : Optional[Literal['Current', 'Historical']], optional
            filter by status, by default None
        region : Optional[Union[str, list[str], Series[str]]], optional
            filter by region, by default None
        country : Optional[Union[str, list[str], Series[str]]], optional
            filter by country, by default None
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
        **Get all scores for country**
        >>> ci.CrudeAnalytics().get_country_scores(country="United States")

        **Get latest scores for all countries**
        >>> ci.CrudeAnalytics().get_country_scores(status="Current")
        """
        endpoint_path = "country-scores"

        filter_param: List[str] = []

        filter_param.append(list_to_filter("scoringDate", scoring_date))
        filter_param.append(list_to_filter("status", status))
        filter_param.append(list_to_filter("region", region))
        filter_param.append(list_to_filter("country", country))

        if scoring_date_gt:
            filter_param.append(f'scoringDate > "{scoring_date_gt}"')
        if scoring_date_gte:
            filter_param.append(f'scoringDate >= "{scoring_date_gte}"')
        if scoring_date_lt:
            filter_param.append(f'scoringDate < "{scoring_date_lt}"')
        if scoring_date_lte:
            filter_param.append(f'scoringDate <= "{scoring_date_lte}"')

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
            df_fn=self._convert_to_df,
            params=params,
            paginate=paginate,
            paginate_fn=self._paginate,
            raw=raw,
        )

    def get_country_total_scores(
        self,
        *,
        status: Optional[Literal["Current", "Historical"]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Analytics data for North America Natural Gas Pipelines .

        Parameters
        ----------

        status : Optional[Literal['Current', 'Historical']], optional
            filter by status, by default None
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
        **Simple Usage**
        >>> ci.CrudeAnalytics().get_country_total_scores()
        """
        endpoint_path = "country-total-scores"

        filter_param: List[str] = []

        filter_param.append(list_to_filter("status", status))
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
            df_fn=self._convert_to_df,
            params=params,
            paginate=paginate,
            paginate_fn=self._paginate,
            raw=raw,
        )
