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
from pandas import DataFrame, Series, to_datetime  # type: ignore
from typing import Union, Optional, List
from spgci.api_client import get_data, Paginator
from spgci.utilities import list_to_filter
from enum import Enum
from datetime import date
from typing_extensions import Literal


class Arbflow:
    """
    Refining Margins & Crude Arbitrage

    Includes
    --------
    ``RefTypes`` to use with the ``get_reference_data`` method.
    ``get_reference_data()`` to get the list of locations, crudes, configurations, etc.. for the Refining Margins & Crude Arbitrage dataset.
    ``get_margins_catalog()`` to get the refining margins catalog which shows the unique combination of factors
                            (the crude, location, refinery configuration and freight) that describe the margins.
    ``get_margins_data()`` to get the refining margins data.
    ``get_arbitrage()`` get the crude arbitrage data.

    """

    _path = "arbflow/data/v1/"

    class RefTypes(Enum):
        """Refining Margins & Crude Arbitrage Reference Data Types"""

        Configurations = "configurations"
        Crudes = "crudes"
        Frequencies = "frequencies"
        Locations = "locations"
        Margins = "margins"

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

    @staticmethod
    def _convert_to_df(resp: Response) -> DataFrame:
        j = resp.json()
        df = DataFrame(j["results"])

        # make date fields the correct datatype

        if "modifiedDate" in df.columns:
            df["modifiedDate"] = to_datetime(df["modifiedDate"])

        if "marginDate" in df.columns:
            df["marginDate"] = to_datetime(df["marginDate"])

        if "baseMarginDate" in df.columns:
            df["baseMarginDate"] = to_datetime(df["baseMarginDate"])

        return df

    def get_margins_catalog(
        self,
        *,
        margin_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        margin_name: Optional[Union[str, list[str], "Series[str]"]] = None,
        crude_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        crude_name: Optional[Union[str, list[str], "Series[str]"]] = None,
        crude_symbol: Optional[Union[str, list[str], "Series[str]"]] = None,
        location_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        location_name: Optional[Union[str, list[str], "Series[str]"]] = None,
        configuration_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        configuration_name: Optional[Union[str, list[str], "Series[str]"]] = None,
        preferred_freight: Optional[bool] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Get the refining margins catalog which shows the unique combination of factors (the crude, location,
        refinery configuration and freight) that describe the margins.

        Parameters
        ----------
        margin_id   :    Optional[Union[int, list[int], "Series[int]"]] = None,
            filter by marginId, by default None
        margin_name :    Optional[Union[str, list[str], "Series[str]"]] = None,
            filter by marginName, by default None
        crude_id    :    Optional[Union[int, list[int], "Series[int]"]] = None,
            filter by crudeId, by default None
        crude_name  :    Optional[Union[str, list[str], "Series[str]"]] = None,
            filter by crudeName, by default None
        crude_symbol    :    Optional[Union[str, list[str], "Series[str]"]] = None,
            filter by crudeSymbol, by default None
        location_id     :    Optional[Union[int, list[int], "Series[int]"]] = None,
            filter by locationId, by default None
        location_name   :   Optional[Union[str, list[str], "Series[str]"]] = None,
            filter by locationName, by default None
        configuration_id   :    Optional[Union[int, list[int], "Series[int]"]] = None,
            filter by configurationId, by default None
        configuration_name  :   Optional[Union[str, list[str], "Series[str]"]] = None,
            filter by configurationName, by default None
        preferred_freight   :   Optional[bool] = None,
            filter by preferredFreight, by default None
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
        **Get Margins Catalog**
        >>> ci.Arbflow().get_margins_catalog(location_id = 34)

        **Get Margins Catalog for multiple locations **
        >>> ci.Arbflow().get_margins_catalog(location_id = [31,34])

        **Get Margins Catalog Data Based On Location Id And Crude Symbol**
        >>> ci.Arbflow().get_margins_catalog(location_id = 34, crude_symbol="AAQZB00")

        """

        endpoint_path = "margins-catalog"

        filter_param: List[str] = []

        filter_param.append(list_to_filter("marginId", margin_id))
        filter_param.append(list_to_filter("marginName", margin_name))
        filter_param.append(list_to_filter("crudeId", crude_id))
        filter_param.append(list_to_filter("crudeName", crude_name))
        filter_param.append(list_to_filter("crudeSymbol", crude_symbol))
        filter_param.append(list_to_filter("locationId", location_id))
        filter_param.append(list_to_filter("locationName", location_name))
        filter_param.append(list_to_filter("configurationId", configuration_id))
        filter_param.append(list_to_filter("configurationName", configuration_name))
        filter_param.append(list_to_filter("preferredFreight", preferred_freight))

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
            df_fn=self._convert_to_df,
            raw=raw,
        )

    def get_margins_data(
        self,
        frequency_id: Literal[1, 2, 3] = 1,
        *,
        margin_id: Optional[Union[int, list[int], "Series[int]"]] = None,
        margin_date: Optional[date] = None,
        margin_date_gt: Optional[date] = None,
        margin_date_gte: Optional[date] = None,
        margin_date_lt: Optional[date] = None,
        margin_date_lte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Get the refining margins data.

        Parameters
        ----------
        margin_id   :    Optional[Union[int, list[int], "Series[int]"]] = None,
            filter by marginId, by default None
        frequency_id    :    Literal[1,2,3] = 1,
            filter by frequencyId - 1 = Daily, 2 = Monthly, 3 = Annual, by default 1
        margin_date: Optional[date] = None,
            filter by marginDate, by default None
        margin_date_lt : Optional[date], optional
            filter by ``marginDate < x``, by default None
        margin_date_lte : Optional[date], optional
            filter by ``marginDate <= x``, by default None
        margin_date_gt : Optional[date], optional
            filter by ``marginDate > x``, by default None
        margin_date_gte : Optional[date], optional
            filter by ``marginDate >= x``, by default None
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
        **Get Margins Data**
        >>> ci.Arbflow().get_margins_data(frequency_id = 1)

        **Get Margins Data Based On Margin Id And Date**
        >>> ci.Arbflow().get_margins_data(margin_id=229, margin_date='2023-08-16')

        """

        endpoint_path = "margins-data"

        filter_param: List[str] = []

        filter_param.append(list_to_filter("marginId", margin_id))
        if margin_date:
            filter_param.append(f'marginDate: "{margin_date}"')
        if margin_date_gt:
            filter_param.append(f'marginDate > "{margin_date_gt}"')
        if margin_date_gte:
            filter_param.append(f'marginDate >= "{margin_date_gte}"')
        if margin_date_lt:
            filter_param.append(f'marginDate < "{margin_date_lt}"')
        if margin_date_lte:
            filter_param.append(f'marginDate <= "{margin_date_lte}"')

        filter_param = [fp for fp in filter_param if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_param)
        elif len(filter_param) > 0:
            filter_exp = " AND ".join(filter_param) + " AND (" + filter_exp + ")"

        params = {
            "frequencyId": frequency_id,
            "pageSize": page_size,
            "filter": filter_exp,
            "page": page,
        }
        return get_data(
            path=f"{self._path}{endpoint_path}",
            params=params,
            paginate=paginate,
            paginate_fn=self._paginate,
            df_fn=self._convert_to_df,
            raw=raw,
        )

    def get_arbitrage(
        self,
        base_margin_id: int,
        margin_id: Union[list[int], "Series[int]", int],
        frequency_id: Literal[1, 2, 3] = 1,
        *,
        base_margin_date: Optional[date] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Get the crude arbitrage data..

        Parameters
        ----------
        margin_id   :  Union[list[int], "Series[int]", int],
            filter by comparisonMarginId, maximum of 5 comparison margin IDs. (required field)
        base_margin_id: int,
            filter by baseMarginId (required field)
        frequency_id    :    Literal[1,2,3] = 1,
            filter by frequencyId - 1 = Daily, 2 = Monthly, 3 = Annual, by default 1 (required field)
        base_margin_date: Optional[datetime] = None,
            filter by marginDate, by default None
        start_date: Optional[datetime] = None,
            filter by startDate, by default None
        end_date: Optional[datetime] = None,
            filter by endDate, by default None
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
        **Get Arbitrage Data**
        >>> ci.Arbflow().get_arbitrage(margin_id=229, base_margin_id=330, frequency_id=1)

        **Get Arbitrage Data Based On Multiple Comparison Margin Id's **
        >>> ci.Arbflow().get_arbitrage(margin_id=[220,330], base_margin_id =330, frequency_id=1)

        **Using Date Range**
        >>> ci.Arbflow().get_arbitrage(frequency_id=1, margin_id=261, base_margin_id=1380, start_date="2024-01-01", end_date="2024-01-31")
        """

        endpoint_path = "arbitrage"

        filter_param: List[str] = []

        filter_param.append(list_to_filter("baseMarginDate", base_margin_date))

        filter_param = [fp for fp in filter_param if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_param)
        elif len(filter_param) > 0:
            filter_exp = " AND ".join(filter_param) + " AND (" + filter_exp + ")"

        if isinstance(margin_id, int):
            cmp_margin_id = margin_id
        elif isinstance(margin_id, list):
            cmp_margin_id = ",".join(map(str, margin_id))
        else:
            # redundant / maybe user will pass a string.
            cmp_margin_id = margin_id

        params = {
            "ComparisonMarginIds": cmp_margin_id,
            "baseMarginId": base_margin_id,
            "frequencyId": frequency_id,
            "pageSize": page_size,
            "filter": filter_exp,
            "page": page,
        }

        if start_date is not None:
            params["startDate"] = start_date
        if end_date is not None:
            params["endDate"] = end_date

        return get_data(
            path=f"{self._path}{endpoint_path}",
            params=params,
            paginate=paginate,
            paginate_fn=self._paginate,
            df_fn=self._convert_to_df,
            raw=raw,
        )

    def get_reference_data(
        self, type: RefTypes, raw: bool = False
    ) -> Union[Response, DataFrame]:
        """
        Fetch reference data for the Refining Margins & Crude Arbitrage dataset.

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
        >>> ci.Arbflow().get_reference_data(type=ci.Arbflow.RefTypes.Crudes)
        >>> ci.Arbflow().get_reference_data(type=ci.Arbflow.RefTypes.Margins)
        >>> ci.Arbflow().get_reference_data(type=ci.Arbflow.RefTypes.Locations)
        """
        endpoint_path = type.value

        params = {"pageSize": 1000}

        return get_data(
            path=f"{self._path}{endpoint_path}",
            params=params,
            paginate=True,
            paginate_fn=self._paginate,
            df_fn=self._convert_to_df,
            raw=raw,
        )
