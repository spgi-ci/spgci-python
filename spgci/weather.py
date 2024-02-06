from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date, datetime
from distutils.version import LooseVersion
import pandas as pd


class Weather:
    def get_actual(
        self,
        market: Optional[Union[list[str], Series[str], str]] = None,
        city: Optional[Union[list[str], Series[str], str]] = None,
        location: Optional[Union[list[str], Series[str], str]] = None,
        weather_date: Optional[Union[list[date], Series[date], date]] = None,
        weather_date_gte: Optional[date] = None,
        weather_date_gt: Optional[date] = None,
        weather_date_lte: Optional[date] = None,
        weather_date_lt: Optional[date] = None,
        modified_date: Optional[
            Union[list[datetime], Series[datetime], datetime]
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
        Fetch the data based on the filter expression.

        Parameters
        ----------

        market: Optional[Union[list[str], Series[str], str]]
            The market in which the weather data is recorded or observed., be default None
        city: Optional[Union[list[str], Series[str], str]]
            The specific city within the country for which the weather data is being reported., be default None
        location: Optional[Union[list[str], Series[str], str]]
            Weather station which provides Differentiator between country weather data and city weather data., be default None
        weather_date: Optional[Union[list[date], Series[date], date]]
            The date for which the weather data is recorded or forecasted., be default None
        weather_date_gte: Optional[date]
            filter by ``weatherDate >= x`` , by default None
        weather_date_gt: Optional[date]
            filter by ``weatherDate > x`` , by default None
        weather_date_lte: Optional[date]
            filter by ``weatherDate <= x`` , by default None
        weather_date_lt: Optional[date]
            filter by ``weatherDate < x`` , by default None
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
        >>> ci.Weather().get_actual(city="Boston")

        **Date Range**
        >>> ci.Weather().get_actual(market="United States", weather_date_gte="2024-01-01", weather_date_lte="2024-01-31")
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("city", city))
        filter_params.append(list_to_filter("location", location))
        filter_params.append(list_to_filter("weatherDate", weather_date))
        filter_params.append(list_to_filter("modifiedDate", modified_date))

        if weather_date_gt is not None:
            filter_params.append(f'weatherDate > "{weather_date_gt}"')
        if weather_date_gte is not None:
            filter_params.append(f'weatherDate >= "{weather_date_gte}"')
        if weather_date_lt is not None:
            filter_params.append(f'weatherDate < "{weather_date_lt}"')
        if weather_date_lte is not None:
            filter_params.append(f'weatherDate <= "{weather_date_lte}"')

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
            path="/weather/v1/actual",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_forecast(
        self,
        market: Optional[Union[list[str], Series[str], str]] = None,
        city: Optional[Union[list[str], Series[str], str]] = None,
        location: Optional[Union[list[str], Series[str], str]] = None,
        recorded_date: Optional[Union[list[date], Series[date], date]] = None,
        recorded_date_gte: Optional[date] = None,
        recorded_date_gt: Optional[date] = None,
        recorded_date_lte: Optional[date] = None,
        recorded_date_lt: Optional[date] = None,
        weather_date: Optional[Union[list[date], Series[date], date]] = None,
        weather_date_gte: Optional[date] = None,
        weather_date_gt: Optional[date] = None,
        weather_date_lte: Optional[date] = None,
        weather_date_lt: Optional[date] = None,
        modified_date: Optional[
            Union[list[datetime], Series[datetime], datetime]
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
        Fetch the data based on the filter expression.

        Parameters
        ----------

        market: Optional[Union[list[str], Series[str], str]]
            The market in which the weather data is recorded or observed., be default None
        city: Optional[Union[list[str], Series[str], str]]
            The specific city within the country for which the weather data is being reported., be default None
        location: Optional[Union[list[str], Series[str], str]]
            Weather station which provides Differentiator between country weather data and city weather data., be default None
        recorded_date: Optional[Union[list[date], Series[date], date]]
            Recorded Date refers to the specific date on which weather updateds are recorded., be default None
        recorded_date_gte: Optional[date]
            filter by ``recordedDate >= x`` , by default None
        recorded_date_gt: Optional[date]
            filter by ``recordedDate > x`` , by default None
        recorded_date_lte: Optional[date]
            filter by ``recordedDate <= x`` , by default None
        recorded_date_lt: Optional[date]
            filter by ``recordedDate < x`` , by default None
        weather_date: Optional[Union[list[date], Series[date], date]]
            The date for which the weather data is recorded or forecasted., be default None
        weather_date_gte: Optional[date]
            filter by ``weatherDate >= x`` , by default None
        weather_date_gt: Optional[date]
            filter by ``weatherDate > x`` , by default None
        weather_date_lte: Optional[date]
            filter by ``weatherDate <= x`` , by default None
        weather_date_lt: Optional[date]
            filter by ``weatherDate < x`` , by default None
        modified_date: Optional[datetime]
            The latest date of modification for the Weather Forecast., be default None
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
        >>> ci.Weather().get_forecast(city="Boston")

        **Date Range**
        >>> ci.Weather().get_forecast(market="United States", weather_date_gte="2024-01-01", weather_date_lte="2024-01-31")

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("city", city))
        filter_params.append(list_to_filter("location", location))
        filter_params.append(list_to_filter("recordedDate", recorded_date))
        filter_params.append(list_to_filter("weatherDate", weather_date))
        filter_params.append(list_to_filter("modifiedDate", modified_date))

        if recorded_date_gt is not None:
            filter_params.append(f'recordedDate > "{recorded_date_gt}"')
        if recorded_date_gte is not None:
            filter_params.append(f'recordedDate >= "{recorded_date_gte}"')
        if recorded_date_lt is not None:
            filter_params.append(f'recordedDate < "{recorded_date_lt}"')
        if recorded_date_lte is not None:
            filter_params.append(f'recordedDate <= "{recorded_date_lte}"')

        if weather_date_gt is not None:
            filter_params.append(f'weatherDate > "{weather_date_gt}"')
        if weather_date_gte is not None:
            filter_params.append(f'weatherDate >= "{weather_date_gte}"')
        if weather_date_lt is not None:
            filter_params.append(f'weatherDate < "{weather_date_lt}"')
        if weather_date_lte is not None:
            filter_params.append(f'weatherDate <= "{weather_date_lte}"')

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
            path="/weather/v1/forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
        j = resp.json()
        df = pd.json_normalize(j["results"])  # type: ignore

        if "weatherDate" in df.columns:
            if LooseVersion(pd.__version__) >= LooseVersion("2"):
                df["weatherDate"] = pd.to_datetime(df["weatherDate"], format="ISO8601")
            else:
                df["weatherDate"] = pd.to_datetime(df["weatherDate"])

        if "modifiedDate" in df.columns:
            if LooseVersion(pd.__version__) >= LooseVersion("2"):
                df["modifiedDate"] = pd.to_datetime(
                    df["modifiedDate"], format="ISO8601"
                )
            else:
                df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])

        if "recordedDate" in df.columns:
            if LooseVersion(pd.__version__) >= LooseVersion("2"):
                df["recordedDate"] = pd.to_datetime(
                    df["recordedDate"], format="ISO8601"
                )
            else:
                df["recordedDate"] = pd.to_datetime(df["recordedDate"])
        return df
