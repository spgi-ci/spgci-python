from __future__ import annotations
from .api_client import get_data, Paginator
from .utilities import list_to_filter
from typing import Union, Optional
from typing_extensions import Literal
from pandas import DataFrame, Series, to_datetime  # type: ignore
from requests import Response
from enum import Enum
from datetime import date


class EnergyPriceForecast:
    """
    The Energy Price Forecast API provides a comprehensive view of S&P Global Platts latest energy price forecasts and historical monthly/yearly averages.
    Short-term forecasts are available up to 18-months out on a monthly granularity and long-term forecasts are available on a yearly granularity up to 30-years out.
    The Energy Price Forecast API also gives users access to historical forecasts to easily compare how Platts' outlook has evolved over time.

    """

    _path = "/energy-price-forecast/v1/"

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
    def _to_df(resp: Response) -> DataFrame:
        j = resp.json()
        df = DataFrame(j["results"])

        if len(df) > 0:
            df["modifiedDate"] = to_datetime(df["modifiedDate"])

        return df

    class RefTypes(Enum):
        """Reference Types to use with the `get_reference_date()` method"""

        Commodities = "commodities"
        Categories = "categories"
        Currencies = "currencies"
        DeliveryRegions = "delivery-regions"
        Groups = "groups"
        Prices = "prices"
        # LTArchiveDates = "long-term-archive-dates" handle these in a separate method.
        # STArchiveDates = "short-term-archive-dates"
        Sectors = "sectors"
        SectorGroups = "sector-groups"
        Units = "units"
        LTYears = "years-long-term"
        STYears = "years-short-term"
        # LTYearsCategory = "years-lt-category" unsupported for now.
        # STYearsCategory = "years-st-category"

    def get_prices_shortterm(
        self,
        *,
        year: Optional[Union[list[int], "Series[int]", int]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        month: Optional[Union[list[int], "Series[int]", int]] = None,
        symbol: Optional[Union[list[str], "Series[str]", str]] = None,
        units: Optional[Union[list[str], "Series[str]", str]] = None,
        currency: Optional[Union[list[str], "Series[str]", str]] = None,
        category: Optional[Union[list[str], "Series[str]", str]] = None,
        group: Optional[Union[list[str], "Series[str]", str]] = None,
        sector: Optional[Union[list[str], "Series[str]", str]] = None,
        commodity: Optional[Union[list[str], "Series[str]", str]] = None,
        delivery_region: Optional[Union[list[str], "Series[str]", str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Latest short-term price forecasts provided on a monthly granularity.

        Parameters
        ----------
        year : Optional[Union[int, list[int], Series[int]]], optional
            filter by ``year = x``, by default None
        year_gt : Optional[int], optional
            filter by ``year > x``, by default None
        year_gte : Optional[int], optional
            filter by ``year >= x``, by default None
        year_lt : Optional[int], optional
            filter by ``year < x``, by default None
        year_lte : Optional[int], optional
            filter by ``year <= x``, by default None
        month : Optional[Union[list[int], Series[int], int]], optional
            filter by month, by default None
        symbol : Optional[Union[list[str], Series[str], str]], optional
            filter by priceSymbol, by default None
        units : Optional[Union[list[str], Series[str], str]], optional
            filter by unitName, by default None
        currency : Optional[Union[list[str], Series[str], str]], optional
            filter by currencySymbol, by default None
        category : Optional[Union[list[str], Series[str], str]], optional
            filter by categoryName, by default None
        group : Optional[Union[list[str], Series[str], str]], optional
            filter by groupName, by default None
        sector : Optional[Union[list[str], Series[str], str]], optional
            filter by sectorName, by default None
        commodity : Optional[Union[list[str], Series[str], str]], optional
            filter by commodityName, by default None
        delivery_region : Optional[Union[list[str], Series[str], str]], optional
            filter by deliveryRegionName, by default None
        filter_exp : Optional[str], optional
            pass-thru ``$filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to select a certain page number, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame, by default False

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
        >>> ci.EnergyPriceForecast().get_prices_shortterm(symbol="PCAAS00", group="Crude")

        **Using Lists**
        >>> ci.EnergyPriceForecast().get_prices_shortterm(month=[10, 11, 12], group="Crude", delivery_region="United States")

        **Using Series**
        >>> groups = ci.EnergyPriceForecast().get_reference_data(type=ci.EnergyPriceForecast.RefTypes.SectorGroups)
        >>> ci.EnergyPriceForecast().get_prices_shortterm(group=groups['groupName'].tail(2))
        """
        path = "prices-short-term"
        filter_params: list[str] = []

        if year_gt != None:
            filter_params.append(f"year > {year_gt}")
        if year_gte != None:
            filter_params.append(f"year >= {year_gte}")
        if year_lt != None:
            filter_params.append(f"year < {year_lt}")
        if year_lte != None:
            filter_params.append(f"year <= {year_lte}")

        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("priceSymbol", symbol))
        filter_params.append(list_to_filter("month", month))
        filter_params.append(list_to_filter("unitName", units))
        filter_params.append(list_to_filter("categoryName", category))
        filter_params.append(list_to_filter("groupName", group))
        filter_params.append(list_to_filter("currencySymbol", currency))
        filter_params.append(list_to_filter("commodityName", commodity))
        filter_params.append(list_to_filter("deliveryRegionName", delivery_region))
        filter_params.append(list_to_filter("sectorName", sector))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"filter": filter_exp, "page": page, "pageSize": page_size}
        return get_data(
            path=f"{self._path}{path}",
            params=params,
            paginate=paginate,
            raw=raw,
            paginate_fn=self._paginate,
            df_fn=self._to_df,
        )

    def get_prices_longterm(
        self,
        *,
        year: Optional[Union[list[int], "Series[int]", int]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        symbol: Optional[Union[list[str], "Series[str]", str]] = None,
        units: Optional[Union[list[str], "Series[str]", str]] = None,
        currency: Optional[Union[list[str], "Series[str]", str]] = None,
        category: Optional[Union[list[str], "Series[str]", str]] = None,
        group: Optional[Union[list[str], "Series[str]", str]] = None,
        sector: Optional[Union[list[str], "Series[str]", str]] = None,
        commodity: Optional[Union[list[str], "Series[str]", str]] = None,
        delivery_region: Optional[Union[list[str], "Series[str]", str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Latest long-term price forecasts provided on a yearly granularity.

        Parameters
        ----------
        year : Optional[Union[int, list[int], Series[int]]], optional
            filter by ``year = x``, by default None
        year_gt : Optional[int], optional
            filter by ``year > x``, by default None
        year_gte : Optional[int], optional
            filter by ``year >= x``, by default None
        year_lt : Optional[int], optional
            filter by ``year < x``, by default None
        year_lte : Optional[int], optional
            filter by ``year <= x``, by default None
        symbol : Optional[Union[list[str], Series[str], str]], optional
            filter by priceSymbol, by default None
        units : Optional[Union[list[str], Series[str], str]], optional
            filter by unitName, by default None
        currency : Optional[Union[list[str], Series[str], str]], optional
            filter by currencySymbol, by default None
        category : Optional[Union[list[str], Series[str], str]], optional
            filter by categoryName, by default None
        group : Optional[Union[list[str], Series[str], str]], optional
            filter by groupName, by default None
        sector : Optional[Union[list[str], Series[str], str]], optional
            filter by sectorName, by default None
        commodity : Optional[Union[list[str], Series[str], str]], optional
            filter by commodityName, by default None
        delivery_region : Optional[Union[list[str], Series[str], str]], optional
            filter by deliveryRegionName, by default None
        filter_exp : Optional[str], optional
            pass-thru ``$filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to select a certain page number, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame, by default False

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
        >>> ci.EnergyPriceForecast().get_prices_longterm(year=2040)

        **Using Lists**
        >>> ci.EnergyPriceForecast().get_prices_longterm(group=["Crude", "Common Crude Spreads"], delivery_region="United States")

        **Using Series**
        >>> years = ci.EnergyPriceForecast().get_reference_data(type=ci.EnergyPriceForecast.RefTypes.LTYears)
        >>> ci.EnergyPriceForecast().get_prices_longterm(year=years['year'][:2])
        """
        path = "prices-long-term"
        filter_params: list[str] = []

        if year_gt != None:
            filter_params.append(f"year > {year_gt}")
        if year_gte != None:
            filter_params.append(f"year >= {year_gte}")
        if year_lt != None:
            filter_params.append(f"year < {year_lt}")
        if year_lte != None:
            filter_params.append(f"year <= {year_lte}")

        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("priceSymbol", symbol))
        filter_params.append(list_to_filter("unitName", units))
        filter_params.append(list_to_filter("categoryName", category))
        filter_params.append(list_to_filter("groupName", group))
        filter_params.append(list_to_filter("currencySymbol", currency))
        filter_params.append(list_to_filter("commodityName", commodity))
        filter_params.append(list_to_filter("deliveryRegionName", delivery_region))
        filter_params.append(list_to_filter("sectorName", sector))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"filter": filter_exp, "page": page, "pageSize": page_size}
        return get_data(
            path=f"{self._path}{path}",
            params=params,
            paginate=paginate,
            raw=raw,
            paginate_fn=self._paginate,
            df_fn=self._to_df,
        )

    def get_prices_shortterm_archive(
        self,
        *,
        modified_date: date,
        category_id: int,
        month: Optional[Union[list[int], "Series[int]", int]] = None,
        year: Optional[Union[list[int], "Series[int]", int]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        symbol: Optional[Union[list[str], "Series[str]", str]] = None,
        units: Optional[Union[list[str], "Series[str]", str]] = None,
        currency: Optional[Union[list[str], "Series[str]", str]] = None,
        category: Optional[Union[list[str], "Series[str]", str]] = None,
        group: Optional[Union[list[str], "Series[str]", str]] = None,
        sector: Optional[Union[list[str], "Series[str]", str]] = None,
        commodity: Optional[Union[list[str], "Series[str]", str]] = None,
        delivery_region: Optional[Union[list[str], "Series[str]", str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Archived short-term price forecasts.

        see `get_archive_dates()` for applicable modified dates/categories.

        Parameters
        ----------
        modified_date : date
            filter on ModifiedDate to see the nearest prior forecast
        category_id : int
            filter on PriceCategoryCode
        year : Optional[Union[int, list[int], Series[int]]], optional
            filter by ``year = x``, by default None
        year_gt : Optional[int], optional
            filter by ``year > x``, by default None
        year_gte : Optional[int], optional
            filter by ``year >= x``, by default None
        year_lt : Optional[int], optional
            filter by ``year < x``, by default None
        year_lte : Optional[int], optional
            filter by ``year <= x``, by default None
        month : Optional[Union[list[int], Series[int], int]], optional
            filter by month, by default None
        symbol : Optional[Union[list[str], Series[str], str]], optional
            filter by priceSymbol, by default None
        units : Optional[Union[list[str], Series[str], str]], optional
            filter by unitName, by default None
        currency : Optional[Union[list[str], Series[str], str]], optional
            filter by currencySymbol, by default None
        category : Optional[Union[list[str], Series[str], str]], optional
            filter by categoryName, by default None
        group : Optional[Union[list[str], Series[str], str]], optional
            filter by groupName, by default None
        sector : Optional[Union[list[str], Series[str], str]], optional
            filter by sectorName, by default None
        commodity : Optional[Union[list[str], Series[str], str]], optional
            filter by commodityName, by default None
        delivery_region : Optional[Union[list[str], Series[str], str]], optional
            filter by deliveryRegionName, by default None
        filter_exp : Optional[str], optional
            pass-thru ``$filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to select a certain page number, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame, by default False

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
        >>> from datetime import date
        >>> ci.EnergyPriceForecast().get_prices_shortterm_archive(modified_date=date(2020, 11, 1), category_id=1, year=2022)

        **Using Lists**
        >>> from datetime import date
        >>> ci.EnergyPriceForecast().get_prices_shortterm_archive(modified_date=date(2020, 11, 1) category_id=1, year=2022, group=["Refinery Margins", "U.S. West Coast"])

        **Using Series**
        >>> cat_dates = ci.EnergyPriceForecast().get_archive_dates(term_type="short", category="Oil")
        >>> cat_dates = cat_dates.iloc[0]
        >>> ci.EnergyPriceForecast().get_prices_shortterm_archive(modified_date=cat_dates['modifiedDate'], category_id=cat_dates['categoryId'], month=12, delivery_region="Singapore")
        """
        path = "prices-short-term-archive"
        filter_params: list[str] = []

        if year_gt != None:
            filter_params.append(f"year > {year_gt}")
        if year_gte != None:
            filter_params.append(f"year >= {year_gte}")
        if year_lt != None:
            filter_params.append(f"year < {year_lt}")
        if year_lte != None:
            filter_params.append(f"year <= {year_lte}")

        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("month", month))
        filter_params.append(list_to_filter("priceSymbol", symbol))
        filter_params.append(list_to_filter("unitName", units))
        filter_params.append(list_to_filter("categoryName", category))
        filter_params.append(list_to_filter("groupName", group))
        filter_params.append(list_to_filter("currencySymbol", currency))
        filter_params.append(list_to_filter("commodityName", commodity))
        filter_params.append(list_to_filter("deliveryRegionName", delivery_region))
        filter_params.append(list_to_filter("sectorName", sector))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {
            "filter": filter_exp,
            "page": page,
            "pageSize": page_size,
            "PriceCategoryCode": category_id,
            "ModifiedDate": modified_date,
        }
        return get_data(
            path=f"{self._path}{path}",
            params=params,
            paginate=paginate,
            raw=raw,
            paginate_fn=self._paginate,
            df_fn=self._to_df,
        )

    def get_prices_longterm_archive(
        self,
        *,
        modified_date: date,
        category_id: int,
        year: Optional[Union[list[int], "Series[int]", int]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        symbol: Optional[Union[list[str], "Series[str]", str]] = None,
        units: Optional[Union[list[str], "Series[str]", str]] = None,
        currency: Optional[Union[list[str], "Series[str]", str]] = None,
        category: Optional[Union[list[str], "Series[str]", str]] = None,
        group: Optional[Union[list[str], "Series[str]", str]] = None,
        sector: Optional[Union[list[str], "Series[str]", str]] = None,
        commodity: Optional[Union[list[str], "Series[str]", str]] = None,
        delivery_region: Optional[Union[list[str], "Series[str]", str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Archived long-term price forecasts.

        see `get_archive_dates()` for applicable modified dates/categories.

        Parameters
        ----------
        modified_date : date
            filter on ModifiedDate to see the nearest prior forecast
        category_id : int
            filter on PriceCategoryCode
        year : Optional[Union[int, list[int], Series[int]]], optional
            filter by ``year = x``, by default None
        year_gt : Optional[int], optional
            filter by ``year > x``, by default None
        year_gte : Optional[int], optional
            filter by ``year >= x``, by default None
        year_lt : Optional[int], optional
            filter by ``year < x``, by default None
        year_lte : Optional[int], optional
            filter by ``year <= x``, by default None
        symbol : Optional[Union[list[str], Series[str], str]], optional
            filter by priceSymbol, by default None
        units : Optional[Union[list[str], Series[str], str]], optional
            filter by unitName, by default None
        currency : Optional[Union[list[str], Series[str], str]], optional
            filter by currencySymbol, by default None
        category : Optional[Union[list[str], Series[str], str]], optional
            filter by categoryName, by default None
        group : Optional[Union[list[str], Series[str], str]], optional
            filter by groupName, by default None
        sector : Optional[Union[list[str], Series[str], str]], optional
            filter by sectorName, by default None
        commodity : Optional[Union[list[str], Series[str], str]], optional
            filter by commodityName, by default None
        delivery_region : Optional[Union[list[str], Series[str], str]], optional
            filter by deliveryRegionName, by default None
        filter_exp : Optional[str], optional
            pass-thru ``$filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to select a certain page number, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 1000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame, by default False

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
        >>> from datetime import date
        >>> ci.EnergyPriceForecast().get_prices_longterm_archive(modified_date=date(2020, 11, 1), category_id=1, year=2022)

        **Using Lists**
        >>> from datetime import date
        >>> ci.EnergyPriceForecast().get_prices_longterm_archive(modified_date=date(2020, 11, 1) category_id=1, year=2022, group=["Refinery Margins", "U.S. West Coast"])

        **Using Series**
        >>> cat_dates = ci.EnergyPriceForecast().get_archive_dates(term_type="long", category="Oil")
        >>> cat_dates = cat_dates.iloc[0]
        >>> ci.EnergyPriceForecast().get_prices_longterm_archive(modified_date=cat_dates['modifiedDate'], category_id=cat_dates['categoryId'], delivery_region="Singapore")
        """
        path = "prices-long-term-archive"
        filter_params: list[str] = []

        if year_gt != None:
            filter_params.append(f"year > {year_gt}")
        if year_gte != None:
            filter_params.append(f"year >= {year_gte}")
        if year_lt != None:
            filter_params.append(f"year < {year_lt}")
        if year_lte != None:
            filter_params.append(f"year <= {year_lte}")

        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("priceSymbol", symbol))
        filter_params.append(list_to_filter("unitName", units))
        filter_params.append(list_to_filter("categoryName", category))
        filter_params.append(list_to_filter("groupName", group))
        filter_params.append(list_to_filter("currencySymbol", currency))
        filter_params.append(list_to_filter("commodityName", commodity))
        filter_params.append(list_to_filter("deliveryRegionName", delivery_region))
        filter_params.append(list_to_filter("sectorName", sector))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {
            "filter": filter_exp,
            "page": page,
            "pageSize": page_size,
            "PriceCategoryCode": category_id,
            "ModifiedDate": modified_date,
        }
        return get_data(
            path=f"{self._path}{path}",
            params=params,
            paginate=paginate,
            raw=raw,
            paginate_fn=self._paginate,
            df_fn=self._to_df,
        )

    def get_reference_data(
        self, type: RefTypes, raw: bool = False
    ) -> Union[Response, DataFrame]:
        """
        Fetch reference data for the Energy Price Forecast dataset.

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
        >>> ci.EnergyPriceForecast().get_reference_data(type=ci.EnergyPriceForecast.RefTypes.Sectors)
        """
        path = type.value

        params = {"pageSize": 1000, "page": 1}

        return get_data(
            path=f"{self._path}{path}",
            params=params,
            paginate=True,
            raw=raw,
        )

    def get_archive_dates(
        self,
        term_type: Literal["short", "long"],
        category: Optional[Union[list[str], "Series[str]", str]],
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Get archive dates to use as a `modified_date` when calling the `short-term-archive` or `long-term-archive` methods.

        Parameters
        ----------
        term_type : Literal[short, long]
            whether to get short-term or long-term forecast dates.
        category : Optional[Union[list[str], Series[str], str]]
            filter on the categoryName
        raw : bool, optional
            return a ``requests.Response`` instead of a ``DataFrame, by default False

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
        >>> ci.EnergyPriceForecast().get_archive_dates(term_type="short", category="Oil")
        """
        path = f"{term_type}-term-archive-dates"

        filter_params: list[str] = []

        filter_params.append(list_to_filter("categoryName", category))

        filter_params = [fp for fp in filter_params if fp != ""]

        params = {
            "filter": filter_params,
            "pageSize": 1000,
            "page": 1,
            "groupBy": "modifiedDate, categoryId, categoryName",
        }
        return get_data(
            path=f"{self._path}{path}",
            params=params,
            raw=raw,
            paginate=True,
            paginate_fn=self._paginate,
            df_fn=self._to_df,
        )
