
from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date
import pandas as pd

class Lng_market_fundamentals:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _gas_balances_endpoint = "/gas-balances"
    _gas_demand_endpoint = "/gas-demand"
    _gas_reserves_endpoint = "/gas-reserves"
    _gas_sales_endpoint = "/gas-sales"
    _power_capacity_endpoint = "/power-capacity"
    _power_generation_endpoint = "/power-generation"
    _storage_endpoint = "/storage"
    _prices_endpoint = "/prices"
    _pipeline_endpoint = "/pipeline"
    _fuel_use_endpoint = "/fuel-use"


    def get_gas_balances(
        self, source: Optional[Union[list[str], Series[str], str]] = None, market: Optional[Union[list[str], Series[str], str]] = None, periodType: Optional[Union[list[str], Series[str], str]] = None, period: Optional[Union[list[date], Series[date], date]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, category: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., be default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., be default None
         periodType: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., be default None
         period: Optional[Union[list[date], Series[date], date]]
             The date that the data refers to. The period’s date will be defined by the Period Type., be default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., be default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., be default None
         value: Optional[Union[list[str], Series[str], str]]
             The specific value for the data., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Gas Balances record latest modified date., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", periodType))
        filter_params.append(list_to_filter("period", period))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
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
        self, source: Optional[Union[list[str], Series[str], str]] = None, market: Optional[Union[list[str], Series[str], str]] = None, periodType: Optional[Union[list[str], Series[str], str]] = None, period: Optional[Union[list[date], Series[date], date]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, category: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., be default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., be default None
         periodType: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., be default None
         period: Optional[Union[list[date], Series[date], date]]
             The date that the data refers to. The period’s date will be defined by the Period Type., be default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., be default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., be default None
         value: Optional[Union[list[str], Series[str], str]]
             The specific value for the data., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Gas Demand record latest modified date., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", periodType))
        filter_params.append(list_to_filter("period", period))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
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
        self, source: Optional[Union[list[str], Series[str], str]] = None, market: Optional[Union[list[str], Series[str], str]] = None, periodType: Optional[Union[list[str], Series[str], str]] = None, period: Optional[Union[list[date], Series[date], date]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, category: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., be default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., be default None
         periodType: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., be default None
         period: Optional[Union[list[date], Series[date], date]]
             The date that the data refers to. The period’s date will be defined by the Period Type., be default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., be default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., be default None
         value: Optional[Union[list[str], Series[str], str]]
             The specific value for the data., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Gas Reserves record latest modified date., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", periodType))
        filter_params.append(list_to_filter("period", period))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
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
        self, source: Optional[Union[list[str], Series[str], str]] = None, market: Optional[Union[list[str], Series[str], str]] = None, periodType: Optional[Union[list[str], Series[str], str]] = None, period: Optional[Union[list[date], Series[date], date]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, category: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., be default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., be default None
         periodType: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., be default None
         period: Optional[Union[list[date], Series[date], date]]
             The date that the data refers to. The period’s date will be defined by the Period Type., be default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., be default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., be default None
         value: Optional[Union[list[str], Series[str], str]]
             The specific value for the data., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Gas Sales record latest modified date., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", periodType))
        filter_params.append(list_to_filter("period", period))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
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
        self, source: Optional[Union[list[str], Series[str], str]] = None, market: Optional[Union[list[str], Series[str], str]] = None, periodType: Optional[Union[list[str], Series[str], str]] = None, period: Optional[Union[list[date], Series[date], date]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, category: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., be default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., be default None
         periodType: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., be default None
         period: Optional[Union[list[date], Series[date], date]]
             The date that the data refers to. The period’s date will be defined by the Period Type., be default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., be default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., be default None
         value: Optional[Union[list[str], Series[str], str]]
             The specific value for the data., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Power Capacity record latest modified date., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", periodType))
        filter_params.append(list_to_filter("period", period))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
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
        self, source: Optional[Union[list[str], Series[str], str]] = None, market: Optional[Union[list[str], Series[str], str]] = None, periodType: Optional[Union[list[str], Series[str], str]] = None, period: Optional[Union[list[date], Series[date], date]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, category: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., be default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., be default None
         periodType: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., be default None
         period: Optional[Union[list[date], Series[date], date]]
             The date that the data refers to. The period’s date will be defined by the Period Type., be default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., be default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., be default None
         value: Optional[Union[list[str], Series[str], str]]
             The specific value for the data., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Power Generation record latest modified date., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", periodType))
        filter_params.append(list_to_filter("period", period))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
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
        self, source: Optional[Union[list[str], Series[str], str]] = None, market: Optional[Union[list[str], Series[str], str]] = None, periodType: Optional[Union[list[str], Series[str], str]] = None, period: Optional[Union[list[date], Series[date], date]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, category: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., be default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., be default None
         periodType: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., be default None
         period: Optional[Union[list[date], Series[date], date]]
             The date that the data refers to. The period’s date will be defined by the Period Type., be default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., be default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., be default None
         value: Optional[Union[list[str], Series[str], str]]
             The specific value for the data., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Storage record latest modified date., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", periodType))
        filter_params.append(list_to_filter("period", period))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
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
        self, source: Optional[Union[list[str], Series[str], str]] = None, market: Optional[Union[list[str], Series[str], str]] = None, periodType: Optional[Union[list[str], Series[str], str]] = None, period: Optional[Union[list[date], Series[date], date]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, category: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., be default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., be default None
         periodType: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., be default None
         period: Optional[Union[list[date], Series[date], date]]
             The date that the data refers to. The period’s date will be defined by the Period Type., be default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., be default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., be default None
         value: Optional[Union[list[str], Series[str], str]]
             The specific value for the data., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Prices record latest modified date., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", periodType))
        filter_params.append(list_to_filter("period", period))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
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
        self, source: Optional[Union[list[str], Series[str], str]] = None, market: Optional[Union[list[str], Series[str], str]] = None, periodType: Optional[Union[list[str], Series[str], str]] = None, period: Optional[Union[list[date], Series[date], date]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, category: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., be default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., be default None
         periodType: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., be default None
         period: Optional[Union[list[date], Series[date], date]]
             The date that the data refers to. The period’s date will be defined by the Period Type., be default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., be default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., be default None
         value: Optional[Union[list[str], Series[str], str]]
             The specific value for the data., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Pipelines record latest modified date., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", periodType))
        filter_params.append(list_to_filter("period", period))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
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
        self, source: Optional[Union[list[str], Series[str], str]] = None, market: Optional[Union[list[str], Series[str], str]] = None, periodType: Optional[Union[list[str], Series[str], str]] = None, period: Optional[Union[list[date], Series[date], date]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, category: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         source: Optional[Union[list[str], Series[str], str]]
             A generalized description of the type of data. This needs to be analyzed in conjunction with the other fields. It is used to avoid confusion in analyzing similar datasets., be default None
         market: Optional[Union[list[str], Series[str], str]]
             The geography that the data refers to., be default None
         periodType: Optional[Union[list[str], Series[str], str]]
             The period type that the data refers to. For example, the data could be in terms of year, quarter, month, or day., be default None
         period: Optional[Union[list[date], Series[date], date]]
             The date that the data refers to. The period’s date will be defined by the Period Type., be default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., be default None
         category: Optional[Union[list[str], Series[str], str]]
             The specific category or grouping for the data., be default None
         value: Optional[Union[list[str], Series[str], str]]
             The specific value for the data., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Fuel Use record latest modified date., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("periodType", periodType))
        filter_params.append(list_to_filter("period", period))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
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


    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
        j = resp.json()
        df = pd.json_normalize(j["results"])  # type: ignore
        
        if "period" in df.columns:
            df["period"] = pd.to_datetime(df["period"])  # type: ignore

        if "modifiedDate" in df.columns:
            df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])  # type: ignore
        return df
    
