
from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date
import pandas as pd

class Lng_global_plus:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _netbacks_endpoint = "/"


    def get_data(
        self, date: Optional[Union[list[date], Series[date], date]] = None, exportGeography: Optional[Union[list[str], Series[str], str]] = None, importGeography: Optional[Union[list[str], Series[str], str]] = None, netback: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         date: Optional[Union[list[date], Series[date], date]]
             The day for which the netback calculation is attributed to, be default None
         exportGeography: Optional[Union[list[str], Series[str], str]]
             Geography where the LNG is exported from, be default None
         importGeography: Optional[Union[list[str], Series[str], str]]
             Geography where the LNG is imported to, be default None
         netback: Optional[Union[list[str], Series[str], str]]
             Price to the supplier after accounting for the 5-day moving average end market price and five-day moving average shipping cost based on the specified supply geography and import geography, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for netbacks, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("date", date))
        filter_params.append(list_to_filter("exportGeography", exportGeography))
        filter_params.append(list_to_filter("importGeography", importGeography))
        filter_params.append(list_to_filter("netback", netback))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/netbacks/",
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
        
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])  # type: ignore

        if "modifiedDate" in df.columns:
            df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])  # type: ignore
        return df
    
