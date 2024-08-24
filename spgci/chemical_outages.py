
from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date
import pandas as pd

class Chemical_outages:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _petchem_outages_endpoint = "/"


    def get_data(
        self, unit_name: Optional[Union[list[str], Series[str], str]] = None, productionUnitCode: Optional[Union[list[str], Series[str], str]] = None, alertStatus: Optional[Union[list[str], Series[str], str]] = None, outage_id: Optional[Union[list[str], Series[str], str]] = None, plant_code: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, region: Optional[Union[list[str], Series[str], str]] = None, owner: Optional[Union[list[str], Series[str], str]] = None, outage_type: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, capacity: Optional[Union[list[str], Series[str], str]] = None, capacity_down: Optional[Union[list[str], Series[str], str]] = None, runRate: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, start_date: Optional[Union[list[str], Series[str], str]] = None, end_date: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         unit_name: Optional[Union[list[str], Series[str], str]]
             Name for Production Unit, be default None
         productionUnitCode: Optional[Union[list[str], Series[str], str]]
             Production Unit ID (Asset ID), be default None
         alertStatus: Optional[Union[list[str], Series[str], str]]
             Alert Status (like Alert, Confirmed, Estimate, Revised Confirmed), be default None
         outage_id: Optional[Union[list[str], Series[str], str]]
             Outage ID, be default None
         plant_code: Optional[Union[list[str], Series[str], str]]
             Plant ID, be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         owner: Optional[Union[list[str], Series[str], str]]
             Plant operator (producer), be default None
         outage_type: Optional[Union[list[str], Series[str], str]]
             Outage Type (like Planned, Unplanned, Economic Run Cut etc), be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         capacity: Optional[Union[list[str], Series[str], str]]
             Capacity Value, be default None
         capacity_down: Optional[Union[list[str], Series[str], str]]
             Capacity Loss, be default None
         runRate: Optional[Union[list[str], Series[str], str]]
             Run Rate, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the data is last modified, be default None
         start_date: Optional[Union[list[str], Series[str], str]]
             Start Date, be default None
         end_date: Optional[Union[list[str], Series[str], str]]
             End Date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("unit_name", unit_name))
        filter_params.append(list_to_filter("productionUnitCode", productionUnitCode))
        filter_params.append(list_to_filter("alertStatus", alertStatus))
        filter_params.append(list_to_filter("outage_id", outage_id))
        filter_params.append(list_to_filter("plant_code", plant_code))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("owner", owner))
        filter_params.append(list_to_filter("outage_type", outage_type))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("capacity", capacity))
        filter_params.append(list_to_filter("capacity_down", capacity_down))
        filter_params.append(list_to_filter("runRate", runRate))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        filter_params.append(list_to_filter("start_date", start_date))
        filter_params.append(list_to_filter("end_date", end_date))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/assets/outages/",
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
        
        if "modifiedDate" in df.columns:
            df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])  # type: ignore

        if "start_date" in df.columns:
            df["start_date"] = pd.to_datetime(df["start_date"])  # type: ignore

        if "end_date" in df.columns:
            df["end_date"] = pd.to_datetime(df["end_date"])  # type: ignore
        return df
    
