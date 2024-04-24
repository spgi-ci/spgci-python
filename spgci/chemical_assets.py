
from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date
import pandas as pd

class Chemical_assets:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _capacity_event_endpoint = "/capacity-events"
    _avg_annual_capacity_endpoint = "/average-annual-capacities"


    def get_capacity_events(
        self, ProductionUnitCode: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, ProductionRoute: Optional[Union[list[str], Series[str], str]] = None, PlantCode: Optional[Union[list[str], Series[str], str]] = None, PlantName: Optional[Union[list[str], Series[str], str]] = None, UnitName: Optional[Union[list[str], Series[str], str]] = None, City: Optional[Union[list[str], Series[str], str]] = None, State: Optional[Union[list[str], Series[str], str]] = None, Country: Optional[Union[list[str], Series[str], str]] = None, Region: Optional[Union[list[str], Series[str], str]] = None, EventBeginDate: Optional[Union[list[date], Series[date], date]] = None, EventType: Optional[Union[list[str], Series[str], str]] = None, Value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, currentOwner: Optional[Union[list[str], Series[str], str]] = None, validFrom: Optional[Union[list[date], Series[date], date]] = None, validTo: Optional[Union[list[date], Series[date], date]] = None, reason: Optional[Union[list[str], Series[str], str]] = None, isActive: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         ProductionUnitCode: Optional[Union[list[str], Series[str], str]]
             Production Unit ID (Asset ID), be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity) , be default None
         ProductionRoute: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, be default None
         PlantCode: Optional[Union[list[str], Series[str], str]]
             Plant ID, be default None
         PlantName: Optional[Union[list[str], Series[str], str]]
             Name for Plant, be default None
         UnitName: Optional[Union[list[str], Series[str], str]]
             Name for Production Unit, be default None
         City: Optional[Union[list[str], Series[str], str]]
             Name for City / Settlement (geography), be default None
         State: Optional[Union[list[str], Series[str], str]]
             Name for State or province (geography), be default None
         Country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         Region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         EventBeginDate: Optional[Union[list[date], Series[date], date]]
             Date of Event, be default None
         EventType: Optional[Union[list[str], Series[str], str]]
             Event Type (like Expand, Reduce, Startup, Shutdown, Restart etc.), be default None
         Value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         currentOwner: Optional[Union[list[str], Series[str], str]]
             Current Plant operator (producer), be default None
         validFrom: Optional[Union[list[date], Series[date], date]]
             As of date for when the data is updated, be default None
         validTo: Optional[Union[list[date], Series[date], date]]
             End Date of Record Validity, be default None
         reason: Optional[Union[list[str], Series[str], str]]
             Reason for having this record, be default None
         isActive: Optional[Union[list[str], Series[str], str]]
             If the record is active, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("ProductionUnitCode", ProductionUnitCode))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("ProductionRoute", ProductionRoute))
        filter_params.append(list_to_filter("PlantCode", PlantCode))
        filter_params.append(list_to_filter("PlantName", PlantName))
        filter_params.append(list_to_filter("UnitName", UnitName))
        filter_params.append(list_to_filter("City", City))
        filter_params.append(list_to_filter("State", State))
        filter_params.append(list_to_filter("Country", Country))
        filter_params.append(list_to_filter("Region", Region))
        filter_params.append(list_to_filter("EventBeginDate", EventBeginDate))
        filter_params.append(list_to_filter("EventType", EventType))
        filter_params.append(list_to_filter("Value", Value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("currentOwner", currentOwner))
        filter_params.append(list_to_filter("validFrom", validFrom))
        filter_params.append(list_to_filter("validTo", validTo))
        filter_params.append(list_to_filter("reason", reason))
        filter_params.append(list_to_filter("isActive", isActive))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/assets/capacity-events",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_average_annual_capacities(
        self, ProductionUnitCode: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, productionRoute: Optional[Union[list[str], Series[str], str]] = None, PlantCode: Optional[Union[list[str], Series[str], str]] = None, PlantName: Optional[Union[list[str], Series[str], str]] = None, UnitName: Optional[Union[list[str], Series[str], str]] = None, City: Optional[Union[list[str], Series[str], str]] = None, State: Optional[Union[list[str], Series[str], str]] = None, Country: Optional[Union[list[str], Series[str], str]] = None, Region: Optional[Union[list[str], Series[str], str]] = None, year: Optional[Union[list[str], Series[str], str]] = None, Average_Annual_Capacity: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, currentOwner: Optional[Union[list[str], Series[str], str]] = None, validFrom: Optional[Union[list[date], Series[date], date]] = None, validTo: Optional[Union[list[date], Series[date], date]] = None, reason: Optional[Union[list[str], Series[str], str]] = None, isActive: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         ProductionUnitCode: Optional[Union[list[str], Series[str], str]]
             Production Unit ID (Asset ID), be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity) , be default None
         productionRoute: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, be default None
         PlantCode: Optional[Union[list[str], Series[str], str]]
             Plant ID, be default None
         PlantName: Optional[Union[list[str], Series[str], str]]
             Name for Plant, be default None
         UnitName: Optional[Union[list[str], Series[str], str]]
             Name for Production Unit, be default None
         City: Optional[Union[list[str], Series[str], str]]
             Name for City / Settlement (geography), be default None
         State: Optional[Union[list[str], Series[str], str]]
             Name for State or province (geography), be default None
         Country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         Region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         year: Optional[Union[list[str], Series[str], str]]
             Date of Data value, be default None
         Average_Annual_Capacity: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         currentOwner: Optional[Union[list[str], Series[str], str]]
             Current Plant operator (producer), be default None
         validFrom: Optional[Union[list[date], Series[date], date]]
             As of date for when the data is updated, be default None
         validTo: Optional[Union[list[date], Series[date], date]]
             End Date of Record Validity, be default None
         reason: Optional[Union[list[str], Series[str], str]]
             Reason for having this record, be default None
         isActive: Optional[Union[list[str], Series[str], str]]
             If the record is active, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("ProductionUnitCode", ProductionUnitCode))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("productionRoute", productionRoute))
        filter_params.append(list_to_filter("PlantCode", PlantCode))
        filter_params.append(list_to_filter("PlantName", PlantName))
        filter_params.append(list_to_filter("UnitName", UnitName))
        filter_params.append(list_to_filter("City", City))
        filter_params.append(list_to_filter("State", State))
        filter_params.append(list_to_filter("Country", Country))
        filter_params.append(list_to_filter("Region", Region))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("Average_Annual_Capacity", Average_Annual_Capacity))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("currentOwner", currentOwner))
        filter_params.append(list_to_filter("validFrom", validFrom))
        filter_params.append(list_to_filter("validTo", validTo))
        filter_params.append(list_to_filter("reason", reason))
        filter_params.append(list_to_filter("isActive", isActive))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/assets/average-annual-capacities",
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
        
        if "EventBeginDate" in df.columns:
            df["EventBeginDate"] = pd.to_datetime(df["EventBeginDate"])  # type: ignore

        if "validFrom" in df.columns:
            df["validFrom"] = pd.to_datetime(df["validFrom"])  # type: ignore

        if "validTo" in df.columns:
            df["validTo"] = pd.to_datetime(df["validTo"])  # type: ignore
        return df
    
