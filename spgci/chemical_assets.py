
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
    _ts_owner_capacity_event_endpoint = "/capacity-events"
    _ts_owner_avg_annual_capacity_endpoint = "/average-annual-capacities"
    _ts_owner_capacity_consume_endpoint = "/capacity-to-consume"


    def get_capacity_events(
        self, productionUnitCode: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, productionRoute: Optional[Union[list[str], Series[str], str]] = None, plantCode: Optional[Union[list[str], Series[str], str]] = None, plantName: Optional[Union[list[str], Series[str], str]] = None, unitName: Optional[Union[list[str], Series[str], str]] = None, city: Optional[Union[list[str], Series[str], str]] = None, state: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, region: Optional[Union[list[str], Series[str], str]] = None, eventBeginDate: Optional[Union[list[date], Series[date], date]] = None, eventType: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, owner: Optional[Union[list[str], Series[str], str]] = None, ownershipPeriod: Optional[Union[list[str], Series[str], str]] = None, validFrom: Optional[Union[list[str], Series[str], str]] = None, validTo: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, reason: Optional[Union[list[str], Series[str], str]] = None, isActive: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         productionUnitCode: Optional[Union[list[str], Series[str], str]]
             Production Unit ID (Asset ID), be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity) , be default None
         productionRoute: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, be default None
         plantCode: Optional[Union[list[str], Series[str], str]]
             Plant ID, be default None
         plantName: Optional[Union[list[str], Series[str], str]]
             Name for Plant, be default None
         unitName: Optional[Union[list[str], Series[str], str]]
             Name for Production Unit, be default None
         city: Optional[Union[list[str], Series[str], str]]
             Name for City / Settlement (geography), be default None
         state: Optional[Union[list[str], Series[str], str]]
             Name for State or province (geography), be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         eventBeginDate: Optional[Union[list[date], Series[date], date]]
             Date of Event, be default None
         eventType: Optional[Union[list[str], Series[str], str]]
             Event Type (like Expand, Reduce, Startup, Shutdown, Restart etc.), be default None
         value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         owner: Optional[Union[list[str], Series[str], str]]
             Plant operator (producer), be default None
         ownershipPeriod: Optional[Union[list[str], Series[str], str]]
             The period a plant operator (producer) owns the facility, be default None
         validFrom: Optional[Union[list[str], Series[str], str]]
             As of date for when the data is updated, be default None
         validTo: Optional[Union[list[str], Series[str], str]]
             End Date of Record Validity, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the data is last modified, be default None
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
        filter_params.append(list_to_filter("productionUnitCode", productionUnitCode))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("productionRoute", productionRoute))
        filter_params.append(list_to_filter("plantCode", plantCode))
        filter_params.append(list_to_filter("plantName", plantName))
        filter_params.append(list_to_filter("unitName", unitName))
        filter_params.append(list_to_filter("city", city))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("eventBeginDate", eventBeginDate))
        filter_params.append(list_to_filter("eventType", eventType))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("owner", owner))
        filter_params.append(list_to_filter("ownershipPeriod", ownershipPeriod))
        filter_params.append(list_to_filter("validFrom", validFrom))
        filter_params.append(list_to_filter("validTo", validTo))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
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
        self, productionUnitCode: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, productionRoute: Optional[Union[list[str], Series[str], str]] = None, plantCode: Optional[Union[list[str], Series[str], str]] = None, plantName: Optional[Union[list[str], Series[str], str]] = None, unitName: Optional[Union[list[str], Series[str], str]] = None, city: Optional[Union[list[str], Series[str], str]] = None, state: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, region: Optional[Union[list[str], Series[str], str]] = None, year: Optional[Union[list[str], Series[str], str]] = None, averageAnnualCapacity: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, owner: Optional[Union[list[str], Series[str], str]] = None, ownershipPeriod: Optional[Union[list[str], Series[str], str]] = None, validFrom: Optional[Union[list[str], Series[str], str]] = None, validTo: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, reason: Optional[Union[list[str], Series[str], str]] = None, isActive: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         productionUnitCode: Optional[Union[list[str], Series[str], str]]
             Production Unit ID (Asset ID), be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity) , be default None
         productionRoute: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, be default None
         plantCode: Optional[Union[list[str], Series[str], str]]
             Plant ID, be default None
         plantName: Optional[Union[list[str], Series[str], str]]
             Name for Plant, be default None
         unitName: Optional[Union[list[str], Series[str], str]]
             Name for Production Unit, be default None
         city: Optional[Union[list[str], Series[str], str]]
             Name for City / Settlement (geography), be default None
         state: Optional[Union[list[str], Series[str], str]]
             Name for State or province (geography), be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         year: Optional[Union[list[str], Series[str], str]]
             Date of Data value, be default None
         averageAnnualCapacity: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         owner: Optional[Union[list[str], Series[str], str]]
             Plant operator (producer), be default None
         ownershipPeriod: Optional[Union[list[str], Series[str], str]]
             The period a plant operator (producer) owns the facility, be default None
         validFrom: Optional[Union[list[str], Series[str], str]]
             As of date for when the data is updated, be default None
         validTo: Optional[Union[list[str], Series[str], str]]
             End Date of Record Validity, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the data is last modified, be default None
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
        filter_params.append(list_to_filter("productionUnitCode", productionUnitCode))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("productionRoute", productionRoute))
        filter_params.append(list_to_filter("plantCode", plantCode))
        filter_params.append(list_to_filter("plantName", plantName))
        filter_params.append(list_to_filter("unitName", unitName))
        filter_params.append(list_to_filter("city", city))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("averageAnnualCapacity", averageAnnualCapacity))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("owner", owner))
        filter_params.append(list_to_filter("ownershipPeriod", ownershipPeriod))
        filter_params.append(list_to_filter("validFrom", validFrom))
        filter_params.append(list_to_filter("validTo", validTo))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
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


    def get_capacity_to_consume(
        self, productionUnitCode: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, productionRoute: Optional[Union[list[str], Series[str], str]] = None, plantCode: Optional[Union[list[str], Series[str], str]] = None, plantName: Optional[Union[list[str], Series[str], str]] = None, unitName: Optional[Union[list[str], Series[str], str]] = None, city: Optional[Union[list[str], Series[str], str]] = None, state: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, region: Optional[Union[list[str], Series[str], str]] = None, concept: Optional[Union[list[str], Series[str], str]] = None, feedstock: Optional[Union[list[str], Series[str], str]] = None, year: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, owner: Optional[Union[list[str], Series[str], str]] = None, ownershipPeriod: Optional[Union[list[str], Series[str], str]] = None, validFrom: Optional[Union[list[str], Series[str], str]] = None, validTo: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, reason: Optional[Union[list[str], Series[str], str]] = None, isActive: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         productionUnitCode: Optional[Union[list[str], Series[str], str]]
             Production Unit ID (Asset ID), be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity) , be default None
         productionRoute: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, be default None
         plantCode: Optional[Union[list[str], Series[str], str]]
             Plant ID, be default None
         plantName: Optional[Union[list[str], Series[str], str]]
             Name for Plant, be default None
         unitName: Optional[Union[list[str], Series[str], str]]
             Name for Production Unit, be default None
         city: Optional[Union[list[str], Series[str], str]]
             Name for City / Settlement (geography), be default None
         state: Optional[Union[list[str], Series[str], str]]
             Name for State or province (geography), be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, be default None
         feedstock: Optional[Union[list[str], Series[str], str]]
             Raw material used in chemical production, be default None
         year: Optional[Union[list[str], Series[str], str]]
             Date of Data value, be default None
         value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         owner: Optional[Union[list[str], Series[str], str]]
             Plant operator (producer), be default None
         ownershipPeriod: Optional[Union[list[str], Series[str], str]]
             The period a plant operator (producer) owns the facility, be default None
         validFrom: Optional[Union[list[str], Series[str], str]]
             As of date for when the data is updated, be default None
         validTo: Optional[Union[list[str], Series[str], str]]
             End Date of Record Validity, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the data is last modified, be default None
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
        filter_params.append(list_to_filter("productionUnitCode", productionUnitCode))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("productionRoute", productionRoute))
        filter_params.append(list_to_filter("plantCode", plantCode))
        filter_params.append(list_to_filter("plantName", plantName))
        filter_params.append(list_to_filter("unitName", unitName))
        filter_params.append(list_to_filter("city", city))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("feedstock", feedstock))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("owner", owner))
        filter_params.append(list_to_filter("ownershipPeriod", ownershipPeriod))
        filter_params.append(list_to_filter("validFrom", validFrom))
        filter_params.append(list_to_filter("validTo", validTo))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        filter_params.append(list_to_filter("reason", reason))
        filter_params.append(list_to_filter("isActive", isActive))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/assets/capacity-to-consume",
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
        
        if "eventBeginDate" in df.columns:
            df["eventBeginDate"] = pd.to_datetime(df["eventBeginDate"])  # type: ignore

        if "validFrom" in df.columns:
            df["validFrom"] = pd.to_datetime(df["validFrom"])  # type: ignore

        if "validTo" in df.columns:
            df["validTo"] = pd.to_datetime(df["validTo"])  # type: ignore

        if "modifiedDate" in df.columns:
            df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])  # type: ignore
        return df
    
