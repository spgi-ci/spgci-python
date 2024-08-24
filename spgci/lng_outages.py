
from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date
import pandas as pd

class Lng_outages:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _vw_lng_outages_endpoint = "/"
    _ref_lng_liquefaction_projects_endpoint = "/liquefaction-projects"
    _ref_lng_liquefaction_trains_endpoint = "/liquefaction-trains"


    def get_data(
        self, liquefactionProjectId: Optional[Union[list[str], Series[str], str]] = None, liquefactionProjectName: Optional[Union[list[str], Series[str], str]] = None, liquefactionTrainId: Optional[Union[list[str], Series[str], str]] = None, liquefactionTrainName: Optional[Union[list[str], Series[str], str]] = None, alertId: Optional[Union[list[str], Series[str], str]] = None, alertName: Optional[Union[list[str], Series[str], str]] = None, statusId: Optional[Union[list[str], Series[str], str]] = None, statusName: Optional[Union[list[str], Series[str], str]] = None, confidenceLevelId: Optional[Union[list[str], Series[str], str]] = None, confidenceLevelName: Optional[Union[list[str], Series[str], str]] = None, reportDate: Optional[Union[list[str], Series[str], str]] = None, reportDateComment: Optional[Union[list[str], Series[str], str]] = None, startDate: Optional[Union[list[str], Series[str], str]] = None, startDateComment: Optional[Union[list[str], Series[str], str]] = None, endDate: Optional[Union[list[str], Series[str], str]] = None, endDateComment: Optional[Union[list[str], Series[str], str]] = None, createDate: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, totalCapacity: Optional[Union[list[str], Series[str], str]] = None, availableCapacity: Optional[Union[list[str], Series[str], str]] = None, offlineCapacity: Optional[Union[list[str], Series[str], str]] = None, offlineCapacityComment: Optional[Union[list[str], Series[str], str]] = None, runRate: Optional[Union[list[str], Series[str], str]] = None, runLoss: Optional[Union[list[str], Series[str], str]] = None, unitOfMeasure: Optional[Union[list[str], Series[str], str]] = None, generalComment: Optional[Union[list[str], Series[str], str]] = None, infrastructureType: Optional[Union[list[str], Series[str], str]] = None, commodityName: Optional[Union[list[str], Series[str], str]] = None, outageId: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         liquefactionProjectId: Optional[Union[list[str], Series[str], str]]
             A unique identifier for a liquefaction project, often used for tracking and categorizing purposes., be default None
         liquefactionProjectName: Optional[Union[list[str], Series[str], str]]
             The name of the liquefaction project., be default None
         liquefactionTrainId: Optional[Union[list[str], Series[str], str]]
             A unique identifier for a specific train within a liquefaction project, used for differentiation and categorization., be default None
         liquefactionTrainName: Optional[Union[list[str], Series[str], str]]
             The name assigned to a specific train within a liquefaction project., be default None
         alertId: Optional[Union[list[str], Series[str], str]]
             A unique identifier associated with an alert or notification, serving as a reference point for identifying and managing alerts., be default None
         alertName: Optional[Union[list[str], Series[str], str]]
             The name or title of the alert, providing a concise description of the outages being reported., be default None
         statusId: Optional[Union[list[str], Series[str], str]]
             An identifier representing the current state or status of the outages in question., be default None
         statusName: Optional[Union[list[str], Series[str], str]]
             The descriptive name of the current status or state of the outages., be default None
         confidenceLevelId: Optional[Union[list[str], Series[str], str]]
             An identifier representing the confidence level or degree of certainty associated with the provided outages data., be default None
         confidenceLevelName: Optional[Union[list[str], Series[str], str]]
             The descriptive name of the confidence level associated with the data, indicating the reliability or certainty., be default None
         reportDate: Optional[Union[list[str], Series[str], str]]
             The date on which the report, alert, or information was generated or recorded., be default None
         reportDateComment: Optional[Union[list[str], Series[str], str]]
             Additional commentary or explanation related to the report date, providing context or details., be default None
         startDate: Optional[Union[list[str], Series[str], str]]
             The date marking the beginning of the LNG outage., be default None
         startDateComment: Optional[Union[list[str], Series[str], str]]
             Extra information or context related to the start date, explaining any relevant details., be default None
         endDate: Optional[Union[list[str], Series[str], str]]
             The date signifying the conclusion or end of a particular LNG outage., be default None
         endDateComment: Optional[Union[list[str], Series[str], str]]
             Supplementary notes or details regarding the end date, offering further insight., be default None
         createDate: Optional[Union[list[str], Series[str], str]]
             The initial outage capture date., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the outage., be default None
         totalCapacity: Optional[Union[list[str], Series[str], str]]
             The overall capacity or quantity associated with a certain measurement or parameter., be default None
         availableCapacity: Optional[Union[list[str], Series[str], str]]
             Available capacity is calculated by subtracting offline capacity from total capacity., be default None
         offlineCapacity: Optional[Union[list[str], Series[str], str]]
             The portion of total capacity that has been impacted by an outage., be default None
         offlineCapacityComment: Optional[Union[list[str], Series[str], str]]
             Comments regarding the impacted capacity value. , be default None
         runRate: Optional[Union[list[str], Series[str], str]]
             The rate or speed at which an outage is occurring or has occurred., be default None
         runLoss: Optional[Union[list[str], Series[str], str]]
             The reduction or loss in efficiency due to an outage., be default None
         unitOfMeasure: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used to quantify the values provided in the data., be default None
         generalComment: Optional[Union[list[str], Series[str], str]]
             A general comment or note providing additional information, context, or insights related to an outage., be default None
         infrastructureType: Optional[Union[list[str], Series[str], str]]
             Indicates the type or category of infrastructure associated with the outage., be default None
         commodityName: Optional[Union[list[str], Series[str], str]]
             The name of the specific commodity (e.g Liquefied Natural Gas, LNG)., be default None
         outageId: Optional[Union[list[str], Series[str], str]]
             A unique identifier assigned to a particular outage event, used for tracking and referencing purposes., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("liquefactionProjectId", liquefactionProjectId))
        filter_params.append(list_to_filter("liquefactionProjectName", liquefactionProjectName))
        filter_params.append(list_to_filter("liquefactionTrainId", liquefactionTrainId))
        filter_params.append(list_to_filter("liquefactionTrainName", liquefactionTrainName))
        filter_params.append(list_to_filter("alertId", alertId))
        filter_params.append(list_to_filter("alertName", alertName))
        filter_params.append(list_to_filter("statusId", statusId))
        filter_params.append(list_to_filter("statusName", statusName))
        filter_params.append(list_to_filter("confidenceLevelId", confidenceLevelId))
        filter_params.append(list_to_filter("confidenceLevelName", confidenceLevelName))
        filter_params.append(list_to_filter("reportDate", reportDate))
        filter_params.append(list_to_filter("reportDateComment", reportDateComment))
        filter_params.append(list_to_filter("startDate", startDate))
        filter_params.append(list_to_filter("startDateComment", startDateComment))
        filter_params.append(list_to_filter("endDate", endDate))
        filter_params.append(list_to_filter("endDateComment", endDateComment))
        filter_params.append(list_to_filter("createDate", createDate))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        filter_params.append(list_to_filter("totalCapacity", totalCapacity))
        filter_params.append(list_to_filter("availableCapacity", availableCapacity))
        filter_params.append(list_to_filter("offlineCapacity", offlineCapacity))
        filter_params.append(list_to_filter("offlineCapacityComment", offlineCapacityComment))
        filter_params.append(list_to_filter("runRate", runRate))
        filter_params.append(list_to_filter("runLoss", runLoss))
        filter_params.append(list_to_filter("unitOfMeasure", unitOfMeasure))
        filter_params.append(list_to_filter("generalComment", generalComment))
        filter_params.append(list_to_filter("infrastructureType", infrastructureType))
        filter_params.append(list_to_filter("commodityName", commodityName))
        filter_params.append(list_to_filter("outageId", outageId))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/outages/",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_liquefaction_projects(
        self, liquefactionProjectId: Optional[Union[list[str], Series[str], str]] = None, liquefactionProjectName: Optional[Union[list[str], Series[str], str]] = None, countryCoastName: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         liquefactionProjectId: Optional[Union[list[str], Series[str], str]]
             Unique LNG Liquefaction Project Id, be default None
         liquefactionProjectName: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction Project Name, be default None
         countryCoastName: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction country coast name, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("liquefactionProjectId", liquefactionProjectId))
        filter_params.append(list_to_filter("liquefactionProjectName", liquefactionProjectName))
        filter_params.append(list_to_filter("countryCoastName", countryCoastName))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/outages/liquefaction-projects",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_liquefaction_trains(
        self, liquefactionTrainId: Optional[Union[list[str], Series[str], str]] = None, liquefactionTrainName: Optional[Union[list[str], Series[str], str]] = None, liquefactionProjectId: Optional[Union[list[str], Series[str], str]] = None, liquefactionProjectName: Optional[Union[list[str], Series[str], str]] = None, initialCapacity: Optional[Union[list[str], Series[str], str]] = None, status: Optional[Union[list[str], Series[str], str]] = None, startDate: Optional[Union[list[str], Series[str], str]] = None, isGreenOrBrownField: Optional[Union[list[str], Series[str], str]] = None, liquefactionTechnologyType: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         liquefactionTrainId: Optional[Union[list[str], Series[str], str]]
             Unique LNG Liquefaction Train Id, be default None
         liquefactionTrainName: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction Train Name, be default None
         liquefactionProjectId: Optional[Union[list[str], Series[str], str]]
             Unique LNG Liquefaction Project Id, be default None
         liquefactionProjectName: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction Project Name, be default None
         initialCapacity: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction initial capacity of the train, be default None
         status: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction status of the train, be default None
         startDate: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction start date, be default None
         isGreenOrBrownField: Optional[Union[list[str], Series[str], str]]
             Is green or brown Field, be default None
         liquefactionTechnologyType: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction technology type, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("liquefactionTrainId", liquefactionTrainId))
        filter_params.append(list_to_filter("liquefactionTrainName", liquefactionTrainName))
        filter_params.append(list_to_filter("liquefactionProjectId", liquefactionProjectId))
        filter_params.append(list_to_filter("liquefactionProjectName", liquefactionProjectName))
        filter_params.append(list_to_filter("initialCapacity", initialCapacity))
        filter_params.append(list_to_filter("status", status))
        filter_params.append(list_to_filter("startDate", startDate))
        filter_params.append(list_to_filter("isGreenOrBrownField", isGreenOrBrownField))
        filter_params.append(list_to_filter("liquefactionTechnologyType", liquefactionTechnologyType))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/outages/liquefaction-trains",
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
        
        if "reportDate" in df.columns:
            df["reportDate"] = pd.to_datetime(df["reportDate"])  # type: ignore

        if "startDate" in df.columns:
            df["startDate"] = pd.to_datetime(df["startDate"])  # type: ignore

        if "endDate" in df.columns:
            df["endDate"] = pd.to_datetime(df["endDate"])  # type: ignore

        if "createDate" in df.columns:
            df["createDate"] = pd.to_datetime(df["createDate"])  # type: ignore

        if "modifiedDate" in df.columns:
            df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])  # type: ignore
        return df
    
