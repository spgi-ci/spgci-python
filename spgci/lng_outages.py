
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
        self, liquefaction_project_id: Optional[Union[list[str], Series[str], str]] = None, liquefaction_project_name: Optional[Union[list[str], Series[str], str]] = None, liquefaction_train_id: Optional[Union[list[str], Series[str], str]] = None, liquefaction_train_name: Optional[Union[list[str], Series[str], str]] = None, alert_id: Optional[Union[list[str], Series[str], str]] = None, alert_name: Optional[Union[list[str], Series[str], str]] = None, status_id: Optional[Union[list[str], Series[str], str]] = None, status_name: Optional[Union[list[str], Series[str], str]] = None, confidence_level_id: Optional[Union[list[str], Series[str], str]] = None, confidence_level_name: Optional[Union[list[str], Series[str], str]] = None, report_date: Optional[Union[list[str], Series[str], str]] = None, report_date_comment: Optional[Union[list[str], Series[str], str]] = None, start_date: Optional[Union[list[str], Series[str], str]] = None, start_date_comment: Optional[Union[list[str], Series[str], str]] = None, end_date: Optional[Union[list[str], Series[str], str]] = None, end_date_comment: Optional[Union[list[str], Series[str], str]] = None, create_date: Optional[Union[list[str], Series[str], str]] = None, modified_date: Optional[Union[list[str], Series[str], str]] = None, total_capacity: Optional[Union[list[str], Series[str], str]] = None, available_capacity: Optional[Union[list[str], Series[str], str]] = None, offline_capacity: Optional[Union[list[str], Series[str], str]] = None, offline_capacity_comment: Optional[Union[list[str], Series[str], str]] = None, run_rate: Optional[Union[list[str], Series[str], str]] = None, run_loss: Optional[Union[list[str], Series[str], str]] = None, unit_of_measure: Optional[Union[list[str], Series[str], str]] = None, general_comment: Optional[Union[list[str], Series[str], str]] = None, infrastructure_type: Optional[Union[list[str], Series[str], str]] = None, commodity_name: Optional[Union[list[str], Series[str], str]] = None, outage_id: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         liquefaction_project_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier for a liquefaction project, often used for tracking and categorizing purposes., be default None
         liquefaction_project_name: Optional[Union[list[str], Series[str], str]]
             The name of the liquefaction project., be default None
         liquefaction_train_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier for a specific train within a liquefaction project, used for differentiation and categorization., be default None
         liquefaction_train_name: Optional[Union[list[str], Series[str], str]]
             The name assigned to a specific train within a liquefaction project., be default None
         alert_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier associated with an alert or notification, serving as a reference point for identifying and managing alerts., be default None
         alert_name: Optional[Union[list[str], Series[str], str]]
             The name or title of the alert, providing a concise description of the outages being reported., be default None
         status_id: Optional[Union[list[str], Series[str], str]]
             An identifier representing the current state or status of the outages in question., be default None
         status_name: Optional[Union[list[str], Series[str], str]]
             The descriptive name of the current status or state of the outages., be default None
         confidence_level_id: Optional[Union[list[str], Series[str], str]]
             An identifier representing the confidence level or degree of certainty associated with the provided outages data., be default None
         confidence_level_name: Optional[Union[list[str], Series[str], str]]
             The descriptive name of the confidence level associated with the data, indicating the reliability or certainty., be default None
         report_date: Optional[Union[list[str], Series[str], str]]
             The date on which the report, alert, or information was generated or recorded., be default None
         report_date_comment: Optional[Union[list[str], Series[str], str]]
             Additional commentary or explanation related to the report date, providing context or details., be default None
         start_date: Optional[Union[list[str], Series[str], str]]
             The date marking the beginning of the LNG outage., be default None
         start_date_comment: Optional[Union[list[str], Series[str], str]]
             Extra information or context related to the start date, explaining any relevant details., be default None
         end_date: Optional[Union[list[str], Series[str], str]]
             The date signifying the conclusion or end of a particular LNG outage., be default None
         end_date_comment: Optional[Union[list[str], Series[str], str]]
             Supplementary notes or details regarding the end date, offering further insight., be default None
         create_date: Optional[Union[list[str], Series[str], str]]
             The initial outage capture date., be default None
         modified_date: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the outage., be default None
         total_capacity: Optional[Union[list[str], Series[str], str]]
             The overall capacity or quantity associated with a certain measurement or parameter., be default None
         available_capacity: Optional[Union[list[str], Series[str], str]]
             Available capacity is calculated by subtracting offline capacity from total capacity., be default None
         offline_capacity: Optional[Union[list[str], Series[str], str]]
             The portion of total capacity that has been impacted by an outage., be default None
         offline_capacity_comment: Optional[Union[list[str], Series[str], str]]
             Comments regarding the impacted capacity value. , be default None
         run_rate: Optional[Union[list[str], Series[str], str]]
             The rate or speed at which an outage is occurring or has occurred., be default None
         run_loss: Optional[Union[list[str], Series[str], str]]
             The reduction or loss in efficiency due to an outage., be default None
         unit_of_measure: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used to quantify the values provided in the data., be default None
         general_comment: Optional[Union[list[str], Series[str], str]]
             A general comment or note providing additional information, context, or insights related to an outage., be default None
         infrastructure_type: Optional[Union[list[str], Series[str], str]]
             Indicates the type or category of infrastructure associated with the outage., be default None
         commodity_name: Optional[Union[list[str], Series[str], str]]
             The name of the specific commodity (e.g Liquefied Natural Gas, LNG)., be default None
         outage_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier assigned to a particular outage event, used for tracking and referencing purposes., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("liquefaction_project_id", liquefaction_project_id))
        filter_params.append(list_to_filter("liquefaction_project_name", liquefaction_project_name))
        filter_params.append(list_to_filter("liquefaction_train_id", liquefaction_train_id))
        filter_params.append(list_to_filter("liquefaction_train_name", liquefaction_train_name))
        filter_params.append(list_to_filter("alert_id", alert_id))
        filter_params.append(list_to_filter("alert_name", alert_name))
        filter_params.append(list_to_filter("status_id", status_id))
        filter_params.append(list_to_filter("status_name", status_name))
        filter_params.append(list_to_filter("confidence_level_id", confidence_level_id))
        filter_params.append(list_to_filter("confidence_level_name", confidence_level_name))
        filter_params.append(list_to_filter("report_date", report_date))
        filter_params.append(list_to_filter("report_date_comment", report_date_comment))
        filter_params.append(list_to_filter("start_date", start_date))
        filter_params.append(list_to_filter("start_date_comment", start_date_comment))
        filter_params.append(list_to_filter("end_date", end_date))
        filter_params.append(list_to_filter("end_date_comment", end_date_comment))
        filter_params.append(list_to_filter("create_date", create_date))
        filter_params.append(list_to_filter("modified_date", modified_date))
        filter_params.append(list_to_filter("total_capacity", total_capacity))
        filter_params.append(list_to_filter("available_capacity", available_capacity))
        filter_params.append(list_to_filter("offline_capacity", offline_capacity))
        filter_params.append(list_to_filter("offline_capacity_comment", offline_capacity_comment))
        filter_params.append(list_to_filter("run_rate", run_rate))
        filter_params.append(list_to_filter("run_loss", run_loss))
        filter_params.append(list_to_filter("unit_of_measure", unit_of_measure))
        filter_params.append(list_to_filter("general_comment", general_comment))
        filter_params.append(list_to_filter("infrastructure_type", infrastructure_type))
        filter_params.append(list_to_filter("commodity_name", commodity_name))
        filter_params.append(list_to_filter("outage_id", outage_id))
        
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
        self, liquefaction_project_id: Optional[Union[list[str], Series[str], str]] = None, liquefaction_project_name: Optional[Union[list[str], Series[str], str]] = None, country_coast_name: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         liquefaction_project_id: Optional[Union[list[str], Series[str], str]]
             Unique LNG Liquefaction Project Id, be default None
         liquefaction_project_name: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction Project Name, be default None
         country_coast_name: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction country coast name, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("liquefaction_project_id", liquefaction_project_id))
        filter_params.append(list_to_filter("liquefaction_project_name", liquefaction_project_name))
        filter_params.append(list_to_filter("country_coast_name", country_coast_name))
        
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
        self, liquefaction_train_id: Optional[Union[list[str], Series[str], str]] = None, liquefaction_train_name: Optional[Union[list[str], Series[str], str]] = None, liquefaction_project_id: Optional[Union[list[str], Series[str], str]] = None, liquefaction_project_name: Optional[Union[list[str], Series[str], str]] = None, initial_capacity: Optional[Union[list[str], Series[str], str]] = None, status: Optional[Union[list[str], Series[str], str]] = None, start_date: Optional[Union[list[str], Series[str], str]] = None, greenfield_or_brownfield: Optional[Union[list[str], Series[str], str]] = None, liquefaction_technology_type: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         liquefaction_train_id: Optional[Union[list[str], Series[str], str]]
             Unique LNG Liquefaction Train Id, be default None
         liquefaction_train_name: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction Train Name, be default None
         liquefaction_project_id: Optional[Union[list[str], Series[str], str]]
             Unique LNG Liquefaction Project Id, be default None
         liquefaction_project_name: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction Project Name, be default None
         initial_capacity: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction initial capacity of the train, be default None
         status: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction status of the train, be default None
         start_date: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction start date, be default None
         greenfield_or_brownfield: Optional[Union[list[str], Series[str], str]]
             Is green or brown Field, be default None
         liquefaction_technology_type: Optional[Union[list[str], Series[str], str]]
             LNG Liquefaction technology type, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("liquefaction_train_id", liquefaction_train_id))
        filter_params.append(list_to_filter("liquefaction_train_name", liquefaction_train_name))
        filter_params.append(list_to_filter("liquefaction_project_id", liquefaction_project_id))
        filter_params.append(list_to_filter("liquefaction_project_name", liquefaction_project_name))
        filter_params.append(list_to_filter("initial_capacity", initial_capacity))
        filter_params.append(list_to_filter("status", status))
        filter_params.append(list_to_filter("start_date", start_date))
        filter_params.append(list_to_filter("greenfield_or_brownfield", greenfield_or_brownfield))
        filter_params.append(list_to_filter("liquefaction_technology_type", liquefaction_technology_type))
        
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
        
        if "report_date" in df.columns:
            df["report_date"] = pd.to_datetime(df["report_date"])  # type: ignore

        if "start_date" in df.columns:
            df["start_date"] = pd.to_datetime(df["start_date"])  # type: ignore

        if "end_date" in df.columns:
            df["end_date"] = pd.to_datetime(df["end_date"])  # type: ignore

        if "create_date" in df.columns:
            df["create_date"] = pd.to_datetime(df["create_date"])  # type: ignore

        if "modified_date" in df.columns:
            df["modified_date"] = pd.to_datetime(df["modified_date"])  # type: ignore
        return df
    