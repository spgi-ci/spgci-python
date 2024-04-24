
from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date
import pandas as pd

class Chemicals_supply_demand:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _capacity_w_names_v_endpoint = "/capacity"
    _production_w_names_v_endpoint = "production"
    _capacity_utilization_w_names_v_endpoint = "capacity-utilization"
    _demand_by_derivative_or_application_w_names_rest_v_endpoint = "demand-by-derivative"
    _demand_by_end_use_w_names_rest_v_endpoint = "demand-by-end-use"
    _trade_w_names_v_endpoint = "trade"
    _inventory_change_w_names_v_endpoint = "/inventory-change"
    _total_supply_with_names_v_endpoint = "total-supply"
    _total_demand_with_names_v_endpoint = "total-demand"


    def get_capacity(
        self, scenario_id: Optional[Union[list[str], Series[str], str]] = None, scenario_description: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, production_route: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, region: Optional[Union[list[str], Series[str], str]] = None, concept: Optional[Union[list[str], Series[str], str]] = None, date: Optional[Union[list[date], Series[date], date]] = None, value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, data_type: Optional[Union[list[str], Series[str], str]] = None, valid_to: Optional[Union[list[date], Series[date], date]] = None, valid_from: Optional[Union[list[date], Series[date], date]] = None, is_active: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         scenario_id: Optional[Union[list[str], Series[str], str]]
             Scenario ID, be default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), be default None
         production_route: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, be default None
         date: Optional[Union[list[date], Series[date], date]]
             Date, be default None
         value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), be default None
         valid_to: Optional[Union[list[date], Series[date], date]]
             End Date of Record Validity, be default None
         valid_from: Optional[Union[list[date], Series[date], date]]
             As of date for when the data is updated, be default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario_id", scenario_id))
        filter_params.append(list_to_filter("scenario_description", scenario_description))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("production_route", production_route))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("data_type", data_type))
        filter_params.append(list_to_filter("valid_to", valid_to))
        filter_params.append(list_to_filter("valid_from", valid_from))
        filter_params.append(list_to_filter("is_active", is_active))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/capacity",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def getproduction(
        self, scenario_id: Optional[Union[list[str], Series[str], str]] = None, scenario_description: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, production_route: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, region: Optional[Union[list[str], Series[str], str]] = None, concept: Optional[Union[list[str], Series[str], str]] = None, date: Optional[Union[list[date], Series[date], date]] = None, value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, data_type: Optional[Union[list[str], Series[str], str]] = None, valid_to: Optional[Union[list[date], Series[date], date]] = None, valid_from: Optional[Union[list[date], Series[date], date]] = None, is_active: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         scenario_id: Optional[Union[list[str], Series[str], str]]
             Scenario ID, be default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), be default None
         production_route: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, be default None
         date: Optional[Union[list[date], Series[date], date]]
             Date, be default None
         value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), be default None
         valid_to: Optional[Union[list[date], Series[date], date]]
             End Date of Record Validity, be default None
         valid_from: Optional[Union[list[date], Series[date], date]]
             As of date for when the data is updated, be default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario_id", scenario_id))
        filter_params.append(list_to_filter("scenario_description", scenario_description))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("production_route", production_route))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("data_type", data_type))
        filter_params.append(list_to_filter("valid_to", valid_to))
        filter_params.append(list_to_filter("valid_from", valid_from))
        filter_params.append(list_to_filter("is_active", is_active))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicalsproduction",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def getcapacity_utilization(
        self, scenario_id: Optional[Union[list[str], Series[str], str]] = None, scenario_description: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, production_route: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, region: Optional[Union[list[str], Series[str], str]] = None, concept: Optional[Union[list[str], Series[str], str]] = None, date: Optional[Union[list[date], Series[date], date]] = None, value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, data_type: Optional[Union[list[str], Series[str], str]] = None, valid_to: Optional[Union[list[date], Series[date], date]] = None, valid_from: Optional[Union[list[date], Series[date], date]] = None, is_active: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         scenario_id: Optional[Union[list[str], Series[str], str]]
             Scenario ID, be default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), be default None
         production_route: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, be default None
         date: Optional[Union[list[date], Series[date], date]]
             Date, be default None
         value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), be default None
         valid_to: Optional[Union[list[date], Series[date], date]]
             End Date of Record Validity, be default None
         valid_from: Optional[Union[list[date], Series[date], date]]
             As of date for when the data is updated, be default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario_id", scenario_id))
        filter_params.append(list_to_filter("scenario_description", scenario_description))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("production_route", production_route))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("data_type", data_type))
        filter_params.append(list_to_filter("valid_to", valid_to))
        filter_params.append(list_to_filter("valid_from", valid_from))
        filter_params.append(list_to_filter("is_active", is_active))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicalscapacity-utilization",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def getdemand_by_derivative(
        self, scenario_id: Optional[Union[list[str], Series[str], str]] = None, scenario_description: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, region: Optional[Union[list[str], Series[str], str]] = None, concept: Optional[Union[list[str], Series[str], str]] = None, date: Optional[Union[list[date], Series[date], date]] = None, value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, data_type: Optional[Union[list[str], Series[str], str]] = None, valid_to: Optional[Union[list[date], Series[date], date]] = None, valid_from: Optional[Union[list[date], Series[date], date]] = None, is_active: Optional[Union[list[str], Series[str], str]] = None, application: Optional[Union[list[str], Series[str], str]] = None, derivative_product: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         scenario_id: Optional[Union[list[str], Series[str], str]]
             Scenario ID, be default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, be default None
         date: Optional[Union[list[date], Series[date], date]]
             Date, be default None
         value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), be default None
         valid_to: Optional[Union[list[date], Series[date], date]]
             End Date of Record Validity, be default None
         valid_from: Optional[Union[list[date], Series[date], date]]
             As of date for when the data is updated, be default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, be default None
         application: Optional[Union[list[str], Series[str], str]]
             Product(chemical commodity) Application, be default None
         derivative_product: Optional[Union[list[str], Series[str], str]]
             Derivative Product (chemical commodity), be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario_id", scenario_id))
        filter_params.append(list_to_filter("scenario_description", scenario_description))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("data_type", data_type))
        filter_params.append(list_to_filter("valid_to", valid_to))
        filter_params.append(list_to_filter("valid_from", valid_from))
        filter_params.append(list_to_filter("is_active", is_active))
        filter_params.append(list_to_filter("application", application))
        filter_params.append(list_to_filter("derivative_product", derivative_product))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicalsdemand-by-derivative",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def getdemand_by_end_use(
        self, scenario_id: Optional[Union[list[str], Series[str], str]] = None, scenario_description: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, region: Optional[Union[list[str], Series[str], str]] = None, concept: Optional[Union[list[str], Series[str], str]] = None, date: Optional[Union[list[date], Series[date], date]] = None, value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, data_type: Optional[Union[list[str], Series[str], str]] = None, valid_to: Optional[Union[list[date], Series[date], date]] = None, valid_from: Optional[Union[list[date], Series[date], date]] = None, is_active: Optional[Union[list[str], Series[str], str]] = None, end_use: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         scenario_id: Optional[Union[list[str], Series[str], str]]
             Scenario ID, be default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, be default None
         date: Optional[Union[list[date], Series[date], date]]
             Date, be default None
         value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), be default None
         valid_to: Optional[Union[list[date], Series[date], date]]
             End Date of Record Validity, be default None
         valid_from: Optional[Union[list[date], Series[date], date]]
             As of date for when the data is updated, be default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, be default None
         end_use: Optional[Union[list[str], Series[str], str]]
             Product (chemical commodity) End Use, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario_id", scenario_id))
        filter_params.append(list_to_filter("scenario_description", scenario_description))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("data_type", data_type))
        filter_params.append(list_to_filter("valid_to", valid_to))
        filter_params.append(list_to_filter("valid_from", valid_from))
        filter_params.append(list_to_filter("is_active", is_active))
        filter_params.append(list_to_filter("end_use", end_use))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicalsdemand-by-end-use",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def gettrade(
        self, scenario_id: Optional[Union[list[str], Series[str], str]] = None, scenario_description: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, region: Optional[Union[list[str], Series[str], str]] = None, concept: Optional[Union[list[str], Series[str], str]] = None, date: Optional[Union[list[date], Series[date], date]] = None, value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, data_type: Optional[Union[list[str], Series[str], str]] = None, valid_to: Optional[Union[list[date], Series[date], date]] = None, valid_from: Optional[Union[list[date], Series[date], date]] = None, is_active: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         scenario_id: Optional[Union[list[str], Series[str], str]]
             Scenario ID, be default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, be default None
         date: Optional[Union[list[date], Series[date], date]]
             Date, be default None
         value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), be default None
         valid_to: Optional[Union[list[date], Series[date], date]]
             End Date of Record Validity, be default None
         valid_from: Optional[Union[list[date], Series[date], date]]
             As of date for when the data is updated, be default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario_id", scenario_id))
        filter_params.append(list_to_filter("scenario_description", scenario_description))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("data_type", data_type))
        filter_params.append(list_to_filter("valid_to", valid_to))
        filter_params.append(list_to_filter("valid_from", valid_from))
        filter_params.append(list_to_filter("is_active", is_active))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicalstrade",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_inventory_change(
        self, scenario_id: Optional[Union[list[str], Series[str], str]] = None, scenario_description: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, region: Optional[Union[list[str], Series[str], str]] = None, concept: Optional[Union[list[str], Series[str], str]] = None, date: Optional[Union[list[date], Series[date], date]] = None, value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, data_type: Optional[Union[list[str], Series[str], str]] = None, valid_to: Optional[Union[list[date], Series[date], date]] = None, valid_from: Optional[Union[list[date], Series[date], date]] = None, is_active: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         scenario_id: Optional[Union[list[str], Series[str], str]]
             Scenario ID, be default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, be default None
         date: Optional[Union[list[date], Series[date], date]]
             Date, be default None
         value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), be default None
         valid_to: Optional[Union[list[date], Series[date], date]]
             End Date of Record Validity, be default None
         valid_from: Optional[Union[list[date], Series[date], date]]
             As of date for when the data is updated, be default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario_id", scenario_id))
        filter_params.append(list_to_filter("scenario_description", scenario_description))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("data_type", data_type))
        filter_params.append(list_to_filter("valid_to", valid_to))
        filter_params.append(list_to_filter("valid_from", valid_from))
        filter_params.append(list_to_filter("is_active", is_active))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/inventory-change",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def gettotal_supply(
        self, scenario_id: Optional[Union[list[str], Series[str], str]] = None, scenario_description: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, region: Optional[Union[list[str], Series[str], str]] = None, concept: Optional[Union[list[str], Series[str], str]] = None, date: Optional[Union[list[date], Series[date], date]] = None, value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, data_type: Optional[Union[list[str], Series[str], str]] = None, valid_to: Optional[Union[list[date], Series[date], date]] = None, valid_from: Optional[Union[list[date], Series[date], date]] = None, is_active: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         scenario_id: Optional[Union[list[str], Series[str], str]]
             Scenario ID, be default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, be default None
         date: Optional[Union[list[date], Series[date], date]]
             Date, be default None
         value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), be default None
         valid_to: Optional[Union[list[date], Series[date], date]]
             End Date of Record Validity, be default None
         valid_from: Optional[Union[list[date], Series[date], date]]
             As of date for when the data is updated, be default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario_id", scenario_id))
        filter_params.append(list_to_filter("scenario_description", scenario_description))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("data_type", data_type))
        filter_params.append(list_to_filter("valid_to", valid_to))
        filter_params.append(list_to_filter("valid_from", valid_from))
        filter_params.append(list_to_filter("is_active", is_active))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicalstotal-supply",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def gettotal_demand(
        self, scenario_id: Optional[Union[list[str], Series[str], str]] = None, scenario_description: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, region: Optional[Union[list[str], Series[str], str]] = None, concept: Optional[Union[list[str], Series[str], str]] = None, date: Optional[Union[list[date], Series[date], date]] = None, value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, data_type: Optional[Union[list[str], Series[str], str]] = None, valid_to: Optional[Union[list[date], Series[date], date]] = None, valid_from: Optional[Union[list[date], Series[date], date]] = None, is_active: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         scenario_id: Optional[Union[list[str], Series[str], str]]
             Scenario ID, be default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), be default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), be default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, be default None
         date: Optional[Union[list[date], Series[date], date]]
             Date, be default None
         value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), be default None
         valid_to: Optional[Union[list[date], Series[date], date]]
             End Date of Record Validity, be default None
         valid_from: Optional[Union[list[date], Series[date], date]]
             As of date for when the data is updated, be default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario_id", scenario_id))
        filter_params.append(list_to_filter("scenario_description", scenario_description))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("data_type", data_type))
        filter_params.append(list_to_filter("valid_to", valid_to))
        filter_params.append(list_to_filter("valid_from", valid_from))
        filter_params.append(list_to_filter("is_active", is_active))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicalstotal-demand",
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

        if "valid_to" in df.columns:
            df["valid_to"] = pd.to_datetime(df["valid_to"])  # type: ignore

        if "valid_from" in df.columns:
            df["valid_from"] = pd.to_datetime(df["valid_from"])  # type: ignore
        return df
    
