
from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date
import pandas as pd

class Lng_global:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _tender_details_endpoint = "/tenders"
    _v_current_fc_demand_endpoint = "/demand-forecast/current"
    _v_current_fc_supply_endpoint = "/supply-forecast/current"
    _v_platts_demand_endpoint = "/demand-forecast/history"
    _v_platts_supply_endpoint = "/supply-forecast/history"


    def get_tenders(
        self, tender_id: Optional[Union[list[str], Series[str], str]] = None, awardee_id: Optional[Union[list[str], Series[str], str]] = None, issued_by: Optional[Union[list[str], Series[str], str]] = None, tender_status: Optional[Union[list[str], Series[str], str]] = None, cargo_type: Optional[Union[list[str], Series[str], str]] = None, contract_type: Optional[Union[list[str], Series[str], str]] = None, contract_option: Optional[Union[list[str], Series[str], str]] = None, size_of_tender: Optional[Union[list[str], Series[str], str]] = None, number_of_cargoes: Optional[Union[list[str], Series[str], str]] = None, volume: Optional[Union[list[str], Series[str], str]] = None, price_marker: Optional[Union[list[str], Series[str], str]] = None, price: Optional[Union[list[str], Series[str], str]] = None, country_name: Optional[Union[list[str], Series[str], str]] = None, awardee_company: Optional[Union[list[str], Series[str], str]] = None, opening_date: Optional[Union[list[date], Series[date], date]] = None, closing_date: Optional[Union[list[date], Series[date], date]] = None, validity_date: Optional[Union[list[date], Series[date], date]] = None, lifting_delivery_period_from: Optional[Union[list[date], Series[date], date]] = None, lifting_delivery_period_to: Optional[Union[list[date], Series[date], date]] = None, loading_period: Optional[Union[list[str], Series[str], str]] = None, external_notes: Optional[Union[list[str], Series[str], str]] = None, result: Optional[Union[list[str], Series[str], str]] = None, last_modified_date: Optional[Union[list[date], Series[date], date]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         tender_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier of tender, be default None
         awardee_id: Optional[Union[list[str], Series[str], str]]
             Distinct identifier for winning a bid in tenders process, be default None
         issued_by: Optional[Union[list[str], Series[str], str]]
             Name of company that issued the Tender, be default None
         tender_status: Optional[Union[list[str], Series[str], str]]
             Whether a tender has been bought or sold, be default None
         cargo_type: Optional[Union[list[str], Series[str], str]]
             Type of cargo offered (Mid Term/ Multiple/ Partial/ Single cargo), be default None
         contract_type: Optional[Union[list[str], Series[str], str]]
             LNG delivery type (FOB or DES), be default None
         contract_option: Optional[Union[list[str], Series[str], str]]
             Buy or sell, be default None
         size_of_tender: Optional[Union[list[str], Series[str], str]]
             Cargo or Volume, be default None
         number_of_cargoes: Optional[Union[list[str], Series[str], str]]
             Count of Cargos for each tender, be default None
         volume: Optional[Union[list[str], Series[str], str]]
             Volume of Cargos for each tender, be default None
         price_marker: Optional[Union[list[str], Series[str], str]]
             Price Marker that the Tender is connected, be default None
         price: Optional[Union[list[str], Series[str], str]]
             Tender's price, be default None
         country_name: Optional[Union[list[str], Series[str], str]]
             Country where the cargo is available, be default None
         awardee_company: Optional[Union[list[str], Series[str], str]]
             Company/ies that the Tender was awarded, be default None
         opening_date: Optional[Union[list[date], Series[date], date]]
             Tender opening date, be default None
         closing_date: Optional[Union[list[date], Series[date], date]]
             Tender closing date, be default None
         validity_date: Optional[Union[list[date], Series[date], date]]
             When the tender is awarded, be default None
         lifting_delivery_period_from: Optional[Union[list[date], Series[date], date]]
             Tender period start date, be default None
         lifting_delivery_period_to: Optional[Union[list[date], Series[date], date]]
             Tender period end date, be default None
         loading_period: Optional[Union[list[str], Series[str], str]]
             At lifting/delivery or average over period, be default None
         external_notes: Optional[Union[list[str], Series[str], str]]
             Any other notes around the tender, be default None
         result: Optional[Union[list[str], Series[str], str]]
             The results of the tender, be default None
         last_modified_date: Optional[Union[list[date], Series[date], date]]
             The latest date of modification for the tenders, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("tender_id", tender_id))
        filter_params.append(list_to_filter("awardee_id", awardee_id))
        filter_params.append(list_to_filter("issued_by", issued_by))
        filter_params.append(list_to_filter("tender_status", tender_status))
        filter_params.append(list_to_filter("cargo_type", cargo_type))
        filter_params.append(list_to_filter("contract_type", contract_type))
        filter_params.append(list_to_filter("contract_option", contract_option))
        filter_params.append(list_to_filter("size_of_tender", size_of_tender))
        filter_params.append(list_to_filter("number_of_cargoes", number_of_cargoes))
        filter_params.append(list_to_filter("volume", volume))
        filter_params.append(list_to_filter("price_marker", price_marker))
        filter_params.append(list_to_filter("price", price))
        filter_params.append(list_to_filter("country_name", country_name))
        filter_params.append(list_to_filter("awardee_company", awardee_company))
        filter_params.append(list_to_filter("opening_date", opening_date))
        filter_params.append(list_to_filter("closing_date", closing_date))
        filter_params.append(list_to_filter("validity_date", validity_date))
        filter_params.append(list_to_filter("lifting_delivery_period_from", lifting_delivery_period_from))
        filter_params.append(list_to_filter("lifting_delivery_period_to", lifting_delivery_period_to))
        filter_params.append(list_to_filter("loading_period", loading_period))
        filter_params.append(list_to_filter("external_notes", external_notes))
        filter_params.append(list_to_filter("result", result))
        filter_params.append(list_to_filter("last_modified_date", last_modified_date))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/tenders",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_demand_forecast_current(
        self, import_market: Optional[Union[list[str], Series[str], str]] = None, month: Optional[Union[list[date], Series[date], date]] = None, demand_million_metric_tons: Optional[Union[list[str], Series[str], str]] = None, point_in_time_month: Optional[Union[list[date], Series[date], date]] = None, modified_date: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         import_market: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG is imported., be default None
         month: Optional[Union[list[date], Series[date], date]]
             A unit of time representing a period of approximately 30 days, be default None
         demand_million_metric_tons: Optional[Union[list[str], Series[str], str]]
             The quantity of LNG demand, typically in metric tons, required or requested for a specific period., be default None
         point_in_time_month: Optional[Union[list[date], Series[date], date]]
             A specific moment within a given month, often used for precise data or event references., be default None
         modified_date: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the current demand forecast, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("import_market", import_market))
        filter_params.append(list_to_filter("month", month))
        filter_params.append(list_to_filter("demand_million_metric_tons", demand_million_metric_tons))
        filter_params.append(list_to_filter("point_in_time_month", point_in_time_month))
        filter_params.append(list_to_filter("modified_date", modified_date))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/demand-forecast/current",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_supply_forecast_current(
        self, export_project: Optional[Union[list[str], Series[str], str]] = None, export_market: Optional[Union[list[str], Series[str], str]] = None, month: Optional[Union[list[date], Series[date], date]] = None, delivered_supply_million_metric_tons: Optional[Union[list[str], Series[str], str]] = None, point_in_time_month: Optional[Union[list[date], Series[date], date]] = None, modified_date: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         export_project: Optional[Union[list[str], Series[str], str]]
             A specific venture or initiative focused on exporting LNG supply  to international markets., be default None
         export_market: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG supply are being sold and shipped from a particular location., be default None
         month: Optional[Union[list[date], Series[date], date]]
             A unit of time representing a period of approximately 30 days, be default None
         delivered_supply_million_metric_tons: Optional[Union[list[str], Series[str], str]]
             The quantity of LNG supply , typically in metric tons, that has been delivered to its destination., be default None
         point_in_time_month: Optional[Union[list[date], Series[date], date]]
             A specific moment within a given month, often used for precise data or event references., be default None
         modified_date: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the current supply forecast, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("export_project", export_project))
        filter_params.append(list_to_filter("export_market", export_market))
        filter_params.append(list_to_filter("month", month))
        filter_params.append(list_to_filter("delivered_supply_million_metric_tons", delivered_supply_million_metric_tons))
        filter_params.append(list_to_filter("point_in_time_month", point_in_time_month))
        filter_params.append(list_to_filter("modified_date", modified_date))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/supply-forecast/current",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_demand_forecast_history(
        self, import_market: Optional[Union[list[str], Series[str], str]] = None, month: Optional[Union[list[date], Series[date], date]] = None, demand_million_metric_tons: Optional[Union[list[str], Series[str], str]] = None, point_in_time_month: Optional[Union[list[date], Series[date], date]] = None, modified_date: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         import_market: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG is imported., be default None
         month: Optional[Union[list[date], Series[date], date]]
             A unit of time representing a period of approximately 30 days, be default None
         demand_million_metric_tons: Optional[Union[list[str], Series[str], str]]
             The quantity of LNG demand, typically in metric tons, required or requested for a specific period., be default None
         point_in_time_month: Optional[Union[list[date], Series[date], date]]
             A specific moment within a given month, often used for precise data or event references., be default None
         modified_date: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the historical demand forecast, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("import_market", import_market))
        filter_params.append(list_to_filter("month", month))
        filter_params.append(list_to_filter("demand_million_metric_tons", demand_million_metric_tons))
        filter_params.append(list_to_filter("point_in_time_month", point_in_time_month))
        filter_params.append(list_to_filter("modified_date", modified_date))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/demand-forecast/history",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_supply_forecast_history(
        self, export_project: Optional[Union[list[str], Series[str], str]] = None, export_market: Optional[Union[list[str], Series[str], str]] = None, month: Optional[Union[list[date], Series[date], date]] = None, delivered_supply_million_metric_tons: Optional[Union[list[str], Series[str], str]] = None, point_in_time_month: Optional[Union[list[date], Series[date], date]] = None, modified_date: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         export_project: Optional[Union[list[str], Series[str], str]]
             A specific venture or initiative focused on exporting LNG supply  to international markets., be default None
         export_market: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG supply are being sold and shipped from a particular location., be default None
         month: Optional[Union[list[date], Series[date], date]]
             A unit of time representing a period of approximately 30 days., be default None
         delivered_supply_million_metric_tons: Optional[Union[list[str], Series[str], str]]
             The quantity of LNG supply , typically in metric tons, that has been delivered to its destination., be default None
         point_in_time_month: Optional[Union[list[date], Series[date], date]]
             A specific moment within a given month, often used for precise data or event references., be default None
         modified_date: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the historical supply forecast, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("export_project", export_project))
        filter_params.append(list_to_filter("export_market", export_market))
        filter_params.append(list_to_filter("month", month))
        filter_params.append(list_to_filter("delivered_supply_million_metric_tons", delivered_supply_million_metric_tons))
        filter_params.append(list_to_filter("point_in_time_month", point_in_time_month))
        filter_params.append(list_to_filter("modified_date", modified_date))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/supply-forecast/history",
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
        
        if "opening_date" in df.columns:
            df["opening_date"] = pd.to_datetime(df["opening_date"])  # type: ignore

        if "closing_date" in df.columns:
            df["closing_date"] = pd.to_datetime(df["closing_date"])  # type: ignore

        if "validity_date" in df.columns:
            df["validity_date"] = pd.to_datetime(df["validity_date"])  # type: ignore

        if "lifting_delivery_period_from" in df.columns:
            df["lifting_delivery_period_from"] = pd.to_datetime(df["lifting_delivery_period_from"])  # type: ignore

        if "lifting_delivery_period_to" in df.columns:
            df["lifting_delivery_period_to"] = pd.to_datetime(df["lifting_delivery_period_to"])  # type: ignore

        if "last_modified_date" in df.columns:
            df["last_modified_date"] = pd.to_datetime(df["last_modified_date"])  # type: ignore

        if "month" in df.columns:
            df["month"] = pd.to_datetime(df["month"])  # type: ignore

        if "point_in_time_month" in df.columns:
            df["point_in_time_month"] = pd.to_datetime(df["point_in_time_month"])  # type: ignore

        if "modified_date" in df.columns:
            df["modified_date"] = pd.to_datetime(df["modified_date"])  # type: ignore
        return df
    