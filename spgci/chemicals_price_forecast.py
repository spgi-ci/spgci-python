
from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date
import pandas as pd

class Chemicals_price_forecast:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _price_forecast_lt_endpoint = "/long-term-prices"
    _price_forecast_st_endpoint = "/short-term-prices"


    def get_long_term_prices(
        self, scenario_id: Optional[Union[list[str], Series[str], str]] = None, scenario_description: Optional[Union[list[str], Series[str], str]] = None, series_description: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, commodity_grade: Optional[Union[list[str], Series[str], str]] = None, associated_platts_symbol: Optional[Union[list[str], Series[str], str]] = None, delivery_region: Optional[Union[list[str], Series[str], str]] = None, shipping_terms: Optional[Union[list[str], Series[str], str]] = None, currency: Optional[Union[list[str], Series[str], str]] = None, contract_type: Optional[Union[list[str], Series[str], str]] = None, concept: Optional[Union[list[str], Series[str], str]] = None, dataType: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, publish_date: Optional[Union[list[str], Series[str], str]] = None, year: Optional[Union[list[str], Series[str], str]] = None, valid_to: Optional[Union[list[str], Series[str], str]] = None, valid_from: Optional[Union[list[str], Series[str], str]] = None, is_active: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         scenario_id: Optional[Union[list[str], Series[str], str]]
             Scenario ID, be default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, be default None
         series_description: Optional[Union[list[str], Series[str], str]]
             Price Series Description, be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), be default None
         commodity_grade: Optional[Union[list[str], Series[str], str]]
             Commodity Grade, be default None
         associated_platts_symbol: Optional[Union[list[str], Series[str], str]]
             Associated Platts Symbol, be default None
         delivery_region: Optional[Union[list[str], Series[str], str]]
             Delivery Region, be default None
         shipping_terms: Optional[Union[list[str], Series[str], str]]
             Shipping Terms, be default None
         currency: Optional[Union[list[str], Series[str], str]]
             Currency, be default None
         contract_type: Optional[Union[list[str], Series[str], str]]
             Contract Type, be default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, be default None
         dataType: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), be default None
         value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         publish_date: Optional[Union[list[str], Series[str], str]]
             Publish Date, be default None
         year: Optional[Union[list[str], Series[str], str]]
             year, be default None
         valid_to: Optional[Union[list[str], Series[str], str]]
             End Date of Record Validity, be default None
         valid_from: Optional[Union[list[str], Series[str], str]]
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
        filter_params.append(list_to_filter("series_description", series_description))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("commodity_grade", commodity_grade))
        filter_params.append(list_to_filter("associated_platts_symbol", associated_platts_symbol))
        filter_params.append(list_to_filter("delivery_region", delivery_region))
        filter_params.append(list_to_filter("shipping_terms", shipping_terms))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("contract_type", contract_type))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("dataType", dataType))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("publish_date", publish_date))
        filter_params.append(list_to_filter("year", year))
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
            path=f"/analytics/v1/chemicals/price-forecast/long-term-prices",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_short_term_prices(
        self, scenario_id: Optional[Union[list[str], Series[str], str]] = None, scenario_description: Optional[Union[list[str], Series[str], str]] = None, series_description: Optional[Union[list[str], Series[str], str]] = None, commodity: Optional[Union[list[str], Series[str], str]] = None, commodity_grade: Optional[Union[list[str], Series[str], str]] = None, associated_platts_symbol: Optional[Union[list[str], Series[str], str]] = None, delivery_region: Optional[Union[list[str], Series[str], str]] = None, shipping_terms: Optional[Union[list[str], Series[str], str]] = None, currency: Optional[Union[list[str], Series[str], str]] = None, contract_type: Optional[Union[list[str], Series[str], str]] = None, concept: Optional[Union[list[str], Series[str], str]] = None, dataType: Optional[Union[list[str], Series[str], str]] = None, value: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, publish_date: Optional[Union[list[str], Series[str], str]] = None, date: Optional[Union[list[date], Series[date], date]] = None, valid_to: Optional[Union[list[str], Series[str], str]] = None, valid_from: Optional[Union[list[str], Series[str], str]] = None, is_active: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         scenario_id: Optional[Union[list[str], Series[str], str]]
             Scenario ID, be default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, be default None
         series_description: Optional[Union[list[str], Series[str], str]]
             Price Series Description, be default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), be default None
         commodity_grade: Optional[Union[list[str], Series[str], str]]
             Commodity Grade, be default None
         associated_platts_symbol: Optional[Union[list[str], Series[str], str]]
             Associated Platts Symbol, be default None
         delivery_region: Optional[Union[list[str], Series[str], str]]
             Delivery Region, be default None
         shipping_terms: Optional[Union[list[str], Series[str], str]]
             Shipping Terms, be default None
         currency: Optional[Union[list[str], Series[str], str]]
             Currency, be default None
         contract_type: Optional[Union[list[str], Series[str], str]]
             Contract Type, be default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, be default None
         dataType: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), be default None
         value: Optional[Union[list[str], Series[str], str]]
             Data Value, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), be default None
         publish_date: Optional[Union[list[str], Series[str], str]]
             Publish Date, be default None
         date: Optional[Union[list[date], Series[date], date]]
             year, be default None
         valid_to: Optional[Union[list[str], Series[str], str]]
             End Date of Record Validity, be default None
         valid_from: Optional[Union[list[str], Series[str], str]]
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
        filter_params.append(list_to_filter("series_description", series_description))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("commodity_grade", commodity_grade))
        filter_params.append(list_to_filter("associated_platts_symbol", associated_platts_symbol))
        filter_params.append(list_to_filter("delivery_region", delivery_region))
        filter_params.append(list_to_filter("shipping_terms", shipping_terms))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("contract_type", contract_type))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("dataType", dataType))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("publish_date", publish_date))
        filter_params.append(list_to_filter("date", date))
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
            path=f"/analytics/v1/chemicals/price-forecast/short-term-prices",
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
        
        if "publish_date" in df.columns:
            df["publish_date"] = pd.to_datetime(df["publish_date"])  # type: ignore

        if "valid_to" in df.columns:
            df["valid_to"] = pd.to_datetime(df["valid_to"])  # type: ignore

        if "valid_from" in df.columns:
            df["valid_from"] = pd.to_datetime(df["valid_from"])  # type: ignore

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])  # type: ignore
        return df
    
