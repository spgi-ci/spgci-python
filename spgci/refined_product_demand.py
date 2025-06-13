from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date, datetime
import pandas as pd


class RefinedProductDemand:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _product_demand_v_endpoint = "/demand"
    _product_demand_latest_v_endpoint = "/demand/latest"

    def get_demand(
        self,
        *,
        sector: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        product_type: Optional[Union[list[str], Series[str], str]] = None,
        outlook_horizon: Optional[Union[list[str], Series[str], str]] = None,
        series_name: Optional[Union[list[str], Series[str], str]] = None,
        from_region: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        frequency: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        vintage_date: Optional[date] = None,
        vintage_date_lt: Optional[date] = None,
        vintage_date_lte: Optional[date] = None,
        vintage_date_gt: Optional[date] = None,
        vintage_date_gte: Optional[date] = None,
        report_for_date: Optional[date] = None,
        report_for_date_lt: Optional[date] = None,
        report_for_date_lte: Optional[date] = None,
        report_for_date_gt: Optional[date] = None,
        report_for_date_gte: Optional[date] = None,
        historical_edge_date: Optional[date] = None,
        historical_edge_date_lt: Optional[date] = None,
        historical_edge_date_lte: Optional[date] = None,
        historical_edge_date_gt: Optional[date] = None,
        historical_edge_date_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides Short-Term and Long-Term Refined Products' Demand Forecast, covering demand by sectors and countries.

        Parameters
        ----------

         sector: Optional[Union[list[str], Series[str], str]]
             Sector name, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             The products on which we do assessments or products closely associated with our assessed products., by default None
         product_type: Optional[Union[list[str], Series[str], str]]
             The refined product (for eg. Gasoline, Diesel Fuel) for which demand forecast is provided., by default None
         outlook_horizon: Optional[Union[list[str], Series[str], str]]
             An indicator to determine the time frame of the oil market outlook., by default None
         series_name: Optional[Union[list[str], Series[str], str]]
             The brief description of the information represented in the data series., by default None
         from_region: Optional[Union[list[str], Series[str], str]]
             The name of the geographic area or location from which the commodities originate., by default None
         region: Optional[Union[list[str], Series[str], str]]
             The name of an area, country division, or the world with characteristics defined by either physical boundaries or human-defined borders., by default None
         country: Optional[Union[list[str], Series[str], str]]
             The name of the country for a particular data series., by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Reference column to describe the metrics being reported in the column Value., by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             The indicator of the data granularity within the time continuum., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit or units in which the value of the commodity is measured., by default None
         vintage_date: Optional[date], optional
             The specific date on which a final forecast revision was issued (Series version label)., by default None
         vintage_date_gt: Optional[date], optional
             filter by `vintage_date > x`, by default None
         vintage_date_gte: Optional[date], optional
             filter by `vintage_date >= x`, by default None
         vintage_date_lt: Optional[date], optional
             filter by `vintage_date < x`, by default None
         vintage_date_lte: Optional[date], optional
             filter by `vintage_date <= x`, by default None
         report_for_date: Optional[date], optional
             The date for which the record applies within the data table, this can be a historical or forecast date., by default None
         report_for_date_gt: Optional[date], optional
             filter by `report_for_date > x`, by default None
         report_for_date_gte: Optional[date], optional
             filter by `report_for_date >= x`, by default None
         report_for_date_lt: Optional[date], optional
             filter by `report_for_date < x`, by default None
         report_for_date_lte: Optional[date], optional
             filter by `report_for_date <= x`, by default None
         historical_edge_date: Optional[date], optional
             The date on which the historical data ends and the forecast data begins., by default None
         historical_edge_date_gt: Optional[date], optional
             filter by `historical_edge_date > x`, by default None
         historical_edge_date_gte: Optional[date], optional
             filter by `historical_edge_date >= x`, by default None
         historical_edge_date_lt: Optional[date], optional
             filter by `historical_edge_date < x`, by default None
         historical_edge_date_lte: Optional[date], optional
             filter by `historical_edge_date <= x`, by default None
         modified_date: Optional[datetime], optional
             The specific date on which a particular data point was last modified., by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             An indicator if the data is active., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("sector", sector))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("productType", product_type))
        filter_params.append(list_to_filter("outlookHorizon", outlook_horizon))
        filter_params.append(list_to_filter("seriesName", series_name))
        filter_params.append(list_to_filter("fromRegion", from_region))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("vintageDate", vintage_date))
        if vintage_date_gt is not None:
            filter_params.append(f'vintageDate > "{vintage_date_gt}"')
        if vintage_date_gte is not None:
            filter_params.append(f'vintageDate >= "{vintage_date_gte}"')
        if vintage_date_lt is not None:
            filter_params.append(f'vintageDate < "{vintage_date_lt}"')
        if vintage_date_lte is not None:
            filter_params.append(f'vintageDate <= "{vintage_date_lte}"')
        filter_params.append(list_to_filter("reportForDate", report_for_date))
        if report_for_date_gt is not None:
            filter_params.append(f'reportForDate > "{report_for_date_gt}"')
        if report_for_date_gte is not None:
            filter_params.append(f'reportForDate >= "{report_for_date_gte}"')
        if report_for_date_lt is not None:
            filter_params.append(f'reportForDate < "{report_for_date_lt}"')
        if report_for_date_lte is not None:
            filter_params.append(f'reportForDate <= "{report_for_date_lte}"')
        filter_params.append(list_to_filter("historicalEdgeDate", historical_edge_date))
        if historical_edge_date_gt is not None:
            filter_params.append(f'historicalEdgeDate > "{historical_edge_date_gt}"')
        if historical_edge_date_gte is not None:
            filter_params.append(f'historicalEdgeDate >= "{historical_edge_date_gte}"')
        if historical_edge_date_lt is not None:
            filter_params.append(f'historicalEdgeDate < "{historical_edge_date_lt}"')
        if historical_edge_date_lte is not None:
            filter_params.append(f'historicalEdgeDate <= "{historical_edge_date_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')
        filter_params.append(list_to_filter("isActive", is_active))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/refined-product/v1/demand",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_demand_latest(
        self,
        *,
        sector: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        outlook_horizon: Optional[Union[list[str], Series[str], str]] = None,
        from_region: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        product_type: Optional[Union[list[str], Series[str], str]] = None,
        series_name: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        frequency: Optional[Union[list[str], Series[str], str]] = None,
        vintage_date: Optional[date] = None,
        vintage_date_lt: Optional[date] = None,
        vintage_date_lte: Optional[date] = None,
        vintage_date_gt: Optional[date] = None,
        vintage_date_gte: Optional[date] = None,
        report_for_date: Optional[date] = None,
        report_for_date_lt: Optional[date] = None,
        report_for_date_lte: Optional[date] = None,
        report_for_date_gt: Optional[date] = None,
        report_for_date_gte: Optional[date] = None,
        historical_edge_date: Optional[date] = None,
        historical_edge_date_lt: Optional[date] = None,
        historical_edge_date_lte: Optional[date] = None,
        historical_edge_date_gt: Optional[date] = None,
        historical_edge_date_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Provides Short-Term Refined Products' Demand, covering demand by sectors, along with House View Short-Term Refined Product Demand.

        Parameters
        ----------

         sector: Optional[Union[list[str], Series[str], str]]
             Sector name, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             The products on which we do assessments or products closely associated with our assessed products., by default None
         outlook_horizon: Optional[Union[list[str], Series[str], str]]
             An indicator to determine the time frame of the oil market outlook., by default None
         from_region: Optional[Union[list[str], Series[str], str]]
             The name of the geographic area or location from which the commodities originate., by default None
         region: Optional[Union[list[str], Series[str], str]]
             The name of an area, country division, or the world with characteristics defined by either physical boundaries or human-defined borders., by default None
         country: Optional[Union[list[str], Series[str], str]]
             The name of the country for a particular data series., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit or units in which the value of the commodity is measured., by default None
         product_type: Optional[Union[list[str], Series[str], str]]
             The refined product (for eg. Gasoline, Diesel Fuel) for which demand forecast is provided., by default None
         series_name: Optional[Union[list[str], Series[str], str]]
             The brief description of the information represented in the data series., by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Reference column to describe the metrics being reported in the column Value., by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             The indicator of the data granularity within the time continuum., by default None
         vintage_date: Optional[date], optional
             The specific date on which a final forecast revision was issued (Series version label)., by default None
         vintage_date_gt: Optional[date], optional
             filter by `vintage_date > x`, by default None
         vintage_date_gte: Optional[date], optional
             filter by `vintage_date >= x`, by default None
         vintage_date_lt: Optional[date], optional
             filter by `vintage_date < x`, by default None
         vintage_date_lte: Optional[date], optional
             filter by `vintage_date <= x`, by default None
         report_for_date: Optional[date], optional
             The date for which the record applies within the data table, this can be a historical or forecast date., by default None
         report_for_date_gt: Optional[date], optional
             filter by `report_for_date > x`, by default None
         report_for_date_gte: Optional[date], optional
             filter by `report_for_date >= x`, by default None
         report_for_date_lt: Optional[date], optional
             filter by `report_for_date < x`, by default None
         report_for_date_lte: Optional[date], optional
             filter by `report_for_date <= x`, by default None
         historical_edge_date: Optional[date], optional
             The date on which the historical data ends and the forecast data begins., by default None
         historical_edge_date_gt: Optional[date], optional
             filter by `historical_edge_date > x`, by default None
         historical_edge_date_gte: Optional[date], optional
             filter by `historical_edge_date >= x`, by default None
         historical_edge_date_lt: Optional[date], optional
             filter by `historical_edge_date < x`, by default None
         historical_edge_date_lte: Optional[date], optional
             filter by `historical_edge_date <= x`, by default None
         modified_date: Optional[datetime], optional
             The specific date on which a particular data point was last modified., by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             An indicator if the data is active., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("sector", sector))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("outlookHorizon", outlook_horizon))
        filter_params.append(list_to_filter("fromRegion", from_region))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("productType", product_type))
        filter_params.append(list_to_filter("seriesName", series_name))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("vintageDate", vintage_date))
        if vintage_date_gt is not None:
            filter_params.append(f'vintageDate > "{vintage_date_gt}"')
        if vintage_date_gte is not None:
            filter_params.append(f'vintageDate >= "{vintage_date_gte}"')
        if vintage_date_lt is not None:
            filter_params.append(f'vintageDate < "{vintage_date_lt}"')
        if vintage_date_lte is not None:
            filter_params.append(f'vintageDate <= "{vintage_date_lte}"')
        filter_params.append(list_to_filter("reportForDate", report_for_date))
        if report_for_date_gt is not None:
            filter_params.append(f'reportForDate > "{report_for_date_gt}"')
        if report_for_date_gte is not None:
            filter_params.append(f'reportForDate >= "{report_for_date_gte}"')
        if report_for_date_lt is not None:
            filter_params.append(f'reportForDate < "{report_for_date_lt}"')
        if report_for_date_lte is not None:
            filter_params.append(f'reportForDate <= "{report_for_date_lte}"')
        filter_params.append(list_to_filter("historicalEdgeDate", historical_edge_date))
        if historical_edge_date_gt is not None:
            filter_params.append(f'historicalEdgeDate > "{historical_edge_date_gt}"')
        if historical_edge_date_gte is not None:
            filter_params.append(f'historicalEdgeDate >= "{historical_edge_date_gte}"')
        if historical_edge_date_lt is not None:
            filter_params.append(f'historicalEdgeDate < "{historical_edge_date_lt}"')
        if historical_edge_date_lte is not None:
            filter_params.append(f'historicalEdgeDate <= "{historical_edge_date_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')
        filter_params.append(list_to_filter("isActive", is_active))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/refined-product/v1/demand/latest",
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

        if "vintageDate" in df.columns:
            df["vintageDate"] = pd.to_datetime(df["vintageDate"])  # type: ignore

        if "reportForDate" in df.columns:
            df["reportForDate"] = pd.to_datetime(df["reportForDate"])  # type: ignore

        if "historicalEdgeDate" in df.columns:
            df["historicalEdgeDate"] = pd.to_datetime(df["historicalEdgeDate"])  # type: ignore

        if "modifiedDate" in df.columns:
            df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])  # type: ignore
        return df