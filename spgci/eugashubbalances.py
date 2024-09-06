from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date, datetime
import pandas as pd


class Eugashubbalances:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _api_storage_facilities_selection_mv_endpoint = "/daily/storage-data-selection"
    _api_physical_flow_points_selection_mv_endpoint = (
        "/daily/physical-flow-point-selection"
    )
    _api_nominations_mv_endpoint = "/hourly/nomination"
    _api_instantaneous_flows_mv_endpoint = "/instant/flow"
    _api_hub_overview_mv_endpoint = "/overview/hub-balance"
    _api_hub_balances_daily_history_mv_endpoint = "/daily/history/hub-balance"
    _api_flow_points_selection_mv_endpoint = "/daily/flow-point-selection"
    _api_field_production_mv_endpoint = "/monthly/field-production"

    def get_daily_storage_data_selection(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        gas_day: Optional[date] = None,
        gas_day_lt: Optional[date] = None,
        gas_day_lte: Optional[date] = None,
        gas_day_gt: Optional[date] = None,
        gas_day_gte: Optional[date] = None,
        secondary_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        detailed_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        name: Optional[Union[list[str], Series[str], str]] = None,
        gas_type: Optional[Union[list[str], Series[str], str]] = None,
        source: Optional[Union[list[str], Series[str], str]] = None,
        summable: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        default_source: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access daily storage data for European Gas resources.

        Parameters
        ----------

         id: Optional[int], optional
             The unique identifier for the gas data field., by default None
         id_gt: Optional[int], optional
             filter by '' id > x '', by default None
         id_gte: Optional[int], optional
             filter by id, by default None
         id_lt: Optional[int], optional
             filter by id, by default None
         id_lte: Optional[int], optional
             filter by id, by default None
         gas_day: Optional[date], optional
             The date for which the gas data is recorded or applicable., by default None
         gas_day_gt: Optional[date], optional
             filter by '' gas_day > x '', by default None
         gas_day_gte: Optional[date], optional
             filter by gas_day, by default None
         gas_day_lt: Optional[date], optional
             filter by gas_day, by default None
         gas_day_lte: Optional[date], optional
             filter by gas_day, by default None
         secondary_flow_type: Optional[Union[list[str], Series[str], str]]
             The secondary type of gas flow, such as production or storage., by default None
         detailed_flow_type: Optional[Union[list[str], Series[str], str]]
             The detailed or specific type of gas flow, such as landing point or LNG terminal., by default None
         country: Optional[Union[list[str], Series[str], str]]
             The country associated with the gas data., by default None
         name: Optional[Union[list[str], Series[str], str]]
             The name or identifier of the storage facility., by default None
         gas_type: Optional[Union[list[str], Series[str], str]]
             The type of gas stored in the facility, such as H-gas (high-calorific gas)., by default None
         source: Optional[Union[list[str], Series[str], str]]
             The source or origin of the gas stored in the facility, such as a specific gas facility or company., by default None
         summable: Optional[Union[list[str], Series[str], str]]
             Indicates whether the gas flows in the storage facility can be summed or aggregated., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used for quantifying the gas flows in the storage facility, such as GWH (gigawatt hours)., by default None
         modified_date: Optional[datetime], optional
             The date and time when the gas data field was last modified or updated., by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         default_source: Optional[Union[list[str], Series[str], str]]
             Indicates whether the specified storage facility is the default source for gas flow data., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        if id_gt is not None:
            filter_params.append(f'id > "{id_gt}"')
        if id_gte is not None:
            filter_params.append(f'id >= "{id_gte}"')
        if id_lt is not None:
            filter_params.append(f'id < "{id_lt}"')
        if id_lte is not None:
            filter_params.append(f'id <= "{id_lte}"')
        filter_params.append(list_to_filter("gasDay", gas_day))
        if gas_day_gt is not None:
            filter_params.append(f'gasDay > "{gas_day_gt}"')
        if gas_day_gte is not None:
            filter_params.append(f'gasDay >= "{gas_day_gte}"')
        if gas_day_lt is not None:
            filter_params.append(f'gasDay < "{gas_day_lt}"')
        if gas_day_lte is not None:
            filter_params.append(f'gasDay <= "{gas_day_lte}"')
        filter_params.append(list_to_filter("secondaryFlowType", secondary_flow_type))
        filter_params.append(list_to_filter("detailedFlowType", detailed_flow_type))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("name", name))
        filter_params.append(list_to_filter("gasType", gas_type))
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("summable", summable))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')
        filter_params.append(list_to_filter("defaultSource", default_source))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/eugas/v1/daily/storage-data-selection",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_daily_physical_flow_point_selection(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        gas_day: Optional[date] = None,
        gas_day_lt: Optional[date] = None,
        gas_day_lte: Optional[date] = None,
        gas_day_gt: Optional[date] = None,
        gas_day_gte: Optional[date] = None,
        main_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        secondary_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        detailed_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        name: Optional[Union[list[str], Series[str], str]] = None,
        gas_type: Optional[Union[list[str], Series[str], str]] = None,
        source: Optional[Union[list[str], Series[str], str]] = None,
        direction: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        default_source: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Obtain daily data on physical flow point selections.

        Parameters
        ----------

         id: Optional[int], optional
             The unique identifier for the gas data field., by default None
         id_gt: Optional[int], optional
             filter by '' id > x '', by default None
         id_gte: Optional[int], optional
             filter by id, by default None
         id_lt: Optional[int], optional
             filter by id, by default None
         id_lte: Optional[int], optional
             filter by id, by default None
         gas_day: Optional[date], optional
             The date for which the gas data is recorded or applicable., by default None
         gas_day_gt: Optional[date], optional
             filter by '' gas_day > x '', by default None
         gas_day_gte: Optional[date], optional
             filter by gas_day, by default None
         gas_day_lt: Optional[date], optional
             filter by gas_day, by default None
         gas_day_lte: Optional[date], optional
             filter by gas_day, by default None
         main_flow_type: Optional[Union[list[str], Series[str], str]]
             The main type of gas flow, such as pipeline or LNG terminal., by default None
         secondary_flow_type: Optional[Union[list[str], Series[str], str]]
             The secondary type of gas flow, such as production or storage., by default None
         detailed_flow_type: Optional[Union[list[str], Series[str], str]]
             The detailed or specific type of gas flow, such as landing point or LNG terminal., by default None
         name: Optional[Union[list[str], Series[str], str]]
             The name or identifier of the specific flow point or location in the gas network., by default None
         gas_type: Optional[Union[list[str], Series[str], str]]
             The type of gas being transported or delivered, such as H-gas (high-calorific gas)., by default None
         source: Optional[Union[list[str], Series[str], str]]
             The source or origin of the gas being transported or delivered, such as a specific gas facility or company., by default None
         direction: Optional[Union[list[str], Series[str], str]]
             The direction of gas flow at the specified flow point, such as entry or exit., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used for quantifying the gas flow at the specified flow point, such as MCM (million cubic meters)., by default None
         modified_date: Optional[datetime], optional
             The date and time when the gas data field was last modified or updated.  , by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         default_source: Optional[Union[list[str], Series[str], str]]
             Indicates whether the specified flow point is the default source for gas flow data., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        if id_gt is not None:
            filter_params.append(f'id > "{id_gt}"')
        if id_gte is not None:
            filter_params.append(f'id >= "{id_gte}"')
        if id_lt is not None:
            filter_params.append(f'id < "{id_lt}"')
        if id_lte is not None:
            filter_params.append(f'id <= "{id_lte}"')
        filter_params.append(list_to_filter("gasDay", gas_day))
        if gas_day_gt is not None:
            filter_params.append(f'gasDay > "{gas_day_gt}"')
        if gas_day_gte is not None:
            filter_params.append(f'gasDay >= "{gas_day_gte}"')
        if gas_day_lt is not None:
            filter_params.append(f'gasDay < "{gas_day_lt}"')
        if gas_day_lte is not None:
            filter_params.append(f'gasDay <= "{gas_day_lte}"')
        filter_params.append(list_to_filter("mainFlowType", main_flow_type))
        filter_params.append(list_to_filter("secondaryFlowType", secondary_flow_type))
        filter_params.append(list_to_filter("detailedFlowType", detailed_flow_type))
        filter_params.append(list_to_filter("name", name))
        filter_params.append(list_to_filter("gasType", gas_type))
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("direction", direction))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')
        filter_params.append(list_to_filter("defaultSource", default_source))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/eugas/v1/daily/physical-flow-point-selection",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_hourly_nomination(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        gas_day: Optional[date] = None,
        gas_day_lt: Optional[date] = None,
        gas_day_lte: Optional[date] = None,
        gas_day_gt: Optional[date] = None,
        gas_day_gte: Optional[date] = None,
        applicable_at: Optional[datetime] = None,
        applicable_at_lt: Optional[datetime] = None,
        applicable_at_lte: Optional[datetime] = None,
        applicable_at_gt: Optional[datetime] = None,
        applicable_at_gte: Optional[datetime] = None,
        main_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        secondary_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        detailed_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        name: Optional[Union[list[str], Series[str], str]] = None,
        gas_type: Optional[Union[list[str], Series[str], str]] = None,
        source: Optional[Union[list[str], Series[str], str]] = None,
        summable: Optional[Union[list[str], Series[str], str]] = None,
        direction: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Get hourly updates on European Gas nominations.

        Parameters
        ----------

         id: Optional[int], optional
             The unique identifier for the gas data field., by default None
         id_gt: Optional[int], optional
             filter by '' id > x '', by default None
         id_gte: Optional[int], optional
             filter by id, by default None
         id_lt: Optional[int], optional
             filter by id, by default None
         id_lte: Optional[int], optional
             filter by id, by default None
         gas_day: Optional[date], optional
             The date for which the gas data is recorded or applicable., by default None
         gas_day_gt: Optional[date], optional
             filter by '' gas_day > x '', by default None
         gas_day_gte: Optional[date], optional
             filter by gas_day, by default None
         gas_day_lt: Optional[date], optional
             filter by gas_day, by default None
         gas_day_lte: Optional[date], optional
             filter by gas_day, by default None
         applicable_at: Optional[datetime], optional
             The specific time or period for which the gas data is applicable., by default None
         applicable_at_gt: Optional[datetime], optional
             filter by '' applicable_at > x '', by default None
         applicable_at_gte: Optional[datetime], optional
             filter by applicable_at, by default None
         applicable_at_lt: Optional[datetime], optional
             filter by applicable_at, by default None
         applicable_at_lte: Optional[datetime], optional
             filter by applicable_at, by default None
         main_flow_type: Optional[Union[list[str], Series[str], str]]
             The main type of gas flow, such as pipeline or LNG terminal., by default None
         secondary_flow_type: Optional[Union[list[str], Series[str], str]]
             The secondary type of gas flow, such as production or storage., by default None
         detailed_flow_type: Optional[Union[list[str], Series[str], str]]
             The detailed or specific type of gas flow, such as landing point or LNG terminal., by default None
         name: Optional[Union[list[str], Series[str], str]]
             The name or identifier of the specific flow point or location in the gas network., by default None
         gas_type: Optional[Union[list[str], Series[str], str]]
             The type of gas being transported or delivered, such as H-gas (high-calorific gas)., by default None
         source: Optional[Union[list[str], Series[str], str]]
             The source or origin of the gas being transported or delivered, such as a specific gas facility or company., by default None
         summable: Optional[Union[list[str], Series[str], str]]
             Indicates whether the gas flows at the specified flow point can be summed or aggregated., by default None
         direction: Optional[Union[list[str], Series[str], str]]
             The direction of gas flow at the specified flow point, such as net flow or total flow., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used for quantifying the gas flow at the specified flow point, such as MCM (million cubic meters)., by default None
         modified_date: Optional[datetime], optional
             The date and time when the gas data field was last modified or updated., by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        if id_gt is not None:
            filter_params.append(f'id > "{id_gt}"')
        if id_gte is not None:
            filter_params.append(f'id >= "{id_gte}"')
        if id_lt is not None:
            filter_params.append(f'id < "{id_lt}"')
        if id_lte is not None:
            filter_params.append(f'id <= "{id_lte}"')
        filter_params.append(list_to_filter("gasDay", gas_day))
        if gas_day_gt is not None:
            filter_params.append(f'gasDay > "{gas_day_gt}"')
        if gas_day_gte is not None:
            filter_params.append(f'gasDay >= "{gas_day_gte}"')
        if gas_day_lt is not None:
            filter_params.append(f'gasDay < "{gas_day_lt}"')
        if gas_day_lte is not None:
            filter_params.append(f'gasDay <= "{gas_day_lte}"')
        filter_params.append(list_to_filter("applicableAt", applicable_at))
        if applicable_at_gt is not None:
            filter_params.append(f'applicableAt > "{applicable_at_gt}"')
        if applicable_at_gte is not None:
            filter_params.append(f'applicableAt >= "{applicable_at_gte}"')
        if applicable_at_lt is not None:
            filter_params.append(f'applicableAt < "{applicable_at_lt}"')
        if applicable_at_lte is not None:
            filter_params.append(f'applicableAt <= "{applicable_at_lte}"')
        filter_params.append(list_to_filter("mainFlowType", main_flow_type))
        filter_params.append(list_to_filter("secondaryFlowType", secondary_flow_type))
        filter_params.append(list_to_filter("detailedFlowType", detailed_flow_type))
        filter_params.append(list_to_filter("name", name))
        filter_params.append(list_to_filter("gasType", gas_type))
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("summable", summable))
        filter_params.append(list_to_filter("direction", direction))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/eugas/v1/hourly/nomination",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_instant_flow(
        self,
        *,
        source: Optional[Union[list[str], Series[str], str]] = None,
        name: Optional[Union[list[str], Series[str], str]] = None,
        time: Optional[datetime] = None,
        time_lt: Optional[datetime] = None,
        time_lte: Optional[datetime] = None,
        time_gt: Optional[datetime] = None,
        time_gte: Optional[datetime] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access real-time data on instantaneous flows from NG and Gassco.

        Parameters
        ----------

         source: Optional[Union[list[str], Series[str], str]]
             The source or origin of the gas flow data, such as NG (natural gas)., by default None
         name: Optional[Union[list[str], Series[str], str]]
             The name or identifier of the specific flow point or location in the gas network., by default None
         time: Optional[datetime], optional
             The date and time when the gas flow data was recorded or measured., by default None
         time_gt: Optional[datetime], optional
             filter by '' time > x '', by default None
         time_gte: Optional[datetime], optional
             filter by time, by default None
         time_lt: Optional[datetime], optional
             filter by time, by default None
         time_lte: Optional[datetime], optional
             filter by time, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used for quantifying the gas flow, such as MCM (million cubic meters)., by default None
         modified_date: Optional[datetime], optional
             The date and time when the gas data field was last modified or updated., by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("name", name))
        filter_params.append(list_to_filter("time", time))
        if time_gt is not None:
            filter_params.append(f'time > "{time_gt}"')
        if time_gte is not None:
            filter_params.append(f'time >= "{time_gte}"')
        if time_lt is not None:
            filter_params.append(f'time < "{time_lt}"')
        if time_lte is not None:
            filter_params.append(f'time <= "{time_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/eugas/v1/instant/flow",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_overview_hub_balance(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        gas_day: Optional[date] = None,
        gas_day_lt: Optional[date] = None,
        gas_day_lte: Optional[date] = None,
        gas_day_gt: Optional[date] = None,
        gas_day_gte: Optional[date] = None,
        hub_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        hub_sub_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        hub_sub_flow_region: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        hub: Optional[Union[list[str], Series[str], str]] = None,
        hub_flow_name: Optional[Union[list[str], Series[str], str]] = None,
        source: Optional[Union[list[str], Series[str], str]] = None,
        direction: Optional[Union[list[str], Series[str], str]] = None,
        type: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        average_type: Optional[Union[list[str], Series[str], str]] = None,
        day_month_ordinal: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Get an overview of current hub balances in the European Gas market.

        Parameters
        ----------

         id: Optional[int], optional
             The unique identifier for the gas data field., by default None
         id_gt: Optional[int], optional
             filter by '' id > x '', by default None
         id_gte: Optional[int], optional
             filter by id, by default None
         id_lt: Optional[int], optional
             filter by id, by default None
         id_lte: Optional[int], optional
             filter by id, by default None
         gas_day: Optional[date], optional
             The date for which the gas data is recorded or applicable., by default None
         gas_day_gt: Optional[date], optional
             filter by '' gas_day > x '', by default None
         gas_day_gte: Optional[date], optional
             filter by gas_day, by default None
         gas_day_lt: Optional[date], optional
             filter by gas_day, by default None
         gas_day_lte: Optional[date], optional
             filter by gas_day, by default None
         hub_flow_type: Optional[Union[list[str], Series[str], str]]
             The type of gas flow at the hub location, such as supply or demand., by default None
         hub_sub_flow_type: Optional[Union[list[str], Series[str], str]]
             The specific sub-type of gas flow at the hub location, such as LNG (liquefied natural gas)., by default None
         hub_sub_flow_region: Optional[Union[list[str], Series[str], str]]
             The region associated with the sub-type of gas flow at the hub location, such as France., by default None
         country: Optional[Union[list[str], Series[str], str]]
             The country associated with the gas data., by default None
         hub: Optional[Union[list[str], Series[str], str]]
             The specific hub location where the gas data is recorded., by default None
         hub_flow_name: Optional[Union[list[str], Series[str], str]]
             The name or identifier of the specific flow of gas at the hub location., by default None
         source: Optional[Union[list[str], Series[str], str]]
             The source or origin of the gas data, such as a specific gas facility or company., by default None
         direction: Optional[Union[list[str], Series[str], str]]
             The direction of gas flow, such as entry or exit., by default None
         type: Optional[Union[list[str], Series[str], str]]
             The type of data or process associated with the gas flow, such as nomination or storage., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used for quantifying the gas data, such as MCM (million cubic meters)., by default None
         average_type: Optional[Union[list[str], Series[str], str]]
             The type of average used for calculating the gas data, such as daily value or monthly accumulated., by default None
         day_month_ordinal: Optional[Union[list[str], Series[str], str]]
             The ordinal value or position of the gas day within the month, such as D+15 (15th day of the month)., by default None
         modified_date: Optional[datetime], optional
             The date and time when the gas data field was last modified or updated., by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        if id_gt is not None:
            filter_params.append(f'id > "{id_gt}"')
        if id_gte is not None:
            filter_params.append(f'id >= "{id_gte}"')
        if id_lt is not None:
            filter_params.append(f'id < "{id_lt}"')
        if id_lte is not None:
            filter_params.append(f'id <= "{id_lte}"')
        filter_params.append(list_to_filter("gasDay", gas_day))
        if gas_day_gt is not None:
            filter_params.append(f'gasDay > "{gas_day_gt}"')
        if gas_day_gte is not None:
            filter_params.append(f'gasDay >= "{gas_day_gte}"')
        if gas_day_lt is not None:
            filter_params.append(f'gasDay < "{gas_day_lt}"')
        if gas_day_lte is not None:
            filter_params.append(f'gasDay <= "{gas_day_lte}"')
        filter_params.append(list_to_filter("hubFlowType", hub_flow_type))
        filter_params.append(list_to_filter("hubSubFlowType", hub_sub_flow_type))
        filter_params.append(list_to_filter("hubSubFlowRegion", hub_sub_flow_region))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("hub", hub))
        filter_params.append(list_to_filter("hubFlowName", hub_flow_name))
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("direction", direction))
        filter_params.append(list_to_filter("type", type))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("averageType", average_type))
        filter_params.append(list_to_filter("dayMonthOrdinal", day_month_ordinal))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/eugas/v1/overview/hub-balance",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_daily_history_hub_balance(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        gas_day: Optional[date] = None,
        gas_day_lt: Optional[date] = None,
        gas_day_lte: Optional[date] = None,
        gas_day_gt: Optional[date] = None,
        gas_day_gte: Optional[date] = None,
        hub_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        hub_sub_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        hub_sub_flow_region: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        hub: Optional[Union[list[str], Series[str], str]] = None,
        hub_flow_name: Optional[Union[list[str], Series[str], str]] = None,
        source: Optional[Union[list[str], Series[str], str]] = None,
        direction: Optional[Union[list[str], Series[str], str]] = None,
        type: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        average_type: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access historical daily data on hub balances.

        Parameters
        ----------

         id: Optional[int], optional
             The unique identifier for the gas data field., by default None
         id_gt: Optional[int], optional
             filter by '' id > x '', by default None
         id_gte: Optional[int], optional
             filter by id, by default None
         id_lt: Optional[int], optional
             filter by id, by default None
         id_lte: Optional[int], optional
             filter by id, by default None
         gas_day: Optional[date], optional
             The date for which the gas data is recorded or applicable., by default None
         gas_day_gt: Optional[date], optional
             filter by '' gas_day > x '', by default None
         gas_day_gte: Optional[date], optional
             filter by gas_day, by default None
         gas_day_lt: Optional[date], optional
             filter by gas_day, by default None
         gas_day_lte: Optional[date], optional
             filter by gas_day, by default None
         hub_flow_type: Optional[Union[list[str], Series[str], str]]
             The type of gas flow at the hub location, such as supply or demand., by default None
         hub_sub_flow_type: Optional[Union[list[str], Series[str], str]]
             The specific sub-type of gas flow at the hub location, such as LNG (liquefied natural gas)., by default None
         hub_sub_flow_region: Optional[Union[list[str], Series[str], str]]
             The region associated with the sub-type of gas flow at the hub location, such as France., by default None
         country: Optional[Union[list[str], Series[str], str]]
             The country associated with the gas data., by default None
         hub: Optional[Union[list[str], Series[str], str]]
             The specific hub location where the gas data is recorded., by default None
         hub_flow_name: Optional[Union[list[str], Series[str], str]]
             The name or identifier of the specific flow of gas at the hub location., by default None
         source: Optional[Union[list[str], Series[str], str]]
             The source or origin of the gas data, such as a specific gas facility or company., by default None
         direction: Optional[Union[list[str], Series[str], str]]
             The direction of gas flow, such as entry or exit., by default None
         type: Optional[Union[list[str], Series[str], str]]
             The type of data or process associated with the gas flow, such as nomination or storage., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used for quantifying the gas data, such as MCM (million cubic meters)., by default None
         average_type: Optional[Union[list[str], Series[str], str]]
             The type of average used for calculating the gas data, such as daily value or monthly accumulated., by default None
         modified_date: Optional[datetime], optional
             The date and time when the gas data field was last modified or updated., by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        if id_gt is not None:
            filter_params.append(f'id > "{id_gt}"')
        if id_gte is not None:
            filter_params.append(f'id >= "{id_gte}"')
        if id_lt is not None:
            filter_params.append(f'id < "{id_lt}"')
        if id_lte is not None:
            filter_params.append(f'id <= "{id_lte}"')
        filter_params.append(list_to_filter("gasDay", gas_day))
        if gas_day_gt is not None:
            filter_params.append(f'gasDay > "{gas_day_gt}"')
        if gas_day_gte is not None:
            filter_params.append(f'gasDay >= "{gas_day_gte}"')
        if gas_day_lt is not None:
            filter_params.append(f'gasDay < "{gas_day_lt}"')
        if gas_day_lte is not None:
            filter_params.append(f'gasDay <= "{gas_day_lte}"')
        filter_params.append(list_to_filter("hubFlowType", hub_flow_type))
        filter_params.append(list_to_filter("hubSubFlowType", hub_sub_flow_type))
        filter_params.append(list_to_filter("hubSubFlowRegion", hub_sub_flow_region))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("hub", hub))
        filter_params.append(list_to_filter("hubFlowName", hub_flow_name))
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("direction", direction))
        filter_params.append(list_to_filter("type", type))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("averageType", average_type))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/eugas/v1/daily/history/hub-balance",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_daily_flow_point_selection(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        gas_day: Optional[date] = None,
        gas_day_lt: Optional[date] = None,
        gas_day_lte: Optional[date] = None,
        gas_day_gt: Optional[date] = None,
        gas_day_gte: Optional[date] = None,
        main_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        secondary_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        detailed_flow_type: Optional[Union[list[str], Series[str], str]] = None,
        name: Optional[Union[list[str], Series[str], str]] = None,
        gas_type: Optional[Union[list[str], Series[str], str]] = None,
        source: Optional[Union[list[str], Series[str], str]] = None,
        summable: Optional[Union[list[str], Series[str], str]] = None,
        direction: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        default_source: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve daily flow point selection data for European Gas distribution.

        Parameters
        ----------

         id: Optional[int], optional
             The unique identifier for the gas data field., by default None
         id_gt: Optional[int], optional
             filter by '' id > x '', by default None
         id_gte: Optional[int], optional
             filter by id, by default None
         id_lt: Optional[int], optional
             filter by id, by default None
         id_lte: Optional[int], optional
             filter by id, by default None
         gas_day: Optional[date], optional
             The date for which the gas data is recorded or applicable., by default None
         gas_day_gt: Optional[date], optional
             filter by '' gas_day > x '', by default None
         gas_day_gte: Optional[date], optional
             filter by gas_day, by default None
         gas_day_lt: Optional[date], optional
             filter by gas_day, by default None
         gas_day_lte: Optional[date], optional
             filter by gas_day, by default None
         main_flow_type: Optional[Union[list[str], Series[str], str]]
             The main type of gas flow, such as pipeline or LNG terminal., by default None
         secondary_flow_type: Optional[Union[list[str], Series[str], str]]
             The secondary type of gas flow, such as production or storage., by default None
         detailed_flow_type: Optional[Union[list[str], Series[str], str]]
             The detailed or specific type of gas flow, such as landing point or LNG terminal., by default None
         name: Optional[Union[list[str], Series[str], str]]
             The name or identifier of the specific flow point or location in the gas network., by default None
         gas_type: Optional[Union[list[str], Series[str], str]]
             The type of gas being transported or delivered, such as H-gas (high-calorific gas)., by default None
         source: Optional[Union[list[str], Series[str], str]]
             The source or origin of the gas being transported or delivered, such as a specific gas facility or company., by default None
         summable: Optional[Union[list[str], Series[str], str]]
             Indicates whether the gas flows at the specified flow point can be summed or aggregated., by default None
         direction: Optional[Union[list[str], Series[str], str]]
             The direction of gas flow at the specified flow point, such as net flow or total flow., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used for quantifying the gas flow at the specified flow point, such as MCM (million cubic meters)., by default None
         modified_date: Optional[datetime], optional
             The date and time when the gas data field was last modified or updated., by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         default_source: Optional[Union[list[str], Series[str], str]]
             Indicates whether the specified flow point is the default source for gas flow data., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        if id_gt is not None:
            filter_params.append(f'id > "{id_gt}"')
        if id_gte is not None:
            filter_params.append(f'id >= "{id_gte}"')
        if id_lt is not None:
            filter_params.append(f'id < "{id_lt}"')
        if id_lte is not None:
            filter_params.append(f'id <= "{id_lte}"')
        filter_params.append(list_to_filter("gasDay", gas_day))
        if gas_day_gt is not None:
            filter_params.append(f'gasDay > "{gas_day_gt}"')
        if gas_day_gte is not None:
            filter_params.append(f'gasDay >= "{gas_day_gte}"')
        if gas_day_lt is not None:
            filter_params.append(f'gasDay < "{gas_day_lt}"')
        if gas_day_lte is not None:
            filter_params.append(f'gasDay <= "{gas_day_lte}"')
        filter_params.append(list_to_filter("mainFlowType", main_flow_type))
        filter_params.append(list_to_filter("secondaryFlowType", secondary_flow_type))
        filter_params.append(list_to_filter("detailedFlowType", detailed_flow_type))
        filter_params.append(list_to_filter("name", name))
        filter_params.append(list_to_filter("gasType", gas_type))
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("summable", summable))
        filter_params.append(list_to_filter("direction", direction))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')
        filter_params.append(list_to_filter("defaultSource", default_source))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/eugas/v1/daily/flow-point-selection",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_monthly_field_production(
        self,
        *,
        production_month: Optional[date] = None,
        production_month_lt: Optional[date] = None,
        production_month_lte: Optional[date] = None,
        production_month_gt: Optional[date] = None,
        production_month_gte: Optional[date] = None,
        region_entrypoint: Optional[Union[list[str], Series[str], str]] = None,
        offshore_onshore: Optional[Union[list[str], Series[str], str]] = None,
        district: Optional[Union[list[str], Series[str], str]] = None,
        granularity: Optional[Union[list[str], Series[str], str]] = None,
        operator: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve monthly field production data from various sources like OGA, NLOG, NPD, ENS, and 'CDU TEK'.

        Parameters
        ----------

         production_month: Optional[date], optional
             The month for which the gas production data is recorded or applicable., by default None
         production_month_gt: Optional[date], optional
             filter by '' production_month > x '', by default None
         production_month_gte: Optional[date], optional
             filter by production_month, by default None
         production_month_lt: Optional[date], optional
             filter by production_month, by default None
         production_month_lte: Optional[date], optional
             filter by production_month, by default None
         region_entrypoint: Optional[Union[list[str], Series[str], str]]
             The specific region or entry point associated with the gas production., by default None
         offshore_onshore: Optional[Union[list[str], Series[str], str]]
             Indicates whether the gas production is from an offshore or onshore location., by default None
         district: Optional[Union[list[str], Series[str], str]]
             The name or identifier of the specific district or field where the gas production occurs., by default None
         granularity: Optional[Union[list[str], Series[str], str]]
             The level of detail or aggregation for the gas production data, such as monthly accumulated., by default None
         operator: Optional[Union[list[str], Series[str], str]]
             The operator or company responsible for the gas production in the specified field or area., by default None
         modified_date: Optional[datetime], optional
             The date and time when the gas data field was last modified or updated., by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("productionMonth", production_month))
        if production_month_gt is not None:
            filter_params.append(f'productionMonth > "{production_month_gt}"')
        if production_month_gte is not None:
            filter_params.append(f'productionMonth >= "{production_month_gte}"')
        if production_month_lt is not None:
            filter_params.append(f'productionMonth < "{production_month_lt}"')
        if production_month_lte is not None:
            filter_params.append(f'productionMonth <= "{production_month_lte}"')
        filter_params.append(list_to_filter("regionEntrypoint", region_entrypoint))
        filter_params.append(list_to_filter("offshoreOnshore", offshore_onshore))
        filter_params.append(list_to_filter("district", district))
        filter_params.append(list_to_filter("granularity", granularity))
        filter_params.append(list_to_filter("Operator", operator))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/eugas/v1/monthly/field-production",
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

        if "gasDay" in df.columns:
            df["gasDay"] = pd.to_datetime(df["gasDay"])  # type: ignore

        if "modifiedDate" in df.columns:
            df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])  # type: ignore

        if "applicableAt" in df.columns:
            df["applicableAt"] = pd.to_datetime(df["applicableAt"])  # type: ignore

        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"])  # type: ignore

        if "productionMonth" in df.columns:
            df["productionMonth"] = pd.to_datetime(df["productionMonth"])  # type: ignore
        return df
