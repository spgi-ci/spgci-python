# Copyright 2025 S&P Global Commodity Insights

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date, datetime
import pandas as pd
from packaging.version import parse


def _custom_sort_key(val):
    if val.startswith("M-"):
        return (0, -int(val[2:]))  # Month values
    elif val == "MTD":
        return (1, 0)  # Month-to-Date
    elif val.startswith("D-"):
        return (2, -int(val[2:]))  # D- values
    elif val.startswith("D+"):
        return (3, int(val[2:]))  # D+ values
    else:
        # Handle other values like "Nom", "Delta", "Forecast"
        if val == "Nom":
            return (4, 0)  # Nom comes first among the non-day/month values
        elif val == "Delta":
            return (4, 1)  # Delta comes after Nom
        elif val == "Forecast":
            return (4, 2)  # Forecast comes after Delta
        else:
            return (5, 0)  # Any other value comes last


class EUGasAnalytics:

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
        storage_system_operator: Optional[Union[list[str], Series[str], str]] = None,
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
        page_size: int = 5000,
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
             filter by `id > x`, by default None
         id_gte: Optional[int], optional
             filter by `id >= x`, by default None
         id_lt: Optional[int], optional
             filter by `id < x`, by default None
         id_lte: Optional[int], optional
             filter by `id <= x`, by default None
         gas_day: Optional[date], optional
             The date for which the gas data is recorded or applicable., by default None
         gas_day_gt: Optional[date], optional
             filter by `gas_day > x`, by default None
         gas_day_gte: Optional[date], optional
             filter by `gas_day >= x`, by default None
         gas_day_lt: Optional[date], optional
             filter by `gas_day < x`, by default None
         gas_day_lte: Optional[date], optional
             filter by `gas_day <= x`, by default None
         secondary_flow_type: Optional[Union[list[str], Series[str], str]]
             The secondary type of gas flow, such as production or storage., by default None
         detailed_flow_type: Optional[Union[list[str], Series[str], str]]
             The detailed or specific type of gas flow, such as landing point or LNG terminal., by default None
         country: Optional[Union[list[str], Series[str], str]]
             The country associated with the gas data., by default None
         storage_system_operator: Optional[Union[list[str], Series[str], str]]
             The system operator responsible for the storage facility., by default None
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
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
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
        filter_params.append(
            list_to_filter("storageSystemOperator", storage_system_operator)
        )
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
        from_country: Optional[Union[list[str], Series[str], str]] = None,
        from_system_operator: Optional[Union[list[str], Series[str], str]] = None,
        to_country: Optional[Union[list[str], Series[str], str]] = None,
        to_system_operator: Optional[Union[list[str], Series[str], str]] = None,
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
        page_size: int = 5000,
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
             filter by `id > x`, by default None
         id_gte: Optional[int], optional
             filter by `id >= x`, by default None
         id_lt: Optional[int], optional
             filter by `id < x`, by default None
         id_lte: Optional[int], optional
             filter by `id <= x`, by default None
         gas_day: Optional[date], optional
             The date for which the gas data is recorded or applicable., by default None
         gas_day_gt: Optional[date], optional
             filter by `gas_day > x`, by default None
         gas_day_gte: Optional[date], optional
             filter by `gas_day >= x`, by default None
         gas_day_lt: Optional[date], optional
             filter by `gas_day < x`, by default None
         gas_day_lte: Optional[date], optional
             filter by `gas_day <= x`, by default None
         main_flow_type: Optional[Union[list[str], Series[str], str]]
             The main type of gas flow, such as pipeline or LNG terminal., by default None
         secondary_flow_type: Optional[Union[list[str], Series[str], str]]
             The secondary type of gas flow, such as production or storage., by default None
         detailed_flow_type: Optional[Union[list[str], Series[str], str]]
             The detailed or specific type of gas flow, such as landing point or LNG terminal., by default None
         from_country: Optional[Union[list[str], Series[str], str]]
             The country from which the gas is being transported or delivered., by default None
         from_system_operator: Optional[Union[list[str], Series[str], str]]
             The system operator responsible for the gas transportation or delivery from the specified country., by default None
         to_country: Optional[Union[list[str], Series[str], str]]
             The country to which the gas is being transported or delivered., by default None
         to_system_operator: Optional[Union[list[str], Series[str], str]]
             The system operator responsible for the gas transportation or delivery to the specified country., by default None
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
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
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
        filter_params.append(list_to_filter("fromCountry", from_country))
        filter_params.append(list_to_filter("fromSystemOperator", from_system_operator))
        filter_params.append(list_to_filter("toCountry", to_country))
        filter_params.append(list_to_filter("toSystemOperator", to_system_operator))
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
        from_country: Optional[Union[list[str], Series[str], str]] = None,
        from_system_operator: Optional[Union[list[str], Series[str], str]] = None,
        to_country: Optional[Union[list[str], Series[str], str]] = None,
        to_system_operator: Optional[Union[list[str], Series[str], str]] = None,
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
        page_size: int = 5000,
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
             filter by `id > x`, by default None
         id_gte: Optional[int], optional
             filter by `id >= x`, by default None
         id_lt: Optional[int], optional
             filter by `id < x`, by default None
         id_lte: Optional[int], optional
             filter by `id <= x`, by default None
         gas_day: Optional[date], optional
             The date for which the gas data is recorded or applicable., by default None
         gas_day_gt: Optional[date], optional
             filter by `gas_day > x`, by default None
         gas_day_gte: Optional[date], optional
             filter by `gas_day >= x`, by default None
         gas_day_lt: Optional[date], optional
             filter by `gas_day < x`, by default None
         gas_day_lte: Optional[date], optional
             filter by `gas_day <= x`, by default None
         applicable_at: Optional[datetime], optional
             The specific time or period for which the gas data is applicable., by default None
         applicable_at_gt: Optional[datetime], optional
             filter by `applicable_at > x`, by default None
         applicable_at_gte: Optional[datetime], optional
             filter by `applicable_at >= x`, by default None
         applicable_at_lt: Optional[datetime], optional
             filter by `applicable_at < x`, by default None
         applicable_at_lte: Optional[datetime], optional
             filter by `applicable_at <= x`, by default None
         main_flow_type: Optional[Union[list[str], Series[str], str]]
             The main type of gas flow, such as pipeline or LNG terminal., by default None
         secondary_flow_type: Optional[Union[list[str], Series[str], str]]
             The secondary type of gas flow, such as production or storage., by default None
         detailed_flow_type: Optional[Union[list[str], Series[str], str]]
             The detailed or specific type of gas flow, such as landing point or LNG terminal., by default None
         from_country: Optional[Union[list[str], Series[str], str]]
             The country from which the gas is being transported or delivered., by default None
         from_system_operator: Optional[Union[list[str], Series[str], str]]
             The system operator responsible for the gas transportation or delivery from the specified country., by default None
         to_country: Optional[Union[list[str], Series[str], str]]
             The country to which the gas is being transported or delivered., by default None
         to_system_operator: Optional[Union[list[str], Series[str], str]]
             The system operator responsible for the gas transportation or delivery to the specified country., by default None
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
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
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
        filter_params.append(list_to_filter("fromCountry", from_country))
        filter_params.append(list_to_filter("fromSystemOperator", from_system_operator))
        filter_params.append(list_to_filter("toCountry", to_country))
        filter_params.append(list_to_filter("toSystemOperator", to_system_operator))
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
        gas_flow_time: Optional[datetime] = None,
        gas_flow_time_lt: Optional[datetime] = None,
        gas_flow_time_lte: Optional[datetime] = None,
        gas_flow_time_gt: Optional[datetime] = None,
        gas_flow_time_gte: Optional[datetime] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
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
         gas_flow_time: Optional[datetime], optional
             The date and time when the gas flow data was recorded or measured., by default None
         gas_flow_time_gt: Optional[datetime], optional
             filter by `gas_flow_time > x`, by default None
         gas_flow_time_gte: Optional[datetime], optional
             filter by `gas_flow_time >= x`, by default None
         gas_flow_time_lt: Optional[datetime], optional
             filter by `gas_flow_time < x`, by default None
         gas_flow_time_lte: Optional[datetime], optional
             filter by `gas_flow_time <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used for quantifying the gas flow, such as MCM (million cubic meters)., by default None
         modified_date: Optional[datetime], optional
             The date and time when the gas data field was last modified or updated., by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("name", name))
        filter_params.append(list_to_filter("gasFlowTime", gas_flow_time))
        if gas_flow_time_gt is not None:
            filter_params.append(f'gasFlowTime > "{gas_flow_time_gt}"')
        if gas_flow_time_gte is not None:
            filter_params.append(f'gasFlowTime >= "{gas_flow_time_gte}"')
        if gas_flow_time_lt is not None:
            filter_params.append(f'gasFlowTime < "{gas_flow_time_lt}"')
        if gas_flow_time_lte is not None:
            filter_params.append(f'gasFlowTime <= "{gas_flow_time_lte}"')
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
        page_size: int = 5000,
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
             filter by `id > x`, by default None
         id_gte: Optional[int], optional
             filter by `id >= x`, by default None
         id_lt: Optional[int], optional
             filter by `id < x`, by default None
         id_lte: Optional[int], optional
             filter by `id <= x`, by default None
         gas_day: Optional[date], optional
             The date for which the gas data is recorded or applicable., by default None
         gas_day_gt: Optional[date], optional
             filter by `gas_day > x`, by default None
         gas_day_gte: Optional[date], optional
             filter by `gas_day >= x`, by default None
         gas_day_lt: Optional[date], optional
             filter by `gas_day < x`, by default None
         gas_day_lte: Optional[date], optional
             filter by `gas_day <= x`, by default None
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
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
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

        if not raw and "dayMonthOrdinal" in response.columns:
            sorted_categories = sorted(
                response["dayMonthOrdinal"].unique(), key=_custom_sort_key
            )
            response["dayMonthOrdinal"] = pd.Categorical(
                response["dayMonthOrdinal"], categories=sorted_categories, ordered=True
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
        page_size: int = 5000,
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
             filter by `id > x`, by default None
         id_gte: Optional[int], optional
             filter by `id >= x`, by default None
         id_lt: Optional[int], optional
             filter by `id < x`, by default None
         id_lte: Optional[int], optional
             filter by `id <= x`, by default None
         gas_day: Optional[date], optional
             The date for which the gas data is recorded or applicable., by default None
         gas_day_gt: Optional[date], optional
             filter by `gas_day > x`, by default None
         gas_day_gte: Optional[date], optional
             filter by `gas_day >= x`, by default None
         gas_day_lt: Optional[date], optional
             filter by `gas_day < x`, by default None
         gas_day_lte: Optional[date], optional
             filter by `gas_day <= x`, by default None
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
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
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
        from_country: Optional[Union[list[str], Series[str], str]] = None,
        from_system_operator: Optional[Union[list[str], Series[str], str]] = None,
        to_country: Optional[Union[list[str], Series[str], str]] = None,
        to_system_operator: Optional[Union[list[str], Series[str], str]] = None,
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
        page_size: int = 5000,
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
             filter by `id > x`, by default None
         id_gte: Optional[int], optional
             filter by `id >= x`, by default None
         id_lt: Optional[int], optional
             filter by `id < x`, by default None
         id_lte: Optional[int], optional
             filter by `id <= x`, by default None
         gas_day: Optional[date], optional
             The date for which the gas data is recorded or applicable., by default None
         gas_day_gt: Optional[date], optional
             filter by `gas_day > x`, by default None
         gas_day_gte: Optional[date], optional
             filter by `gas_day >= x`, by default None
         gas_day_lt: Optional[date], optional
             filter by `gas_day < x`, by default None
         gas_day_lte: Optional[date], optional
             filter by `gas_day <= x`, by default None
         main_flow_type: Optional[Union[list[str], Series[str], str]]
             The main type of gas flow, such as pipeline or LNG terminal., by default None
         secondary_flow_type: Optional[Union[list[str], Series[str], str]]
             The secondary type of gas flow, such as production or storage., by default None
         detailed_flow_type: Optional[Union[list[str], Series[str], str]]
             The detailed or specific type of gas flow, such as landing point or LNG terminal., by default None
         from_country: Optional[Union[list[str], Series[str], str]]
             The country from which the gas is being transported or delivered., by default None
         from_system_operator: Optional[Union[list[str], Series[str], str]]
             The system operator responsible for the gas transportation or delivery from the specified country., by default None
         to_country: Optional[Union[list[str], Series[str], str]]
             The country to which the gas is being transported or delivered., by default None
         to_system_operator: Optional[Union[list[str], Series[str], str]]
             The system operator responsible for the gas transportation or delivery to the specified country., by default None
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
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
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
        filter_params.append(list_to_filter("fromCountry", from_country))
        filter_params.append(list_to_filter("fromSystemOperator", from_system_operator))
        filter_params.append(list_to_filter("toCountry", to_country))
        filter_params.append(list_to_filter("toSystemOperator", to_system_operator))
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
        page_size: int = 5000,
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
             filter by `production_month > x`, by default None
         production_month_gte: Optional[date], optional
             filter by `production_month >= x`, by default None
         production_month_lt: Optional[date], optional
             filter by `production_month < x`, by default None
         production_month_lte: Optional[date], optional
             filter by `production_month <= x`, by default None
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
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
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

    def get_outages_event(
        self,
        *,
        flow_item_id: Optional[int] = None,
        flow_item_id_lt: Optional[int] = None,
        flow_item_id_lte: Optional[int] = None,
        flow_item_id_gt: Optional[int] = None,
        flow_item_id_gte: Optional[int] = None,
        flow_field_id: Optional[int] = None,
        flow_field_id_lt: Optional[int] = None,
        flow_field_id_lte: Optional[int] = None,
        flow_field_id_gt: Optional[int] = None,
        flow_field_id_gte: Optional[int] = None,
        start_date: Optional[datetime] = None,
        start_date_lt: Optional[datetime] = None,
        start_date_lte: Optional[datetime] = None,
        start_date_gt: Optional[datetime] = None,
        start_date_gte: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        end_date_lt: Optional[datetime] = None,
        end_date_lte: Optional[datetime] = None,
        end_date_gt: Optional[datetime] = None,
        end_date_gte: Optional[datetime] = None,
        point_name: Optional[Union[list[str], Series[str], str]] = None,
        reported_date: Optional[datetime] = None,
        reported_date_lt: Optional[datetime] = None,
        reported_date_lte: Optional[datetime] = None,
        reported_date_gt: Optional[datetime] = None,
        reported_date_gte: Optional[datetime] = None,
        last_modified: Optional[datetime] = None,
        last_modified_lt: Optional[datetime] = None,
        last_modified_lte: Optional[datetime] = None,
        last_modified_gt: Optional[datetime] = None,
        last_modified_gte: Optional[datetime] = None,
        from_country: Optional[Union[list[str], Series[str], str]] = None,
        to_country: Optional[Union[list[str], Series[str], str]] = None,
        from_system_operator: Optional[Union[list[str], Series[str], str]] = None,
        to_system_operator: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        flow_item_type: Optional[Union[list[str], Series[str], str]] = None,
        is_current_revision: Optional[Union[list[str], Series[str], str]] = None,
        infrastructure_type: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access current and historical version of Eugas Outage Events data.

        Parameters
        ----------

         flow_item_id: Optional[int], optional
             A unique identificator of the flow item for the EU gas outages., by default None
         flow_item_id_gt: Optional[int], optional
             filter by `flow_item_id > x`, by default None
         flow_item_id_gte: Optional[int], optional
             filter by `flow_item_id >= x`, by default None
         flow_item_id_lt: Optional[int], optional
             filter by `flow_item_id < x`, by default None
         flow_item_id_lte: Optional[int], optional
             filter by `flow_item_id <= x`, by default None
         flow_field_id: Optional[int], optional
             A unique identificator of the flow field for the EU gas outages., by default None
         flow_field_id_gt: Optional[int], optional
             filter by `flow_field_id > x`, by default None
         flow_field_id_gte: Optional[int], optional
             filter by `flow_field_id >= x`, by default None
         flow_field_id_lt: Optional[int], optional
             filter by `flow_field_id < x`, by default None
         flow_field_id_lte: Optional[int], optional
             filter by `flow_field_id <= x`, by default None
         start_date: Optional[datetime], optional
             The start date of the outage., by default None
         start_date_gt: Optional[datetime], optional
             filter by `start_date > x`, by default None
         start_date_gte: Optional[datetime], optional
             filter by `start_date >= x`, by default None
         start_date_lt: Optional[datetime], optional
             filter by `start_date < x`, by default None
         start_date_lte: Optional[datetime], optional
             filter by `start_date <= x`, by default None
         end_date: Optional[datetime], optional
             The end date of the outage., by default None
         end_date_gt: Optional[datetime], optional
             filter by `end_date > x`, by default None
         end_date_gte: Optional[datetime], optional
             filter by `end_date >= x`, by default None
         end_date_lt: Optional[datetime], optional
             filter by `end_date < x`, by default None
         end_date_lte: Optional[datetime], optional
             filter by `end_date <= x`, by default None
         point_name: Optional[Union[list[str], Series[str], str]]
             Name of the European gas outages point., by default None
         reported_date: Optional[datetime], optional
             The Date when outage was reported., by default None
         reported_date_gt: Optional[datetime], optional
             filter by `reported_date > x`, by default None
         reported_date_gte: Optional[datetime], optional
             filter by `reported_date >= x`, by default None
         reported_date_lt: Optional[datetime], optional
             filter by `reported_date < x`, by default None
         reported_date_lte: Optional[datetime], optional
             filter by `reported_date <= x`, by default None
         last_modified: Optional[datetime], optional
             The Date when outage event was last changed., by default None
         last_modified_gt: Optional[datetime], optional
             filter by `last_modified > x`, by default None
         last_modified_gte: Optional[datetime], optional
             filter by `last_modified >= x`, by default None
         last_modified_lt: Optional[datetime], optional
             filter by `last_modified < x`, by default None
         last_modified_lte: Optional[datetime], optional
             filter by `last_modified <= x`, by default None
         from_country: Optional[Union[list[str], Series[str], str]]
             Name of the country from which the outage is happening., by default None
         to_country: Optional[Union[list[str], Series[str], str]]
             Name of the country to which the outage is happening., by default None
         from_system_operator: Optional[Union[list[str], Series[str], str]]
             Name of the operator from which the outage is happening., by default None
         to_system_operator: Optional[Union[list[str], Series[str], str]]
             Name of the operator to which the gas outage is happening., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used for quantifying gas flow., by default None
         flow_item_type: Optional[Union[list[str], Series[str], str]]
             Type of the flow item for EU gas outages., by default None
         is_current_revision: Optional[Union[list[str], Series[str], str]]
             Indicates if the current entry for an outage is the active record., by default None
         infrastructure_type: Optional[Union[list[str], Series[str], str]]
             The type of infrastructure associated with the outage., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("flowItemId", flow_item_id))
        if flow_item_id_gt is not None:
            filter_params.append(f'flowItemId > "{flow_item_id_gt}"')
        if flow_item_id_gte is not None:
            filter_params.append(f'flowItemId >= "{flow_item_id_gte}"')
        if flow_item_id_lt is not None:
            filter_params.append(f'flowItemId < "{flow_item_id_lt}"')
        if flow_item_id_lte is not None:
            filter_params.append(f'flowItemId <= "{flow_item_id_lte}"')
        filter_params.append(list_to_filter("flowFieldId", flow_field_id))
        if flow_field_id_gt is not None:
            filter_params.append(f'flowFieldId > "{flow_field_id_gt}"')
        if flow_field_id_gte is not None:
            filter_params.append(f'flowFieldId >= "{flow_field_id_gte}"')
        if flow_field_id_lt is not None:
            filter_params.append(f'flowFieldId < "{flow_field_id_lt}"')
        if flow_field_id_lte is not None:
            filter_params.append(f'flowFieldId <= "{flow_field_id_lte}"')
        filter_params.append(list_to_filter("startDate", start_date))
        if start_date_gt is not None:
            filter_params.append(f'startDate > "{start_date_gt}"')
        if start_date_gte is not None:
            filter_params.append(f'startDate >= "{start_date_gte}"')
        if start_date_lt is not None:
            filter_params.append(f'startDate < "{start_date_lt}"')
        if start_date_lte is not None:
            filter_params.append(f'startDate <= "{start_date_lte}"')
        filter_params.append(list_to_filter("endDate", end_date))
        if end_date_gt is not None:
            filter_params.append(f'endDate > "{end_date_gt}"')
        if end_date_gte is not None:
            filter_params.append(f'endDate >= "{end_date_gte}"')
        if end_date_lt is not None:
            filter_params.append(f'endDate < "{end_date_lt}"')
        if end_date_lte is not None:
            filter_params.append(f'endDate <= "{end_date_lte}"')
        filter_params.append(list_to_filter("pointName", point_name))
        filter_params.append(list_to_filter("reportedDate", reported_date))
        if reported_date_gt is not None:
            filter_params.append(f'reportedDate > "{reported_date_gt}"')
        if reported_date_gte is not None:
            filter_params.append(f'reportedDate >= "{reported_date_gte}"')
        if reported_date_lt is not None:
            filter_params.append(f'reportedDate < "{reported_date_lt}"')
        if reported_date_lte is not None:
            filter_params.append(f'reportedDate <= "{reported_date_lte}"')
        filter_params.append(list_to_filter("lastModified", last_modified))
        if last_modified_gt is not None:
            filter_params.append(f'lastModified > "{last_modified_gt}"')
        if last_modified_gte is not None:
            filter_params.append(f'lastModified >= "{last_modified_gte}"')
        if last_modified_lt is not None:
            filter_params.append(f'lastModified < "{last_modified_lt}"')
        if last_modified_lte is not None:
            filter_params.append(f'lastModified <= "{last_modified_lte}"')
        filter_params.append(list_to_filter("fromCountry", from_country))
        filter_params.append(list_to_filter("toCountry", to_country))
        filter_params.append(list_to_filter("fromSystemOperator", from_system_operator))
        filter_params.append(list_to_filter("toSystemOperator", to_system_operator))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("flowItemType", flow_item_type))
        filter_params.append(list_to_filter("isCurrentRevision", is_current_revision))
        filter_params.append(list_to_filter("infrastructureType", infrastructure_type))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/eugas/v2/outages/event",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_outages_time_series(
        self,
        *,
        flow_item_id: Optional[int] = None,
        flow_item_id_lt: Optional[int] = None,
        flow_item_id_lte: Optional[int] = None,
        flow_item_id_gt: Optional[int] = None,
        flow_item_id_gte: Optional[int] = None,
        flow_field_id: Optional[int] = None,
        flow_field_id_lt: Optional[int] = None,
        flow_field_id_lte: Optional[int] = None,
        flow_field_id_gt: Optional[int] = None,
        flow_field_id_gte: Optional[int] = None,
        gas_day: Optional[datetime] = None,
        gas_day_lt: Optional[datetime] = None,
        gas_day_lte: Optional[datetime] = None,
        gas_day_gt: Optional[datetime] = None,
        gas_day_gte: Optional[datetime] = None,
        start_date: Optional[datetime] = None,
        start_date_lt: Optional[datetime] = None,
        start_date_lte: Optional[datetime] = None,
        start_date_gt: Optional[datetime] = None,
        start_date_gte: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        end_date_lt: Optional[datetime] = None,
        end_date_lte: Optional[datetime] = None,
        end_date_gt: Optional[datetime] = None,
        end_date_gte: Optional[datetime] = None,
        point_name: Optional[Union[list[str], Series[str], str]] = None,
        reported_date: Optional[datetime] = None,
        reported_date_lt: Optional[datetime] = None,
        reported_date_lte: Optional[datetime] = None,
        reported_date_gt: Optional[datetime] = None,
        reported_date_gte: Optional[datetime] = None,
        last_modified: Optional[datetime] = None,
        last_modified_lt: Optional[datetime] = None,
        last_modified_lte: Optional[datetime] = None,
        last_modified_gt: Optional[datetime] = None,
        last_modified_gte: Optional[datetime] = None,
        from_country: Optional[Union[list[str], Series[str], str]] = None,
        to_country: Optional[Union[list[str], Series[str], str]] = None,
        from_system_operator: Optional[Union[list[str], Series[str], str]] = None,
        to_system_operator: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        flow_item_type: Optional[Union[list[str], Series[str], str]] = None,
        infrastructure_type: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access Eugas Outage events data in Time Series format.

        Parameters
        ----------

         flow_item_id: Optional[int], optional
             A unique identificator of the flow item for the EU gas outages., by default None
         flow_item_id_gt: Optional[int], optional
             filter by `flow_item_id > x`, by default None
         flow_item_id_gte: Optional[int], optional
             filter by `flow_item_id >= x`, by default None
         flow_item_id_lt: Optional[int], optional
             filter by `flow_item_id < x`, by default None
         flow_item_id_lte: Optional[int], optional
             filter by `flow_item_id <= x`, by default None
         flow_field_id: Optional[int], optional
             A unique identificator of the flow field for the EU gas outages., by default None
         flow_field_id_gt: Optional[int], optional
             filter by `flow_field_id > x`, by default None
         flow_field_id_gte: Optional[int], optional
             filter by `flow_field_id >= x`, by default None
         flow_field_id_lt: Optional[int], optional
             filter by `flow_field_id < x`, by default None
         flow_field_id_lte: Optional[int], optional
             filter by `flow_field_id <= x`, by default None
         gas_day: Optional[datetime], optional
             The date or gasday for which the outage data is applicable., by default None
         gas_day_gt: Optional[datetime], optional
             filter by `gas_day > x`, by default None
         gas_day_gte: Optional[datetime], optional
             filter by `gas_day >= x`, by default None
         gas_day_lt: Optional[datetime], optional
             filter by `gas_day < x`, by default None
         gas_day_lte: Optional[datetime], optional
             filter by `gas_day <= x`, by default None
         start_date: Optional[datetime], optional
             The start date of the outage., by default None
         start_date_gt: Optional[datetime], optional
             filter by `start_date > x`, by default None
         start_date_gte: Optional[datetime], optional
             filter by `start_date >= x`, by default None
         start_date_lt: Optional[datetime], optional
             filter by `start_date < x`, by default None
         start_date_lte: Optional[datetime], optional
             filter by `start_date <= x`, by default None
         end_date: Optional[datetime], optional
             The end date of the outage., by default None
         end_date_gt: Optional[datetime], optional
             filter by `end_date > x`, by default None
         end_date_gte: Optional[datetime], optional
             filter by `end_date >= x`, by default None
         end_date_lt: Optional[datetime], optional
             filter by `end_date < x`, by default None
         end_date_lte: Optional[datetime], optional
             filter by `end_date <= x`, by default None
         point_name: Optional[Union[list[str], Series[str], str]]
             Name of the European gas outages point., by default None
         reported_date: Optional[datetime], optional
             The Date when outage was reported., by default None
         reported_date_gt: Optional[datetime], optional
             filter by `reported_date > x`, by default None
         reported_date_gte: Optional[datetime], optional
             filter by `reported_date >= x`, by default None
         reported_date_lt: Optional[datetime], optional
             filter by `reported_date < x`, by default None
         reported_date_lte: Optional[datetime], optional
             filter by `reported_date <= x`, by default None
         last_modified: Optional[datetime], optional
             The Date when outage event was last changed., by default None
         last_modified_gt: Optional[datetime], optional
             filter by `last_modified > x`, by default None
         last_modified_gte: Optional[datetime], optional
             filter by `last_modified >= x`, by default None
         last_modified_lt: Optional[datetime], optional
             filter by `last_modified < x`, by default None
         last_modified_lte: Optional[datetime], optional
             filter by `last_modified <= x`, by default None
         from_country: Optional[Union[list[str], Series[str], str]]
             Name of the country from which the outage is happening., by default None
         to_country: Optional[Union[list[str], Series[str], str]]
             Name of the country to which the outage is happening., by default None
         from_system_operator: Optional[Union[list[str], Series[str], str]]
             Name of the operator from which the outage is happening., by default None
         to_system_operator: Optional[Union[list[str], Series[str], str]]
             Name of the operator to which the gas outage is happening., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used for quantifying gas flow., by default None
         flow_item_type: Optional[Union[list[str], Series[str], str]]
             Type of the flow item for EU gas outages., by default None
         infrastructure_type: Optional[Union[list[str], Series[str], str]]
             The type of infrastructure associated with the outage., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("flowItemId", flow_item_id))
        if flow_item_id_gt is not None:
            filter_params.append(f'flowItemId > "{flow_item_id_gt}"')
        if flow_item_id_gte is not None:
            filter_params.append(f'flowItemId >= "{flow_item_id_gte}"')
        if flow_item_id_lt is not None:
            filter_params.append(f'flowItemId < "{flow_item_id_lt}"')
        if flow_item_id_lte is not None:
            filter_params.append(f'flowItemId <= "{flow_item_id_lte}"')
        filter_params.append(list_to_filter("flowFieldId", flow_field_id))
        if flow_field_id_gt is not None:
            filter_params.append(f'flowFieldId > "{flow_field_id_gt}"')
        if flow_field_id_gte is not None:
            filter_params.append(f'flowFieldId >= "{flow_field_id_gte}"')
        if flow_field_id_lt is not None:
            filter_params.append(f'flowFieldId < "{flow_field_id_lt}"')
        if flow_field_id_lte is not None:
            filter_params.append(f'flowFieldId <= "{flow_field_id_lte}"')
        filter_params.append(list_to_filter("gasDay", gas_day))
        if gas_day_gt is not None:
            filter_params.append(f'gasDay > "{gas_day_gt}"')
        if gas_day_gte is not None:
            filter_params.append(f'gasDay >= "{gas_day_gte}"')
        if gas_day_lt is not None:
            filter_params.append(f'gasDay < "{gas_day_lt}"')
        if gas_day_lte is not None:
            filter_params.append(f'gasDay <= "{gas_day_lte}"')
        filter_params.append(list_to_filter("startDate", start_date))
        if start_date_gt is not None:
            filter_params.append(f'startDate > "{start_date_gt}"')
        if start_date_gte is not None:
            filter_params.append(f'startDate >= "{start_date_gte}"')
        if start_date_lt is not None:
            filter_params.append(f'startDate < "{start_date_lt}"')
        if start_date_lte is not None:
            filter_params.append(f'startDate <= "{start_date_lte}"')
        filter_params.append(list_to_filter("endDate", end_date))
        if end_date_gt is not None:
            filter_params.append(f'endDate > "{end_date_gt}"')
        if end_date_gte is not None:
            filter_params.append(f'endDate >= "{end_date_gte}"')
        if end_date_lt is not None:
            filter_params.append(f'endDate < "{end_date_lt}"')
        if end_date_lte is not None:
            filter_params.append(f'endDate <= "{end_date_lte}"')
        filter_params.append(list_to_filter("pointName", point_name))
        filter_params.append(list_to_filter("reportedDate", reported_date))
        if reported_date_gt is not None:
            filter_params.append(f'reportedDate > "{reported_date_gt}"')
        if reported_date_gte is not None:
            filter_params.append(f'reportedDate >= "{reported_date_gte}"')
        if reported_date_lt is not None:
            filter_params.append(f'reportedDate < "{reported_date_lt}"')
        if reported_date_lte is not None:
            filter_params.append(f'reportedDate <= "{reported_date_lte}"')
        filter_params.append(list_to_filter("lastModified", last_modified))
        if last_modified_gt is not None:
            filter_params.append(f'lastModified > "{last_modified_gt}"')
        if last_modified_gte is not None:
            filter_params.append(f'lastModified >= "{last_modified_gte}"')
        if last_modified_lt is not None:
            filter_params.append(f'lastModified < "{last_modified_lt}"')
        if last_modified_lte is not None:
            filter_params.append(f'lastModified <= "{last_modified_lte}"')
        filter_params.append(list_to_filter("fromCountry", from_country))
        filter_params.append(list_to_filter("toCountry", to_country))
        filter_params.append(list_to_filter("fromSystemOperator", from_system_operator))
        filter_params.append(list_to_filter("toSystemOperator", to_system_operator))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("flowItemType", flow_item_type))
        filter_params.append(list_to_filter("infrastructureType", infrastructure_type))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/eugas/v2/outages/time-series",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    

    def get_supply_demand_short_term_forecast(
        self,
        *,
        forecast_week: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        dataset: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        level1: Optional[Union[list[str], Series[str], str]] = None,
        level2: Optional[Union[list[str], Series[str], str]] = None,
        level3: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access Weekly Supply, Demand, Bidirectional and Storage forecast for European Gas markets.

        Parameters
        ----------

         forecast_week: Optional[Union[list[str], Series[str], str]]
             The week of the forecast, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measurement, by default None
         dataset: Optional[Union[list[str], Series[str], str]]
             The dataset providing this data, by default None
         region: Optional[Union[list[str], Series[str], str]]
             The code for the region, by default None
         level1: Optional[Union[list[str], Series[str], str]]
             The top-level classification of this dataset, by default None
         level2: Optional[Union[list[str], Series[str], str]]
             The level 2 classification of this dataset, by default None
         level3: Optional[Union[list[str], Series[str], str]]
             The level 3 classification of this dataset, by default None
         date: Optional[date], optional
             The date forecast, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         modified_date: Optional[datetime], optional
             The date when this data was imported., by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("forecastWeek", forecast_week))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("dataset", dataset))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("level1", level1))
        filter_params.append(list_to_filter("level2", level2))
        filter_params.append(list_to_filter("level3", level3))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
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
            path=f"/eugas/v2/analytics/supply-demand/short-term-forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_supply_demand_forecast_switching(
        self,
        *,
        forecast_week: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        dataset: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        level1: Optional[Union[list[str], Series[str], str]] = None,
        level2: Optional[Union[list[str], Series[str], str]] = None,
        level3: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access Weekly Coal to Gas Switching Forecast for European Gas Markets.

        Parameters
        ----------

         forecast_week: Optional[Union[list[str], Series[str], str]]
             The week of the forecast, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measurement, by default None
         dataset: Optional[Union[list[str], Series[str], str]]
             The dataset providing this data, by default None
         region: Optional[Union[list[str], Series[str], str]]
             The code for the region, by default None
         level1: Optional[Union[list[str], Series[str], str]]
             The top-level classification of this dataset, by default None
         level2: Optional[Union[list[str], Series[str], str]]
             The level 2 classification of this dataset, by default None
         level3: Optional[Union[list[str], Series[str], str]]
             The level 3 classification of this dataset, by default None
         date: Optional[date], optional
             The date forecast, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         modified_date: Optional[datetime], optional
             The date when this data was imported., by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("forecastWeek", forecast_week))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("dataset", dataset))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("level1", level1))
        filter_params.append(list_to_filter("level2", level2))
        filter_params.append(list_to_filter("level3", level3))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
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
            path=f"/eugas/v2/analytics/supply-demand/forecast/switching",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_price_forecast(
        self,
        *,
        category: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[datetime] = None,
        date_lt: Optional[datetime] = None,
        date_lte: Optional[datetime] = None,
        date_gt: Optional[datetime] = None,
        date_gte: Optional[datetime] = None,
        forecast_week: Optional[Union[list[str], Series[str], str]] = None,
        hub: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access Weekly price forecast for TTF, NBP and PSV European Gas Hubs.

        Parameters
        ----------

         category: Optional[Union[list[str], Series[str], str]]
             The category of the forecast data, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit for the value, by default None
         date: Optional[datetime], optional
             Date for the imported data, by default None
         date_gt: Optional[datetime], optional
             filter by `date > x`, by default None
         date_gte: Optional[datetime], optional
             filter by `date >= x`, by default None
         date_lt: Optional[datetime], optional
             filter by `date < x`, by default None
         date_lte: Optional[datetime], optional
             filter by `date <= x`, by default None
         forecast_week: Optional[Union[list[str], Series[str], str]]
             The month imported, by default None
         hub: Optional[Union[list[str], Series[str], str]]
             The hub code, by default None
         region: Optional[Union[list[str], Series[str], str]]
             The code for the region, by default None
         modified_date: Optional[datetime], optional
             The date when this data was imported., by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("forecastWeek", forecast_week))
        filter_params.append(list_to_filter("hub", hub))
        filter_params.append(list_to_filter("region", region))
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
            path=f"/eugas/v2/analytics/price/forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    

    def get_daily_country_overview(
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
        from_country: Optional[Union[list[str], Series[str], str]] = None,
        from_system_operator: Optional[Union[list[str], Series[str], str]] = None,
        to_system_operator: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
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
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve The daily supply and demand overview of the specified country.

        Parameters
        ----------

         id: Optional[int], optional
             The unique identifier for the gas data field., by default None
         id_gt: Optional[int], optional
             filter by `id > x`, by default None
         id_gte: Optional[int], optional
             filter by `id >= x`, by default None
         id_lt: Optional[int], optional
             filter by `id < x`, by default None
         id_lte: Optional[int], optional
             filter by `id <= x`, by default None
         gas_day: Optional[date], optional
             The date for which the gas data is recorded or applicable., by default None
         gas_day_gt: Optional[date], optional
             filter by `gas_day > x`, by default None
         gas_day_gte: Optional[date], optional
             filter by `gas_day >= x`, by default None
         gas_day_lt: Optional[date], optional
             filter by `gas_day < x`, by default None
         gas_day_lte: Optional[date], optional
             filter by `gas_day <= x`, by default None
         main_flow_type: Optional[Union[list[str], Series[str], str]]
             The main type of gas flow, such as pipeline or LNG terminal., by default None
         secondary_flow_type: Optional[Union[list[str], Series[str], str]]
             The secondary type of gas flow, such as production or storage., by default None
         detailed_flow_type: Optional[Union[list[str], Series[str], str]]
             The detailed or specific type of gas flow, such as landing point or LNG terminal., by default None
         from_country: Optional[Union[list[str], Series[str], str]]
             The country from which the gas is being transported or delivered., by default None
         from_system_operator: Optional[Union[list[str], Series[str], str]]
             The system operator responsible for the gas transportation or delivery from the specified country., by default None
         to_system_operator: Optional[Union[list[str], Series[str], str]]
             The system operator responsible for the gas transportation or delivery to the specified country., by default None
         country: Optional[Union[list[str], Series[str], str]]
             The country from which the gas is being transported or delivered or the country to which the gas is being transported or delivered, by default None
         name: Optional[Union[list[str], Series[str], str]]
             The name or identifier of the specific flow point or location in the gas network., by default None
         gas_type: Optional[Union[list[str], Series[str], str]]
             The type of gas being transported or delivered, such as H-gas (high-calorific gas)., by default None
         source: Optional[Union[list[str], Series[str], str]]
             The source or origin of the gas being transported or delivered, such as a specific gas facility or company., by default None
         direction: Optional[Union[list[str], Series[str], str]]
             The direction of gas flow at the specified flow point, such as net flow or total flow., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used for quantifying the gas flow at the specified flow point, such as MCM (million cubic meters)., by default None
         modified_date: Optional[datetime], optional
             The date and time when the gas data field was last modified or updated., by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         default_source: Optional[Union[list[str], Series[str], str]]
             Indicates whether the specified flow point is the default source for gas flow data., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
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
        filter_params.append(list_to_filter("fromCountry", from_country))
        filter_params.append(list_to_filter("fromSystemOperator", from_system_operator))
        filter_params.append(list_to_filter("toSystemOperator", to_system_operator))
        filter_params.append(list_to_filter("country", country))
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
            path=f"/eugas/v1/daily/country-overview",
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

        date_columns = [
            "gasDay",
            "modifiedDate",
            "applicableAt",
            "gasFlowTime",
            "productionMonth",
            "reportedDate",
            "endDate",
            "createDate",
            "startDate",
            "lastModified",
        ]

        for column in date_columns:
            if column in df.columns:
                if parse(pd.__version__) >= parse("2"):
                    df[column] = pd.to_datetime(
                        df[column], utc=True, format="ISO8601", errors="coerce"
                    )
                else:
                    df[column] = pd.to_datetime(df[column], errors="coerce", utc=True)  # type: ignore

        if "dayMonthOrdinal" in df.columns:
            sorted_categories = sorted(
                df["dayMonthOrdinal"].unique(), key=_custom_sort_key
            )
            df["dayMonthOrdinal"] = pd.Categorical(
                df["dayMonthOrdinal"], categories=sorted_categories, ordered=True
            )

        return df
