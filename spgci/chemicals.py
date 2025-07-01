from __future__ import annotations
from typing import List, Optional, Union, Literal
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date, datetime
from packaging.version import parse
import pandas as pd


class Chemicals:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _ts_owner_capacity_event_endpoint = "/capacity-events"
    _ts_owner_avg_annual_capacity_endpoint = "/average-annual-capacities"
    _ts_owner_capacity_consume_endpoint = "/capacity-to-consume"

    _datasets = Literal[
        "capacity",
        "production",
        "capacity-utilization",
        "demand-by-derivative",
        "demand-by-end-use",
        "trade",
        "inventory-change",
        "total-supply",
        "total-demand",
        "assumptions",
        "country-supply-demand-balance",
        "region-supply-demand-balance",
        "world-supply-demand-balance",
        "long-term_prices",
        "short-term-prices",
        "capacity-events",
        "average-annual-capacities",
        "capacity-to-consume",
        "outages",
        "time-series-outages",
    ]

    def get_unique_values(
        self,
        dataset: _datasets,
        columns: Optional[Union[list[str], str]],
    ) -> DataFrame:
        dataset_to_path = {
            "capacity": "analytics/v1/chemicals/capacity",
            "production": "analytics/v1/chemicals/production",
            "capacity-utilization": "analytics/v1/chemicals/capacity-utilization",
            "demand-by-derivative": "analytics/v1/chemicals/demand-by-derivative",
            "demand-by-end-use": "analytics/v1/chemicals/demand-by-end-use",
            "trade": "analytics/v1/chemicals/trade",
            "inventory-change": "analytics/v1/chemicals/inventory-change",
            "total-supply": "analytics/v1/chemicals/total-supply",
            "total-demand": "analytics/v1/chemicals/total-demand",
            "assumptions": "analytics/v1/chemicals/assumptions",
            "country-supply-demand-balance": "analytics/v1/chemicals/country-supply-demand-balance",
            "region-supply-demand-balance": "analytics/v1/chemicals/region-supply-demand-balance",
            "world-supply-demand-balance": "analytics/v1/chemicals/world-supply-demand-balance",
            "long-term_prices": "analytics/v1/chemicals/price-forecast/long-term-prices",
            "short-term-prices": "analytics/v1/chemicals/price-forecast/short-term-prices",
            "capacity-events": "analytics/v1/chemicals/assets/capacity-events",
            "average-annual-capacities": "analytics/v1/chemicals/assets/average-annual-capacities",
            "capacity-to-consume": "analytics/v1/chemicals/assets/capacity-to-consume",
            "outages": "analytics/v1/chemicals/assets/outages",
            "time-series-outages": "analytics/v1/chemicals/assets/outages/time-series-outages",
        }

        if dataset not in dataset_to_path:
            valid = "\n".join(dataset_to_path.keys())
            print(f"Dataset '{dataset}' not found. Valid Datasets:\n", valid)
            raise ValueError(
                f"dataset '{dataset}' not found ",
            )
            return
        else:
            path = dataset_to_path[dataset]

        col_value = ", ".join(columns) if isinstance(columns, list) else columns or ""
        params = {"GroupBy": col_value, "pageSize": 5000}

        def to_df(resp: Response):
            j = resp.json()
            return DataFrame(j["aggResultValue"])

        return get_data(path, params, to_df, paginate=True)

    def get_outages(
        self,
        *,
        unit_name: Optional[Union[list[str], Series[str], str]] = None,
        production_unit_code: Optional[Union[list[str], Series[str], str]] = None,
        alert_status: Optional[Union[list[str], Series[str], str]] = None,
        outage_id: Optional[Union[list[str], Series[str], str]] = None,
        plant_code: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        owner: Optional[Union[list[str], Series[str], str]] = None,
        outage_type: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        capacity: Optional[float] = None,
        capacity_lt: Optional[float] = None,
        capacity_lte: Optional[float] = None,
        capacity_gt: Optional[float] = None,
        capacity_gte: Optional[float] = None,
        capacity_down: Optional[float] = None,
        capacity_down_lt: Optional[float] = None,
        capacity_down_lte: Optional[float] = None,
        capacity_down_gt: Optional[float] = None,
        capacity_down_gte: Optional[float] = None,
        run_rate: Optional[float] = None,
        run_rate_lt: Optional[float] = None,
        run_rate_lte: Optional[float] = None,
        run_rate_gt: Optional[float] = None,
        run_rate_gte: Optional[float] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
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
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Plant outage data including run rates, capacity loss, estimated start/end dates and products affected

        Parameters
        ----------

         unit_name: Optional[Union[list[str], Series[str], str]]
             Name for Production Unit, by default None
         production_unit_code: Optional[Union[list[str], Series[str], str]]
             Production Unit ID (Asset ID), by default None
         alert_status: Optional[Union[list[str], Series[str], str]]
             Alert Status (like Alert, Confirmed, Estimate, Revised Confirmed), by default None
         outage_id: Optional[Union[list[str], Series[str], str]]
             Outage ID, by default None
         plant_code: Optional[Union[list[str], Series[str], str]]
             Plant ID, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         owner: Optional[Union[list[str], Series[str], str]]
             Plant operator (producer), by default None
         outage_type: Optional[Union[list[str], Series[str], str]]
             Outage Type (like Planned, Unplanned, Economic Run Cut etc), by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         capacity: Optional[float], optional
             Capacity Value, by default None
         capacity_gt: Optional[float], optional
             filter by '' capacity > x '', by default None
         capacity_gte: Optional[float], optional
             filter by capacity, by default None
         capacity_lt: Optional[float], optional
             filter by capacity, by default None
         capacity_lte: Optional[float], optional
             filter by capacity, by default None
         capacity_down: Optional[float], optional
             Capacity Loss, by default None
         capacity_down_gt: Optional[float], optional
             filter by '' capacity_down > x '', by default None
         capacity_down_gte: Optional[float], optional
             filter by capacity_down, by default None
         capacity_down_lt: Optional[float], optional
             filter by capacity_down, by default None
         capacity_down_lte: Optional[float], optional
             filter by capacity_down, by default None
         run_rate: Optional[float], optional
             Run Rate, by default None
         run_rate_gt: Optional[float], optional
             filter by '' run_rate > x '', by default None
         run_rate_gte: Optional[float], optional
             filter by run_rate, by default None
         run_rate_lt: Optional[float], optional
             filter by run_rate, by default None
         run_rate_lte: Optional[float], optional
             filter by run_rate, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         start_date: Optional[datetime], optional
             Start Date, by default None
         start_date_gt: Optional[datetime], optional
             filter by '' start_date > x '', by default None
         start_date_gte: Optional[datetime], optional
             filter by start_date, by default None
         start_date_lt: Optional[datetime], optional
             filter by start_date, by default None
         start_date_lte: Optional[datetime], optional
             filter by start_date, by default None
         end_date: Optional[datetime], optional
             End Date, by default None
         end_date_gt: Optional[datetime], optional
             filter by '' end_date > x '', by default None
         end_date_gte: Optional[datetime], optional
             filter by end_date, by default None
         end_date_lt: Optional[datetime], optional
             filter by end_date, by default None
         end_date_lte: Optional[datetime], optional
             filter by end_date, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("unit_name", unit_name))
        filter_params.append(list_to_filter("productionUnitCode", production_unit_code))
        filter_params.append(list_to_filter("alertStatus", alert_status))
        filter_params.append(list_to_filter("outageId", outage_id))
        filter_params.append(list_to_filter("plantCode", plant_code))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("owner", owner))
        filter_params.append(list_to_filter("outageType", outage_type))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("capacity", capacity))
        if capacity_gt is not None:
            filter_params.append(f'capacity > "{capacity_gt}"')
        if capacity_gte is not None:
            filter_params.append(f'capacity >= "{capacity_gte}"')
        if capacity_lt is not None:
            filter_params.append(f'capacity < "{capacity_lt}"')
        if capacity_lte is not None:
            filter_params.append(f'capacity <= "{capacity_lte}"')
        filter_params.append(list_to_filter("capacity_down", capacity_down))
        if capacity_down_gt is not None:
            filter_params.append(f'capacityDown > "{capacity_down_gt}"')
        if capacity_down_gte is not None:
            filter_params.append(f'capacityDown >= "{capacity_down_gte}"')
        if capacity_down_lt is not None:
            filter_params.append(f'capacityDown < "{capacity_down_lt}"')
        if capacity_down_lte is not None:
            filter_params.append(f'capacityDown <= "{capacity_down_lte}"')
        filter_params.append(list_to_filter("runRate", run_rate))
        if run_rate_gt is not None:
            filter_params.append(f'runRate > "{run_rate_gt}"')
        if run_rate_gte is not None:
            filter_params.append(f'runRate >= "{run_rate_gte}"')
        if run_rate_lt is not None:
            filter_params.append(f'runRate < "{run_rate_lt}"')
        if run_rate_lte is not None:
            filter_params.append(f'runRate <= "{run_rate_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')
        filter_params.append(list_to_filter("start_date", start_date))
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

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/assets/outages/",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_time_series_outages(
        self,
        *,
        unit_name: Optional[Union[list[str], Series[str], str]] = None,
        production_unit_code: Optional[Union[list[str], Series[str], str]] = None,
        alert_status: Optional[Union[list[str], Series[str], str]] = None,
        outage_id: Optional[Union[list[str], Series[str], str]] = None,
        plant_code: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        owner: Optional[Union[list[str], Series[str], str]] = None,
        outage_type: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        capacity: Optional[float] = None,
        capacity_lt: Optional[float] = None,
        capacity_lte: Optional[float] = None,
        capacity_gt: Optional[float] = None,
        capacity_gte: Optional[float] = None,
        capacity_down: Optional[str] = None,
        capacity_down_lt: Optional[str] = None,
        capacity_down_lte: Optional[str] = None,
        capacity_down_gt: Optional[str] = None,
        capacity_down_gte: Optional[str] = None,
        run_rate: Optional[float] = None,
        run_rate_lt: Optional[float] = None,
        run_rate_lte: Optional[float] = None,
        run_rate_gt: Optional[float] = None,
        run_rate_gte: Optional[float] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        date: Optional[datetime] = None,
        date_lt: Optional[datetime] = None,
        date_lte: Optional[datetime] = None,
        date_gt: Optional[datetime] = None,
        date_gte: Optional[datetime] = None,
        duration: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Time series monthly plant outage data including run rates, capacity loss and products affected

        Parameters
        ----------

         unit_name: Optional[Union[list[str], Series[str], str]]
             Name for Production Unit, by default None
         production_unit_code: Optional[Union[list[str], Series[str], str]]
             Production Unit ID (Asset ID), by default None
         alert_status: Optional[Union[list[str], Series[str], str]]
             Alert Status (like Alert, Confirmed, Estimate, Revised Confirmed), by default None
         outage_id: Optional[Union[list[str], Series[str], str]]
             Outage ID, by default None
         plant_code: Optional[Union[list[str], Series[str], str]]
             Plant ID, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         owner: Optional[Union[list[str], Series[str], str]]
             Plant operator (producer), by default None
         outage_type: Optional[Union[list[str], Series[str], str]]
             Outage Type (like Planned, Unplanned, Economic Run Cut etc), by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         capacity: Optional[float], optional
             Capacity Value, by default None
         capacity_gt: Optional[float], optional
             filter by `capacity > x`, by default None
         capacity_gte: Optional[float], optional
             filter by `capacity >= x`, by default None
         capacity_lt: Optional[float], optional
             filter by `capacity < x`, by default None
         capacity_lte: Optional[float], optional
             filter by `capacity <= x`, by default None
         capacity_down: Optional[str], optional
             Capacity Loss, by default None
         capacity_down_gt: Optional[str], optional
             filter by `capacity_down > x`, by default None
         capacity_down_gte: Optional[str], optional
             filter by `capacity_down >= x`, by default None
         capacity_down_lt: Optional[str], optional
             filter by `capacity_down < x`, by default None
         capacity_down_lte: Optional[str], optional
             filter by `capacity_down <= x`, by default None
         run_rate: Optional[float], optional
             Run Rate, by default None
         run_rate_gt: Optional[float], optional
             filter by `run_rate > x`, by default None
         run_rate_gte: Optional[float], optional
             filter by `run_rate >= x`, by default None
         run_rate_lt: Optional[float], optional
             filter by `run_rate < x`, by default None
         run_rate_lte: Optional[float], optional
             filter by `run_rate <= x`, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         date: Optional[datetime], optional
             Date, by default None
         date_gt: Optional[datetime], optional
             filter by `date > x`, by default None
         date_gte: Optional[datetime], optional
             filter by `date >= x`, by default None
         date_lt: Optional[datetime], optional
             filter by `date < x`, by default None
         date_lte: Optional[datetime], optional
             filter by `date <= x`, by default None
         duration: Optional[Union[list[str], Series[str], str]]
             Number of outage days for the plant that month, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("unit_name", unit_name))
        filter_params.append(list_to_filter("productionUnitCode", production_unit_code))
        filter_params.append(list_to_filter("alertStatus", alert_status))
        filter_params.append(list_to_filter("outage_id", outage_id))
        filter_params.append(list_to_filter("plant_code", plant_code))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("owner", owner))
        filter_params.append(list_to_filter("outage_type", outage_type))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("capacity", capacity))
        if capacity_gt is not None:
            filter_params.append(f'capacity > "{capacity_gt}"')
        if capacity_gte is not None:
            filter_params.append(f'capacity >= "{capacity_gte}"')
        if capacity_lt is not None:
            filter_params.append(f'capacity < "{capacity_lt}"')
        if capacity_lte is not None:
            filter_params.append(f'capacity <= "{capacity_lte}"')
        filter_params.append(list_to_filter("capacity_down", capacity_down))
        if capacity_down_gt is not None:
            filter_params.append(f'capacity_down > "{capacity_down_gt}"')
        if capacity_down_gte is not None:
            filter_params.append(f'capacity_down >= "{capacity_down_gte}"')
        if capacity_down_lt is not None:
            filter_params.append(f'capacity_down < "{capacity_down_lt}"')
        if capacity_down_lte is not None:
            filter_params.append(f'capacity_down <= "{capacity_down_lte}"')
        filter_params.append(list_to_filter("runRate", run_rate))
        if run_rate_gt is not None:
            filter_params.append(f'runRate > "{run_rate_gt}"')
        if run_rate_gte is not None:
            filter_params.append(f'runRate >= "{run_rate_gte}"')
        if run_rate_lt is not None:
            filter_params.append(f'runRate < "{run_rate_lt}"')
        if run_rate_lte is not None:
            filter_params.append(f'runRate <= "{run_rate_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("duration", duration))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/assets/outages/time-series-outages",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_capacity_events(
        self,
        *,
        production_unit_code: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        production_route: Optional[Union[list[str], Series[str], str]] = None,
        plant_code: Optional[Union[list[str], Series[str], str]] = None,
        plant_name: Optional[Union[list[str], Series[str], str]] = None,
        unit_name: Optional[Union[list[str], Series[str], str]] = None,
        city: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        event_begin_date: Optional[date] = None,
        event_begin_date_lt: Optional[date] = None,
        event_begin_date_lte: Optional[date] = None,
        event_begin_date_gt: Optional[date] = None,
        event_begin_date_gte: Optional[date] = None,
        event_type: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        owner: Optional[Union[list[str], Series[str], str]] = None,
        ownership_period: Optional[Union[list[str], Series[str], str]] = None,
        valid_from: Optional[datetime] = None,
        valid_from_lt: Optional[datetime] = None,
        valid_from_lte: Optional[datetime] = None,
        valid_from_gt: Optional[datetime] = None,
        valid_from_gte: Optional[datetime] = None,
        valid_to: Optional[datetime] = None,
        valid_to_lt: Optional[datetime] = None,
        valid_to_lte: Optional[datetime] = None,
        valid_to_gt: Optional[datetime] = None,
        valid_to_gte: Optional[datetime] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        reason: Optional[Union[list[str], Series[str], str]] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = True,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Global chemical production capacity events (such as expand, reduce, startup, shutdown, etc.) by plant with company, location and production route details

        Parameters
        ----------

         production_unit_code: Optional[Union[list[str], Series[str], str]]
             Production Unit ID (Asset ID), by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity) , by default None
         production_route: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, by default None
         plant_code: Optional[Union[list[str], Series[str], str]]
             Plant ID, by default None
         plant_name: Optional[Union[list[str], Series[str], str]]
             Name for Plant, by default None
         unit_name: Optional[Union[list[str], Series[str], str]]
             Name for Production Unit, by default None
         city: Optional[Union[list[str], Series[str], str]]
             Name for City / Settlement (geography), by default None
         state: Optional[Union[list[str], Series[str], str]]
             Name for State or province (geography), by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         event_begin_date: Optional[date], optional
             Date of Event, by default None
         event_begin_date_gt: Optional[date], optional
             filter by '' event_begin_date > x '', by default None
         event_begin_date_gte: Optional[date], optional
             filter by event_begin_date, by default None
         event_begin_date_lt: Optional[date], optional
             filter by event_begin_date, by default None
         event_begin_date_lte: Optional[date], optional
             filter by event_begin_date, by default None
         event_type: Optional[Union[list[str], Series[str], str]]
             Event Type (like Expand, Reduce, Startup, Shutdown, Restart etc.), by default None
         value: Optional[float], optional
             Data Value, by default None
         value_gt: Optional[float], optional
             filter by '' value > x '', by default None
         value_gte: Optional[float], optional
             filter by value, by default None
         value_lt: Optional[float], optional
             filter by value, by default None
         value_lte: Optional[float], optional
             filter by value, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         owner: Optional[Union[list[str], Series[str], str]]
             Plant operator (producer), by default None
         ownership_period: Optional[Union[list[str], Series[str], str]]
             The period a plant operator (producer) owns the facility, by default None
         valid_from: Optional[datetime], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[datetime], optional
             filter by '' valid_from > x '', by default None
         valid_from_gte: Optional[datetime], optional
             filter by valid_from, by default None
         valid_from_lt: Optional[datetime], optional
             filter by valid_from, by default None
         valid_from_lte: Optional[datetime], optional
             filter by valid_from, by default None
         valid_to: Optional[datetime], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[datetime], optional
             filter by '' valid_to > x '', by default None
         valid_to_gte: Optional[datetime], optional
             filter by valid_to, by default None
         valid_to_lt: Optional[datetime], optional
             filter by valid_to, by default None
         valid_to_lte: Optional[datetime], optional
             filter by valid_to, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         reason: Optional[Union[list[str], Series[str], str]]
             Reason for having this record, by default None
         is_active: Optional[Union[list[bool], Series[bool], bool]]
             If the record is active, by default True
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("productionUnitCode", production_unit_code))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("productionRoute", production_route))
        filter_params.append(list_to_filter("plantCode", plant_code))
        filter_params.append(list_to_filter("plantName", plant_name))
        filter_params.append(list_to_filter("unitName", unit_name))
        filter_params.append(list_to_filter("city", city))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("eventBeginDate", event_begin_date))
        if event_begin_date_gt is not None:
            filter_params.append(f'eventBeginDate > "{event_begin_date_gt}"')
        if event_begin_date_gte is not None:
            filter_params.append(f'eventBeginDate >= "{event_begin_date_gte}"')
        if event_begin_date_lt is not None:
            filter_params.append(f'eventBeginDate < "{event_begin_date_lt}"')
        if event_begin_date_lte is not None:
            filter_params.append(f'eventBeginDate <= "{event_begin_date_lte}"')
        filter_params.append(list_to_filter("eventType", event_type))
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("owner", owner))
        filter_params.append(list_to_filter("ownershipPeriod", ownership_period))
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')
        filter_params.append(list_to_filter("reason", reason))
        filter_params.append(list_to_filter("isActive", is_active))

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
        self,
        *,
        production_unit_code: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        production_route: Optional[Union[list[str], Series[str], str]] = None,
        plant_code: Optional[Union[list[str], Series[str], str]] = None,
        plant_name: Optional[Union[list[str], Series[str], str]] = None,
        unit_name: Optional[Union[list[str], Series[str], str]] = None,
        city: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        average_annual_capacity: Optional[float] = None,
        average_annual_capacity_lt: Optional[float] = None,
        average_annual_capacity_lte: Optional[float] = None,
        average_annual_capacity_gt: Optional[float] = None,
        average_annual_capacity_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        owner: Optional[Union[list[str], Series[str], str]] = None,
        ownership_period: Optional[Union[list[str], Series[str], str]] = None,
        valid_from: Optional[datetime] = None,
        valid_from_lt: Optional[datetime] = None,
        valid_from_lte: Optional[datetime] = None,
        valid_from_gt: Optional[datetime] = None,
        valid_from_gte: Optional[datetime] = None,
        valid_to: Optional[datetime] = None,
        valid_to_lt: Optional[datetime] = None,
        valid_to_lte: Optional[datetime] = None,
        valid_to_gt: Optional[datetime] = None,
        valid_to_gte: Optional[datetime] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        reason: Optional[Union[list[str], Series[str], str]] = None,
        is_active: Optional[Union[list[bool], Series[bool], bool]] = True,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Annual global chemical production capacity by plant with company, location and production route details

        Parameters
        ----------

         production_unit_code: Optional[Union[list[str], Series[str], str]]
             Production Unit ID (Asset ID), by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity) , by default None
         production_route: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, by default None
         plant_code: Optional[Union[list[str], Series[str], str]]
             Plant ID, by default None
         plant_name: Optional[Union[list[str], Series[str], str]]
             Name for Plant, by default None
         unit_name: Optional[Union[list[str], Series[str], str]]
             Name for Production Unit, by default None
         city: Optional[Union[list[str], Series[str], str]]
             Name for City / Settlement (geography), by default None
         state: Optional[Union[list[str], Series[str], str]]
             Name for State or province (geography), by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         year: Optional[int], optional
             Date of Data value, by default None
         year_gt: Optional[int], optional
             filter by '' year > x '', by default None
         year_gte: Optional[int], optional
             filter by year, by default None
         year_lt: Optional[int], optional
             filter by year, by default None
         year_lte: Optional[int], optional
             filter by year, by default None
         average_annual_capacity: Optional[float], optional
             Data Value, by default None
         average_annual_capacity_gt: Optional[float], optional
             filter by '' average_annual_capacity > x '', by default None
         average_annual_capacity_gte: Optional[float], optional
             filter by average_annual_capacity, by default None
         average_annual_capacity_lt: Optional[float], optional
             filter by average_annual_capacity, by default None
         average_annual_capacity_lte: Optional[float], optional
             filter by average_annual_capacity, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         owner: Optional[Union[list[str], Series[str], str]]
             Plant operator (producer), by default None
         ownership_period: Optional[Union[list[str], Series[str], str]]
             The period a plant operator (producer) owns the facility, by default None
         valid_from: Optional[datetime], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[datetime], optional
             filter by '' valid_from > x '', by default None
         valid_from_gte: Optional[datetime], optional
             filter by valid_from, by default None
         valid_from_lt: Optional[datetime], optional
             filter by valid_from, by default None
         valid_from_lte: Optional[datetime], optional
             filter by valid_from, by default None
         valid_to: Optional[datetime], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[datetime], optional
             filter by '' valid_to > x '', by default None
         valid_to_gte: Optional[datetime], optional
             filter by valid_to, by default None
         valid_to_lt: Optional[datetime], optional
             filter by valid_to, by default None
         valid_to_lte: Optional[datetime], optional
             filter by valid_to, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         reason: Optional[Union[list[str], Series[str], str]]
             Reason for having this record, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default True
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("productionUnitCode", production_unit_code))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("productionRoute", production_route))
        filter_params.append(list_to_filter("plantCode", plant_code))
        filter_params.append(list_to_filter("plantName", plant_name))
        filter_params.append(list_to_filter("unitName", unit_name))
        filter_params.append(list_to_filter("city", city))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(
            list_to_filter("averageAnnualCapacity", average_annual_capacity)
        )
        if average_annual_capacity_gt is not None:
            filter_params.append(
                f'averageAnnualCapacity > "{average_annual_capacity_gt}"'
            )
        if average_annual_capacity_gte is not None:
            filter_params.append(
                f'averageAnnualCapacity >= "{average_annual_capacity_gte}"'
            )
        if average_annual_capacity_lt is not None:
            filter_params.append(
                f'averageAnnualCapacity < "{average_annual_capacity_lt}"'
            )
        if average_annual_capacity_lte is not None:
            filter_params.append(
                f'averageAnnualCapacity <= "{average_annual_capacity_lte}"'
            )
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("owner", owner))
        filter_params.append(list_to_filter("ownershipPeriod", ownership_period))
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')
        filter_params.append(list_to_filter("reason", reason))
        filter_params.append(list_to_filter("isActive", is_active))

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
        self,
        *,
        production_unit_code: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        production_route: Optional[Union[list[str], Series[str], str]] = None,
        plant_code: Optional[Union[list[str], Series[str], str]] = None,
        plant_name: Optional[Union[list[str], Series[str], str]] = None,
        unit_name: Optional[Union[list[str], Series[str], str]] = None,
        city: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        feedstock: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        owner: Optional[Union[list[str], Series[str], str]] = None,
        ownership_period: Optional[Union[list[str], Series[str], str]] = None,
        valid_from: Optional[datetime] = None,
        valid_from_lt: Optional[datetime] = None,
        valid_from_lte: Optional[datetime] = None,
        valid_from_gt: Optional[datetime] = None,
        valid_from_gte: Optional[datetime] = None,
        valid_to: Optional[datetime] = None,
        valid_to_lt: Optional[datetime] = None,
        valid_to_lte: Optional[datetime] = None,
        valid_to_gt: Optional[datetime] = None,
        valid_to_gte: Optional[datetime] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        reason: Optional[Union[list[str], Series[str], str]] = None,
        is_active: Optional[Union[list[bool], Series[bool], bool]] = True,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Capacities to consume by plant with company, location and production route details

        Parameters
        ----------

         production_unit_code: Optional[Union[list[str], Series[str], str]]
             Production Unit ID (Asset ID), by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity) , by default None
         production_route: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, by default None
         plant_code: Optional[Union[list[str], Series[str], str]]
             Plant ID, by default None
         plant_name: Optional[Union[list[str], Series[str], str]]
             Name for Plant, by default None
         unit_name: Optional[Union[list[str], Series[str], str]]
             Name for Production Unit, by default None
         city: Optional[Union[list[str], Series[str], str]]
             Name for City / Settlement (geography), by default None
         state: Optional[Union[list[str], Series[str], str]]
             Name for State or province (geography), by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         feedstock: Optional[Union[list[str], Series[str], str]]
             Raw material used in chemical production, by default None
         year: Optional[int], optional
             Date of Data value, by default None
         year_gt: Optional[int], optional
             filter by '' year > x '', by default None
         year_gte: Optional[int], optional
             filter by year, by default None
         year_lt: Optional[int], optional
             filter by year, by default None
         year_lte: Optional[int], optional
             filter by year, by default None
         value: Optional[float], optional
             Data Value, by default None
         value_gt: Optional[float], optional
             filter by '' value > x '', by default None
         value_gte: Optional[float], optional
             filter by value, by default None
         value_lt: Optional[float], optional
             filter by value, by default None
         value_lte: Optional[float], optional
             filter by value, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         owner: Optional[Union[list[str], Series[str], str]]
             Plant operator (producer), by default None
         ownership_period: Optional[Union[list[str], Series[str], str]]
             The period a plant operator (producer) owns the facility, by default None
         valid_from: Optional[datetime], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[datetime], optional
             filter by '' valid_from > x '', by default None
         valid_from_gte: Optional[datetime], optional
             filter by valid_from, by default None
         valid_from_lt: Optional[datetime], optional
             filter by valid_from, by default None
         valid_from_lte: Optional[datetime], optional
             filter by valid_from, by default None
         valid_to: Optional[datetime], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[datetime], optional
             filter by '' valid_to > x '', by default None
         valid_to_gte: Optional[datetime], optional
             filter by valid_to, by default None
         valid_to_lt: Optional[datetime], optional
             filter by valid_to, by default None
         valid_to_lte: Optional[datetime], optional
             filter by valid_to, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         reason: Optional[Union[list[str], Series[str], str]]
             Reason for having this record, by default None
         is_active: Optional[Union[list[bool], Series[bool], bool]]
             If the record is active, by default True
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("productionUnitCode", production_unit_code))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("productionRoute", production_route))
        filter_params.append(list_to_filter("plantCode", plant_code))
        filter_params.append(list_to_filter("plantName", plant_name))
        filter_params.append(list_to_filter("unitName", unit_name))
        filter_params.append(list_to_filter("city", city))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("feedstock", feedstock))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("owner", owner))
        filter_params.append(list_to_filter("ownershipPeriod", ownership_period))
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')
        filter_params.append(list_to_filter("reason", reason))
        filter_params.append(list_to_filter("isActive", is_active))

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

    def get_long_term_prices(
        self,
        *,
        scenario_id: Optional[Union[list[int], Series[int], int]] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        series_description: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        commodity_grade: Optional[Union[list[str], Series[str], str]] = None,
        associated_platts_symbol: Optional[Union[list[str], Series[str], str]] = None,
        delivery_region: Optional[Union[list[str], Series[str], str]] = None,
        shipping_terms: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        contract_type: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        publish_date: Optional[datetime] = None,
        publish_date_lt: Optional[datetime] = None,
        publish_date_lte: Optional[datetime] = None,
        publish_date_gt: Optional[datetime] = None,
        publish_date_gte: Optional[datetime] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        valid_to: Optional[datetime] = None,
        valid_to_lt: Optional[datetime] = None,
        valid_to_lte: Optional[datetime] = None,
        valid_to_gt: Optional[datetime] = None,
        valid_to_gte: Optional[datetime] = None,
        valid_from: Optional[datetime] = None,
        valid_from_lt: Optional[datetime] = None,
        valid_from_lte: Optional[datetime] = None,
        valid_from_gt: Optional[datetime] = None,
        valid_from_gte: Optional[datetime] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[bool], Series[bool], bool]] = True,
        region: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Long-term price and margin forecasts

        Parameters
        ----------

         scenario_id: Optional[Union[list[int], Series[int], int]], optional
             Scenario ID, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         series_description: Optional[Union[list[str], Series[str], str]]
             Price Series Description, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         commodity_grade: Optional[Union[list[str], Series[str], str]]
             Commodity Grade, by default None
         associated_platts_symbol: Optional[Union[list[str], Series[str], str]]
             Associated Platts Symbol, by default None
         delivery_region: Optional[Union[list[str], Series[str], str]]
             Delivery Region, by default None
         shipping_terms: Optional[Union[list[str], Series[str], str]]
             Shipping Terms, by default None
         currency: Optional[Union[list[str], Series[str], str]]
             Currency, by default None
         contract_type: Optional[Union[list[str], Series[str], str]]
             Contract Type, by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         value: Optional[float], optional
             Data Value, by default None
         value_gt: Optional[float], optional
             filter by '' value > x '', by default None
         value_gte: Optional[float], optional
             filter by value, by default None
         value_lt: Optional[float], optional
             filter by value, by default None
         value_lte: Optional[float], optional
             filter by value, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         publish_date: Optional[datetime], optional
             Publish Date, by default None
         publish_date_gt: Optional[datetime], optional
             filter by '' publish_date > x '', by default None
         publish_date_gte: Optional[datetime], optional
             filter by publish_date, by default None
         publish_date_lt: Optional[datetime], optional
             filter by publish_date, by default None
         publish_date_lte: Optional[datetime], optional
             filter by publish_date, by default None
         year: Optional[int], optional
             year, by default None
         year_gt: Optional[int], optional
             filter by '' year > x '', by default None
         year_gte: Optional[int], optional
             filter by year, by default None
         year_lt: Optional[int], optional
             filter by year, by default None
         year_lte: Optional[int], optional
             filter by year, by default None
         valid_to: Optional[datetime], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[datetime], optional
             filter by '' valid_to > x '', by default None
         valid_to_gte: Optional[datetime], optional
             filter by valid_to, by default None
         valid_to_lt: Optional[datetime], optional
             filter by valid_to, by default None
         valid_to_lte: Optional[datetime], optional
             filter by valid_to, by default None
         valid_from: Optional[datetime], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[datetime], optional
             filter by '' valid_from > x '', by default None
         valid_from_gte: Optional[datetime], optional
             filter by valid_from, by default None
         valid_from_lt: Optional[datetime], optional
             filter by valid_from, by default None
         valid_from_lte: Optional[datetime], optional
             filter by valid_from, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         is_active: Optional[Union[list[bool], Series[bool], bool]]
             If the record is active, by default True
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("seriesDescription", series_description))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("commodityGrade", commodity_grade))
        filter_params.append(
            list_to_filter("associatedPlattsSymbol", associated_platts_symbol)
        )
        filter_params.append(list_to_filter("deliveryRegion", delivery_region))
        filter_params.append(list_to_filter("shippingTerms", shipping_terms))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("contractType", contract_type))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("publishDate", publish_date))
        if publish_date_gt is not None:
            filter_params.append(f'publishDate > "{publish_date_gt}"')
        if publish_date_gte is not None:
            filter_params.append(f'publishDate >= "{publish_date_gte}"')
        if publish_date_lt is not None:
            filter_params.append(f'publishDate < "{publish_date_lt}"')
        if publish_date_lte is not None:
            filter_params.append(f'publishDate <= "{publish_date_lte}"')
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
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
        filter_params.append(list_to_filter("region", region))

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
        self,
        *,
        scenario_id: Optional[Union[list[int], Series[int], int]] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        series_description: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        commodity_grade: Optional[Union[list[str], Series[str], str]] = None,
        associated_platts_symbol: Optional[Union[list[str], Series[str], str]] = None,
        delivery_region: Optional[Union[list[str], Series[str], str]] = None,
        shipping_terms: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        contract_type: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        publish_date: Optional[datetime] = None,
        publish_date_lt: Optional[datetime] = None,
        publish_date_lte: Optional[datetime] = None,
        publish_date_gt: Optional[datetime] = None,
        publish_date_gte: Optional[datetime] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        valid_to: Optional[datetime] = None,
        valid_to_lt: Optional[datetime] = None,
        valid_to_lte: Optional[datetime] = None,
        valid_to_gt: Optional[datetime] = None,
        valid_to_gte: Optional[datetime] = None,
        valid_from: Optional[datetime] = None,
        valid_from_lt: Optional[datetime] = None,
        valid_from_lte: Optional[datetime] = None,
        valid_from_gt: Optional[datetime] = None,
        valid_from_gte: Optional[datetime] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[bool], Series[bool], bool]] = True,
        region: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Short-term price and margin forecasts

        Parameters
        ----------

         scenario_id: Optional[int], optional
             Scenario ID, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         series_description: Optional[Union[list[str], Series[str], str]]
             Price Series Description, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         commodity_grade: Optional[Union[list[str], Series[str], str]]
             Commodity Grade, by default None
         associated_platts_symbol: Optional[Union[list[str], Series[str], str]]
             Associated Platts Symbol, by default None
         delivery_region: Optional[Union[list[str], Series[str], str]]
             Delivery Region, by default None
         shipping_terms: Optional[Union[list[str], Series[str], str]]
             Shipping Terms, by default None
         currency: Optional[Union[list[str], Series[str], str]]
             Currency, by default None
         contract_type: Optional[Union[list[str], Series[str], str]]
             Contract Type, by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         value: Optional[float], optional
             Data Value, by default None
         value_gt: Optional[float], optional
             filter by '' value > x '', by default None
         value_gte: Optional[float], optional
             filter by value, by default None
         value_lt: Optional[float], optional
             filter by value, by default None
         value_lte: Optional[float], optional
             filter by value, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         publish_date: Optional[datetime], optional
             Publish Date, by default None
         publish_date_gt: Optional[datetime], optional
             filter by '' publish_date > x '', by default None
         publish_date_gte: Optional[datetime], optional
             filter by publish_date, by default None
         publish_date_lt: Optional[datetime], optional
             filter by publish_date, by default None
         publish_date_lte: Optional[datetime], optional
             filter by publish_date, by default None
         date: Optional[date], optional
             year, by default None
         date_gt: Optional[date], optional
             filter by '' date > x '', by default None
         date_gte: Optional[date], optional
             filter by date, by default None
         date_lt: Optional[date], optional
             filter by date, by default None
         date_lte: Optional[date], optional
             filter by date, by default None
         valid_to: Optional[datetime], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[datetime], optional
             filter by '' valid_to > x '', by default None
         valid_to_gte: Optional[datetime], optional
             filter by valid_to, by default None
         valid_to_lt: Optional[datetime], optional
             filter by valid_to, by default None
         valid_to_lte: Optional[datetime], optional
             filter by valid_to, by default None
         valid_from: Optional[datetime], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[datetime], optional
             filter by '' valid_from > x '', by default None
         valid_from_gte: Optional[datetime], optional
             filter by valid_from, by default None
         valid_from_lt: Optional[datetime], optional
             filter by valid_from, by default None
         valid_from_lte: Optional[datetime], optional
             filter by valid_from, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by '' modified_date > x '', by default None
         modified_date_gte: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lt: Optional[datetime], optional
             filter by modified_date, by default None
         modified_date_lte: Optional[datetime], optional
             filter by modified_date, by default None
         is_active: Optional[Union[list[bool], Series[bool], bool]]
             If the record is active, by default True
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("seriesDescription", series_description))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("commodityGrade", commodity_grade))
        filter_params.append(
            list_to_filter("associatedPlattsSymbol", associated_platts_symbol)
        )
        filter_params.append(list_to_filter("deliveryRegion", delivery_region))
        filter_params.append(list_to_filter("shippingTerms", shipping_terms))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("contractType", contract_type))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("publishDate", publish_date))
        if publish_date_gt is not None:
            filter_params.append(f'publishDate > "{publish_date_gt}"')
        if publish_date_gte is not None:
            filter_params.append(f'publishDate >= "{publish_date_gte}"')
        if publish_date_lt is not None:
            filter_params.append(f'publishDate < "{publish_date_lt}"')
        if publish_date_lte is not None:
            filter_params.append(f'publishDate <= "{publish_date_lte}"')
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
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
        filter_params.append(list_to_filter("region", region))

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

        if "eventBeginDate" in df.columns:
            if parse(pd.__version__) >= parse("2"):
                df["eventBeginDate"] = pd.to_datetime(
                    df["eventBeginDate"], utc=True, format="ISO8601", errors="coerce"
                )
            else:
                df["eventBeginDate"] = pd.to_datetime(df["eventBeginDate"], errors="coerce", utc=True)  # type: ignore

        if "validFrom" in df.columns:
            if parse(pd.__version__) >= parse("2"):
                df["validFrom"] = pd.to_datetime(
                    df["validFrom"], utc=True, format="ISO8601", errors="coerce"
                )
            else:
                df["validFrom"] = pd.to_datetime(df["validFrom"], errors="coerce", utc=True)  # type: ignore

        if "validTo" in df.columns:
            if parse(pd.__version__) >= parse("2"):
                df["validTo"] = pd.to_datetime(
                    df["validTo"], utc=True, format="ISO8601", errors="coerce"
                )
            else:
                df["validTo"] = pd.to_datetime(df["validTo"], errors="coerce", utc=True)  # type: ignore

        if "modifiedDate" in df.columns:
            if parse(pd.__version__) >= parse("2"):
                df["modifiedDate"] = pd.to_datetime(
                    df["modifiedDate"], utc=True, format="ISO8601", errors="coerce"
                )
            else:
                df["modifiedDate"] = pd.to_datetime(df["modifiedDate"], errors="coerce", utc=True)  # type: ignore

        if "startDate" in df.columns:
            if parse(pd.__version__) >= parse("2"):
                df["startDate"] = pd.to_datetime(
                    df["startDate"], utc=True, format="ISO8601", errors="coerce"
                )
            else:
                df["startDate"] = pd.to_datetime(df["startDate"], errors="coerce", utc=True)  # type: ignore

        if "endDate" in df.columns:
            if parse(pd.__version__) >= parse("2"):
                df["endDate"] = pd.to_datetime(
                    df["endDate"], utc=True, format="ISO8601", errors="coerce"
                )
            else:
                df["endDate"] = pd.to_datetime(df["endDate"], errors="coerce", utc=True)  # type: ignore

        if "publishDate" in df.columns:
            if parse(pd.__version__) >= parse("2"):
                df["publishDate"] = pd.to_datetime(
                    df["publishDate"], utc=True, format="ISO8601", errors="coerce"
                )
            else:
                df["publishDate"] = pd.to_datetime(df["publishDate"], errors="coerce", utc=True)  # type: ignore

        if "date" in df.columns:
            if parse(pd.__version__) >= parse("2"):
                df["date"] = pd.to_datetime(
                    df["date"], utc=True, format="ISO8601", errors="coerce"
                )
            else:
                df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)  # type: ignore

        if "lastModifiedDate" in df.columns:
            if parse(pd.__version__) >= parse("2"):
                df["lastModifiedDate"] = pd.to_datetime(
                    df["lastModifiedDate"], utc=True, format="ISO8601", errors="coerce"
                )
            else:
                df["lastModifiedDate"] = pd.to_datetime(df["lastModifiedDate"], errors="coerce", utc=True)  # type: ignore

        return df

    def get_capacity(
        self,
        *,
        forecast_period: Optional[Union[list[str], Series[str], str]] = None,
        scenario_id: Optional[int] = None,
        scenario_id_lt: Optional[int] = None,
        scenario_id_lte: Optional[int] = None,
        scenario_id_gt: Optional[int] = None,
        scenario_id_gte: Optional[int] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        production_route: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        valid_to: Optional[date] = None,
        valid_to_lt: Optional[date] = None,
        valid_to_lte: Optional[date] = None,
        valid_to_gt: Optional[date] = None,
        valid_to_gte: Optional[date] = None,
        valid_from: Optional[date] = None,
        valid_from_lt: Optional[date] = None,
        valid_from_lte: Optional[date] = None,
        valid_from_gt: Optional[date] = None,
        valid_from_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = True,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Country-level capacity data by production route for a product

        Parameters
        ----------

         forecast_period: Optional[Union[list[str], Series[str], str]]
             Long term or short term, by default None
         scenario_id: Optional[int], optional
             Scenario ID, by default None
         scenario_id_gt: Optional[int], optional
             filter by `scenario_id > x`, by default None
         scenario_id_gte: Optional[int], optional
             filter by `scenario_id >= x`, by default None
         scenario_id_lt: Optional[int], optional
             filter by `scenario_id < x`, by default None
         scenario_id_lte: Optional[int], optional
             filter by `scenario_id <= x`, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         production_route: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         date: Optional[date], optional
             Date, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         value: Optional[str], optional
             Data Value, by default None
         value_gt: Optional[str], optional
             filter by `value > x`, by default None
         value_gte: Optional[str], optional
             filter by `value >= x`, by default None
         value_lt: Optional[str], optional
             filter by `value < x`, by default None
         value_lte: Optional[str], optional
             filter by `value <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         valid_to: Optional[date], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[date], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[date], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[date], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[date], optional
             filter by `valid_to <= x`, by default None
         valid_from: Optional[date], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[date], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[date], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[date], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[date], optional
             filter by `valid_from <= x`, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        if scenario_id_gt is not None:
            filter_params.append(f'scenarioId > "{scenario_id_gt}"')
        if scenario_id_gte is not None:
            filter_params.append(f'scenarioId >= "{scenario_id_gte}"')
        if scenario_id_lt is not None:
            filter_params.append(f'scenarioId < "{scenario_id_lt}"')
        if scenario_id_lte is not None:
            filter_params.append(f'scenarioId <= "{scenario_id_lte}"')
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("productionRoute", production_route))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
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
            path=f"/analytics/v1/chemicals/capacity",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_production(
        self,
        *,
        forecast_period: Optional[Union[list[str], Series[str], str]] = None,
        scenario_id: Optional[int] = None,
        scenario_id_lt: Optional[int] = None,
        scenario_id_lte: Optional[int] = None,
        scenario_id_gt: Optional[int] = None,
        scenario_id_gte: Optional[int] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        production_route: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        valid_to: Optional[date] = None,
        valid_to_lt: Optional[date] = None,
        valid_to_lte: Optional[date] = None,
        valid_to_gt: Optional[date] = None,
        valid_to_gte: Optional[date] = None,
        valid_from: Optional[date] = None,
        valid_from_lt: Optional[date] = None,
        valid_from_lte: Optional[date] = None,
        valid_from_gt: Optional[date] = None,
        valid_from_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = True,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Country-level production data by production route for a product

        Parameters
        ----------

         forecast_period: Optional[Union[list[str], Series[str], str]]
             Long term or short term, by default None
         scenario_id: Optional[int], optional
             Scenario ID, by default None
         scenario_id_gt: Optional[int], optional
             filter by `scenario_id > x`, by default None
         scenario_id_gte: Optional[int], optional
             filter by `scenario_id >= x`, by default None
         scenario_id_lt: Optional[int], optional
             filter by `scenario_id < x`, by default None
         scenario_id_lte: Optional[int], optional
             filter by `scenario_id <= x`, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         production_route: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         date: Optional[date], optional
             Date, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         value: Optional[str], optional
             Data Value, by default None
         value_gt: Optional[str], optional
             filter by `value > x`, by default None
         value_gte: Optional[str], optional
             filter by `value >= x`, by default None
         value_lt: Optional[str], optional
             filter by `value < x`, by default None
         value_lte: Optional[str], optional
             filter by `value <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         valid_to: Optional[date], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[date], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[date], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[date], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[date], optional
             filter by `valid_to <= x`, by default None
         valid_from: Optional[date], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[date], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[date], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[date], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[date], optional
             filter by `valid_from <= x`, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        if scenario_id_gt is not None:
            filter_params.append(f'scenarioId > "{scenario_id_gt}"')
        if scenario_id_gte is not None:
            filter_params.append(f'scenarioId >= "{scenario_id_gte}"')
        if scenario_id_lt is not None:
            filter_params.append(f'scenarioId < "{scenario_id_lt}"')
        if scenario_id_lte is not None:
            filter_params.append(f'scenarioId <= "{scenario_id_lte}"')
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("productionRoute", production_route))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
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
            path=f"/analytics/v1/chemicals/production",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_capacity_utilization(
        self,
        *,
        forecast_period: Optional[Union[list[str], Series[str], str]] = None,
        scenario_id: Optional[int] = None,
        scenario_id_lt: Optional[int] = None,
        scenario_id_lte: Optional[int] = None,
        scenario_id_gt: Optional[int] = None,
        scenario_id_gte: Optional[int] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        production_route: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        valid_to: Optional[date] = None,
        valid_to_lt: Optional[date] = None,
        valid_to_lte: Optional[date] = None,
        valid_to_gt: Optional[date] = None,
        valid_to_gte: Optional[date] = None,
        valid_from: Optional[date] = None,
        valid_from_lt: Optional[date] = None,
        valid_from_lte: Optional[date] = None,
        valid_from_gt: Optional[date] = None,
        valid_from_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = True,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Capacity Utilization data for a product

        Parameters
        ----------

         forecast_period: Optional[Union[list[str], Series[str], str]]
             Long term or short term, by default None
         scenario_id: Optional[int], optional
             Scenario ID, by default None
         scenario_id_gt: Optional[int], optional
             filter by `scenario_id > x`, by default None
         scenario_id_gte: Optional[int], optional
             filter by `scenario_id >= x`, by default None
         scenario_id_lt: Optional[int], optional
             filter by `scenario_id < x`, by default None
         scenario_id_lte: Optional[int], optional
             filter by `scenario_id <= x`, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         production_route: Optional[Union[list[str], Series[str], str]]
             Name for Production Route, by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         date: Optional[date], optional
             Date, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         value: Optional[str], optional
             Data Value, by default None
         value_gt: Optional[str], optional
             filter by `value > x`, by default None
         value_gte: Optional[str], optional
             filter by `value >= x`, by default None
         value_lt: Optional[str], optional
             filter by `value < x`, by default None
         value_lte: Optional[str], optional
             filter by `value <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         valid_to: Optional[date], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[date], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[date], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[date], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[date], optional
             filter by `valid_to <= x`, by default None
         valid_from: Optional[date], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[date], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[date], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[date], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[date], optional
             filter by `valid_from <= x`, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        if scenario_id_gt is not None:
            filter_params.append(f'scenarioId > "{scenario_id_gt}"')
        if scenario_id_gte is not None:
            filter_params.append(f'scenarioId >= "{scenario_id_gte}"')
        if scenario_id_lt is not None:
            filter_params.append(f'scenarioId < "{scenario_id_lt}"')
        if scenario_id_lte is not None:
            filter_params.append(f'scenarioId <= "{scenario_id_lte}"')
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("productionRoute", production_route))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
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
            path=f"/analytics/v1/chemicals/capacity-utilization",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_demand_by_derivative(
        self,
        *,
        forecast_period: Optional[Union[list[str], Series[str], str]] = None,
        scenario_id: Optional[int] = None,
        scenario_id_lt: Optional[int] = None,
        scenario_id_lte: Optional[int] = None,
        scenario_id_gt: Optional[int] = None,
        scenario_id_gte: Optional[int] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        valid_to: Optional[date] = None,
        valid_to_lt: Optional[date] = None,
        valid_to_lte: Optional[date] = None,
        valid_to_gt: Optional[date] = None,
        valid_to_gte: Optional[date] = None,
        valid_from: Optional[date] = None,
        valid_from_lt: Optional[date] = None,
        valid_from_lte: Optional[date] = None,
        valid_from_gt: Optional[date] = None,
        valid_from_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = True,
        application: Optional[Union[list[str], Series[str], str]] = None,
        derivative_product: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Country-level demand data for a product categorized by derivative or specific application

        Parameters
        ----------

         forecast_period: Optional[Union[list[str], Series[str], str]]
             Long term or short term, by default None
         scenario_id: Optional[int], optional
             Scenario ID, by default None
         scenario_id_gt: Optional[int], optional
             filter by `scenario_id > x`, by default None
         scenario_id_gte: Optional[int], optional
             filter by `scenario_id >= x`, by default None
         scenario_id_lt: Optional[int], optional
             filter by `scenario_id < x`, by default None
         scenario_id_lte: Optional[int], optional
             filter by `scenario_id <= x`, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         date: Optional[date], optional
             Date, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         value: Optional[str], optional
             Data Value, by default None
         value_gt: Optional[str], optional
             filter by `value > x`, by default None
         value_gte: Optional[str], optional
             filter by `value >= x`, by default None
         value_lt: Optional[str], optional
             filter by `value < x`, by default None
         value_lte: Optional[str], optional
             filter by `value <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         valid_to: Optional[date], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[date], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[date], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[date], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[date], optional
             filter by `valid_to <= x`, by default None
         valid_from: Optional[date], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[date], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[date], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[date], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[date], optional
             filter by `valid_from <= x`, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default None
         application: Optional[Union[list[str], Series[str], str]]
             Product(chemical commodity) Application, by default None
         derivative_product: Optional[Union[list[str], Series[str], str]]
             Derivative Product (chemical commodity), by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        if scenario_id_gt is not None:
            filter_params.append(f'scenarioId > "{scenario_id_gt}"')
        if scenario_id_gte is not None:
            filter_params.append(f'scenarioId >= "{scenario_id_gte}"')
        if scenario_id_lt is not None:
            filter_params.append(f'scenarioId < "{scenario_id_lt}"')
        if scenario_id_lte is not None:
            filter_params.append(f'scenarioId <= "{scenario_id_lte}"')
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
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
        filter_params.append(list_to_filter("application", application))
        filter_params.append(list_to_filter("derivative_product", derivative_product))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/demand-by-derivative",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_demand_by_end_use(
        self,
        *,
        forecast_period: Optional[Union[list[str], Series[str], str]] = None,
        scenario_id: Optional[int] = None,
        scenario_id_lt: Optional[int] = None,
        scenario_id_lte: Optional[int] = None,
        scenario_id_gt: Optional[int] = None,
        scenario_id_gte: Optional[int] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        valid_to: Optional[date] = None,
        valid_to_lt: Optional[date] = None,
        valid_to_lte: Optional[date] = None,
        valid_to_gt: Optional[date] = None,
        valid_to_gte: Optional[date] = None,
        valid_from: Optional[date] = None,
        valid_from_lt: Optional[date] = None,
        valid_from_lte: Optional[date] = None,
        valid_from_gt: Optional[date] = None,
        valid_from_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = True,
        end_use: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Country-level demand data for a product categorized by end use

        Parameters
        ----------

         forecast_period: Optional[Union[list[str], Series[str], str]]
             Long term or short term, by default None
         scenario_id: Optional[int], optional
             Scenario ID, by default None
         scenario_id_gt: Optional[int], optional
             filter by `scenario_id > x`, by default None
         scenario_id_gte: Optional[int], optional
             filter by `scenario_id >= x`, by default None
         scenario_id_lt: Optional[int], optional
             filter by `scenario_id < x`, by default None
         scenario_id_lte: Optional[int], optional
             filter by `scenario_id <= x`, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         date: Optional[date], optional
             Date, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         value: Optional[str], optional
             Data Value, by default None
         value_gt: Optional[str], optional
             filter by `value > x`, by default None
         value_gte: Optional[str], optional
             filter by `value >= x`, by default None
         value_lt: Optional[str], optional
             filter by `value < x`, by default None
         value_lte: Optional[str], optional
             filter by `value <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         valid_to: Optional[date], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[date], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[date], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[date], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[date], optional
             filter by `valid_to <= x`, by default None
         valid_from: Optional[date], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[date], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[date], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[date], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[date], optional
             filter by `valid_from <= x`, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default None
         end_use: Optional[Union[list[str], Series[str], str]]
             Product (chemical commodity) End Use, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        if scenario_id_gt is not None:
            filter_params.append(f'scenarioId > "{scenario_id_gt}"')
        if scenario_id_gte is not None:
            filter_params.append(f'scenarioId >= "{scenario_id_gte}"')
        if scenario_id_lt is not None:
            filter_params.append(f'scenarioId < "{scenario_id_lt}"')
        if scenario_id_lte is not None:
            filter_params.append(f'scenarioId <= "{scenario_id_lte}"')
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
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
        filter_params.append(list_to_filter("endUse", end_use))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/demand-by-end-use",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_trade(
        self,
        *,
        forecast_period: Optional[Union[list[str], Series[str], str]] = None,
        scenario_id: Optional[int] = None,
        scenario_id_lt: Optional[int] = None,
        scenario_id_lte: Optional[int] = None,
        scenario_id_gt: Optional[int] = None,
        scenario_id_gte: Optional[int] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        valid_to: Optional[date] = None,
        valid_to_lt: Optional[date] = None,
        valid_to_lte: Optional[date] = None,
        valid_to_gt: Optional[date] = None,
        valid_to_gte: Optional[date] = None,
        valid_from: Optional[date] = None,
        valid_from_lt: Optional[date] = None,
        valid_from_lte: Optional[date] = None,
        valid_from_gt: Optional[date] = None,
        valid_from_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = True,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Country-level trade (import, export and net trade) for a product

        Parameters
        ----------

         forecast_period: Optional[Union[list[str], Series[str], str]]
             Long term or short term, by default None
         scenario_id: Optional[int], optional
             Scenario ID, by default None
         scenario_id_gt: Optional[int], optional
             filter by `scenario_id > x`, by default None
         scenario_id_gte: Optional[int], optional
             filter by `scenario_id >= x`, by default None
         scenario_id_lt: Optional[int], optional
             filter by `scenario_id < x`, by default None
         scenario_id_lte: Optional[int], optional
             filter by `scenario_id <= x`, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         date: Optional[date], optional
             Date, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         value: Optional[str], optional
             Data Value, by default None
         value_gt: Optional[str], optional
             filter by `value > x`, by default None
         value_gte: Optional[str], optional
             filter by `value >= x`, by default None
         value_lt: Optional[str], optional
             filter by `value < x`, by default None
         value_lte: Optional[str], optional
             filter by `value <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         valid_to: Optional[date], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[date], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[date], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[date], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[date], optional
             filter by `valid_to <= x`, by default None
         valid_from: Optional[date], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[date], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[date], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[date], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[date], optional
             filter by `valid_from <= x`, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        if scenario_id_gt is not None:
            filter_params.append(f'scenarioId > "{scenario_id_gt}"')
        if scenario_id_gte is not None:
            filter_params.append(f'scenarioId >= "{scenario_id_gte}"')
        if scenario_id_lt is not None:
            filter_params.append(f'scenarioId < "{scenario_id_lt}"')
        if scenario_id_lte is not None:
            filter_params.append(f'scenarioId <= "{scenario_id_lte}"')
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
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
            path=f"/analytics/v1/chemicals/trade",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_inventory_change(
        self,
        *,
        forecast_period: Optional[Union[list[str], Series[str], str]] = None,
        scenario_id: Optional[int] = None,
        scenario_id_lt: Optional[int] = None,
        scenario_id_lte: Optional[int] = None,
        scenario_id_gt: Optional[int] = None,
        scenario_id_gte: Optional[int] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        valid_to: Optional[date] = None,
        valid_to_lt: Optional[date] = None,
        valid_to_lte: Optional[date] = None,
        valid_to_gt: Optional[date] = None,
        valid_to_gte: Optional[date] = None,
        valid_from: Optional[date] = None,
        valid_from_lt: Optional[date] = None,
        valid_from_lte: Optional[date] = None,
        valid_from_gt: Optional[date] = None,
        valid_from_gte: Optional[date] = None,
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
        Country-level inventory change data

        Parameters
        ----------

         forecast_period: Optional[Union[list[str], Series[str], str]]
             Long term or short term, by default None
         scenario_id: Optional[int], optional
             Scenario ID, by default None
         scenario_id_gt: Optional[int], optional
             filter by `scenario_id > x`, by default None
         scenario_id_gte: Optional[int], optional
             filter by `scenario_id >= x`, by default None
         scenario_id_lt: Optional[int], optional
             filter by `scenario_id < x`, by default None
         scenario_id_lte: Optional[int], optional
             filter by `scenario_id <= x`, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         date: Optional[date], optional
             Date, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         value: Optional[str], optional
             Data Value, by default None
         value_gt: Optional[str], optional
             filter by `value > x`, by default None
         value_gte: Optional[str], optional
             filter by `value >= x`, by default None
         value_lt: Optional[str], optional
             filter by `value < x`, by default None
         value_lte: Optional[str], optional
             filter by `value <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         valid_to: Optional[date], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[date], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[date], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[date], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[date], optional
             filter by `valid_to <= x`, by default None
         valid_from: Optional[date], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[date], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[date], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[date], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[date], optional
             filter by `valid_from <= x`, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        if scenario_id_gt is not None:
            filter_params.append(f'scenarioId > "{scenario_id_gt}"')
        if scenario_id_gte is not None:
            filter_params.append(f'scenarioId >= "{scenario_id_gte}"')
        if scenario_id_lt is not None:
            filter_params.append(f'scenarioId < "{scenario_id_lt}"')
        if scenario_id_lte is not None:
            filter_params.append(f'scenarioId <= "{scenario_id_lte}"')
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
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
            path=f"/analytics/v1/chemicals/inventory-change",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_total_supply(
        self,
        *,
        forecast_period: Optional[Union[list[str], Series[str], str]] = None,
        scenario_id: Optional[int] = None,
        scenario_id_lt: Optional[int] = None,
        scenario_id_lte: Optional[int] = None,
        scenario_id_gt: Optional[int] = None,
        scenario_id_gte: Optional[int] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        valid_to: Optional[date] = None,
        valid_to_lt: Optional[date] = None,
        valid_to_lte: Optional[date] = None,
        valid_to_gt: Optional[date] = None,
        valid_to_gte: Optional[date] = None,
        valid_from: Optional[date] = None,
        valid_from_lt: Optional[date] = None,
        valid_from_lte: Optional[date] = None,
        valid_from_gt: Optional[date] = None,
        valid_from_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = True,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Country-level total supply data for a product

        Parameters
        ----------

         forecast_period: Optional[Union[list[str], Series[str], str]]
             Long term or short term, by default None
         scenario_id: Optional[int], optional
             Scenario ID, by default None
         scenario_id_gt: Optional[int], optional
             filter by `scenario_id > x`, by default None
         scenario_id_gte: Optional[int], optional
             filter by `scenario_id >= x`, by default None
         scenario_id_lt: Optional[int], optional
             filter by `scenario_id < x`, by default None
         scenario_id_lte: Optional[int], optional
             filter by `scenario_id <= x`, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         date: Optional[date], optional
             Date, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         value: Optional[str], optional
             Data Value, by default None
         value_gt: Optional[str], optional
             filter by `value > x`, by default None
         value_gte: Optional[str], optional
             filter by `value >= x`, by default None
         value_lt: Optional[str], optional
             filter by `value < x`, by default None
         value_lte: Optional[str], optional
             filter by `value <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         valid_to: Optional[date], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[date], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[date], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[date], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[date], optional
             filter by `valid_to <= x`, by default None
         valid_from: Optional[date], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[date], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[date], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[date], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[date], optional
             filter by `valid_from <= x`, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        if scenario_id_gt is not None:
            filter_params.append(f'scenarioId > "{scenario_id_gt}"')
        if scenario_id_gte is not None:
            filter_params.append(f'scenarioId >= "{scenario_id_gte}"')
        if scenario_id_lt is not None:
            filter_params.append(f'scenarioId < "{scenario_id_lt}"')
        if scenario_id_lte is not None:
            filter_params.append(f'scenarioId <= "{scenario_id_lte}"')
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
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
            path=f"/analytics/v1/chemicals/total-supply",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_total_demand(
        self,
        *,
        forecast_period: Optional[Union[list[str], Series[str], str]] = None,
        scenario_id: Optional[int] = None,
        scenario_id_lt: Optional[int] = None,
        scenario_id_lte: Optional[int] = None,
        scenario_id_gt: Optional[int] = None,
        scenario_id_gte: Optional[int] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        valid_to: Optional[date] = None,
        valid_to_lt: Optional[date] = None,
        valid_to_lte: Optional[date] = None,
        valid_to_gt: Optional[date] = None,
        valid_to_gte: Optional[date] = None,
        valid_from: Optional[date] = None,
        valid_from_lt: Optional[date] = None,
        valid_from_lte: Optional[date] = None,
        valid_from_gt: Optional[date] = None,
        valid_from_gte: Optional[date] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = True,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Country-level total demand data for a product

        Parameters
        ----------

         forecast_period: Optional[Union[list[str], Series[str], str]]
             Long term or short term, by default None
         scenario_id: Optional[int], optional
             Scenario ID, by default None
         scenario_id_gt: Optional[int], optional
             filter by `scenario_id > x`, by default None
         scenario_id_gte: Optional[int], optional
             filter by `scenario_id >= x`, by default None
         scenario_id_lt: Optional[int], optional
             filter by `scenario_id < x`, by default None
         scenario_id_lte: Optional[int], optional
             filter by `scenario_id <= x`, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         date: Optional[date], optional
             Date, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         value: Optional[str], optional
             Data Value, by default None
         value_gt: Optional[str], optional
             filter by `value > x`, by default None
         value_gte: Optional[str], optional
             filter by `value >= x`, by default None
         value_lt: Optional[str], optional
             filter by `value < x`, by default None
         value_lte: Optional[str], optional
             filter by `value <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         valid_to: Optional[date], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[date], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[date], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[date], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[date], optional
             filter by `valid_to <= x`, by default None
         valid_from: Optional[date], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[date], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[date], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[date], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[date], optional
             filter by `valid_from <= x`, by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        if scenario_id_gt is not None:
            filter_params.append(f'scenarioId > "{scenario_id_gt}"')
        if scenario_id_gte is not None:
            filter_params.append(f'scenarioId >= "{scenario_id_gte}"')
        if scenario_id_lt is not None:
            filter_params.append(f'scenarioId < "{scenario_id_lt}"')
        if scenario_id_lte is not None:
            filter_params.append(f'scenarioId <= "{scenario_id_lte}"')
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
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
            path=f"/analytics/v1/chemicals/total-demand",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_assumptions(
        self,
        *,
        forecast_period: Optional[Union[list[str], Series[str], str]] = None,
        scenario_id: Optional[int] = None,
        scenario_id_lt: Optional[int] = None,
        scenario_id_lte: Optional[int] = None,
        scenario_id_gt: Optional[int] = None,
        scenario_id_gte: Optional[int] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        valid_from: Optional[date] = None,
        valid_from_lt: Optional[date] = None,
        valid_from_lte: Optional[date] = None,
        valid_from_gt: Optional[date] = None,
        valid_from_gte: Optional[date] = None,
        valid_to: Optional[date] = None,
        valid_to_lt: Optional[date] = None,
        valid_to_lte: Optional[date] = None,
        valid_to_gt: Optional[date] = None,
        valid_to_gte: Optional[date] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = True,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        GDP and population assumptions data

        Parameters
        ----------

         forecast_period: Optional[Union[list[str], Series[str], str]]
             Long term or short term, by default None
         scenario_id: Optional[int], optional
             Scenario ID, by default None
         scenario_id_gt: Optional[int], optional
             filter by `scenario_id > x`, by default None
         scenario_id_gte: Optional[int], optional
             filter by `scenario_id >= x`, by default None
         scenario_id_lt: Optional[int], optional
             filter by `scenario_id < x`, by default None
         scenario_id_lte: Optional[int], optional
             filter by `scenario_id <= x`, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         date: Optional[date], optional
             Date, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         value: Optional[str], optional
             Data Value, by default None
         value_gt: Optional[str], optional
             filter by `value > x`, by default None
         value_gte: Optional[str], optional
             filter by `value >= x`, by default None
         value_lt: Optional[str], optional
             filter by `value < x`, by default None
         value_lte: Optional[str], optional
             filter by `value <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         valid_from: Optional[date], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[date], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[date], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[date], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[date], optional
             filter by `valid_from <= x`, by default None
         valid_to: Optional[date], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[date], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[date], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[date], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[date], optional
             filter by `valid_to <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default None
         last_modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        if scenario_id_gt is not None:
            filter_params.append(f'scenarioId > "{scenario_id_gt}"')
        if scenario_id_gte is not None:
            filter_params.append(f'scenarioId >= "{scenario_id_gte}"')
        if scenario_id_lt is not None:
            filter_params.append(f'scenarioId < "{scenario_id_lt}"')
        if scenario_id_lte is not None:
            filter_params.append(f'scenarioId <= "{scenario_id_lte}"')
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("isActive", is_active))
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/assumptions",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_country_supply_demand_balance(
        self,
        *,
        scenario_id: Optional[Union[list[int], Series[int], int]] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        forecast_period: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        display_order: Optional[Union[list[int], Series[int], int]] = None,
        supply_demand_component: Optional[Union[list[str], Series[str], str]] = None,
        component_driver: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = True,
        valid_to: Optional[date] = None,
        valid_to_lt: Optional[date] = None,
        valid_to_lte: Optional[date] = None,
        valid_to_gt: Optional[date] = None,
        valid_to_gte: Optional[date] = None,
        valid_from: Optional[date] = None,
        valid_from_lt: Optional[date] = None,
        valid_from_lte: Optional[date] = None,
        valid_from_gt: Optional[date] = None,
        valid_from_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Country level supply and demand balance for a product

        Parameters
        ----------

         scenario_id: Optional[Union[list[int], Series[int], int]]
             Scenario ID, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         forecast_period: Optional[Union[list[str], Series[str], str]]
             Long term or short term, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         country: Optional[Union[list[str], Series[str], str]]
             Name for Country (geography), by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         display_order: Optional[Union[list[int], Series[int], int]]
             Dataset display order, by default None
         supply_demand_component: Optional[Union[list[str], Series[str], str]]
             Component for breaking down the dataset, e.g. Capacity by production route, by default None
         component_driver: Optional[Union[list[str], Series[str], str]]
             Value for each component, by default None
         date: Optional[date], optional
             Date, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         value: Optional[str], optional
             Data Value, by default None
         value_gt: Optional[str], optional
             filter by `value > x`, by default None
         value_gte: Optional[str], optional
             filter by `value >= x`, by default None
         value_lt: Optional[str], optional
             filter by `value < x`, by default None
         value_lte: Optional[str], optional
             filter by `value <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default None
         valid_to: Optional[date], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[date], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[date], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[date], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[date], optional
             filter by `valid_to <= x`, by default None
         valid_from: Optional[date], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[date], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[date], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[date], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[date], optional
             filter by `valid_from <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("displayOrder", display_order))
        filter_params.append(
            list_to_filter("supplyDemandComponent", supply_demand_component)
        )
        filter_params.append(list_to_filter("componentDriver", component_driver))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
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
        filter_params.append(list_to_filter("isActive", is_active))
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/country-supply-demand-balance",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_region_supply_demand_balance(
        self,
        *,
        scenario_id: Optional[Union[list[int], Series[int], int]] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        forecast_period: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        display_order: Optional[Union[list[int], Series[int], int]] = None,
        supply_demand_component: Optional[Union[list[str], Series[str], str]] = None,
        component_driver: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = True,
        valid_to: Optional[date] = None,
        valid_to_lt: Optional[date] = None,
        valid_to_lte: Optional[date] = None,
        valid_to_gt: Optional[date] = None,
        valid_to_gte: Optional[date] = None,
        valid_from: Optional[date] = None,
        valid_from_lt: Optional[date] = None,
        valid_from_lte: Optional[date] = None,
        valid_from_gt: Optional[date] = None,
        valid_from_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Region level supply and demand balance for a product

        Parameters
        ----------

         scenario_id: Optional[Union[list[int], Series[int], int]]
             Scenario ID, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         forecast_period: Optional[Union[list[str], Series[str], str]]
             Long term or short term, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         region: Optional[Union[list[str], Series[str], str]]
             Name for Region (geography), by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         display_order: Optional[Union[list[int], Series[int], int]]
             Dataset display order, by default None
         supply_demand_component: Optional[Union[list[str], Series[str], str]]
             Component for breaking down the dataset, e.g. Capacity by production route, by default None
         component_driver: Optional[Union[list[str], Series[str], str]]
             Value for each component, by default None
         date: Optional[date], optional
             Date, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         value: Optional[str], optional
             Data Value, by default None
         value_gt: Optional[str], optional
             filter by `value > x`, by default None
         value_gte: Optional[str], optional
             filter by `value >= x`, by default None
         value_lt: Optional[str], optional
             filter by `value < x`, by default None
         value_lte: Optional[str], optional
             filter by `value <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default None
         valid_to: Optional[date], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[date], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[date], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[date], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[date], optional
             filter by `valid_to <= x`, by default None
         valid_from: Optional[date], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[date], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[date], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[date], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[date], optional
             filter by `valid_from <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("displayOrder", display_order))
        filter_params.append(
            list_to_filter("supplyDemandComponent", supply_demand_component)
        )
        filter_params.append(list_to_filter("componentDriver", component_driver))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
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
        filter_params.append(list_to_filter("isActive", is_active))
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/region-supply-demand-balance",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_world_supply_demand_balance(
        self,
        *,
        scenario_id: Optional[Union[list[int], Series[int], int]] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        forecast_period: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        display_order: Optional[Union[list[int], Series[int], int]] = None,
        supply_demand_component: Optional[Union[list[str], Series[str], str]] = None,
        component_driver: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        data_type: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = True,
        valid_to: Optional[date] = None,
        valid_to_lt: Optional[date] = None,
        valid_to_lte: Optional[date] = None,
        valid_to_gt: Optional[date] = None,
        valid_to_gte: Optional[date] = None,
        valid_from: Optional[date] = None,
        valid_from_lt: Optional[date] = None,
        valid_from_lte: Optional[date] = None,
        valid_from_gt: Optional[date] = None,
        valid_from_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        World level supply and demand balance for a product

        Parameters
        ----------

         scenario_id: Optional[Union[list[int], Series[int], int]]
             Scenario ID, by default None
         scenario_description: Optional[Union[list[str], Series[str], str]]
             Scenario Description, by default None
         forecast_period: Optional[Union[list[str], Series[str], str]]
             Long term or short term, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             Name for Product (chemical commodity), by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Concept that describes what the dataset is, by default None
         display_order: Optional[Union[list[int], Series[int], int]]
             Dataset display order, by default None
         supply_demand_component: Optional[Union[list[str], Series[str], str]]
             Component for breaking down the dataset, e.g. Capacity by production route, by default None
         component_driver: Optional[Union[list[str], Series[str], str]]
             Value for each component, by default None
         date: Optional[date], optional
             Date, by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         data_type: Optional[Union[list[str], Series[str], str]]
             Data Type (history or forecast), by default None
         value: Optional[str], optional
             Data Value, by default None
         value_gt: Optional[str], optional
             filter by `value > x`, by default None
         value_gte: Optional[str], optional
             filter by `value >= x`, by default None
         value_lt: Optional[str], optional
             filter by `value < x`, by default None
         value_lte: Optional[str], optional
             filter by `value <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Name for Unit of Measure (volume), by default None
         modified_date: Optional[datetime], optional
             Date when the data is last modified, by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             If the record is active, by default None
         valid_to: Optional[date], optional
             End Date of Record Validity, by default None
         valid_to_gt: Optional[date], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[date], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[date], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[date], optional
             filter by `valid_to <= x`, by default None
         valid_from: Optional[date], optional
             As of date for when the data is updated, by default None
         valid_from_gt: Optional[date], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[date], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[date], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[date], optional
             filter by `valid_from <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenarioId", scenario_id))
        filter_params.append(
            list_to_filter("scenarioDescription", scenario_description)
        )
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("displayOrder", display_order))
        filter_params.append(
            list_to_filter("supplyDemandComponent", supply_demand_component)
        )
        filter_params.append(list_to_filter("componentDriver", component_driver))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("dataType", data_type))
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
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
        filter_params.append(list_to_filter("isActive", is_active))
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/v1/chemicals/world-supply-demand-balance",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
