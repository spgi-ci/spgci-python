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
from typing import List, Optional, Union, Literal
from requests import Response
from packaging.version import parse
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date, datetime
import pandas as pd


class OilNGLAnalytics:

    _datasets = Literal[
        "arbflow-arbitrage",
        "oil-inventory",
        "oil-inventory-latest",
        "refinery-production",
        "refinery-production-latest",
        "refinery-runs",
        "refinery-runs-latest",
        "refinery-utilization-rate",
        "refinery-utilization-rate-latest",
        "demand",
        "demand-latest",
    ]

    def get_unique_values(
        self,
        dataset: _datasets,
        columns: Optional[list[str], str],
    ) -> DataFrame:
        dataset_to_path = {
            "arbflow-arbitrage": "analytics/fuels-refining/v1/arbflow/arbitrage",
            "oil-inventory": "analytics/fuels-refining/v1/oil-inventory",
            "oil-inventory-latest": "analytics/fuels-refining/v1/oil-inventory/latest",
            "refinery-production": "analytics/fuels-refining/v1/refinery-production",
            "refinery-production-latest": "analytics/fuels-refining/v1/refinery-production/latest",
            "refinery-runs": "analytics/fuels-refining/v1/refinery-runs",
            "refinery-runs-latest": "analytics/fuels-refining/v1/refinery-runs/latest",
            "refinery-utilization-rate": "analytics/fuels-refining/v1/refinery-utilization-rate",
            "refinery-utilization-rate-latest": "analytics/fuels-refining/v1/refinery-utilization-rate/latest",
            "demand": "analytics/refined-product/v1/demand",
            "demand-latest": "analytics/refined-product/v1/demand/latest",
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

    def get_arbflow_arbitrage(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        report_for_date: Optional[date] = None,
        report_for_date_lt: Optional[date] = None,
        report_for_date_lte: Optional[date] = None,
        report_for_date_gt: Optional[date] = None,
        report_for_date_gte: Optional[date] = None,
        from_region: Optional[Union[list[str], Series[str], str]] = None,
        to_region: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        frequency: Optional[Union[list[str], Series[str], str]] = None,
        vintage_date: Optional[date] = None,
        vintage_date_lt: Optional[date] = None,
        vintage_date_lte: Optional[date] = None,
        vintage_date_gt: Optional[date] = None,
        vintage_date_gte: Optional[date] = None,
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
        The Refined Product Arbitrage dataset provides arbitrage information for five refined product commodities across key routes around the world. The dataset includes arbitrage incentive with a breakdown of the key components used to calculate it, enabling users to compare the profitability of sourcing, or supplying refined products by location.

        Parameters
        ----------

         commodity: Optional[Union[list[str], Series[str], str]]
             , by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Data Metric Type - Reference column to describe the metrics being reported in the column Value., by default None
         report_for_date: Optional[date], optional
             The date for which the record applies within the data series., by default None
         report_for_date_gt: Optional[date], optional
             filter by `report_for_date > x`, by default None
         report_for_date_gte: Optional[date], optional
             filter by `report_for_date >= x`, by default None
         report_for_date_lt: Optional[date], optional
             filter by `report_for_date < x`, by default None
         report_for_date_lte: Optional[date], optional
             filter by `report_for_date <= x`, by default None
         from_region: Optional[Union[list[str], Series[str], str]]
             Originating Region Name - The name of the geographic area or location from which the commodities originate in a trade or flow., by default None
         to_region: Optional[Union[list[str], Series[str], str]]
             Delivery Region Name - The name of the geographic region where the product is delivered., by default None
         currency: Optional[Union[list[str], Series[str], str]]
             Reported Currency Code - The standardized three-letter code used to identify the currency in which the data is reported., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measurement., by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             The indicator of how often the data is refreshed or collected., by default None
         vintage_date: Optional[date], optional
             The specific date on which a particular data series was created., by default None
         vintage_date_gt: Optional[date], optional
             filter by `vintage_date > x`, by default None
         vintage_date_gte: Optional[date], optional
             filter by `vintage_date >= x`, by default None
         vintage_date_lt: Optional[date], optional
             filter by `vintage_date < x`, by default None
         vintage_date_lte: Optional[date], optional
             filter by `vintage_date <= x`, by default None
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
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("reportForDate", report_for_date))
        if report_for_date_gt is not None:
            filter_params.append(f'reportForDate > "{report_for_date_gt}"')
        if report_for_date_gte is not None:
            filter_params.append(f'reportForDate >= "{report_for_date_gte}"')
        if report_for_date_lt is not None:
            filter_params.append(f'reportForDate < "{report_for_date_lt}"')
        if report_for_date_lte is not None:
            filter_params.append(f'reportForDate <= "{report_for_date_lte}"')
        filter_params.append(list_to_filter("fromRegion", from_region))
        filter_params.append(list_to_filter("toRegion", to_region))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("uom", uom))
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
            path="/analytics/fuels-refining/v1/arbflow/arbitrage",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_oil_inventory(
        self,
        *,
        sector: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        data_series_short: Optional[Union[list[str], Series[str], str]] = None,
        outlook_horizon: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides historical, and forecast data for crude and refined products (including NGLs) inventories. The dataset provides Short-Term inventories forecast up to 2 years (Monthly, Quarterly, and Yearly ) for countries like U.S.A, Japan, etc. It also covers global oil inventories change quarterly outlook, along with weekly historical global oil inventories data with coverage also focused on regions like U.S.A, Singapore, Fujairah, and ARA.

        Parameters
        ----------

         sector: Optional[Union[list[str], Series[str], str]]
             Sector name, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             The products on which we do assessments or products closely associated with our assessed products., by default None
         data_series_short: Optional[Union[list[str], Series[str], str]]
             The brief description of the information represented in the data series., by default None
         outlook_horizon: Optional[Union[list[str], Series[str], str]]
             An indicator to determine the time frame of the oil market outlook., by default None
         geography: Optional[Union[list[str], Series[str], str]]
             The name of an area, country division, or the world with characteristics defined by either physical boundaries or human-defined borders., by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Reference column to describe the metrics being reported in the column Value., by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             The indicator of the data granularity within the time continuum., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The standardized unit or units in which the value of the commodity is measured., by default None
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
             The date for a record applies within the data series., by default None
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
        filter_params.append(list_to_filter("dataSeriesShort", data_series_short))
        filter_params.append(list_to_filter("outlookHorizon", outlook_horizon))
        filter_params.append(list_to_filter("geography", geography))
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
            path=f"/analytics/fuels-refining/v1/oil-inventory",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_oil_inventory_latest(
        self,
        *,
        sector: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        data_series_short: Optional[Union[list[str], Series[str], str]] = None,
        outlook_horizon: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides historical, and forecast data for crude and refined products (including NGLs) inventories. The dataset provides Short-Term inventories forecast up to 2 years (Monthly, Quarterly, and Yearly ) for countries like U.S.A, Japan, etc. It also covers global oil inventories change quarterly outlook, along with weekly historical global oil inventories data with coverage also focused on regions like U.S.A, Singapore, Fujairah, and ARA.

        Parameters
        ----------

         sector: Optional[Union[list[str], Series[str], str]]
             Sector name, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             The products on which we do assessments or products closely associated with our assessed products., by default None
         data_series_short: Optional[Union[list[str], Series[str], str]]
             The brief description of the information represented in the data series., by default None
         outlook_horizon: Optional[Union[list[str], Series[str], str]]
             An indicator to determine the time frame of the oil market outlook., by default None
         geography: Optional[Union[list[str], Series[str], str]]
             The name of an area, country division, or the world with characteristics defined by either physical boundaries or human-defined borders., by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Reference column to describe the metrics being reported in the column Value., by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             The indicator of the data granularity within the time continuum., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The standardized unit or units in which the value of the commodity is measured., by default None
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
             The date for a record applies within the data series., by default None
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
        filter_params.append(list_to_filter("dataSeriesShort", data_series_short))
        filter_params.append(list_to_filter("outlookHorizon", outlook_horizon))
        filter_params.append(list_to_filter("geography", geography))
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
            path=f"/analytics/fuels-refining/v1/oil-inventory/latest",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_refinery_production(
        self,
        *,
        sector: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        outlook_horizon: Optional[Union[list[str], Series[str], str]] = None,
        zone: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        data_series_short: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides monthly and yearly updates of country- and region-level refinery production- historical data and forecasts. The data dates back to 2002 and up to 2 year forecast for the short-term dataset and for the long-term data dates back to 1985 and goes out for the next 25 years. Long-term is updated annually, while short-term is updated monthly.

        Parameters
        ----------

         sector: Optional[Union[list[str], Series[str], str]]
             Sector name, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             The name of an economic good, usually a resource, being traded in the derivatives markets., by default None
         outlook_horizon: Optional[Union[list[str], Series[str], str]]
             An indicator to determine the time frame of the oil market outlook., by default None
         zone: Optional[Union[list[str], Series[str], str]]
             Name of a geographical area within a single country used for petroleum product analysis., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The name of the state or province where the refinery is located., by default None
         data_series_short: Optional[Union[list[str], Series[str], str]]
             The brief description of the information represented in the data series., by default None
         from_region: Optional[Union[list[str], Series[str], str]]
             The name of the geographic area or location from which the commodities originate., by default None
         region: Optional[Union[list[str], Series[str], str]]
             The name of an area, country division, or the world with characteristics defined by either physical boundaries or human-defined borders., by default None
         country: Optional[Union[list[str], Series[str], str]]
             The name of the country where the refinery is located., by default None
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
             For point in time data, indicator if this record is currently an active record., by default None
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
        filter_params.append(list_to_filter("zone", zone))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("dataSeriesShort", data_series_short))
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
            path=f"/analytics/fuels-refining/v1/refinery-production",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_refinery_production_latest(
        self,
        *,
        sector: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        outlook_horizon: Optional[Union[list[str], Series[str], str]] = None,
        zone: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        data_series_short: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides monthly and yearly updates of country- and region-level refinery production- historical data and forecasts. The data dates back to 2002 and up to 2 year forecast for the short-term dataset and for the long-term data dates back to 1985 and goes out for the next 25 years. Long-term is updated annually, while short-term is updated monthly.

        Parameters
        ----------

         sector: Optional[Union[list[str], Series[str], str]]
             Sector name, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             The name of an economic good, usually a resource, being traded in the derivatives markets., by default None
         outlook_horizon: Optional[Union[list[str], Series[str], str]]
             An indicator to determine the time frame of the oil market outlook., by default None
         zone: Optional[Union[list[str], Series[str], str]]
             Name of a geographical area within a single country used for petroleum product analysis., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The name of the state or province where the refinery is located., by default None
         data_series_short: Optional[Union[list[str], Series[str], str]]
             The brief description of the information represented in the data series., by default None
         from_region: Optional[Union[list[str], Series[str], str]]
             The name of the geographic area or location from which the commodities originate., by default None
         region: Optional[Union[list[str], Series[str], str]]
             The name of an area, country division, or the world with characteristics defined by either physical boundaries or human-defined borders., by default None
         country: Optional[Union[list[str], Series[str], str]]
             The name of the country where the refinery is located., by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Reference column to describe the metrics being reported in the column Value., by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             The indicator of the data granularity within the time continuum., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The standardized unit or units in which the value of the commodity is measured., by default None
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
             For point in time data, indicator if this record is currently an active record., by default None
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
        filter_params.append(list_to_filter("zone", zone))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("dataSeriesShort", data_series_short))
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
            path=f"/analytics/fuels-refining/v1/refinery-production/latest",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_refinery_runs(
        self,
        *,
        sector: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        data_series_short: Optional[Union[list[str], Series[str], str]] = None,
        outlook_horizon: Optional[Union[list[str], Series[str], str]] = None,
        zone: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides monthly and yearly updates of country- and region-level refinery crude runs - historical data and forecasts. The data dates back to 2017 and up to 2 year forecast for the short-term dataset and for the long-term data dates back to 1985 and goes out for the next 25 years.Long-term is updated annually, while short-term is updated monthly.

        Parameters
        ----------

         sector: Optional[Union[list[str], Series[str], str]]
             Sector name, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             The name of an economic good, usually a resource, being traded in the derivatives markets., by default None
         data_series_short: Optional[Union[list[str], Series[str], str]]
             The brief description of the information represented in the data series., by default None
         outlook_horizon: Optional[Union[list[str], Series[str], str]]
             An indicator to determine the time frame of the oil market outlook., by default None
         zone: Optional[Union[list[str], Series[str], str]]
             Name of a geographical area within a single country used for petroleum product analysis., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The name of the state or province where the refinery is located., by default None
         from_region: Optional[Union[list[str], Series[str], str]]
             The name of the geographic area or location from which the commodities originate., by default None
         region: Optional[Union[list[str], Series[str], str]]
             The name of an area, country division, or the world with characteristics defined by either physical boundaries or human-defined borders., by default None
         country: Optional[Union[list[str], Series[str], str]]
             The name of the country where the refinery is located., by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Reference column to describe the metrics being reported in the column Value., by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             The indicator of the data granularity within the time continuum., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The standardized unit or units in which the value of the commodity is measured., by default None
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
             For point in time data, indicator if this record is currently an active record., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("sector", sector))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("dataSeriesShort", data_series_short))
        filter_params.append(list_to_filter("outlookHorizon", outlook_horizon))
        filter_params.append(list_to_filter("zone", zone))
        filter_params.append(list_to_filter("state", state))
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
            path=f"/analytics/fuels-refining/v1/refinery-runs",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_refinery_runs_latest(
        self,
        *,
        sector: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        data_series_short: Optional[Union[list[str], Series[str], str]] = None,
        outlook_horizon: Optional[Union[list[str], Series[str], str]] = None,
        zone: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
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
        Provides monthly and yearly updates of country- and region-level refinery crude runs - historical data and forecasts. The data dates back to 2017 and up to 2 year forecast for the short-term dataset and for the long-term data dates back to 1985 and goes out for the next 25 years.Long-term is updated annually, while short-term is updated monthly.

        Parameters
        ----------

         sector: Optional[Union[list[str], Series[str], str]]
             Sector name, by default None
         commodity: Optional[Union[list[str], Series[str], str]]
             The name of an economic good, usually a resource, being traded in the derivatives markets., by default None
         data_series_short: Optional[Union[list[str], Series[str], str]]
             The brief description of the information represented in the data series., by default None
         outlook_horizon: Optional[Union[list[str], Series[str], str]]
             An indicator to determine the time frame of the oil market outlook., by default None
         zone: Optional[Union[list[str], Series[str], str]]
             Name of a geographical area within a single country used for petroleum product analysis., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The name of the state or province where the refinery is located., by default None
         from_region: Optional[Union[list[str], Series[str], str]]
             The name of the geographic area or location from which the commodities originate., by default None
         region: Optional[Union[list[str], Series[str], str]]
             The name of an area, country division, or the world with characteristics defined by either physical boundaries or human-defined borders., by default None
         country: Optional[Union[list[str], Series[str], str]]
             The name of the country where the refinery is located., by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Reference column to describe the metrics being reported in the column Value., by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             The indicator of the data granularity within the time continuum., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The standardized unit or units in which the value of the commodity is measured., by default None
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
             For point in time data, indicator if this record is currently an active record., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("sector", sector))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("dataSeriesShort", data_series_short))
        filter_params.append(list_to_filter("outlookHorizon", outlook_horizon))
        filter_params.append(list_to_filter("zone", zone))
        filter_params.append(list_to_filter("state", state))
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
            path=f"/analytics/fuels-refining/v1/refinery-runs/latest",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_refinery_utilization_rate(
        self,
        *,
        sector: Optional[Union[list[str], Series[str], str]] = None,
        data_series_short: Optional[Union[list[str], Series[str], str]] = None,
        outlook_horizon: Optional[Union[list[str], Series[str], str]] = None,
        zone: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
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
        Utilization rate is a throughput of crude runs divided by capacity of a refinery unit, reported in percentages. The data available includes historical data and short term and long-term forecasts. Short term data goes back to 2017 and the forecast goes out for up to 2 years. Long-term dates back to 1985 and goes out for up to 25 years. Long-term is updated annually, while short-term is updated monthly.

        Parameters
        ----------

         sector: Optional[Union[list[str], Series[str], str]]
             Sector name, by default None
         data_series_short: Optional[Union[list[str], Series[str], str]]
             The brief description of the information represented in the data series., by default None
         outlook_horizon: Optional[Union[list[str], Series[str], str]]
             An indicator to determine the time frame of the oil market outlook., by default None
         zone: Optional[Union[list[str], Series[str], str]]
             Name of a geographical area within a single country used for petroleum product analysis., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The name of the state or province where the refinery is located., by default None
         from_region: Optional[Union[list[str], Series[str], str]]
             The name of the geographic area or location from which the commodities originate., by default None
         region: Optional[Union[list[str], Series[str], str]]
             The name of an area, country division, or the world with characteristics defined by either physical boundaries or human-defined borders., by default None
         country: Optional[Union[list[str], Series[str], str]]
             The name of the country where the refinery is located., by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Reference column to describe the metrics being reported in the column Value., by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             The indicator of the data granularity within the time continuum., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The standardized unit or units in which the value of the commodity is measured., by default None
         vintage_date: Optional[date], optional
             The specific date on which a final forecast revision was issued (Series version label).., by default None
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
             For point in time data, indicator if this record is currently an active record., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("sector", sector))
        filter_params.append(list_to_filter("dataSeriesShort", data_series_short))
        filter_params.append(list_to_filter("outlookHorizon", outlook_horizon))
        filter_params.append(list_to_filter("zone", zone))
        filter_params.append(list_to_filter("state", state))
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
            path=f"/analytics/fuels-refining/v1/refinery-utilization-rate",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_refinery_utilization_rate_latest(
        self,
        *,
        sector: Optional[Union[list[str], Series[str], str]] = None,
        data_series_short: Optional[Union[list[str], Series[str], str]] = None,
        outlook_horizon: Optional[Union[list[str], Series[str], str]] = None,
        zone: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
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
        Utilization rate is a throughput of crude runs divided by capacity of a refinery unit, reported in percentages. The data available includes historical data and short term and long-term forecasts. Short term data goes back to 2017 and the forecast goes out for up to 2 years. Long-term dates back to 1985 and goes out for up to 25 years. Long-term is updated annually, while short-term is updated monthly.

        Parameters
        ----------

         sector: Optional[Union[list[str], Series[str], str]]
             Sector name, by default None
         data_series_short: Optional[Union[list[str], Series[str], str]]
             The brief description of the information represented in the data series., by default None
         outlook_horizon: Optional[Union[list[str], Series[str], str]]
             An indicator to determine the time frame of the oil market outlook., by default None
         zone: Optional[Union[list[str], Series[str], str]]
             Name of a geographical area within a single country used for petroleum product analysis., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The name of the state or province where the refinery is located., by default None
         from_region: Optional[Union[list[str], Series[str], str]]
             The name of the geographic area or location from which the commodities originate., by default None
         region: Optional[Union[list[str], Series[str], str]]
             The name of an area, country division, or the world with characteristics defined by either physical boundaries or human-defined borders., by default None
         country: Optional[Union[list[str], Series[str], str]]
             The name of the country where the refinery is located., by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Reference column to describe the metrics being reported in the column Value., by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             The indicator of the data granularity within the time continuum., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The standardized unit or units in which the value of the commodity is measured., by default None
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
             For point in time data, indicator if this record is currently an active record., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("sector", sector))
        filter_params.append(list_to_filter("dataSeriesShort", data_series_short))
        filter_params.append(list_to_filter("outlookHorizon", outlook_horizon))
        filter_params.append(list_to_filter("zone", zone))
        filter_params.append(list_to_filter("state", state))
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
            path=f"/analytics/fuels-refining/v1/refinery-utilization-rate/latest",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

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

        columns = ["vintageDate", "reportForDate", "historicalEdgeDate", "modifiedDate"]

        for c in columns:
            if c in df.columns:
                df[c] = pd.to_datetime(
                    df[c], utc=True, format="ISO8601", errors="coerce"
                )

        return df
