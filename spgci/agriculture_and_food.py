# Copyright 2026 S&P Global Energy (previously S&P Global Commodity Insights)

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
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date, datetime
import pandas as pd


class AgriAndFood:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _cop_forecast_data_mv_endpoint = "/cost-of-production"

    _datasets = Literal[
        "cost-of-production",
        "global-long-term-forecast",
        "price-purchase-forecast",
        "proteins-short-term-forecast",
        "softs-short-term-forecast",
        "crops-short-term-forecast",
        "baseline-forecast"
    ]

    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
        """
        Converts the API response to a pandas DataFrame and ensures proper datetime conversion
        for all relevant date/time fields.
        """
        j = resp.json()
        df = pd.json_normalize(j["results"])

        datetime_fields = [
            "modifiedDate",
            "startDate",
            "endDate",
            "observationDate"
        ]

        for field in datetime_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors="coerce")

        return df

    def get_unique_values(
        self,
        dataset: _datasets,
        columns: Optional[Union[list[str], str]],
        filter_exp: Optional[str] = None,
    ) -> DataFrame:
        """
        Get unique values for specified columns in a dataset, optionally filtered by an expression.

        This method is crucial for data discovery and validation before making actual data queries.
        Use this to understand what values are available in the dataset and what combinations
        actually exist before attempting to filter your main data queries.

        Args:
            dataset (str): The dataset name converted from method name using kebab-case format:
                - get_region_supply_demand_balance → "region-supply-demand-balance"
                - get_demand_latest → "demand-latest"
                - get_cargo_flows → "cargo-flows"
            columns (list[str] or str): Column names to get unique values for.
                - Use camelCase format: ["commodity", "region", "outlookHorizon"]
                - Can be single string: "commodity"
                - Can be multiple columns: ["commodity", "region", "outlookHorizon"]
            filter_exp (str, optional): Filter expression to limit results to specific subsets.
                Use ci.utilities.build_filter_expression() to construct this properly.

        Returns:
            pd.DataFrame: DataFrame with unique combinations of the specified columns,
            optionally filtered by the provided expression.

        Example Usage:
            # Step 1: Get all available commodities
            commodities = rp.get_unique_values('demand-latest', 'commodity')

            # Step 2: Get filtered combinations for specific commodities and regions
            selected_commodities = ["Jet fuel", "Jet/Kero"]
            selected_regions = ["Europe"]

            filter_exp = ci.utilities.build_filter_expression({
                "commodity": selected_commodities,
                "region": selected_regions
            })

            combos = rp.get_unique_values(
                'demand-latest',
                ['commodity', 'region', 'outlookHorizon', 'vintageDate'],
                filter_exp=filter_exp
            )
        """
        dataset_to_path = {
            "cost-of-production": "analytics/agri-food/v1/cost-of-production",
            "global-long-term-forecast": "analytics/agriculture-food/v1/global-long-term-forecast",
            "price-purchase-forecast": "analytics/agriculture-food/v1/price-purchase-forecast",
            "proteins-short-term-forecast": "analytics/agriculture-food/v1/proteins-short-term-forecast",
            "softs-short-term-forecast": "analytics/agriculture-food/v1/softs-short-term-forecast",
            "crops-short-term-forecast": "analytics/agriculture-food/v1/crops-short-term-forecast",
            "baseline-forecast":  "analytics/ags-food/commodity-price-publish/v1/commodity-price-publish"
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
        params = {"groupBy": col_value, "pageSize": 5000}

        if filter_exp is not None:
            params.update({"filter": filter_exp})

        def to_df(resp: Response):
            j = resp.json()
            return DataFrame(j["aggResultValue"])

        return get_data(path, params, to_df, paginate=True)

    def get_cost_of_production(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        sub_commodity: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
        parent_item: Optional[Union[list[str], Series[str], str]] = None,
        item: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        unit_type: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        full_unit_name: Optional[Union[list[str], Series[str], str]] = None,
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
        Access the Cost Of Production Forecast data.

        Parameters
        ----------

         commodity: Optional[Union[list[str], Series[str], str]]
             The name of the commodity., by default None
         sub_commodity: Optional[Union[list[str], Series[str], str]]
             The name of a sub commodity., by default None
         geography: Optional[Union[list[str], Series[str], str]]
             The name of an area, country division, or the world with characteristics defined by either physical boundaries or human-defined borders for which the report or model output is reported., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A smaller, distinct area within a larger region, characterized by specific geographical, cultural, economic, or political features defined by either natural or human-made boundaries for which the report or model output is reported., by default None
         year: Optional[int], optional
             The year for which the record applies within the data table, this can be a historical or forecast date., by default None
         year_gt: Optional[int], optional
             filter by `year > x`, by default None
         year_gte: Optional[int], optional
             filter by `year >= x`, by default None
         year_lt: Optional[int], optional
             filter by `year < x`, by default None
         year_lte: Optional[int], optional
             filter by `year <= x`, by default None
         category: Optional[Union[list[str], Series[str], str]]
             The high-level grouping of categories of quantitative measures that provide insights into the economic performance of operations., by default None
         parent_item: Optional[Union[list[str], Series[str], str]]
             The categories of quantitative measures that provide insights into the economic performance of operations., by default None
         item: Optional[Union[list[str], Series[str], str]]
             The detailed reporting level for the economic cost, margin, and yield data., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The short code identifying the standardized unit or units in which the value of the commodity is measured., by default None
         unit_type: Optional[Union[list[str], Series[str], str]]
             A classification that specifies the type of measurement being used to quantify a physical property., by default None
         currency: Optional[Union[list[str], Series[str], str]]
             The code representing a standard unit of monetary value of a country or region., by default None
         full_unit_name: Optional[Union[list[str], Series[str], str]]
             The standardized unit or units in which the value of the commodity is measured., by default None
         modified_date: Optional[datetime], optional
             The date and time when a particular record was last updated or modified., by default None
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
        filter_params.append(list_to_filter("subCommodity", sub_commodity))
        filter_params.append(list_to_filter("geography", geography))
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
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("parentItem", parent_item))
        filter_params.append(list_to_filter("item", item))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("unitType", unit_type))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("fullUnitName", full_unit_name))
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
            path=f"/analytics/agri-food/v1/cost-of-production",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_global_long_term_forecast(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        short_label: Optional[Union[list[str], Series[str], str]] = None,
        reporting_region: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        frequency: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        mnemonic: Optional[Union[list[str], Series[str], str]] = None,
        report_for_date: Optional[date] = None,
        report_for_date_lt: Optional[date] = None,
        report_for_date_lte: Optional[date] = None,
        report_for_date_gt: Optional[date] = None,
        report_for_date_gte: Optional[date] = None,
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

        Parameters
        ----------

         commodity: Optional[Union[list[str], Series[str], str]]
             The name of an economic good, usually a resource, being traded in the derivatives markets., by default None
         short_label: Optional[Union[list[str], Series[str], str]]
             The brief description of the information represented in the data series., by default None
         reporting_region: Optional[Union[list[str], Series[str], str]]
             The geographic region for which the report or model output is reported., by default None
         concept: Optional[Union[list[str], Series[str], str]]
             The logical grouping or classification of related data elements and entities that are relevant to a particular subject or topic., by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             The indicator of how often the data is refreshed or collected., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit or units in which the value of the commodity is measured., by default None
         currency: Optional[Union[list[str], Series[str], str]]
             A code representing a standard unit of value of a country or region., by default None
         mnemonic: Optional[Union[list[str], Series[str], str]]
             , by default None
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
         modified_date: Optional[datetime], optional
             The date and time when a particular record was last updated or modified., by default None
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
        filter_params.append(list_to_filter("shortLabel", short_label))
        filter_params.append(list_to_filter("reportingRegion", reporting_region))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("mnemonic", mnemonic))
        filter_params.append(list_to_filter("reportForDate", report_for_date))
        if report_for_date_gt is not None:
            filter_params.append(f'reportForDate > "{report_for_date_gt}"')
        if report_for_date_gte is not None:
            filter_params.append(f'reportForDate >= "{report_for_date_gte}"')
        if report_for_date_lt is not None:
            filter_params.append(f'reportForDate < "{report_for_date_lt}"')
        if report_for_date_lte is not None:
            filter_params.append(f'reportForDate <= "{report_for_date_lte}"')
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
            path=f"analytics/agriculture-food/v1/global-long-term-forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_price_purchase_forecast(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        description: Optional[Union[list[str], Series[str], str]] = None,
        reporting_region: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        frequency: Optional[Union[list[str], Series[str], str]] = None,
        report_for_date: Optional[date] = None,
        report_for_date_lt: Optional[date] = None,
        report_for_date_lte: Optional[date] = None,
        report_for_date_gt: Optional[date] = None,
        report_for_date_gte: Optional[date] = None,
        source: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        series_type: Optional[str] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """


        Parameters
        ----------

         commodity: Optional[Union[list[str], Series[str], str]]
             The name of an economic good, usually a resource, being traded in the derivatives markets., by default None
         description: Optional[Union[list[str], Series[str], str]]
             The brief description of the information represented in the data series., by default None
         reporting_region: Optional[Union[list[str], Series[str], str]]
             The geographic region for which the report or model output is reported., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Numeric value used to convert between units of measure of different fuel types., by default None
         currency: Optional[Union[list[str], Series[str], str]]
             A code representing a standard unit of value of a country or region., by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             The indicator of how often the data is refreshed or collected., by default None
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
         source: Optional[Union[list[str], Series[str], str]]
             The name of the source providing the information in the data series., by default None
         modified_date: Optional[datetime], optional
             The date and time when a particular record was last updated or modified., by default None
         modified_date_gt: Optional[datetime], optional
             filter by `modified_date > x`, by default None
         modified_date_gte: Optional[datetime], optional
             filter by `modified_date >= x`, by default None
         modified_date_lt: Optional[datetime], optional
             filter by `modified_date < x`, by default None
         modified_date_lte: Optional[datetime], optional
             filter by `modified_date <= x`, by default None
         series_type: Optional[str], optional
             `Forecast` or `Historical`
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("description", description))
        filter_params.append(list_to_filter("reportingRegion", reporting_region))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("reportForDate", report_for_date))
        if report_for_date_gt is not None:
            filter_params.append(f'reportForDate > "{report_for_date_gt}"')
        if report_for_date_gte is not None:
            filter_params.append(f'reportForDate >= "{report_for_date_gte}"')
        if report_for_date_lt is not None:
            filter_params.append(f'reportForDate < "{report_for_date_lt}"')
        if report_for_date_lte is not None:
            filter_params.append(f'reportForDate <= "{report_for_date_lte}"')
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("seriesType", series_type))
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
            path=f"analytics/agriculture-food/v1/price-purchase-forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_proteins_short_term_forecast(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        crop_month: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        frequency: Optional[Union[list[str], Series[str], str]] = None,
        unit: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        observation_date: Optional[date] = None,
        observation_date_lt: Optional[date] = None,
        observation_date_lte: Optional[date] = None,
        observation_date_gt: Optional[date] = None,
        observation_date_gte: Optional[date] = None,
        start_date: Optional[date] = None,
        start_date_lt: Optional[date] = None,
        start_date_lte: Optional[date] = None,
        start_date_gt: Optional[date] = None,
        start_date_gte: Optional[date] = None,
        end_date: Optional[date] = None,
        end_date_lt: Optional[date] = None,
        end_date_lte: Optional[date] = None,
        end_date_gt: Optional[date] = None,
        end_date_gte: Optional[date] = None,
        last_updated: Optional[date] = None,
        short_label: Optional[Union[list[str], Series[str], str]] = None,
        series_type: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[bool] = None,
        source_attribute_id: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the Proteins Short-Term Forecast data.

        Parameters
        ----------
        commodity : Optional[Union[list[str], Series[str], str]]
            The commodity represented by the data series, by default None.
        crop_month : Optional[Union[list[str], Series[str], str]]
            The crop month associated with the data series, by default None.
        geography : Optional[Union[list[str], Series[str], str]]
            The geography for which the data is reported, by default None.
        concept : Optional[Union[list[str], Series[str], str]]
            The subject or concept represented by the data series, by default None.
        frequency : Optional[Union[list[str], Series[str], str]]
            The frequency of the data series, by default None.
        unit: Optional[Union[list[str], Series[str], str]]
            The full unit name for the observation value, by default None.
        uom : Optional[Union[list[str], Series[str], str]]
            The unit-of-measure code, by default None.
        currency : Optional[Union[list[str], Series[str], str]]
            The currency associated with the observation value, by default None.
        observation_date : Optional[date]
            The observation date for which the record applies, by default None.
        observation_date_gt, observation_date_gte, observation_date_lt, observation_date_lte : Optional[date]
            Comparison filters for the observation date, by default None.
        start_date : Optional[date]
            The start date of the observation period, by default None.
        start_date_gt, start_date_gte, start_date_lt, start_date_lte : Optional[date]
            Comparison filters for the start date, by default None.
        end_date : Optional[date]
            The end date of the observation period, by default None.
        end_date_gt, end_date_gte, end_date_lt, end_date_lte : Optional[date]
            Comparison filters for the end date, by default None.
        last_updated : Optional[date]
            The date on which the series was last updated, by default None.
        short_label : Optional[Union[list[str], Series[str], str]]
            The short descriptive label for the data series, by default None.
        series_type : Optional[Union[list[str], Series[str], str]]
            Indicates whether the series is historical or forecast, by default None.
        modified_date : Optional[datetime]
            The timestamp when the record was last modified, by default None.
        modified_date_gt, modified_date_gte, modified_date_lt, modified_date_lte : Optional[datetime]
            Comparison filters for the modified timestamp, by default None.
        is_active : Optional[bool]
            Whether the record is active, by default None.
        source_attribute_id : Optional[Union[list[str], Series[str], str]]
            The source attribute identifier, by default None.
        filter_exp : Optional[str]
            An additional API filter expression, by default None.
        page : int
            Page number to retrieve, by default 1.
        page_size : int
            Number of records per page, by default 5000.
        raw : bool
            Return the raw API response when True, by default False.
        paginate : bool
            Retrieve all available pages when True, by default False.

        Returns
        -------
        Union[DataFrame, Response]
            A pandas DataFrame or the raw API response.
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("cropMonth", crop_month))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("unit", unit))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("observationDate", observation_date))
        if observation_date_gt is not None:
            filter_params.append(f'observationDate > "{observation_date_gt}"')
        if observation_date_gte is not None:
            filter_params.append(f'observationDate >= "{observation_date_gte}"')
        if observation_date_lt is not None:
            filter_params.append(f'observationDate < "{observation_date_lt}"')
        if observation_date_lte is not None:
            filter_params.append(f'observationDate <= "{observation_date_lte}"')
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
        filter_params.append(list_to_filter("lastUpdated", last_updated))
        filter_params.append(list_to_filter("shortLabel", short_label))
        filter_params.append(list_to_filter("seriesType", series_type))
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
        filter_params.append(list_to_filter("sourceAttributeId", source_attribute_id))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path="analytics/agriculture-food/v1/proteins-short-term-forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_softs_short_term_forecast(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        crop_month: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        frequency: Optional[Union[list[str], Series[str], str]] = None,
        unit: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        observation_date: Optional[date] = None,
        observation_date_lt: Optional[date] = None,
        observation_date_lte: Optional[date] = None,
        observation_date_gt: Optional[date] = None,
        observation_date_gte: Optional[date] = None,
        start_date: Optional[date] = None,
        start_date_lt: Optional[date] = None,
        start_date_lte: Optional[date] = None,
        start_date_gt: Optional[date] = None,
        start_date_gte: Optional[date] = None,
        end_date: Optional[date] = None,
        end_date_lt: Optional[date] = None,
        end_date_lte: Optional[date] = None,
        end_date_gt: Optional[date] = None,
        end_date_gte: Optional[date] = None,
        last_updated: Optional[date] = None,
        short_label: Optional[Union[list[str], Series[str], str]] = None,
        series_type: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[bool] = None,
        source_attribute_id: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the Softs Short-Term Forecast data.

        Parameters
        ----------
        commodity : Optional[Union[list[str], Series[str], str]]
            The commodity represented by the data series, by default None.
        crop_month : Optional[Union[list[str], Series[str], str]]
            The crop month associated with the data series, by default None.
        geography : Optional[Union[list[str], Series[str], str]]
            The geography for which the data is reported, by default None.
        concept : Optional[Union[list[str], Series[str], str]]
            The subject or concept represented by the data series, by default None.
        frequency : Optional[Union[list[str], Series[str], str]]
            The frequency of the data series, by default None.
        unit: Optional[Union[list[str], Series[str], str]]
            The full unit name for the observation value, by default None.
        uom : Optional[Union[list[str], Series[str], str]]
            The unit-of-measure code, by default None.
        currency : Optional[Union[list[str], Series[str], str]]
            The currency associated with the observation value, by default None.
        observation_date : Optional[date]
            The observation date for which the record applies, by default None.
        observation_date_gt, observation_date_gte, observation_date_lt, observation_date_lte : Optional[date]
            Comparison filters for the observation date, by default None.
        start_date : Optional[date]
            The start date of the observation period, by default None.
        start_date_gt, start_date_gte, start_date_lt, start_date_lte : Optional[date]
            Comparison filters for the start date, by default None.
        end_date : Optional[date]
            The end date of the observation period, by default None.
        end_date_gt, end_date_gte, end_date_lt, end_date_lte : Optional[date]
            Comparison filters for the end date, by default None.
        last_updated : Optional[date]
            The date on which the series was last updated, by default None.
        short_label : Optional[Union[list[str], Series[str], str]]
            The short descriptive label for the data series, by default None.
        series_type : Optional[Union[list[str], Series[str], str]]
            Indicates whether the series is historical or forecast, by default None.
        modified_date : Optional[datetime]
            The timestamp when the record was last modified, by default None.
        modified_date_gt, modified_date_gte, modified_date_lt, modified_date_lte : Optional[datetime]
            Comparison filters for the modified timestamp, by default None.
        is_active : Optional[bool]
            Whether the record is active, by default None.
        source_attribute_id : Optional[Union[list[str], Series[str], str]]
            The source attribute identifier, by default None.
        filter_exp : Optional[str]
            An additional API filter expression, by default None.
        page : int
            Page number to retrieve, by default 1.
        page_size : int
            Number of records per page, by default 5000.
        raw : bool
            Return the raw API response when True, by default False.
        paginate : bool
            Retrieve all available pages when True, by default False.

        Returns
        -------
        Union[DataFrame, Response]
            A pandas DataFrame or the raw API response.
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("cropMonth", crop_month))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("unit", unit))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("observationDate", observation_date))
        if observation_date_gt is not None:
            filter_params.append(f'observationDate > "{observation_date_gt}"')
        if observation_date_gte is not None:
            filter_params.append(f'observationDate >= "{observation_date_gte}"')
        if observation_date_lt is not None:
            filter_params.append(f'observationDate < "{observation_date_lt}"')
        if observation_date_lte is not None:
            filter_params.append(f'observationDate <= "{observation_date_lte}"')
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
        filter_params.append(list_to_filter("lastUpdated", last_updated))
        filter_params.append(list_to_filter("shortLabel", short_label))
        filter_params.append(list_to_filter("seriesType", series_type))
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
        filter_params.append(list_to_filter("sourceAttributeId", source_attribute_id))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path="analytics/agriculture-food/v1/softs-short-term-forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_crops_short_term_forecast(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        crop_month: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        frequency: Optional[Union[list[str], Series[str], str]] = None,
        unit: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        observation_date: Optional[date] = None,
        observation_date_lt: Optional[date] = None,
        observation_date_lte: Optional[date] = None,
        observation_date_gt: Optional[date] = None,
        observation_date_gte: Optional[date] = None,
        start_date: Optional[date] = None,
        start_date_lt: Optional[date] = None,
        start_date_lte: Optional[date] = None,
        start_date_gt: Optional[date] = None,
        start_date_gte: Optional[date] = None,
        end_date: Optional[date] = None,
        end_date_lt: Optional[date] = None,
        end_date_lte: Optional[date] = None,
        end_date_gt: Optional[date] = None,
        end_date_gte: Optional[date] = None,
        last_updated: Optional[date] = None,
        short_label: Optional[Union[list[str], Series[str], str]] = None,
        series_type: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        is_active: Optional[bool] = None,
        source_attribute_id: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the Crops Short-Term Forecast data.

        Parameters
        ----------
        commodity : Optional[Union[list[str], Series[str], str]]
            The commodity represented by the data series, by default None.
        crop_month : Optional[Union[list[str], Series[str], str]]
            The crop month associated with the data series, by default None.
        geography : Optional[Union[list[str], Series[str], str]]
            The geography for which the data is reported, by default None.
        concept : Optional[Union[list[str], Series[str], str]]
            The subject or concept represented by the data series, by default None.
        frequency : Optional[Union[list[str], Series[str], str]]
            The frequency of the data series, by default None.
        unit_name : Optional[Union[list[str], Series[str], str]]
            The full unit name for the observation value, by default None.
        uom : Optional[Union[list[str], Series[str], str]]
            The unit-of-measure code, by default None.
        currency : Optional[Union[list[str], Series[str], str]]
            The currency associated with the observation value, by default None.
        observation_date : Optional[date]
            The observation date for which the record applies, by default None.
        observation_date_gt, observation_date_gte, observation_date_lt, observation_date_lte : Optional[date]
            Comparison filters for the observation date, by default None.
        start_date : Optional[date]
            The start date of the observation period, by default None.
        start_date_gt, start_date_gte, start_date_lt, start_date_lte : Optional[date]
            Comparison filters for the start date, by default None.
        end_date : Optional[date]
            The end date of the observation period, by default None.
        end_date_gt, end_date_gte, end_date_lt, end_date_lte : Optional[date]
            Comparison filters for the end date, by default None.
        last_updated : Optional[date]
            The date on which the series was last updated, by default None.
        short_label : Optional[Union[list[str], Series[str], str]]
            The short descriptive label for the data series, by default None.
        series_type : Optional[Union[list[str], Series[str], str]]
            Indicates whether the series is historical or forecast, by default None.
        modified_date : Optional[datetime]
            The timestamp when the record was last modified, by default None.
        modified_date_gt, modified_date_gte, modified_date_lt, modified_date_lte : Optional[datetime]
            Comparison filters for the modified timestamp, by default None.
        is_active : Optional[bool]
            Whether the record is active, by default None.
        source_attribute_id : Optional[Union[list[str], Series[str], str]]
            The source attribute identifier, by default None.
        filter_exp : Optional[str]
            An additional API filter expression, by default None.
        page : int
            Page number to retrieve, by default 1.
        page_size : int
            Number of records per page, by default 5000.
        raw : bool
            Return the raw API response when True, by default False.
        paginate : bool
            Retrieve all available pages when True, by default False.

        Returns
        -------
        Union[DataFrame, Response]
            A pandas DataFrame or the raw API response.
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("cropMonth", crop_month))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("unit", unit))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("observationDate", observation_date))
        if observation_date_gt is not None:
            filter_params.append(f'observationDate > "{observation_date_gt}"')
        if observation_date_gte is not None:
            filter_params.append(f'observationDate >= "{observation_date_gte}"')
        if observation_date_lt is not None:
            filter_params.append(f'observationDate < "{observation_date_lt}"')
        if observation_date_lte is not None:
            filter_params.append(f'observationDate <= "{observation_date_lte}"')
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
        filter_params.append(list_to_filter("lastUpdated", last_updated))
        filter_params.append(list_to_filter("shortLabel", short_label))
        filter_params.append(list_to_filter("seriesType", series_type))
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
        filter_params.append(list_to_filter("sourceAttributeId", source_attribute_id))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path="analytics/agriculture-food/v1/crops-short-term-forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
    def get_baseline_forecast(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        dataset: Optional[Union[list[str], Series[str], str]] = None,
        forecast_type: Optional[Union[list[str], Series[str], str]] = None,
        frequency: Optional[Union[list[str], Series[str], str]] = None,
        observation_date: Optional[date] = None,
        observation_date_lt: Optional[date] = None,
        observation_date_lte: Optional[date] = None,
        observation_date_gt: Optional[date] = None,
        observation_date_gte: Optional[date] = None,
        reporting_region: Optional[Union[list[str], Series[str], str]] = None,
        source_dataset: Optional[Union[list[str], Series[str], str]] = None,
        is_active: Optional[bool] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        series_type: Optional[Union[list[str], Series[str], str]] = None,
        short_label: Optional[Union[list[str], Series[str], str]] = None,
        price_symbol: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the Baseline Forecast API

        Parameters
        ----------
        commodity : Optional[Union[list[str], Series[str], str]]
            The name of the commodity, by default None.
        currency : Optional[Union[list[str], Series[str], str]]
            The currency associated with the observation value, by default None.
        dataset : Optional[Union[list[str], Series[str], str]]
            The dataset associated with the price series, by default None.
        forecast_type : Optional[Union[list[str], Series[str], str]]
            The type of forecast represented by the series, by default None.
        frequency : Optional[Union[list[str], Series[str], str]]
            The frequency of the data series, by default None.
        observation_date : Optional[date]
            The date for which the observation applies, by default None.
        observation_date_gt : Optional[date]
            Filter by `observation_date > x`, by default None.
        observation_date_gte : Optional[date]
            Filter by `observation_date >= x`, by default None.
        observation_date_lt : Optional[date]
            Filter by `observation_date < x`, by default None.
        observation_date_lte : Optional[date]
            Filter by `observation_date <= x`, by default None.
        reporting_region : Optional[Union[list[str], Series[str], str]]
            The geographic region for which the series is reported, by default None.
        source_dataset : Optional[Union[list[str], Series[str], str]]
            The source dataset from which the series originates, by default None.
        is_active : Optional[bool]
            Whether the record is active, by default None.
        uom : Optional[Union[list[str], Series[str], str]]
            The unit of measure associated with the observation value, by default None.
        series_type : Optional[Union[list[str], Series[str], str]]
            The type of data series, such as historical or forecast, by default None.
        short_label : Optional[Union[list[str], Series[str], str]]
            The short label identifying the data series, by default None.
        price_symbol : Optional[Union[list[str], Series[str], str]]
            The symbol identifying the price series, by default None.
        filter_exp : Optional[str]
            An additional API filter expression, by default None.
        page : int
            Page number to retrieve, by default 1.
        page_size : int
            Number of records per page, by default 5000.
        raw : bool
            Return the raw API response when True, by default False.
        paginate : bool
            Retrieve all available pages when True, by default False.

        Returns
        -------
        Union[DataFrame, Response]
            A pandas DataFrame or the raw API response.
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("dataset", dataset))
        filter_params.append(list_to_filter("forecastType", forecast_type))
        filter_params.append(list_to_filter("frequency", frequency))
        filter_params.append(list_to_filter("observationDate", observation_date))
        if observation_date_gt is not None:
            filter_params.append(f'observationDate > "{observation_date_gt}"')
        if observation_date_gte is not None:
            filter_params.append(f'observationDate >= "{observation_date_gte}"')
        if observation_date_lt is not None:
            filter_params.append(f'observationDate < "{observation_date_lt}"')
        if observation_date_lte is not None:
            filter_params.append(f'observationDate <= "{observation_date_lte}"')
        filter_params.append(list_to_filter("reportingRegion", reporting_region))
        filter_params.append(list_to_filter("sourceDataset", source_dataset))
        filter_params.append(list_to_filter("isActive", is_active))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("seriesType", series_type))
        filter_params.append(list_to_filter("shortLabel", short_label))
        filter_params.append(list_to_filter("priceSymbol", price_symbol))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path="analytics/ags-food/commodity-price-publish/v1/commodity-price-publish",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
