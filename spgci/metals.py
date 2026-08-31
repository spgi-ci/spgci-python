from __future__ import annotations
from typing import List, Optional, Union, Literal
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date, datetime
import pandas as pd


class Metals:
    _endpoint = "api/v1/"
    _tbl_metals_market_outlook_endpoint = "ferrous-and-non-ferrous-metals"

    _datasets = Literal[
        "market-outlook",
        "steel-production",
        "steel-raw-production",
        "trade-exports",
        "trade-imports",
        "domestic-shipments",
        "endusemarket-shipments",
    ]

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
            dataset (str): The dataset name in kebab-case format:
                - get_market_outlook → "market-outlook"
            columns (list[str] or str): Column names to get unique values for.
                - Use camelCase format: ["commodity", "metalType", "frequency"]
                - Can be single string: "commodity"
                - Can be multiple columns: ["commodity", "metalType", "currency"]
            filter_exp (str, optional): Filter expression to limit results to specific subsets.
                Use ci.utilities.build_filter_expression() to construct this properly.

        Returns:
            pd.DataFrame: DataFrame with unique combinations of the specified columns,
            optionally filtered by the provided expression.

        Example Usage:
            # Step 1: Get all available commodities
            commodities = mt.get_unique_values('market-outlook', 'commodity')

            # Step 2: Get filtered combinations for specific commodities
            filter_exp = ci.utilities.build_filter_expression({
                "commodity": ["Steel"],
            })
            combos = mt.get_unique_values(
                'market-outlook',
                ['commodity', 'metalType', 'frequency'],
                filter_exp=filter_exp,
            )
        """

        dataset_to_path = {
            "market-outlook": (
                "analytics/metals/metal-market-outlook/v1/"
                "ferrous-and-non-ferrous-metals"
            ),
            "steel-production": "analytics/metals/us-steel/v1/steel-production",
            "steel-raw-production": "analytics/metals/us-steel/v1/steel-raw-production",
            "trade-exports": "analytics/metals/us-steel/v1/trade-exports",
            "trade-imports": "analytics/metals/us-steel/v1/trade-imports",
            "domestic-shipments": "analytics/metals/us-steel/v1/domestic-shipments",
            "endusemarket-shipments": (
                "analytics/metals/us-steel/v1/endusemarket-shipments"
            ),
        }

        if dataset not in dataset_to_path:
            valid = "\n".join(dataset_to_path.keys())
            print(f"Dataset '{dataset}' not found. Valid Datasets:\n", valid)
            raise ValueError(
                f"dataset '{dataset}' not found ",
            )
        else:
            path = dataset_to_path[dataset]

        col_value = ", ".join(columns) if isinstance(columns, list) else columns or ""
        params = {"GroupBy": col_value, "pageSize": 5000}

        if filter_exp is not None:
            params.update({"filter": filter_exp})

        def to_df(resp: Response) -> pd.DataFrame:
            j = resp.json()
            df = pd.json_normalize(j["aggResultValue"])
            columns_dt = ["reportForDate", "forecastPeriod", "forecastAsofdate"]
            for c in columns_dt:
                if c in df.columns:
                    df[c] = pd.to_datetime(
                        df[c], utc=True, format="ISO8601", errors="coerce"
                    )
            return df

        return get_data(path, params, to_df, paginate=True)

    def get_market_outlook(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        dataset: Optional[Union[list[str], Series[str], str]] = None,
        metal_type: Optional[Union[list[str], Series[str], str]] = None,
        frequency: Optional[Union[list[str], Series[str], str]] = None,
        report_for_date: Optional[datetime] = None,
        report_for_date_lt: Optional[datetime] = None,
        report_for_date_lte: Optional[datetime] = None,
        report_for_date_gt: Optional[datetime] = None,
        report_for_date_gte: Optional[datetime] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        measure_magnitude: Optional[Union[list[str], Series[str], str]] = None,
        forecast_period: Optional[datetime] = None,
        forecast_period_lt: Optional[datetime] = None,
        forecast_period_lte: Optional[datetime] = None,
        forecast_period_gt: Optional[datetime] = None,
        forecast_period_gte: Optional[datetime] = None,
        u_o_m: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        forecast_asofdate: Optional[Union[list[str], Series[str], str]] = None,
        is_active: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Ferrous and Non-Ferrous metal's market outlook

        Parameters
        ----------

         commodity: Optional[Union[list[str], Series[str], str]]
             Commodity type, by default None
         dataset: Optional[Union[list[str], Series[str], str]]
             Metals Market Outlook, by default None
         metal_type: Optional[Union[list[str], Series[str], str]]
             Type of metal, by default None
         frequency: Optional[Union[list[str], Series[str], str]]
             Frequency of the report, by default None
         report_for_date: Optional[datetime], optional
             Report date in the format DD-MM-YYYY, by default None
         report_for_date_gt: Optional[datetime], optional
             filter by `report_for_date > x`, by default None
         report_for_date_gte: Optional[datetime], optional
             filter by `report_for_date >= x`, by default None
         report_for_date_lt: Optional[datetime], optional
             filter by `report_for_date < x`, by default None
         report_for_date_lte: Optional[datetime], optional
             filter by `report_for_date <= x`, by default None
         value: Optional[float], optional
             Value associated with the report, by default None
         value_gt: Optional[float], optional
             filter by `value > x`, by default None
         value_gte: Optional[float], optional
             filter by `value >= x`, by default None
         value_lt: Optional[float], optional
             filter by `value < x`, by default None
         value_lte: Optional[float], optional
             filter by `value <= x`, by default None
         measure_magnitude: Optional[Union[list[str], Series[str], str]]
             Price Detail, by default None
         forecast_period: Optional[datetime], optional
             Forecast period, by default None
         forecast_period_gt: Optional[datetime], optional
             filter by `forecast_period > x`, by default None
         forecast_period_gte: Optional[datetime], optional
             filter by `forecast_period >= x`, by default None
         forecast_period_lt: Optional[datetime], optional
             filter by `forecast_period < x`, by default None
         forecast_period_lte: Optional[datetime], optional
             filter by `forecast_period <= x`, by default None
         u_o_m: Optional[Union[list[str], Series[str], str]]
             UOM, by default None
         currency: Optional[Union[list[str], Series[str], str]]
             Currency, by default None
         forecast_asofdate: Optional[Union[list[str], Series[str], str]]
             Forecast as of date, by default None
         is_active: Optional[Union[list[str], Series[str], str]]
             For point in time data, indicator if this record is currently an active record., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("dataset", dataset))
        filter_params.append(list_to_filter("metalType", metal_type))
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
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')
        filter_params.append(list_to_filter("measureMagnitude", measure_magnitude))
        filter_params.append(list_to_filter("forecastPeriod", forecast_period))
        if forecast_period_gt is not None:
            filter_params.append(f'forecastPeriod > "{forecast_period_gt}"')
        if forecast_period_gte is not None:
            filter_params.append(f'forecastPeriod >= "{forecast_period_gte}"')
        if forecast_period_lt is not None:
            filter_params.append(f'forecastPeriod < "{forecast_period_lt}"')
        if forecast_period_lte is not None:
            filter_params.append(f'forecastPeriod <= "{forecast_period_lte}"')
        filter_params.append(list_to_filter("uOM", u_o_m))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("forecastAsofdate", forecast_asofdate))
        filter_params.append(list_to_filter("isActive", is_active))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/metals/metal-market-outlook/v1/ferrous-and-non-ferrous-metals",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
      j = resp.json()
      df = pd.json_normalize(j["results"])

      if "reportForDate" in df.columns:
          df["reportForDate"] = pd.to_datetime(
              df["reportForDate"],
              # format="%Y-%m-%d",
              errors="coerce",
          )

      if "forecastPeriod" in df.columns:
          df["forecastPeriod"] = pd.to_datetime(
              df["forecastPeriod"],
              # format="%Y-%m-%d",
              errors="coerce",
          )

      return df

    def get_steel_production(
        self,
        *,
        report_date: Optional[date] = None,
        report_date_lt: Optional[date] = None,
        report_date_lte: Optional[date] = None,
        report_date_gt: Optional[date] = None,
        report_date_gte: Optional[date] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        month: Optional[int] = None,
        month_lt: Optional[int] = None,
        month_lte: Optional[int] = None,
        month_gt: Optional[int] = None,
        month_gte: Optional[int] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        dataset_name: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        report_type_name: Optional[Union[list[str], Series[str], str]] = None,
        production_report_type_id: Optional[int] = None,
        production_report_type_id_lt: Optional[int] = None,
        production_report_type_id_lte: Optional[int] = None,
        production_report_type_id_gt: Optional[int] = None,
        production_report_type_id_gte: Optional[int] = None,
        weekly_state_grouping: Optional[Union[list[str], Series[str], str]] = None,
        weekly_state_grouping_id: Optional[int] = None,
        weekly_state_grouping_id_lt: Optional[int] = None,
        weekly_state_grouping_id_lte: Optional[int] = None,
        weekly_state_grouping_id_gt: Optional[int] = None,
        weekly_state_grouping_id_gte: Optional[int] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        value_type: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the U.S. Steel Production data.

        Parameters
        ----------
        report_date : Optional[date]
            The start date of the reporting period., by default None.
        report_date_gt, report_date_gte, report_date_lt, report_date_lte : Optional[date]
            Comparison filters for `report_date`, by default None.
        year : Optional[int]
            The calendar year of the reporting period., by default None.
        year_gt, year_gte, year_lt, year_lte : Optional[int]
            Comparison filters for `year`, by default None.
        month : Optional[int]
            The calendar month of the reporting period., by default None.
        month_gt, month_gte, month_lt, month_lte : Optional[int]
            Comparison filters for `month`, by default None.
        date_frequency : Optional[Union[list[str], Series[str], str]]
            The reporting frequency of the record., by default None.
        dataset_name : Optional[Union[list[str], Series[str], str]]
            The name of the source dataset., by default None.
        commodity : Optional[Union[list[str], Series[str], str]]
            The commodity classification., by default None.
        currency : Optional[Union[list[str], Series[str], str]]
            The reporting currency., by default None.
        report_type_name : Optional[Union[list[str], Series[str], str]]
            The production report type classification., by default None.
        production_report_type_id : Optional[int]
            The production report type identifier., by default None.
        production_report_type_id_gt, production_report_type_id_gte, production_report_type_id_lt, production_report_type_id_lte : Optional[int]
            Comparison filters for `production_report_type_id`, by default None.
        weekly_state_grouping : Optional[Union[list[str], Series[str], str]]
            The weekly geographic grouping., by default None.
        weekly_state_grouping_id : Optional[int]
            The weekly geographic grouping identifier., by default None.
        weekly_state_grouping_id_gt, weekly_state_grouping_id_gte, weekly_state_grouping_id_lt, weekly_state_grouping_id_lte : Optional[int]
            Comparison filters for `weekly_state_grouping_id`, by default None.
        uom : Optional[Union[list[str], Series[str], str]]
            The unit of measure., by default None.
        value_type : Optional[Union[list[str], Series[str], str]]
            The type of measured value., by default None.
        value : Optional[float]
            The numeric observation value., by default None.
        value_gt, value_gte, value_lt, value_lte : Optional[float]
            Comparison filters for `value`, by default None.
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
        filter_params.append(list_to_filter("reportDate", report_date))
        if report_date_gt is not None:
            filter_params.append(f'reportDate > "{report_date_gt}"')
        if report_date_gte is not None:
            filter_params.append(f'reportDate >= "{report_date_gte}"')
        if report_date_lt is not None:
            filter_params.append(f'reportDate < "{report_date_lt}"')
        if report_date_lte is not None:
            filter_params.append(f'reportDate <= "{report_date_lte}"')
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("month", month))
        if month_gt is not None:
            filter_params.append(f'month > "{month_gt}"')
        if month_gte is not None:
            filter_params.append(f'month >= "{month_gte}"')
        if month_lt is not None:
            filter_params.append(f'month < "{month_lt}"')
        if month_lte is not None:
            filter_params.append(f'month <= "{month_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("datasetName", dataset_name))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("reportTypeName", report_type_name))
        filter_params.append(list_to_filter("productionReportTypeId", production_report_type_id))
        if production_report_type_id_gt is not None:
            filter_params.append(f'productionReportTypeId > "{production_report_type_id_gt}"')
        if production_report_type_id_gte is not None:
            filter_params.append(f'productionReportTypeId >= "{production_report_type_id_gte}"')
        if production_report_type_id_lt is not None:
            filter_params.append(f'productionReportTypeId < "{production_report_type_id_lt}"')
        if production_report_type_id_lte is not None:
            filter_params.append(f'productionReportTypeId <= "{production_report_type_id_lte}"')
        filter_params.append(list_to_filter("weeklyStateGrouping", weekly_state_grouping))
        filter_params.append(list_to_filter("weeklyStateGroupingId", weekly_state_grouping_id))
        if weekly_state_grouping_id_gt is not None:
            filter_params.append(f'weeklyStateGroupingId > "{weekly_state_grouping_id_gt}"')
        if weekly_state_grouping_id_gte is not None:
            filter_params.append(f'weeklyStateGroupingId >= "{weekly_state_grouping_id_gte}"')
        if weekly_state_grouping_id_lt is not None:
            filter_params.append(f'weeklyStateGroupingId < "{weekly_state_grouping_id_lt}"')
        if weekly_state_grouping_id_lte is not None:
            filter_params.append(f'weeklyStateGroupingId <= "{weekly_state_grouping_id_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("valueType", value_type))
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path="analytics/metals/us-steel/v1/steel-production",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_steel_raw_production(
        self,
        *,
        report_date: Optional[date] = None,
        report_date_lt: Optional[date] = None,
        report_date_lte: Optional[date] = None,
        report_date_gt: Optional[date] = None,
        report_date_gte: Optional[date] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        month: Optional[int] = None,
        month_lt: Optional[int] = None,
        month_lte: Optional[int] = None,
        month_gt: Optional[int] = None,
        month_gte: Optional[int] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        dataset_name: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        report_type_name: Optional[Union[list[str], Series[str], str]] = None,
        production_report_type_id: Optional[int] = None,
        production_report_type_id_lt: Optional[int] = None,
        production_report_type_id_lte: Optional[int] = None,
        production_report_type_id_gt: Optional[int] = None,
        production_report_type_id_gte: Optional[int] = None,
        grade_description: Optional[Union[list[str], Series[str], str]] = None,
        grade_id: Optional[int] = None,
        grade_id_lt: Optional[int] = None,
        grade_id_lte: Optional[int] = None,
        grade_id_gt: Optional[int] = None,
        grade_id_gte: Optional[int] = None,
        state_group: Optional[Union[list[str], Series[str], str]] = None,
        state_group_id: Optional[int] = None,
        state_group_id_lt: Optional[int] = None,
        state_group_id_lte: Optional[int] = None,
        state_group_id_gt: Optional[int] = None,
        state_group_id_gte: Optional[int] = None,
        furnace_description: Optional[Union[list[str], Series[str], str]] = None,
        furnace_type_id: Optional[int] = None,
        furnace_type_id_lt: Optional[int] = None,
        furnace_type_id_lte: Optional[int] = None,
        furnace_type_id_gt: Optional[int] = None,
        furnace_type_id_gte: Optional[int] = None,
        casting_description: Optional[Union[list[str], Series[str], str]] = None,
        casting_type_id: Optional[int] = None,
        casting_type_id_lt: Optional[int] = None,
        casting_type_id_lte: Optional[int] = None,
        casting_type_id_gt: Optional[int] = None,
        casting_type_id_gte: Optional[int] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        value_type: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the U.S. Raw Steel Production data.

        Parameters
        ----------
        report_date : Optional[date]
            The start date of the reporting period., by default None.
        report_date_gt, report_date_gte, report_date_lt, report_date_lte : Optional[date]
            Comparison filters for `report_date`, by default None.
        year : Optional[int]
            The calendar year of the reporting period., by default None.
        year_gt, year_gte, year_lt, year_lte : Optional[int]
            Comparison filters for `year`, by default None.
        month : Optional[int]
            The calendar month of the reporting period., by default None.
        month_gt, month_gte, month_lt, month_lte : Optional[int]
            Comparison filters for `month`, by default None.
        date_frequency : Optional[Union[list[str], Series[str], str]]
            The reporting frequency of the record., by default None.
        dataset_name : Optional[Union[list[str], Series[str], str]]
            The name of the source dataset., by default None.
        commodity : Optional[Union[list[str], Series[str], str]]
            The commodity classification., by default None.
        currency : Optional[Union[list[str], Series[str], str]]
            The reporting currency., by default None.
        report_type_name : Optional[Union[list[str], Series[str], str]]
            The production report type classification., by default None.
        production_report_type_id : Optional[int]
            The production report type identifier., by default None.
        production_report_type_id_gt, production_report_type_id_gte, production_report_type_id_lt, production_report_type_id_lte : Optional[int]
            Comparison filters for `production_report_type_id`, by default None.
        grade_description : Optional[Union[list[str], Series[str], str]]
            The steel grade classification., by default None.
        grade_id : Optional[int]
            The steel grade identifier., by default None.
        grade_id_gt, grade_id_gte, grade_id_lt, grade_id_lte : Optional[int]
            Comparison filters for `grade_id`, by default None.
        state_group : Optional[Union[list[str], Series[str], str]]
            The state or regional grouping., by default None.
        state_group_id : Optional[int]
            The state or regional grouping identifier., by default None.
        state_group_id_gt, state_group_id_gte, state_group_id_lt, state_group_id_lte : Optional[int]
            Comparison filters for `state_group_id`, by default None.
        furnace_description : Optional[Union[list[str], Series[str], str]]
            The furnace classification., by default None.
        furnace_type_id : Optional[int]
            The furnace type identifier., by default None.
        furnace_type_id_gt, furnace_type_id_gte, furnace_type_id_lt, furnace_type_id_lte : Optional[int]
            Comparison filters for `furnace_type_id`, by default None.
        casting_description : Optional[Union[list[str], Series[str], str]]
            The casting method classification., by default None.
        casting_type_id : Optional[int]
            The casting type identifier., by default None.
        casting_type_id_gt, casting_type_id_gte, casting_type_id_lt, casting_type_id_lte : Optional[int]
            Comparison filters for `casting_type_id`, by default None.
        uom : Optional[Union[list[str], Series[str], str]]
            The unit of measure., by default None.
        value_type : Optional[Union[list[str], Series[str], str]]
            The type of measured value., by default None.
        value : Optional[float]
            The numeric observation value., by default None.
        value_gt, value_gte, value_lt, value_lte : Optional[float]
            Comparison filters for `value`, by default None.
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
        filter_params.append(list_to_filter("reportDate", report_date))
        if report_date_gt is not None:
            filter_params.append(f'reportDate > "{report_date_gt}"')
        if report_date_gte is not None:
            filter_params.append(f'reportDate >= "{report_date_gte}"')
        if report_date_lt is not None:
            filter_params.append(f'reportDate < "{report_date_lt}"')
        if report_date_lte is not None:
            filter_params.append(f'reportDate <= "{report_date_lte}"')
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("month", month))
        if month_gt is not None:
            filter_params.append(f'month > "{month_gt}"')
        if month_gte is not None:
            filter_params.append(f'month >= "{month_gte}"')
        if month_lt is not None:
            filter_params.append(f'month < "{month_lt}"')
        if month_lte is not None:
            filter_params.append(f'month <= "{month_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("datasetName", dataset_name))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("reportTypeName", report_type_name))
        filter_params.append(list_to_filter("productionReportTypeId", production_report_type_id))
        if production_report_type_id_gt is not None:
            filter_params.append(f'productionReportTypeId > "{production_report_type_id_gt}"')
        if production_report_type_id_gte is not None:
            filter_params.append(f'productionReportTypeId >= "{production_report_type_id_gte}"')
        if production_report_type_id_lt is not None:
            filter_params.append(f'productionReportTypeId < "{production_report_type_id_lt}"')
        if production_report_type_id_lte is not None:
            filter_params.append(f'productionReportTypeId <= "{production_report_type_id_lte}"')
        filter_params.append(list_to_filter("gradeDescription", grade_description))
        filter_params.append(list_to_filter("gradeId", grade_id))
        if grade_id_gt is not None:
            filter_params.append(f'gradeId > "{grade_id_gt}"')
        if grade_id_gte is not None:
            filter_params.append(f'gradeId >= "{grade_id_gte}"')
        if grade_id_lt is not None:
            filter_params.append(f'gradeId < "{grade_id_lt}"')
        if grade_id_lte is not None:
            filter_params.append(f'gradeId <= "{grade_id_lte}"')
        filter_params.append(list_to_filter("stateGroup", state_group))
        filter_params.append(list_to_filter("stateGroupId", state_group_id))
        if state_group_id_gt is not None:
            filter_params.append(f'stateGroupId > "{state_group_id_gt}"')
        if state_group_id_gte is not None:
            filter_params.append(f'stateGroupId >= "{state_group_id_gte}"')
        if state_group_id_lt is not None:
            filter_params.append(f'stateGroupId < "{state_group_id_lt}"')
        if state_group_id_lte is not None:
            filter_params.append(f'stateGroupId <= "{state_group_id_lte}"')
        filter_params.append(list_to_filter("furnaceDescription", furnace_description))
        filter_params.append(list_to_filter("furnaceTypeId", furnace_type_id))
        if furnace_type_id_gt is not None:
            filter_params.append(f'furnaceTypeId > "{furnace_type_id_gt}"')
        if furnace_type_id_gte is not None:
            filter_params.append(f'furnaceTypeId >= "{furnace_type_id_gte}"')
        if furnace_type_id_lt is not None:
            filter_params.append(f'furnaceTypeId < "{furnace_type_id_lt}"')
        if furnace_type_id_lte is not None:
            filter_params.append(f'furnaceTypeId <= "{furnace_type_id_lte}"')
        filter_params.append(list_to_filter("castingDescription", casting_description))
        filter_params.append(list_to_filter("castingTypeId", casting_type_id))
        if casting_type_id_gt is not None:
            filter_params.append(f'castingTypeId > "{casting_type_id_gt}"')
        if casting_type_id_gte is not None:
            filter_params.append(f'castingTypeId >= "{casting_type_id_gte}"')
        if casting_type_id_lt is not None:
            filter_params.append(f'castingTypeId < "{casting_type_id_lt}"')
        if casting_type_id_lte is not None:
            filter_params.append(f'castingTypeId <= "{casting_type_id_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("valueType", value_type))
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path="analytics/metals/us-steel/v1/steel-raw-production",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_trade_exports(
        self,
        *,
        report_date: Optional[date] = None,
        report_date_lt: Optional[date] = None,
        report_date_lte: Optional[date] = None,
        report_date_gt: Optional[date] = None,
        report_date_gte: Optional[date] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        month: Optional[int] = None,
        month_lt: Optional[int] = None,
        month_lte: Optional[int] = None,
        month_gt: Optional[int] = None,
        month_gte: Optional[int] = None,
        quarter: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        dataset_name: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        hts_code: Optional[Union[list[str], Series[str], str]] = None,
        concordance_id: Optional[int] = None,
        concordance_id_lt: Optional[int] = None,
        concordance_id_lte: Optional[int] = None,
        concordance_id_gt: Optional[int] = None,
        concordance_id_gte: Optional[int] = None,
        domestic_foreign: Optional[Union[list[str], Series[str], str]] = None,
        domestic_foreign_id: Optional[int] = None,
        domestic_foreign_id_lt: Optional[int] = None,
        domestic_foreign_id_lte: Optional[int] = None,
        domestic_foreign_id_gt: Optional[int] = None,
        domestic_foreign_id_gte: Optional[int] = None,
        country_name: Optional[Union[list[str], Series[str], str]] = None,
        country_id: Optional[int] = None,
        country_id_lt: Optional[int] = None,
        country_id_lte: Optional[int] = None,
        country_id_gt: Optional[int] = None,
        country_id_gte: Optional[int] = None,
        district_lading_name: Optional[Union[list[str], Series[str], str]] = None,
        district_lading_id: Optional[int] = None,
        district_lading_id_lt: Optional[int] = None,
        district_lading_id_lte: Optional[int] = None,
        district_lading_id_gt: Optional[int] = None,
        district_lading_id_gte: Optional[int] = None,
        product_name: Optional[Union[list[str], Series[str], str]] = None,
        product_id: Optional[int] = None,
        product_id_lt: Optional[int] = None,
        product_id_lte: Optional[int] = None,
        product_id_gt: Optional[int] = None,
        product_id_gte: Optional[int] = None,
        product_group: Optional[Union[list[str], Series[str], str]] = None,
        product_group_id: Optional[int] = None,
        product_group_id_lt: Optional[int] = None,
        product_group_id_lte: Optional[int] = None,
        product_group_id_gt: Optional[int] = None,
        product_group_id_gte: Optional[int] = None,
        product_sub_group: Optional[Union[list[str], Series[str], str]] = None,
        product_sub_group_id: Optional[int] = None,
        product_sub_group_id_lt: Optional[int] = None,
        product_sub_group_id_lte: Optional[int] = None,
        product_sub_group_id_gt: Optional[int] = None,
        product_sub_group_id_gte: Optional[int] = None,
        product_type: Optional[Union[list[str], Series[str], str]] = None,
        product_type_id: Optional[int] = None,
        product_type_id_lt: Optional[int] = None,
        product_type_id_lte: Optional[int] = None,
        product_type_id_gt: Optional[int] = None,
        product_type_id_gte: Optional[int] = None,
        product_sub_type: Optional[Union[list[str], Series[str], str]] = None,
        product_sub_type_id: Optional[int] = None,
        product_sub_type_id_lt: Optional[int] = None,
        product_sub_type_id_lte: Optional[int] = None,
        product_sub_type_id_gt: Optional[int] = None,
        product_sub_type_id_gte: Optional[int] = None,
        grade: Optional[Union[list[str], Series[str], str]] = None,
        grade_id: Optional[int] = None,
        grade_id_lt: Optional[int] = None,
        grade_id_lte: Optional[int] = None,
        grade_id_gt: Optional[int] = None,
        grade_id_gte: Optional[int] = None,
        value_type: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the U.S. Steel Trade Exports data.

        Parameters
        ----------
        report_date : Optional[date]
            The start date of the reporting period., by default None.
        report_date_gt, report_date_gte, report_date_lt, report_date_lte : Optional[date]
            Comparison filters for `report_date`, by default None.
        year : Optional[int]
            The calendar year of the reporting period., by default None.
        year_gt, year_gte, year_lt, year_lte : Optional[int]
            Comparison filters for `year`, by default None.
        month : Optional[int]
            The calendar month of the reporting period., by default None.
        month_gt, month_gte, month_lt, month_lte : Optional[int]
            Comparison filters for `month`, by default None.
        quarter : Optional[Union[list[str], Series[str], str]]
            The quarter of the reporting period., by default None.
        date_frequency : Optional[Union[list[str], Series[str], str]]
            The reporting frequency of the record., by default None.
        dataset_name : Optional[Union[list[str], Series[str], str]]
            The name of the source dataset., by default None.
        commodity : Optional[Union[list[str], Series[str], str]]
            The commodity classification., by default None.
        currency : Optional[Union[list[str], Series[str], str]]
            The reporting currency., by default None.
        hts_code : Optional[Union[list[str], Series[str], str]]
            The Harmonized Tariff Schedule code., by default None.
        concordance_id : Optional[int]
            The internal product concordance identifier., by default None.
        concordance_id_gt, concordance_id_gte, concordance_id_lt, concordance_id_lte : Optional[int]
            Comparison filters for `concordance_id`, by default None.
        domestic_foreign : Optional[Union[list[str], Series[str], str]]
            Indicates whether the export is domestic or foreign., by default None.
        domestic_foreign_id : Optional[int]
            The domestic or foreign classification identifier., by default None.
        domestic_foreign_id_gt, domestic_foreign_id_gte, domestic_foreign_id_lt, domestic_foreign_id_lte : Optional[int]
            Comparison filters for `domestic_foreign_id`, by default None.
        country_name : Optional[Union[list[str], Series[str], str]]
            The destination country name., by default None.
        country_id : Optional[int]
            The destination country identifier., by default None.
        country_id_gt, country_id_gte, country_id_lt, country_id_lte : Optional[int]
            Comparison filters for `country_id`, by default None.
        district_lading_name : Optional[Union[list[str], Series[str], str]]
            The U.S. customs district of lading., by default None.
        district_lading_id : Optional[int]
            The customs district of lading identifier., by default None.
        district_lading_id_gt, district_lading_id_gte, district_lading_id_lt, district_lading_id_lte : Optional[int]
            Comparison filters for `district_lading_id`, by default None.
        product_name : Optional[Union[list[str], Series[str], str]]
            The steel mill product name., by default None.
        product_id : Optional[int]
            The steel mill product identifier., by default None.
        product_id_gt, product_id_gte, product_id_lt, product_id_lte : Optional[int]
            Comparison filters for `product_id`, by default None.
        product_group : Optional[Union[list[str], Series[str], str]]
            The high-level steel mill product category., by default None.
        product_group_id : Optional[int]
            The product group identifier., by default None.
        product_group_id_gt, product_group_id_gte, product_group_id_lt, product_group_id_lte : Optional[int]
            Comparison filters for `product_group_id`, by default None.
        product_sub_group : Optional[Union[list[str], Series[str], str]]
            The intermediate steel mill product category., by default None.
        product_sub_group_id : Optional[int]
            The product subgroup identifier., by default None.
        product_sub_group_id_gt, product_sub_group_id_gte, product_sub_group_id_lt, product_sub_group_id_lte : Optional[int]
            Comparison filters for `product_sub_group_id`, by default None.
        product_type : Optional[Union[list[str], Series[str], str]]
            The specific steel mill product classification., by default None.
        product_type_id : Optional[int]
            The product type identifier., by default None.
        product_type_id_gt, product_type_id_gte, product_type_id_lt, product_type_id_lte : Optional[int]
            Comparison filters for `product_type_id`, by default None.
        product_sub_type : Optional[Union[list[str], Series[str], str]]
            The detailed steel mill product classification., by default None.
        product_sub_type_id : Optional[int]
            The product subtype identifier., by default None.
        product_sub_type_id_gt, product_sub_type_id_gte, product_sub_type_id_lt, product_sub_type_id_lte : Optional[int]
            Comparison filters for `product_sub_type_id`, by default None.
        grade : Optional[Union[list[str], Series[str], str]]
            The steel grade description., by default None.
        grade_id : Optional[int]
            The steel grade identifier., by default None.
        grade_id_gt, grade_id_gte, grade_id_lt, grade_id_lte : Optional[int]
            Comparison filters for `grade_id`, by default None.
        value_type : Optional[Union[list[str], Series[str], str]]
            The type of measured value., by default None.
        value : Optional[float]
            The numeric observation value., by default None.
        value_gt, value_gte, value_lt, value_lte : Optional[float]
            Comparison filters for `value`, by default None.
        uom : Optional[Union[list[str], Series[str], str]]
            The unit of measure., by default None.
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
        filter_params.append(list_to_filter("reportDate", report_date))
        if report_date_gt is not None:
            filter_params.append(f'reportDate > "{report_date_gt}"')
        if report_date_gte is not None:
            filter_params.append(f'reportDate >= "{report_date_gte}"')
        if report_date_lt is not None:
            filter_params.append(f'reportDate < "{report_date_lt}"')
        if report_date_lte is not None:
            filter_params.append(f'reportDate <= "{report_date_lte}"')
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("month", month))
        if month_gt is not None:
            filter_params.append(f'month > "{month_gt}"')
        if month_gte is not None:
            filter_params.append(f'month >= "{month_gte}"')
        if month_lt is not None:
            filter_params.append(f'month < "{month_lt}"')
        if month_lte is not None:
            filter_params.append(f'month <= "{month_lte}"')
        filter_params.append(list_to_filter("quarter", quarter))
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("datasetName", dataset_name))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("htsCode", hts_code))
        filter_params.append(list_to_filter("concordanceId", concordance_id))
        if concordance_id_gt is not None:
            filter_params.append(f'concordanceId > "{concordance_id_gt}"')
        if concordance_id_gte is not None:
            filter_params.append(f'concordanceId >= "{concordance_id_gte}"')
        if concordance_id_lt is not None:
            filter_params.append(f'concordanceId < "{concordance_id_lt}"')
        if concordance_id_lte is not None:
            filter_params.append(f'concordanceId <= "{concordance_id_lte}"')
        filter_params.append(list_to_filter("domesticForeign", domestic_foreign))
        filter_params.append(list_to_filter("domesticForeignId", domestic_foreign_id))
        if domestic_foreign_id_gt is not None:
            filter_params.append(f'domesticForeignId > "{domestic_foreign_id_gt}"')
        if domestic_foreign_id_gte is not None:
            filter_params.append(f'domesticForeignId >= "{domestic_foreign_id_gte}"')
        if domestic_foreign_id_lt is not None:
            filter_params.append(f'domesticForeignId < "{domestic_foreign_id_lt}"')
        if domestic_foreign_id_lte is not None:
            filter_params.append(f'domesticForeignId <= "{domestic_foreign_id_lte}"')
        filter_params.append(list_to_filter("countryName", country_name))
        filter_params.append(list_to_filter("countryId", country_id))
        if country_id_gt is not None:
            filter_params.append(f'countryId > "{country_id_gt}"')
        if country_id_gte is not None:
            filter_params.append(f'countryId >= "{country_id_gte}"')
        if country_id_lt is not None:
            filter_params.append(f'countryId < "{country_id_lt}"')
        if country_id_lte is not None:
            filter_params.append(f'countryId <= "{country_id_lte}"')
        filter_params.append(list_to_filter("districtLadingName", district_lading_name))
        filter_params.append(list_to_filter("districtLadingId", district_lading_id))
        if district_lading_id_gt is not None:
            filter_params.append(f'districtLadingId > "{district_lading_id_gt}"')
        if district_lading_id_gte is not None:
            filter_params.append(f'districtLadingId >= "{district_lading_id_gte}"')
        if district_lading_id_lt is not None:
            filter_params.append(f'districtLadingId < "{district_lading_id_lt}"')
        if district_lading_id_lte is not None:
            filter_params.append(f'districtLadingId <= "{district_lading_id_lte}"')
        filter_params.append(list_to_filter("productName", product_name))
        filter_params.append(list_to_filter("productId", product_id))
        if product_id_gt is not None:
            filter_params.append(f'productId > "{product_id_gt}"')
        if product_id_gte is not None:
            filter_params.append(f'productId >= "{product_id_gte}"')
        if product_id_lt is not None:
            filter_params.append(f'productId < "{product_id_lt}"')
        if product_id_lte is not None:
            filter_params.append(f'productId <= "{product_id_lte}"')
        filter_params.append(list_to_filter("productGroup", product_group))
        filter_params.append(list_to_filter("productGroupId", product_group_id))
        if product_group_id_gt is not None:
            filter_params.append(f'productGroupId > "{product_group_id_gt}"')
        if product_group_id_gte is not None:
            filter_params.append(f'productGroupId >= "{product_group_id_gte}"')
        if product_group_id_lt is not None:
            filter_params.append(f'productGroupId < "{product_group_id_lt}"')
        if product_group_id_lte is not None:
            filter_params.append(f'productGroupId <= "{product_group_id_lte}"')
        filter_params.append(list_to_filter("productSubGroup", product_sub_group))
        filter_params.append(list_to_filter("productSubGroupId", product_sub_group_id))
        if product_sub_group_id_gt is not None:
            filter_params.append(f'productSubGroupId > "{product_sub_group_id_gt}"')
        if product_sub_group_id_gte is not None:
            filter_params.append(f'productSubGroupId >= "{product_sub_group_id_gte}"')
        if product_sub_group_id_lt is not None:
            filter_params.append(f'productSubGroupId < "{product_sub_group_id_lt}"')
        if product_sub_group_id_lte is not None:
            filter_params.append(f'productSubGroupId <= "{product_sub_group_id_lte}"')
        filter_params.append(list_to_filter("productType", product_type))
        filter_params.append(list_to_filter("productTypeId", product_type_id))
        if product_type_id_gt is not None:
            filter_params.append(f'productTypeId > "{product_type_id_gt}"')
        if product_type_id_gte is not None:
            filter_params.append(f'productTypeId >= "{product_type_id_gte}"')
        if product_type_id_lt is not None:
            filter_params.append(f'productTypeId < "{product_type_id_lt}"')
        if product_type_id_lte is not None:
            filter_params.append(f'productTypeId <= "{product_type_id_lte}"')
        filter_params.append(list_to_filter("productSubType", product_sub_type))
        filter_params.append(list_to_filter("productSubTypeId", product_sub_type_id))
        if product_sub_type_id_gt is not None:
            filter_params.append(f'productSubTypeId > "{product_sub_type_id_gt}"')
        if product_sub_type_id_gte is not None:
            filter_params.append(f'productSubTypeId >= "{product_sub_type_id_gte}"')
        if product_sub_type_id_lt is not None:
            filter_params.append(f'productSubTypeId < "{product_sub_type_id_lt}"')
        if product_sub_type_id_lte is not None:
            filter_params.append(f'productSubTypeId <= "{product_sub_type_id_lte}"')
        filter_params.append(list_to_filter("grade", grade))
        filter_params.append(list_to_filter("gradeId", grade_id))
        if grade_id_gt is not None:
            filter_params.append(f'gradeId > "{grade_id_gt}"')
        if grade_id_gte is not None:
            filter_params.append(f'gradeId >= "{grade_id_gte}"')
        if grade_id_lt is not None:
            filter_params.append(f'gradeId < "{grade_id_lt}"')
        if grade_id_lte is not None:
            filter_params.append(f'gradeId <= "{grade_id_lte}"')
        filter_params.append(list_to_filter("valueType", value_type))
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

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path="analytics/metals/us-steel/v1/trade-exports",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_trade_imports(
        self,
        *,
        report_date: Optional[date] = None,
        report_date_lt: Optional[date] = None,
        report_date_lte: Optional[date] = None,
        report_date_gt: Optional[date] = None,
        report_date_gte: Optional[date] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        month: Optional[int] = None,
        month_lt: Optional[int] = None,
        month_lte: Optional[int] = None,
        month_gt: Optional[int] = None,
        month_gte: Optional[int] = None,
        quarter: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        dataset_name: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        hts_code: Optional[Union[list[str], Series[str], str]] = None,
        concordance_id: Optional[int] = None,
        concordance_id_lt: Optional[int] = None,
        concordance_id_lte: Optional[int] = None,
        concordance_id_gt: Optional[int] = None,
        concordance_id_gte: Optional[int] = None,
        country_name: Optional[Union[list[str], Series[str], str]] = None,
        country_id: Optional[int] = None,
        country_id_lt: Optional[int] = None,
        country_id_lte: Optional[int] = None,
        country_id_gt: Optional[int] = None,
        country_id_gte: Optional[int] = None,
        district_entry_name: Optional[Union[list[str], Series[str], str]] = None,
        district_entry_id: Optional[int] = None,
        district_entry_id_lt: Optional[int] = None,
        district_entry_id_lte: Optional[int] = None,
        district_entry_id_gt: Optional[int] = None,
        district_entry_id_gte: Optional[int] = None,
        district_unlading_name: Optional[Union[list[str], Series[str], str]] = None,
        district_unlading_id: Optional[int] = None,
        district_unlading_id_lt: Optional[int] = None,
        district_unlading_id_lte: Optional[int] = None,
        district_unlading_id_gt: Optional[int] = None,
        district_unlading_id_gte: Optional[int] = None,
        product_name: Optional[Union[list[str], Series[str], str]] = None,
        product_id: Optional[int] = None,
        product_id_lt: Optional[int] = None,
        product_id_lte: Optional[int] = None,
        product_id_gt: Optional[int] = None,
        product_id_gte: Optional[int] = None,
        product_group: Optional[Union[list[str], Series[str], str]] = None,
        product_group_id: Optional[int] = None,
        product_group_id_lt: Optional[int] = None,
        product_group_id_lte: Optional[int] = None,
        product_group_id_gt: Optional[int] = None,
        product_group_id_gte: Optional[int] = None,
        product_sub_group: Optional[Union[list[str], Series[str], str]] = None,
        product_sub_group_id: Optional[int] = None,
        product_sub_group_id_lt: Optional[int] = None,
        product_sub_group_id_lte: Optional[int] = None,
        product_sub_group_id_gt: Optional[int] = None,
        product_sub_group_id_gte: Optional[int] = None,
        product_type: Optional[Union[list[str], Series[str], str]] = None,
        product_type_id: Optional[int] = None,
        product_type_id_lt: Optional[int] = None,
        product_type_id_lte: Optional[int] = None,
        product_type_id_gt: Optional[int] = None,
        product_type_id_gte: Optional[int] = None,
        product_sub_type: Optional[Union[list[str], Series[str], str]] = None,
        product_sub_type_id: Optional[int] = None,
        product_sub_type_id_lt: Optional[int] = None,
        product_sub_type_id_lte: Optional[int] = None,
        product_sub_type_id_gt: Optional[int] = None,
        product_sub_type_id_gte: Optional[int] = None,
        grade: Optional[Union[list[str], Series[str], str]] = None,
        grade_id: Optional[int] = None,
        grade_id_lt: Optional[int] = None,
        grade_id_lte: Optional[int] = None,
        grade_id_gt: Optional[int] = None,
        grade_id_gte: Optional[int] = None,
        value_type: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the U.S. Steel Trade Imports data.

        Parameters
        ----------
        report_date : Optional[date]
            The start date of the reporting period., by default None.
        report_date_gt, report_date_gte, report_date_lt, report_date_lte : Optional[date]
            Comparison filters for `report_date`, by default None.
        year : Optional[int]
            The calendar year of the reporting period., by default None.
        year_gt, year_gte, year_lt, year_lte : Optional[int]
            Comparison filters for `year`, by default None.
        month : Optional[int]
            The calendar month of the reporting period., by default None.
        month_gt, month_gte, month_lt, month_lte : Optional[int]
            Comparison filters for `month`, by default None.
        quarter : Optional[Union[list[str], Series[str], str]]
            The quarter of the reporting period., by default None.
        date_frequency : Optional[Union[list[str], Series[str], str]]
            The reporting frequency of the record., by default None.
        dataset_name : Optional[Union[list[str], Series[str], str]]
            The name of the source dataset., by default None.
        commodity : Optional[Union[list[str], Series[str], str]]
            The commodity classification., by default None.
        currency : Optional[Union[list[str], Series[str], str]]
            The reporting currency., by default None.
        hts_code : Optional[Union[list[str], Series[str], str]]
            The Harmonized Tariff Schedule code., by default None.
        concordance_id : Optional[int]
            The internal product concordance identifier., by default None.
        concordance_id_gt, concordance_id_gte, concordance_id_lt, concordance_id_lte : Optional[int]
            Comparison filters for `concordance_id`, by default None.
        country_name : Optional[Union[list[str], Series[str], str]]
            The origin country name., by default None.
        country_id : Optional[int]
            The origin country identifier., by default None.
        country_id_gt, country_id_gte, country_id_lt, country_id_lte : Optional[int]
            Comparison filters for `country_id`, by default None.
        district_entry_name : Optional[Union[list[str], Series[str], str]]
            The U.S. customs district of entry., by default None.
        district_entry_id : Optional[int]
            The customs district of entry identifier., by default None.
        district_entry_id_gt, district_entry_id_gte, district_entry_id_lt, district_entry_id_lte : Optional[int]
            Comparison filters for `district_entry_id`, by default None.
        district_unlading_name : Optional[Union[list[str], Series[str], str]]
            The U.S. customs district of unlading., by default None.
        district_unlading_id : Optional[int]
            The customs district of unlading identifier., by default None.
        district_unlading_id_gt, district_unlading_id_gte, district_unlading_id_lt, district_unlading_id_lte : Optional[int]
            Comparison filters for `district_unlading_id`, by default None.
        product_name : Optional[Union[list[str], Series[str], str]]
            The steel mill product name., by default None.
        product_id : Optional[int]
            The steel mill product identifier., by default None.
        product_id_gt, product_id_gte, product_id_lt, product_id_lte : Optional[int]
            Comparison filters for `product_id`, by default None.
        product_group : Optional[Union[list[str], Series[str], str]]
            The high-level steel mill product category., by default None.
        product_group_id : Optional[int]
            The product group identifier., by default None.
        product_group_id_gt, product_group_id_gte, product_group_id_lt, product_group_id_lte : Optional[int]
            Comparison filters for `product_group_id`, by default None.
        product_sub_group : Optional[Union[list[str], Series[str], str]]
            The intermediate steel mill product category., by default None.
        product_sub_group_id : Optional[int]
            The product subgroup identifier., by default None.
        product_sub_group_id_gt, product_sub_group_id_gte, product_sub_group_id_lt, product_sub_group_id_lte : Optional[int]
            Comparison filters for `product_sub_group_id`, by default None.
        product_type : Optional[Union[list[str], Series[str], str]]
            The specific steel mill product classification., by default None.
        product_type_id : Optional[int]
            The product type identifier., by default None.
        product_type_id_gt, product_type_id_gte, product_type_id_lt, product_type_id_lte : Optional[int]
            Comparison filters for `product_type_id`, by default None.
        product_sub_type : Optional[Union[list[str], Series[str], str]]
            The detailed steel mill product classification., by default None.
        product_sub_type_id : Optional[int]
            The product subtype identifier., by default None.
        product_sub_type_id_gt, product_sub_type_id_gte, product_sub_type_id_lt, product_sub_type_id_lte : Optional[int]
            Comparison filters for `product_sub_type_id`, by default None.
        grade : Optional[Union[list[str], Series[str], str]]
            The steel grade description., by default None.
        grade_id : Optional[int]
            The steel grade identifier., by default None.
        grade_id_gt, grade_id_gte, grade_id_lt, grade_id_lte : Optional[int]
            Comparison filters for `grade_id`, by default None.
        value_type : Optional[Union[list[str], Series[str], str]]
            The type of measured value., by default None.
        value : Optional[float]
            The numeric observation value., by default None.
        value_gt, value_gte, value_lt, value_lte : Optional[float]
            Comparison filters for `value`, by default None.
        uom : Optional[Union[list[str], Series[str], str]]
            The unit of measure., by default None.
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
        filter_params.append(list_to_filter("reportDate", report_date))
        if report_date_gt is not None:
            filter_params.append(f'reportDate > "{report_date_gt}"')
        if report_date_gte is not None:
            filter_params.append(f'reportDate >= "{report_date_gte}"')
        if report_date_lt is not None:
            filter_params.append(f'reportDate < "{report_date_lt}"')
        if report_date_lte is not None:
            filter_params.append(f'reportDate <= "{report_date_lte}"')
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("month", month))
        if month_gt is not None:
            filter_params.append(f'month > "{month_gt}"')
        if month_gte is not None:
            filter_params.append(f'month >= "{month_gte}"')
        if month_lt is not None:
            filter_params.append(f'month < "{month_lt}"')
        if month_lte is not None:
            filter_params.append(f'month <= "{month_lte}"')
        filter_params.append(list_to_filter("quarter", quarter))
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("datasetName", dataset_name))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("htsCode", hts_code))
        filter_params.append(list_to_filter("concordanceId", concordance_id))
        if concordance_id_gt is not None:
            filter_params.append(f'concordanceId > "{concordance_id_gt}"')
        if concordance_id_gte is not None:
            filter_params.append(f'concordanceId >= "{concordance_id_gte}"')
        if concordance_id_lt is not None:
            filter_params.append(f'concordanceId < "{concordance_id_lt}"')
        if concordance_id_lte is not None:
            filter_params.append(f'concordanceId <= "{concordance_id_lte}"')
        filter_params.append(list_to_filter("countryName", country_name))
        filter_params.append(list_to_filter("countryId", country_id))
        if country_id_gt is not None:
            filter_params.append(f'countryId > "{country_id_gt}"')
        if country_id_gte is not None:
            filter_params.append(f'countryId >= "{country_id_gte}"')
        if country_id_lt is not None:
            filter_params.append(f'countryId < "{country_id_lt}"')
        if country_id_lte is not None:
            filter_params.append(f'countryId <= "{country_id_lte}"')
        filter_params.append(list_to_filter("districtEntryName", district_entry_name))
        filter_params.append(list_to_filter("districtEntryId", district_entry_id))
        if district_entry_id_gt is not None:
            filter_params.append(f'districtEntryId > "{district_entry_id_gt}"')
        if district_entry_id_gte is not None:
            filter_params.append(f'districtEntryId >= "{district_entry_id_gte}"')
        if district_entry_id_lt is not None:
            filter_params.append(f'districtEntryId < "{district_entry_id_lt}"')
        if district_entry_id_lte is not None:
            filter_params.append(f'districtEntryId <= "{district_entry_id_lte}"')
        filter_params.append(list_to_filter("districtUnladingName", district_unlading_name))
        filter_params.append(list_to_filter("districtUnladingId", district_unlading_id))
        if district_unlading_id_gt is not None:
            filter_params.append(f'districtUnladingId > "{district_unlading_id_gt}"')
        if district_unlading_id_gte is not None:
            filter_params.append(f'districtUnladingId >= "{district_unlading_id_gte}"')
        if district_unlading_id_lt is not None:
            filter_params.append(f'districtUnladingId < "{district_unlading_id_lt}"')
        if district_unlading_id_lte is not None:
            filter_params.append(f'districtUnladingId <= "{district_unlading_id_lte}"')
        filter_params.append(list_to_filter("productName", product_name))
        filter_params.append(list_to_filter("productId", product_id))
        if product_id_gt is not None:
            filter_params.append(f'productId > "{product_id_gt}"')
        if product_id_gte is not None:
            filter_params.append(f'productId >= "{product_id_gte}"')
        if product_id_lt is not None:
            filter_params.append(f'productId < "{product_id_lt}"')
        if product_id_lte is not None:
            filter_params.append(f'productId <= "{product_id_lte}"')
        filter_params.append(list_to_filter("productGroup", product_group))
        filter_params.append(list_to_filter("productGroupId", product_group_id))
        if product_group_id_gt is not None:
            filter_params.append(f'productGroupId > "{product_group_id_gt}"')
        if product_group_id_gte is not None:
            filter_params.append(f'productGroupId >= "{product_group_id_gte}"')
        if product_group_id_lt is not None:
            filter_params.append(f'productGroupId < "{product_group_id_lt}"')
        if product_group_id_lte is not None:
            filter_params.append(f'productGroupId <= "{product_group_id_lte}"')
        filter_params.append(list_to_filter("productSubGroup", product_sub_group))
        filter_params.append(list_to_filter("productSubGroupId", product_sub_group_id))
        if product_sub_group_id_gt is not None:
            filter_params.append(f'productSubGroupId > "{product_sub_group_id_gt}"')
        if product_sub_group_id_gte is not None:
            filter_params.append(f'productSubGroupId >= "{product_sub_group_id_gte}"')
        if product_sub_group_id_lt is not None:
            filter_params.append(f'productSubGroupId < "{product_sub_group_id_lt}"')
        if product_sub_group_id_lte is not None:
            filter_params.append(f'productSubGroupId <= "{product_sub_group_id_lte}"')
        filter_params.append(list_to_filter("productType", product_type))
        filter_params.append(list_to_filter("productTypeId", product_type_id))
        if product_type_id_gt is not None:
            filter_params.append(f'productTypeId > "{product_type_id_gt}"')
        if product_type_id_gte is not None:
            filter_params.append(f'productTypeId >= "{product_type_id_gte}"')
        if product_type_id_lt is not None:
            filter_params.append(f'productTypeId < "{product_type_id_lt}"')
        if product_type_id_lte is not None:
            filter_params.append(f'productTypeId <= "{product_type_id_lte}"')
        filter_params.append(list_to_filter("productSubType", product_sub_type))
        filter_params.append(list_to_filter("productSubTypeId", product_sub_type_id))
        if product_sub_type_id_gt is not None:
            filter_params.append(f'productSubTypeId > "{product_sub_type_id_gt}"')
        if product_sub_type_id_gte is not None:
            filter_params.append(f'productSubTypeId >= "{product_sub_type_id_gte}"')
        if product_sub_type_id_lt is not None:
            filter_params.append(f'productSubTypeId < "{product_sub_type_id_lt}"')
        if product_sub_type_id_lte is not None:
            filter_params.append(f'productSubTypeId <= "{product_sub_type_id_lte}"')
        filter_params.append(list_to_filter("grade", grade))
        filter_params.append(list_to_filter("gradeId", grade_id))
        if grade_id_gt is not None:
            filter_params.append(f'gradeId > "{grade_id_gt}"')
        if grade_id_gte is not None:
            filter_params.append(f'gradeId >= "{grade_id_gte}"')
        if grade_id_lt is not None:
            filter_params.append(f'gradeId < "{grade_id_lt}"')
        if grade_id_lte is not None:
            filter_params.append(f'gradeId <= "{grade_id_lte}"')
        filter_params.append(list_to_filter("valueType", value_type))
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

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path="analytics/metals/us-steel/v1/trade-imports",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_domestic_shipments(
        self,
        *,
        report_date: Optional[date] = None,
        report_date_lt: Optional[date] = None,
        report_date_lte: Optional[date] = None,
        report_date_gt: Optional[date] = None,
        report_date_gte: Optional[date] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        month: Optional[int] = None,
        month_lt: Optional[int] = None,
        month_lte: Optional[int] = None,
        month_gt: Optional[int] = None,
        month_gte: Optional[int] = None,
        quarter: Optional[Union[list[str], Series[str], str]] = None,
        dataset_name: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        product_name: Optional[Union[list[str], Series[str], str]] = None,
        product_id: Optional[int] = None,
        product_id_lt: Optional[int] = None,
        product_id_lte: Optional[int] = None,
        product_id_gt: Optional[int] = None,
        product_id_gte: Optional[int] = None,
        product_group: Optional[Union[list[str], Series[str], str]] = None,
        product_group_id: Optional[int] = None,
        product_group_id_lt: Optional[int] = None,
        product_group_id_lte: Optional[int] = None,
        product_group_id_gt: Optional[int] = None,
        product_group_id_gte: Optional[int] = None,
        product_sub_group: Optional[Union[list[str], Series[str], str]] = None,
        product_sub_group_id: Optional[int] = None,
        product_sub_group_id_lt: Optional[int] = None,
        product_sub_group_id_lte: Optional[int] = None,
        product_sub_group_id_gt: Optional[int] = None,
        product_sub_group_id_gte: Optional[int] = None,
        product_type: Optional[Union[list[str], Series[str], str]] = None,
        product_type_id: Optional[int] = None,
        product_type_id_lt: Optional[int] = None,
        product_type_id_lte: Optional[int] = None,
        product_type_id_gt: Optional[int] = None,
        product_type_id_gte: Optional[int] = None,
        product_sub_type: Optional[Union[list[str], Series[str], str]] = None,
        product_sub_type_id: Optional[int] = None,
        product_sub_type_id_lt: Optional[int] = None,
        product_sub_type_id_lte: Optional[int] = None,
        product_sub_type_id_gt: Optional[int] = None,
        product_sub_type_id_gte: Optional[int] = None,
        grade: Optional[Union[list[str], Series[str], str]] = None,
        grade_id: Optional[int] = None,
        grade_id_lt: Optional[int] = None,
        grade_id_lte: Optional[int] = None,
        grade_id_gt: Optional[int] = None,
        grade_id_gte: Optional[int] = None,
        value_type: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the U.S. Domestic Steel Shipments data.

        Parameters
        ----------
        report_date : Optional[date]
            The start date of the reporting period., by default None.
        report_date_gt, report_date_gte, report_date_lt, report_date_lte : Optional[date]
            Comparison filters for `report_date`, by default None.
        year : Optional[int]
            The calendar year of the reporting period., by default None.
        year_gt, year_gte, year_lt, year_lte : Optional[int]
            Comparison filters for `year`, by default None.
        month : Optional[int]
            The calendar month of the reporting period., by default None.
        month_gt, month_gte, month_lt, month_lte : Optional[int]
            Comparison filters for `month`, by default None.
        quarter : Optional[Union[list[str], Series[str], str]]
            The quarter of the reporting period., by default None.
        dataset_name : Optional[Union[list[str], Series[str], str]]
            The name of the source dataset., by default None.
        commodity : Optional[Union[list[str], Series[str], str]]
            The commodity classification., by default None.
        currency : Optional[Union[list[str], Series[str], str]]
            The reporting currency., by default None.
        product_name : Optional[Union[list[str], Series[str], str]]
            The steel mill product name., by default None.
        product_id : Optional[int]
            The steel mill product identifier., by default None.
        product_id_gt, product_id_gte, product_id_lt, product_id_lte : Optional[int]
            Comparison filters for `product_id`, by default None.
        product_group : Optional[Union[list[str], Series[str], str]]
            The high-level steel mill product category., by default None.
        product_group_id : Optional[int]
            The product group identifier., by default None.
        product_group_id_gt, product_group_id_gte, product_group_id_lt, product_group_id_lte : Optional[int]
            Comparison filters for `product_group_id`, by default None.
        product_sub_group : Optional[Union[list[str], Series[str], str]]
            The intermediate steel mill product category., by default None.
        product_sub_group_id : Optional[int]
            The product subgroup identifier., by default None.
        product_sub_group_id_gt, product_sub_group_id_gte, product_sub_group_id_lt, product_sub_group_id_lte : Optional[int]
            Comparison filters for `product_sub_group_id`, by default None.
        product_type : Optional[Union[list[str], Series[str], str]]
            The specific steel mill product classification., by default None.
        product_type_id : Optional[int]
            The product type identifier., by default None.
        product_type_id_gt, product_type_id_gte, product_type_id_lt, product_type_id_lte : Optional[int]
            Comparison filters for `product_type_id`, by default None.
        product_sub_type : Optional[Union[list[str], Series[str], str]]
            The detailed steel mill product classification., by default None.
        product_sub_type_id : Optional[int]
            The product subtype identifier., by default None.
        product_sub_type_id_gt, product_sub_type_id_gte, product_sub_type_id_lt, product_sub_type_id_lte : Optional[int]
            Comparison filters for `product_sub_type_id`, by default None.
        grade : Optional[Union[list[str], Series[str], str]]
            The steel grade classification., by default None.
        grade_id : Optional[int]
            The steel grade identifier., by default None.
        grade_id_gt, grade_id_gte, grade_id_lt, grade_id_lte : Optional[int]
            Comparison filters for `grade_id`, by default None.
        value_type : Optional[Union[list[str], Series[str], str]]
            The type of shipment measure., by default None.
        value : Optional[float]
            The numeric shipment value., by default None.
        value_gt, value_gte, value_lt, value_lte : Optional[float]
            Comparison filters for `value`, by default None.
        uom : Optional[Union[list[str], Series[str], str]]
            The unit of measure., by default None.
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
        filter_params.append(list_to_filter("reportDate", report_date))
        if report_date_gt is not None:
            filter_params.append(f'reportDate > "{report_date_gt}"')
        if report_date_gte is not None:
            filter_params.append(f'reportDate >= "{report_date_gte}"')
        if report_date_lt is not None:
            filter_params.append(f'reportDate < "{report_date_lt}"')
        if report_date_lte is not None:
            filter_params.append(f'reportDate <= "{report_date_lte}"')
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("month", month))
        if month_gt is not None:
            filter_params.append(f'month > "{month_gt}"')
        if month_gte is not None:
            filter_params.append(f'month >= "{month_gte}"')
        if month_lt is not None:
            filter_params.append(f'month < "{month_lt}"')
        if month_lte is not None:
            filter_params.append(f'month <= "{month_lte}"')
        filter_params.append(list_to_filter("quarter", quarter))
        filter_params.append(list_to_filter("datasetName", dataset_name))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("productName", product_name))
        filter_params.append(list_to_filter("productId", product_id))
        if product_id_gt is not None:
            filter_params.append(f'productId > "{product_id_gt}"')
        if product_id_gte is not None:
            filter_params.append(f'productId >= "{product_id_gte}"')
        if product_id_lt is not None:
            filter_params.append(f'productId < "{product_id_lt}"')
        if product_id_lte is not None:
            filter_params.append(f'productId <= "{product_id_lte}"')
        filter_params.append(list_to_filter("productGroup", product_group))
        filter_params.append(list_to_filter("productGroupId", product_group_id))
        if product_group_id_gt is not None:
            filter_params.append(f'productGroupId > "{product_group_id_gt}"')
        if product_group_id_gte is not None:
            filter_params.append(f'productGroupId >= "{product_group_id_gte}"')
        if product_group_id_lt is not None:
            filter_params.append(f'productGroupId < "{product_group_id_lt}"')
        if product_group_id_lte is not None:
            filter_params.append(f'productGroupId <= "{product_group_id_lte}"')
        filter_params.append(list_to_filter("productSubGroup", product_sub_group))
        filter_params.append(list_to_filter("productSubGroupId", product_sub_group_id))
        if product_sub_group_id_gt is not None:
            filter_params.append(f'productSubGroupId > "{product_sub_group_id_gt}"')
        if product_sub_group_id_gte is not None:
            filter_params.append(f'productSubGroupId >= "{product_sub_group_id_gte}"')
        if product_sub_group_id_lt is not None:
            filter_params.append(f'productSubGroupId < "{product_sub_group_id_lt}"')
        if product_sub_group_id_lte is not None:
            filter_params.append(f'productSubGroupId <= "{product_sub_group_id_lte}"')
        filter_params.append(list_to_filter("productType", product_type))
        filter_params.append(list_to_filter("productTypeId", product_type_id))
        if product_type_id_gt is not None:
            filter_params.append(f'productTypeId > "{product_type_id_gt}"')
        if product_type_id_gte is not None:
            filter_params.append(f'productTypeId >= "{product_type_id_gte}"')
        if product_type_id_lt is not None:
            filter_params.append(f'productTypeId < "{product_type_id_lt}"')
        if product_type_id_lte is not None:
            filter_params.append(f'productTypeId <= "{product_type_id_lte}"')
        filter_params.append(list_to_filter("productSubType", product_sub_type))
        filter_params.append(list_to_filter("productSubTypeId", product_sub_type_id))
        if product_sub_type_id_gt is not None:
            filter_params.append(f'productSubTypeId > "{product_sub_type_id_gt}"')
        if product_sub_type_id_gte is not None:
            filter_params.append(f'productSubTypeId >= "{product_sub_type_id_gte}"')
        if product_sub_type_id_lt is not None:
            filter_params.append(f'productSubTypeId < "{product_sub_type_id_lt}"')
        if product_sub_type_id_lte is not None:
            filter_params.append(f'productSubTypeId <= "{product_sub_type_id_lte}"')
        filter_params.append(list_to_filter("grade", grade))
        filter_params.append(list_to_filter("gradeId", grade_id))
        if grade_id_gt is not None:
            filter_params.append(f'gradeId > "{grade_id_gt}"')
        if grade_id_gte is not None:
            filter_params.append(f'gradeId >= "{grade_id_gte}"')
        if grade_id_lt is not None:
            filter_params.append(f'gradeId < "{grade_id_lt}"')
        if grade_id_lte is not None:
            filter_params.append(f'gradeId <= "{grade_id_lte}"')
        filter_params.append(list_to_filter("valueType", value_type))
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

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path="analytics/metals/us-steel/v1/domestic-shipments",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_endusemarket_shipments(
        self,
        *,
        report_date: Optional[date] = None,
        report_date_lt: Optional[date] = None,
        report_date_lte: Optional[date] = None,
        report_date_gt: Optional[date] = None,
        report_date_gte: Optional[date] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        quarter: Optional[Union[list[str], Series[str], str]] = None,
        dataset_name: Optional[Union[list[str], Series[str], str]] = None,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        grade: Optional[Union[list[str], Series[str], str]] = None,
        grade_id: Optional[int] = None,
        grade_id_lt: Optional[int] = None,
        grade_id_lte: Optional[int] = None,
        grade_id_gt: Optional[int] = None,
        grade_id_gte: Optional[int] = None,
        market_classification_description: Optional[Union[list[str], Series[str], str]] = None,
        market_classification_id: Optional[int] = None,
        market_classification_id_lt: Optional[int] = None,
        market_classification_id_lte: Optional[int] = None,
        market_classification_id_gt: Optional[int] = None,
        market_classification_id_gte: Optional[int] = None,
        market_classification_group: Optional[Union[list[str], Series[str], str]] = None,
        market_classification_group_id: Optional[int] = None,
        market_classification_group_id_lt: Optional[int] = None,
        market_classification_group_id_lte: Optional[int] = None,
        market_classification_group_id_gt: Optional[int] = None,
        market_classification_group_id_gte: Optional[int] = None,
        product_name: Optional[Union[list[str], Series[str], str]] = None,
        product_id: Optional[int] = None,
        product_id_lt: Optional[int] = None,
        product_id_lte: Optional[int] = None,
        product_id_gt: Optional[int] = None,
        product_id_gte: Optional[int] = None,
        product_group: Optional[Union[list[str], Series[str], str]] = None,
        product_group_id: Optional[int] = None,
        product_group_id_lt: Optional[int] = None,
        product_group_id_lte: Optional[int] = None,
        product_group_id_gt: Optional[int] = None,
        product_group_id_gte: Optional[int] = None,
        product_sub_group: Optional[Union[list[str], Series[str], str]] = None,
        product_sub_group_id: Optional[int] = None,
        product_sub_group_id_lt: Optional[int] = None,
        product_sub_group_id_lte: Optional[int] = None,
        product_sub_group_id_gt: Optional[int] = None,
        product_sub_group_id_gte: Optional[int] = None,
        product_type: Optional[Union[list[str], Series[str], str]] = None,
        product_type_id: Optional[int] = None,
        product_type_id_lt: Optional[int] = None,
        product_type_id_lte: Optional[int] = None,
        product_type_id_gt: Optional[int] = None,
        product_type_id_gte: Optional[int] = None,
        product_sub_type: Optional[Union[list[str], Series[str], str]] = None,
        product_sub_type_id: Optional[int] = None,
        product_sub_type_id_lt: Optional[int] = None,
        product_sub_type_id_lte: Optional[int] = None,
        product_sub_type_id_gt: Optional[int] = None,
        product_sub_type_id_gte: Optional[int] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        value_type: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[float] = None,
        value_lt: Optional[float] = None,
        value_lte: Optional[float] = None,
        value_gt: Optional[float] = None,
        value_gte: Optional[float] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the U.S. Steel End-Use Market Shipments data.

        Parameters
        ----------
        report_date : Optional[date]
            The start date of the reporting period., by default None.
        report_date_gt, report_date_gte, report_date_lt, report_date_lte : Optional[date]
            Comparison filters for `report_date`, by default None.
        year : Optional[int]
            The calendar year of the reporting period., by default None.
        year_gt, year_gte, year_lt, year_lte : Optional[int]
            Comparison filters for `year`, by default None.
        quarter : Optional[Union[list[str], Series[str], str]]
            The quarter of the reporting period., by default None.
        dataset_name : Optional[Union[list[str], Series[str], str]]
            The name of the source dataset., by default None.
        commodity : Optional[Union[list[str], Series[str], str]]
            The commodity classification., by default None.
        currency : Optional[Union[list[str], Series[str], str]]
            The reporting currency., by default None.
        grade : Optional[Union[list[str], Series[str], str]]
            The steel grade classification., by default None.
        grade_id : Optional[int]
            The steel grade identifier., by default None.
        grade_id_gt, grade_id_gte, grade_id_lt, grade_id_lte : Optional[int]
            Comparison filters for `grade_id`, by default None.
        market_classification_description : Optional[Union[list[str], Series[str], str]]
            The end-use market classification description., by default None.
        market_classification_id : Optional[int]
            The end-use market classification identifier., by default None.
        market_classification_id_gt, market_classification_id_gte, market_classification_id_lt, market_classification_id_lte : Optional[int]
            Comparison filters for `market_classification_id`, by default None.
        market_classification_group : Optional[Union[list[str], Series[str], str]]
            The end-use market classification group., by default None.
        market_classification_group_id : Optional[int]
            The end-use market classification group identifier., by default None.
        market_classification_group_id_gt, market_classification_group_id_gte, market_classification_group_id_lt, market_classification_group_id_lte : Optional[int]
            Comparison filters for `market_classification_group_id`, by default None.
        product_name : Optional[Union[list[str], Series[str], str]]
            The steel mill product name., by default None.
        product_id : Optional[int]
            The steel mill product identifier., by default None.
        product_id_gt, product_id_gte, product_id_lt, product_id_lte : Optional[int]
            Comparison filters for `product_id`, by default None.
        product_group : Optional[Union[list[str], Series[str], str]]
            The high-level steel mill product category., by default None.
        product_group_id : Optional[int]
            The product group identifier., by default None.
        product_group_id_gt, product_group_id_gte, product_group_id_lt, product_group_id_lte : Optional[int]
            Comparison filters for `product_group_id`, by default None.
        product_sub_group : Optional[Union[list[str], Series[str], str]]
            The intermediate steel mill product category., by default None.
        product_sub_group_id : Optional[int]
            The product subgroup identifier., by default None.
        product_sub_group_id_gt, product_sub_group_id_gte, product_sub_group_id_lt, product_sub_group_id_lte : Optional[int]
            Comparison filters for `product_sub_group_id`, by default None.
        product_type : Optional[Union[list[str], Series[str], str]]
            The specific steel mill product classification., by default None.
        product_type_id : Optional[int]
            The product type identifier., by default None.
        product_type_id_gt, product_type_id_gte, product_type_id_lt, product_type_id_lte : Optional[int]
            Comparison filters for `product_type_id`, by default None.
        product_sub_type : Optional[Union[list[str], Series[str], str]]
            The detailed steel mill product classification., by default None.
        product_sub_type_id : Optional[int]
            The product subtype identifier., by default None.
        product_sub_type_id_gt, product_sub_type_id_gte, product_sub_type_id_lt, product_sub_type_id_lte : Optional[int]
            Comparison filters for `product_sub_type_id`, by default None.
        uom : Optional[Union[list[str], Series[str], str]]
            The unit of measure., by default None.
        value_type : Optional[Union[list[str], Series[str], str]]
            The type of shipment measure., by default None.
        value : Optional[float]
            The numeric shipment value., by default None.
        value_gt, value_gte, value_lt, value_lte : Optional[float]
            Comparison filters for `value`, by default None.
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
        filter_params.append(list_to_filter("reportDate", report_date))
        if report_date_gt is not None:
            filter_params.append(f'reportDate > "{report_date_gt}"')
        if report_date_gte is not None:
            filter_params.append(f'reportDate >= "{report_date_gte}"')
        if report_date_lt is not None:
            filter_params.append(f'reportDate < "{report_date_lt}"')
        if report_date_lte is not None:
            filter_params.append(f'reportDate <= "{report_date_lte}"')
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("quarter", quarter))
        filter_params.append(list_to_filter("datasetName", dataset_name))
        filter_params.append(list_to_filter("commodity", commodity))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("grade", grade))
        filter_params.append(list_to_filter("gradeId", grade_id))
        if grade_id_gt is not None:
            filter_params.append(f'gradeId > "{grade_id_gt}"')
        if grade_id_gte is not None:
            filter_params.append(f'gradeId >= "{grade_id_gte}"')
        if grade_id_lt is not None:
            filter_params.append(f'gradeId < "{grade_id_lt}"')
        if grade_id_lte is not None:
            filter_params.append(f'gradeId <= "{grade_id_lte}"')
        filter_params.append(list_to_filter("marketClassificationDescription", market_classification_description))
        filter_params.append(list_to_filter("marketClassificationId", market_classification_id))
        if market_classification_id_gt is not None:
            filter_params.append(f'marketClassificationId > "{market_classification_id_gt}"')
        if market_classification_id_gte is not None:
            filter_params.append(f'marketClassificationId >= "{market_classification_id_gte}"')
        if market_classification_id_lt is not None:
            filter_params.append(f'marketClassificationId < "{market_classification_id_lt}"')
        if market_classification_id_lte is not None:
            filter_params.append(f'marketClassificationId <= "{market_classification_id_lte}"')
        filter_params.append(list_to_filter("marketClassificationGroup", market_classification_group))
        filter_params.append(list_to_filter("marketClassificationGroupId", market_classification_group_id))
        if market_classification_group_id_gt is not None:
            filter_params.append(f'marketClassificationGroupId > "{market_classification_group_id_gt}"')
        if market_classification_group_id_gte is not None:
            filter_params.append(f'marketClassificationGroupId >= "{market_classification_group_id_gte}"')
        if market_classification_group_id_lt is not None:
            filter_params.append(f'marketClassificationGroupId < "{market_classification_group_id_lt}"')
        if market_classification_group_id_lte is not None:
            filter_params.append(f'marketClassificationGroupId <= "{market_classification_group_id_lte}"')
        filter_params.append(list_to_filter("productName", product_name))
        filter_params.append(list_to_filter("productId", product_id))
        if product_id_gt is not None:
            filter_params.append(f'productId > "{product_id_gt}"')
        if product_id_gte is not None:
            filter_params.append(f'productId >= "{product_id_gte}"')
        if product_id_lt is not None:
            filter_params.append(f'productId < "{product_id_lt}"')
        if product_id_lte is not None:
            filter_params.append(f'productId <= "{product_id_lte}"')
        filter_params.append(list_to_filter("productGroup", product_group))
        filter_params.append(list_to_filter("productGroupId", product_group_id))
        if product_group_id_gt is not None:
            filter_params.append(f'productGroupId > "{product_group_id_gt}"')
        if product_group_id_gte is not None:
            filter_params.append(f'productGroupId >= "{product_group_id_gte}"')
        if product_group_id_lt is not None:
            filter_params.append(f'productGroupId < "{product_group_id_lt}"')
        if product_group_id_lte is not None:
            filter_params.append(f'productGroupId <= "{product_group_id_lte}"')
        filter_params.append(list_to_filter("productSubGroup", product_sub_group))
        filter_params.append(list_to_filter("productSubGroupId", product_sub_group_id))
        if product_sub_group_id_gt is not None:
            filter_params.append(f'productSubGroupId > "{product_sub_group_id_gt}"')
        if product_sub_group_id_gte is not None:
            filter_params.append(f'productSubGroupId >= "{product_sub_group_id_gte}"')
        if product_sub_group_id_lt is not None:
            filter_params.append(f'productSubGroupId < "{product_sub_group_id_lt}"')
        if product_sub_group_id_lte is not None:
            filter_params.append(f'productSubGroupId <= "{product_sub_group_id_lte}"')
        filter_params.append(list_to_filter("productType", product_type))
        filter_params.append(list_to_filter("productTypeId", product_type_id))
        if product_type_id_gt is not None:
            filter_params.append(f'productTypeId > "{product_type_id_gt}"')
        if product_type_id_gte is not None:
            filter_params.append(f'productTypeId >= "{product_type_id_gte}"')
        if product_type_id_lt is not None:
            filter_params.append(f'productTypeId < "{product_type_id_lt}"')
        if product_type_id_lte is not None:
            filter_params.append(f'productTypeId <= "{product_type_id_lte}"')
        filter_params.append(list_to_filter("productSubType", product_sub_type))
        filter_params.append(list_to_filter("productSubTypeId", product_sub_type_id))
        if product_sub_type_id_gt is not None:
            filter_params.append(f'productSubTypeId > "{product_sub_type_id_gt}"')
        if product_sub_type_id_gte is not None:
            filter_params.append(f'productSubTypeId >= "{product_sub_type_id_gte}"')
        if product_sub_type_id_lt is not None:
            filter_params.append(f'productSubTypeId < "{product_sub_type_id_lt}"')
        if product_sub_type_id_lte is not None:
            filter_params.append(f'productSubTypeId <= "{product_sub_type_id_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("valueType", value_type))
        filter_params.append(list_to_filter("value", value))
        if value_gt is not None:
            filter_params.append(f'value > "{value_gt}"')
        if value_gte is not None:
            filter_params.append(f'value >= "{value_gte}"')
        if value_lt is not None:
            filter_params.append(f'value < "{value_lt}"')
        if value_lte is not None:
            filter_params.append(f'value <= "{value_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path="analytics/metals/us-steel/v1/endusemarket-shipments",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
