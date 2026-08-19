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
                - get_market_outlook → "ferrous-and-non-ferrous-metals"
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
            commodities = mt.get_unique_values('ferrous-and-non-ferrous-metals', 'commodity')

            # Step 2: Get filtered combinations for specific commodities
            filter_exp = ci.utilities.build_filter_expression({
                "commodity": ["Steel"],
            })
            combos = mt.get_unique_values(
                'ferrous-and-non-ferrous-metals',
                ['commodity', 'metalType', 'frequency'],
                filter_exp=filter_exp,
            )
        """

        dataset_to_path = {
            "market-outlook": "analytics/metals/metal-market-outlook/v1/ferrous-and-non-ferrous-metals",
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