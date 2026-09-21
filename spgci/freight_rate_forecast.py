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

from datetime import date, datetime
from typing import List, Literal, Optional, Union

import pandas as pd
from pandas import DataFrame, Series
from requests import Response

from spgci.api_client import get_data
from spgci.utilities import list_to_filter


class FreightRateForecast:
    """Access Supply Chain Freight Rate Forecast datasets."""

    _datasets = Literal[
        "freight-rate-forecast",
        "freight-rate-forecast-latest",
        "freight-rate-forecast-drivers",
        "freight-rate-forecast-drivers-latest",
    ]

    _dataset_to_path = {
        "freight-rate-forecast": "analytics/scf/v1/freightrateforecast",
        "freight-rate-forecast-latest": "analytics/scf/v1/freightrateforecastlatest",
        "freight-rate-forecast-drivers": "analytics/scf/v1/freightrateforecastdrivers",
        "freight-rate-forecast-drivers-latest": "analytics/scf/v1/freightrateforecastdriverslatest",
    }

    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
        """Convert an API response to a DataFrame and normalize date fields."""
        payload = resp.json()
        df = pd.json_normalize(payload["results"])

        for field in (
            "reportForDate",
            "lastModifiedDatetime",
            "createdDateTime",
        ):
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors="coerce")

        return df

    @staticmethod
    def _build_filter(
        filter_params: List[str], filter_exp: Optional[str]
    ) -> str:
        """Combine generated filters with an optional custom filter expression."""
        filter_params = [value for value in filter_params if value != ""]

        if filter_exp is None:
            return " AND ".join(filter_params)
        if filter_params:
            return " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        return filter_exp

    def get_unique_values(
        self,
        dataset: _datasets,
        columns: Optional[Union[list[str], str]],
        filter_exp: Optional[str] = None,
    ) -> DataFrame:
        """
        Get unique values for specified columns in a Freight Rate Forecast dataset.

        Use this method to discover available values and valid combinations before
        filtering a data request.

        Parameters
        ----------
        dataset : _datasets
            Dataset to inspect.
        columns : Optional[Union[list[str], str]]
            Property name or names to group by, using API camelCase names.
        filter_exp : Optional[str]
            Additional API filter expression, by default None.

        Returns
        -------
        DataFrame
            Unique values or combinations for the requested columns.
        """
        if dataset not in self._dataset_to_path:
            valid = "\n".join(self._dataset_to_path)
            raise ValueError(
                f"dataset '{dataset}' not found. Valid datasets:\n{valid}"
            )

        column_value = ", ".join(columns) if isinstance(columns, list) else columns or ""
        params = {"groupBy": column_value, "pageSize": 5000}
        if filter_exp is not None:
            params["filter"] = filter_exp

        def to_df(resp: Response) -> DataFrame:
            return DataFrame(resp.json()["aggResultValue"])

        return get_data(
            self._dataset_to_path[dataset],
            params,
            to_df,
            paginate=True,
        )

    def get_freight_rate_forecast(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        report_for_date: Optional[date] = None,
        report_for_date_lt: Optional[date] = None,
        report_for_date_lte: Optional[date] = None,
        report_for_date_gt: Optional[date] = None,
        report_for_date_gte: Optional[date] = None,
        symbol: Optional[Union[list[str], Series[str], str]] = None,
        vessel_size_class: Optional[Union[list[str], Series[str], str]] = None,
        description_of_symbol: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        created_date: Optional[datetime] = None,
        created_date_lt: Optional[datetime] = None,
        created_date_lte: Optional[datetime] = None,
        created_date_gt: Optional[datetime] = None,
        created_date_gte: Optional[datetime] = None,
        sys_is_active: Optional[bool] = None,
        as_of_date: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the Freight Rate Forecast historical dataset.

        Parameters
        ----------
        commodity : Optional[Union[list[str], Series[str], str]]
            Freight market commodity category, by default None.
        uom : Optional[Union[list[str], Series[str], str]]
            Unit of measure, by default None.
        currency : Optional[Union[list[str], Series[str], str]]
            Currency associated with the forecast value, by default None.
        report_for_date : Optional[date]
            Date for which the forecast applies, by default None.
        report_for_date_gt, report_for_date_gte, report_for_date_lt, report_for_date_lte : Optional[date]
            Comparison filters for the report date, by default None.
        symbol : Optional[Union[list[str], Series[str], str]]
            Assessment symbol, by default None.
        vessel_size_class : Optional[Union[list[str], Series[str], str]]
            Vessel size class, by default None.
        description_of_symbol : Optional[Union[list[str], Series[str], str]]
            Description of the assessment symbol, by default None.
        modified_date : Optional[datetime]
            Timestamp when the record was last modified, by default None.
        modified_date_gt, modified_date_gte, modified_date_lt, modified_date_lte : Optional[datetime]
            Comparison filters for the modified timestamp, by default None.
        created_date : Optional[datetime]
            Timestamp when the record was created, by default None.
        created_date_gt, created_date_gte, created_date_lt, created_date_lte : Optional[datetime]
            Comparison filters for the created timestamp, by default None.
        sys_is_active : Optional[bool]
            Whether the record is active, by default None.
        as_of_date : Optional[Union[list[str], Series[str], str]]
            Date when the model was run, by default None.
        filter_exp : Optional[str]
            Additional API filter expression, by default None.
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
        filter_params: List[str] = [
            list_to_filter("commodity", commodity),
            list_to_filter("uom", uom),
            list_to_filter("currency", currency),
            list_to_filter("reportForDate", report_for_date),
            list_to_filter("symbol", symbol),
            list_to_filter("vesselSizeClass", vessel_size_class),
            list_to_filter("descriptionOfSymbol", description_of_symbol),
            list_to_filter("lastModifiedDatetime", modified_date),
            list_to_filter("createdDateTime", created_date),
            list_to_filter("sysIsActive", sys_is_active),
            list_to_filter("asOfDate", as_of_date),
        ]
        self._append_comparison_filters(filter_params, "reportForDate", report_for_date_lt, report_for_date_lte, report_for_date_gt, report_for_date_gte)
        self._append_comparison_filters(filter_params, "lastModifiedDatetime", modified_date_lt, modified_date_lte, modified_date_gt, modified_date_gte)
        self._append_comparison_filters(filter_params, "createdDateTime", created_date_lt, created_date_lte, created_date_gt, created_date_gte)

        params = {
            "page": page,
            "pageSize": page_size,
            "filter": self._build_filter(filter_params, filter_exp),
        }

        return get_data(
            path="analytics/scf/v1/freightrateforecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_freight_rate_forecast_latest(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        report_for_date: Optional[date] = None,
        report_for_date_lt: Optional[date] = None,
        report_for_date_lte: Optional[date] = None,
        report_for_date_gt: Optional[date] = None,
        report_for_date_gte: Optional[date] = None,
        symbol: Optional[Union[list[str], Series[str], str]] = None,
        vessel_size_class: Optional[Union[list[str], Series[str], str]] = None,
        description_of_symbol: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        created_date: Optional[datetime] = None,
        sys_is_active: Optional[bool] = None,
        as_of_date: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the latest Freight Rate Forecast dataset.

        Parameters
        ----------
        commodity : Optional[Union[list[str], Series[str], str]]
            Freight market commodity category, by default None.
        uom : Optional[Union[list[str], Series[str], str]]
            Unit of measure, by default None.
        currency : Optional[Union[list[str], Series[str], str]]
            Currency associated with the forecast value, by default None.
        report_for_date : Optional[date]
            Date for which the forecast applies, by default None.
        report_for_date_gt, report_for_date_gte, report_for_date_lt, report_for_date_lte : Optional[date]
            Comparison filters for the report date, by default None.
        symbol : Optional[Union[list[str], Series[str], str]]
            Assessment symbol, by default None.
        vessel_size_class : Optional[Union[list[str], Series[str], str]]
            Vessel size class, by default None.
        description_of_symbol : Optional[Union[list[str], Series[str], str]]
            Description of the assessment symbol, by default None.
        modified_date : Optional[datetime]
            Timestamp when the record was last modified, by default None.
        created_date : Optional[datetime]
            Timestamp when the record was created, by default None.
        sys_is_active : Optional[bool]
            Whether the record is active, by default None.
        as_of_date : Optional[Union[list[str], Series[str], str]]
            Date when the model was run, by default None.
        filter_exp : Optional[str]
            Additional API filter expression, by default None.
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
        filter_params: List[str] = [
            list_to_filter("commodity", commodity),
            list_to_filter("uom", uom),
            list_to_filter("currency", currency),
            list_to_filter("reportForDate", report_for_date),
            list_to_filter("symbol", symbol),
            list_to_filter("vesselSizeClass", vessel_size_class),
            list_to_filter("descriptionOfSymbol", description_of_symbol),
            list_to_filter("lastModifiedDatetime", modified_date),
            list_to_filter("createdDateTime", created_date),
            list_to_filter("isActive", sys_is_active),
            list_to_filter("asOfDate", as_of_date),
        ]
        self._append_comparison_filters(filter_params, "reportForDate", report_for_date_lt, report_for_date_lte, report_for_date_gt, report_for_date_gte)

        params = {
            "page": page,
            "pageSize": page_size,
            "filter": self._build_filter(filter_params, filter_exp),
        }

        return get_data(
            path="analytics/scf/v1/freightrateforecastlatest",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_freight_rate_forecast_drivers(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        report_for_date: Optional[date] = None,
        report_for_date_lt: Optional[date] = None,
        report_for_date_lte: Optional[date] = None,
        report_for_date_gt: Optional[date] = None,
        report_for_date_gte: Optional[date] = None,
        symbol: Optional[Union[list[str], Series[str], str]] = None,
        as_of_date: Optional[Union[list[str], Series[str], str]] = None,
        vessel_size_class: Optional[Union[list[str], Series[str], str]] = None,
        description_of_the_driver: Optional[Union[list[str], Series[str], str]] = None,
        impact_on_freight: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        created_date: Optional[datetime] = None,
        created_date_lt: Optional[datetime] = None,
        created_date_lte: Optional[datetime] = None,
        created_date_gt: Optional[datetime] = None,
        created_date_gte: Optional[datetime] = None,
        sys_is_active: Optional[bool] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the Freight Rate Forecast Drivers historical dataset.

        Parameters
        ----------
        commodity : Optional[Union[list[str], Series[str], str]]
            Freight market commodity category, by default None.
        report_for_date : Optional[date]
            Date for which the forecast applies, by default None.
        report_for_date_gt, report_for_date_gte, report_for_date_lt, report_for_date_lte : Optional[date]
            Comparison filters for the report date, by default None.
        symbol : Optional[Union[list[str], Series[str], str]]
            Assessment symbol, by default None.
        as_of_date : Optional[Union[list[str], Series[str], str]]
            Date when the model was run, by default None.
        vessel_size_class : Optional[Union[list[str], Series[str], str]]
            Vessel size class, by default None.
        description_of_the_driver : Optional[Union[list[str], Series[str], str]]
            Name or description of the forecast driver, by default None.
        impact_on_freight : Optional[Union[list[str], Series[str], str]]
            Direction of the driver's impact on freight, by default None.
        modified_date : Optional[datetime]
            Timestamp when the record was last modified, by default None.
        modified_date_gt, modified_date_gte, modified_date_lt, modified_date_lte : Optional[datetime]
            Comparison filters for the modified timestamp, by default None.
        created_date : Optional[datetime]
            Timestamp when the record was created, by default None.
        created_date_gt, created_date_gte, created_date_lt, created_date_lte : Optional[datetime]
            Comparison filters for the created timestamp, by default None.
        sys_is_active : Optional[bool]
            Whether the record is active, by default None.
        filter_exp : Optional[str]
            Additional API filter expression, by default None.
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
        filter_params: List[str] = [
            list_to_filter("commodity", commodity),
            list_to_filter("reportForDate", report_for_date),
            list_to_filter("symbol", symbol),
            list_to_filter("asOfDate", as_of_date),
            list_to_filter("vesselSizeClass", vessel_size_class),
            list_to_filter("descriptionOfTheDriver", description_of_the_driver),
            list_to_filter("impactOnFreight", impact_on_freight),
            list_to_filter("lastModifiedDatetime", modified_date),
            list_to_filter("createdDateTime", created_date),
            list_to_filter("isActive", sys_is_active),
        ]
        self._append_comparison_filters(
            filter_params, "reportForDate", report_for_date_lt,
            report_for_date_lte, report_for_date_gt, report_for_date_gte
        )
        self._append_comparison_filters(
            filter_params, "lastModifiedDatetime", modified_date_lt,
            modified_date_lte, modified_date_gt, modified_date_gte
        )
        self._append_comparison_filters(
            filter_params, "createdDateTime", created_date_lt,
            created_date_lte, created_date_gt, created_date_gte
        )

        params = {
            "page": page,
            "pageSize": page_size,
            "filter": self._build_filter(filter_params, filter_exp),
        }

        return get_data(
            path="analytics/scf/v1/freightrateforecastdrivers",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_freight_rate_forecast_drivers_latest(
        self,
        *,
        commodity: Optional[Union[list[str], Series[str], str]] = None,
        report_for_date: Optional[date] = None,
        report_for_date_lt: Optional[date] = None,
        report_for_date_lte: Optional[date] = None,
        report_for_date_gt: Optional[date] = None,
        report_for_date_gte: Optional[date] = None,
        symbol: Optional[Union[list[str], Series[str], str]] = None,
        vessel_size_class: Optional[Union[list[str], Series[str], str]] = None,
        description_of_the_driver: Optional[Union[list[str], Series[str], str]] = None,
        impact_on_freight: Optional[Union[list[str], Series[str], str]] = None,
        modified_date: Optional[datetime] = None,
        created_date: Optional[datetime] = None,
        sys_is_active: Optional[bool] = None,
        as_of_date: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Access the latest Freight Rate Forecast Drivers dataset.

        Parameters
        ----------
        commodity : Optional[Union[list[str], Series[str], str]]
            Freight market commodity category, by default None.
        report_for_date : Optional[date]
            Date for which the forecast applies, by default None.
        report_for_date_gt, report_for_date_gte, report_for_date_lt, report_for_date_lte : Optional[date]
            Comparison filters for the report date, by default None.
        symbol : Optional[Union[list[str], Series[str], str]]
            Assessment symbol, by default None.
        vessel_size_class : Optional[Union[list[str], Series[str], str]]
            Vessel size class, by default None.
        description_of_the_driver : Optional[Union[list[str], Series[str], str]]
            Name or description of the forecast driver, by default None.
        impact_on_freight : Optional[Union[list[str], Series[str], str]]
            Direction of the driver's impact on freight, by default None.
        modified_date : Optional[datetime]
            Timestamp when the record was last modified, by default None.
        created_date : Optional[datetime]
            Timestamp when the record was created, by default None.
        sys_is_active : Optional[bool]
            Whether the record is active, by default None.
        as_of_date : Optional[Union[list[str], Series[str], str]]
            Date when the model was run, by default None.
        filter_exp : Optional[str]
            Additional API filter expression, by default None.
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
        filter_params: List[str] = [
            list_to_filter("commodity", commodity),
            list_to_filter("reportForDate", report_for_date),
            list_to_filter("symbol", symbol),
            list_to_filter("vesselSizeClass", vessel_size_class),
            list_to_filter("descriptionOfTheDriver", description_of_the_driver),
            list_to_filter("impactOnFreight", impact_on_freight),
            list_to_filter("lastModifiedDatetime", modified_date),
            list_to_filter("createdDateTime", created_date),
            list_to_filter("sysIsActive", sys_is_active),
            list_to_filter("asOfDate", as_of_date),
        ]
        self._append_comparison_filters(filter_params, "reportForDate", report_for_date_lt, report_for_date_lte, report_for_date_gt, report_for_date_gte)

        params = {
            "page": page,
            "pageSize": page_size,
            "filter": self._build_filter(filter_params, filter_exp),
        }

        return get_data(
            path="analytics/scf/v1/freightrateforecastdriverslatest",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    @staticmethod
    def _append_comparison_filters(
        filter_params: List[str],
        field: str,
        lt: Optional[Union[date, datetime]],
        lte: Optional[Union[date, datetime]],
        gt: Optional[Union[date, datetime]],
        gte: Optional[Union[date, datetime]],
    ) -> None:
        """Append comparison expressions for a date or timestamp field."""
        if gt is not None:
            filter_params.append(f'{field} > "{gt}"')
        if gte is not None:
            filter_params.append(f'{field} >= "{gte}"')
        if lt is not None:
            filter_params.append(f'{field} < "{lt}"')
        if lte is not None:
            filter_params.append(f'{field} <= "{lte}"')
