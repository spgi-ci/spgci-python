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

from datetime import date
from typing import List, Optional, Union

import pandas as pd
from pandas import DataFrame, Series
from requests import Response

from spgci.api_client import get_data
from spgci.utilities import list_to_filter


class ChemicalsCostMargin:
    """
    Chemicals Cost and Margin Analysis (CCMA) data.

    Includes
    --------
    ``get_details()`` get chemicals cost, margin and related metrics by site and period.
    """

    def get_details(
        self,
        *,
        category: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        metric: Optional[Union[list[str], Series[str], str]] = None,
        metric_value: Optional[Union[float, list[float]]] = None,
        period: Optional[Union[date, list[date], Series[date]]] = None,
        period_gt: Optional[date] = None,
        period_gte: Optional[date] = None,
        period_lt: Optional[date] = None,
        period_lte: Optional[date] = None,
        period_type: Optional[Union[list[str], Series[str], str]] = None,
        site_id: Optional[Union[list[str], Series[str], str]] = None,
        company: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        city: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        is_product_integrated: Optional[bool] = None,
        additional_product: Optional[Union[list[str], Series[str], str]] = None,
        publish_period: Optional[Union[date, list[date], Series[date]]] = None,
        publish_period_gt: Optional[date] = None,
        publish_period_gte: Optional[date] = None,
        publish_period_lt: Optional[date] = None,
        publish_period_lte: Optional[date] = None,
        asset: Optional[Union[list[str], Series[str], str]] = None,
        product_name: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve chemicals cost, margin and related metrics by site and period.

        Parameters
        ----------
        category : list[str] | Series[str] | str, optional
            Whether the metric is a cost, margin, etc.
        currency : list[str] | Series[str] | str, optional
            Currency of the metric value.
        uom : list[str] | Series[str] | str, optional
            Unit of the metric value.
        metric : list[str] | Series[str] | str, optional
            Cost segment details.
        metric_value : float | list[float], optional
            Value of the metric.
        period : date | list[date] | Series[date], optional
            Period the value applies to.
        period_gt, period_gte, period_lt, period_lte : date, optional
            Filter by ``period`` with the given comparison.
        period_type : list[str] | Series[str] | str, optional
            Type of period.
        site_id : list[str] | Series[str] | str, optional
            Site identifier.
        company : list[str] | Series[str] | str, optional
            Name of the company operating the site.
        region : list[str] | Series[str] | str, optional
            Region of the site.
        country : list[str] | Series[str] | str, optional
            Country of the site.
        city : list[str] | Series[str] | str, optional
            City of the site.
        state : list[str] | Series[str] | str, optional
            State of the site.
        is_product_integrated : bool, optional
            Whether the product is integrated.
        additional_product : list[str] | Series[str] | str, optional
            Additional product populated for the record.
        publish_period : date | list[date] | Series[date], optional
            Publish period of the record.
        publish_period_gt, publish_period_gte, publish_period_lt, publish_period_lte : date, optional
            Filter by ``publishPeriod`` with the given comparison.
        asset : list[str] | Series[str] | str, optional
            Asset identifier.
        product_name : list[str] | Series[str] | str, optional
            Name of the product.
        filter_exp : str, optional
            Additional filter expression. Use this for columns without a
            dedicated parameter, e.g. ``process``.
        page : int, optional
            Page number, by default 1.
        page_size : int, optional
            Number of rows per page, by default 1000.
        raw : bool, optional
            Return the raw `requests.Response`, by default false.
        paginate : bool, optional
            Retrieve all available pages, by default false.

        Returns
        -------
        DataFrame | Response
            Cost and margin details as a DataFrame, or the raw response when `raw=True`.

        Examples
        --------
        >>> ci.ChemicalsCostMargin().get_details(country="Germany", period_gte=date(2026, 1, 1))
        """
        filter_params: List[str] = [
            list_to_filter("category", category),
            list_to_filter("currency", currency),
            list_to_filter("uom", uom),
            list_to_filter("metric", metric),
            list_to_filter("metricValue", metric_value),
            list_to_filter("period", period),
            list_to_filter("periodtype", period_type),
            list_to_filter("siteid", site_id),
            list_to_filter("company", company),
            list_to_filter("region", region),
            list_to_filter("country", country),
            list_to_filter("city", city),
            list_to_filter("state", state),
            list_to_filter("isProductIntegrated", is_product_integrated),
            list_to_filter("additionalproduct", additional_product),
            list_to_filter("publishPeriod", publish_period),
            list_to_filter("asset", asset),
            list_to_filter("productname", product_name),
        ]
        for col, op, val in (
            ("period", ">", period_gt),
            ("period", ">=", period_gte),
            ("period", "<", period_lt),
            ("period", "<=", period_lte),
            ("publishPeriod", ">", publish_period_gt),
            ("publishPeriod", ">=", publish_period_gte),
            ("publishPeriod", "<", publish_period_lt),
            ("publishPeriod", "<=", publish_period_lte),
        ):
            if val is not None:
                filter_params.append(f'{col} {op} "{val}"')
        filter_params = [value for value in filter_params if value != ""]
        filter_exp = self._join_filters(filter_params, filter_exp)

        return get_data(
            path="/analytics/v1/chemicals-cost-margin/details",
            params={
                "page": page,
                "pageSize": page_size,
                "filter": filter_exp,
            },
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_unique_values(
        self,
        columns: Optional[Union[list[str], str]],
        filter_exp: Optional[str] = None,
    ) -> DataFrame:
        """
        Get unique values (or combinations) of columns in the details dataset.

        Use this to discover valid filter values before calling ``get_details()``.

        Parameters
        ----------
        columns : list[str] | str
            API (camelCase) column names, e.g. ``["category", "metric", "uom"]``.
        filter_exp : str, optional
            Filter expression limiting the rows considered.

        Returns
        -------
        DataFrame
            Unique combinations of the requested columns.

        Examples
        --------
        >>> ci.ChemicalsCostMargin().get_unique_values(["category", "metric"])
        """
        col_value = ", ".join(columns) if isinstance(columns, list) else columns or ""
        params = {"GroupBy": col_value, "pageSize": 5000}
        if filter_exp is not None:
            params.update({"filter": filter_exp})

        return get_data(
            "/analytics/v1/chemicals-cost-margin/details",
            params,
            lambda resp: self._convert_to_df_agg(resp),
            paginate=True,
        )

    @staticmethod
    def _convert_to_df_agg(resp: Response) -> DataFrame:
        df = pd.json_normalize(resp.json()["aggResultValue"])  # type: ignore
        for column in ("period", "publishPeriod"):
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column], format="ISO8601", errors="coerce"
                )
        return df

    @staticmethod
    def _join_filters(
        required_filters: List[str],
        filter_exp: Optional[str],
    ) -> str:
        """Combine generated filters with an optional caller filter."""
        generated = " AND ".join(required_filters)
        if filter_exp is None:
            return generated
        if generated:
            return f"{generated} AND ({filter_exp})"
        return filter_exp

    @staticmethod
    def _convert_to_df(resp: Response) -> DataFrame:
        df = pd.json_normalize(resp.json()["results"])  # type: ignore

        # `errors="coerce"` turns out-of-bounds sentinel timestamps into NaT.
        for column in ("period", "publishPeriod"):
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column], format="ISO8601", errors="coerce"
                )

        return df
