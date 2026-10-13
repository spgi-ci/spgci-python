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


class Rcma:
    """
    Refinery Cost and Margin Analysis (RCMA) data.

    Includes
    --------
    ``get_cost()`` get refinery cost, margin and related metrics by quarter.
    """

    def get_cost(
        self,
        *,
        case_name: Optional[Union[list[str], Series[str], str]] = None,
        price_dataset: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        city: Optional[Union[list[str], Series[str], str]] = None,
        company: Optional[Union[list[str], Series[str], str]] = None,
        refinery_name: Optional[Union[list[str], Series[str], str]] = None,
        yield_date: Optional[Union[date, list[date], Series[date]]] = None,
        yield_date_gt: Optional[date] = None,
        yield_date_gte: Optional[date] = None,
        yield_date_lt: Optional[date] = None,
        yield_date_lte: Optional[date] = None,
        metric: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
        subcategory: Optional[Union[list[str], Series[str], str]] = None,
        is_active: Optional[bool] = None,
        vintage_date: Optional[Union[date, list[date], Series[date]]] = None,
        vintage_date_gt: Optional[date] = None,
        vintage_date_gte: Optional[date] = None,
        vintage_date_lt: Optional[date] = None,
        vintage_date_lte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve refinery cost, margin and related metrics by refinery and quarter.

        Parameters
        ----------
        case_name : list[str] | Series[str] | str, optional
            Base case executed by the model.
        price_dataset : list[str] | Series[str] | str, optional
            Price dataset the case was run against.
        country : list[str] | Series[str] | str, optional
            Country where the refinery is located.
        city : list[str] | Series[str] | str, optional
            City where the refinery is located.
        company : list[str] | Series[str] | str, optional
            Company operating the refinery.
        refinery_name : list[str] | Series[str] | str, optional
            Name of the refinery unit.
        yield_date : date | list[date] | Series[date], optional
            Quarterly date.
        yield_date_gt, yield_date_gte, yield_date_lt, yield_date_lte : date, optional
            Filter by ``yieldDate`` with the given comparison.
        metric : list[str] | Series[str] | str, optional
            Cost segment detail.
        currency : list[str] | Series[str] | str, optional
            Currency of the metric value.
        uom : list[str] | Series[str] | str, optional
            Unit of the metric value.
        region : list[str] | Series[str] | str, optional
            Region (geography) of the refinery.
        category : list[str] | Series[str] | str, optional
            Whether the metric is a cost, margin, etc.
        subcategory : list[str] | Series[str] | str, optional
            Least granular level of cost/margin.
        is_active : bool, optional
            Whether the record is the current one.
        vintage_date : date | list[date] | Series[date], optional
            Vintage of the data.
        vintage_date_gt, vintage_date_gte, vintage_date_lt, vintage_date_lte : date, optional
            Filter by ``vintageDate`` with the given comparison.
        filter_exp : str, optional
            Additional filter expression.
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
            Cost data as a DataFrame, or the raw response when `raw=True`.

        Examples
        --------
        >>> ci.Rcma().get_cost(country="Germany", yield_date_gte=date(2026, 1, 1))
        """
        filter_params: List[str] = [
            list_to_filter("caseName", case_name),
            list_to_filter("priceDataset", price_dataset),
            list_to_filter("country", country),
            list_to_filter("city", city),
            list_to_filter("company", company),
            list_to_filter("refineryName", refinery_name),
            list_to_filter("yieldDate", yield_date),
            list_to_filter("metric", metric),
            list_to_filter("currency", currency),
            list_to_filter("uom", uom),
            list_to_filter("region", region),
            list_to_filter("category", category),
            list_to_filter("subcategory", subcategory),
            list_to_filter("isActive", is_active),
            list_to_filter("vintageDate", vintage_date),
        ]
        for col, op, val in (
            ("yieldDate", ">", yield_date_gt),
            ("yieldDate", ">=", yield_date_gte),
            ("yieldDate", "<", yield_date_lt),
            ("yieldDate", "<=", yield_date_lte),
            ("vintageDate", ">", vintage_date_gt),
            ("vintageDate", ">=", vintage_date_gte),
            ("vintageDate", "<", vintage_date_lt),
            ("vintageDate", "<=", vintage_date_lte),
        ):
            if val is not None:
                filter_params.append(f'{col} {op} "{val}"')
        filter_params = [value for value in filter_params if value != ""]
        filter_exp = self._join_filters(filter_params, filter_exp)

        return get_data(
            path="/analytics/v1/rcma/cost",
            params={
                "page": page,
                "pageSize": page_size,
                "filter": filter_exp,
            },
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

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
        for column in ("yieldDate", "vintageDate", "validFrom", "validTo"):
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column], format="ISO8601", errors="coerce"
                )

        return df
