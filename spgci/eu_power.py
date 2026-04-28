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
from typing import List, Literal, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import datetime
import pandas as pd


class EUPower:
    _endpoint = "api/v1/"
    _api_eu_power_vision_assets_endpoint = "power-assets"
    
    _datasets = Literal["power-assets"]

    def get_unique_values(
        self,
        dataset: _datasets,
        columns: Optional[Union[list[str], str]],
        filter_exp: Optional[str] = None,
    ) -> DataFrame:
        """
        Get unique values for specified columns in a dataset, optionally filtered by an expression.

        Args:
            dataset (str): The EU power dataset name:
                - "power-assets"
            columns (list[str] or str): Column names to get unique values for.
            filter_exp (str, optional): Filter expression to limit results.

        Returns:
            pd.DataFrame: DataFrame with unique combinations of the specified columns.

        Example:
            >>> power.get_unique_values("power-assets", "country")
        """
        dataset_to_path = {
            "power-assets": "egp/v1/eupower/power-assets",
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
        params = {"groupBy": col_value, "pageSize": 5000}

        if filter_exp is not None:
            params.update({"filter": filter_exp})

        def to_df(resp: Response):
            j = resp.json()
            return DataFrame(j["aggResultValue"])

        return get_data(path, params, to_df, paginate=True)

    def get_power_assets(
        self,
        *,
        plant_name: Optional[Union[list[str], Series[str], str]] = None,
        unit_id: Optional[Union[list[str], Series[str], str]] = None,
        operator: Optional[Union[list[str], Series[str], str]] = None,
        plant_technology: Optional[Union[list[str], Series[str], str]] = None,
        status: Optional[Union[list[str], Series[str], str]] = None,
        unit_primary_fuel: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
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
        The European Power Assets API provides details of European Power Infrastructure Data.

        Parameters
        ----------
        plant_name      :   Optional[Union[list[str], Series[str], str]] = None,
            filter by plantName, by default None
        unit_id         :   Optional[Union[list[str], Series[str], str]] = None,
            filter by unitId, by default None
        operator        :   Optional[Union[list[str], Series[str], str]] = None,
            filter by operator, by default None
        plant_technology:   Optional[Union[list[str], Series[str], str]] = None,
            filter by plantTechnology, by default None
        status          :   Optional[Union[list[str], Series[str], str]] = None,
            filter by status, by default None
        unit_primary_fuel:  Optional[Union[list[str], Series[str], str]] = None,
            filter by unitPrimaryFuel, by default None
        country         :   Optional[Union[list[str], Series[str], str]] = None,
            filter by country, by default None
        modified_date   :   Optional[datetime] = None,
            filter by ``lastModifiedDate = x`` , by default None
        modified_date_gt:   Optional[datetime] = None,
            filter by ``lastModifiedDate > x`` , by default None
        modified_date_gte:  Optional[datetime] = None,
            filter by ``lastModifiedDate >= x`` , by default None
        modified_date_lt:   Optional[datetime] = None,
            filter by ``lastModifiedDate < x`` , by default None
        modified_date_lte:  Optional[datetime] = None,
            filter by ``lastModifiedDate <= x`` , by default None
        filter_exp      :   Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page            :   int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size       :   int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 5000
        paginate        :   bool, optional
            whether to auto-paginate the response, by default False
        raw             :   bool, optional
            return a ``requests.Response`` instead of a ``DataFrame``, by default False

        Returns
        -------
        Union[pd.DataFrame, Response]
              DataFrame
                  DataFrame of the ``response.json()``
              Response
                  Raw ``requests.Response`` object

        Examples
        --------
        **Simple**
        >>> power.get_power_assets(plant_technology="Solar", paginate=True)

        **Filter by multiple fields**
        >>> power.get_power_assets(country="Spain", status="Operating")

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("plantName", plant_name))
        filter_params.append(list_to_filter("unitId", unit_id))
        filter_params.append(list_to_filter("operator", operator))
        filter_params.append(list_to_filter("plantTechnology", plant_technology))
        filter_params.append(list_to_filter("status", status))
        filter_params.append(list_to_filter("unitPrimaryFuel", unit_primary_fuel))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("lastModifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path="egp/v1/eupower/power-assets",
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

        if "lastModifiedDate" in df.columns:
            df["lastModifiedDate"] = pd.to_datetime(df["lastModifiedDate"])  # type: ignore
        return df