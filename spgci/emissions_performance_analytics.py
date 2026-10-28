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

from typing import Dict, List, Literal, Optional, Union

import pandas as pd
from pandas import DataFrame, Series
from requests import Response

from spgci.api_client import get_data
from spgci.utilities import list_to_filter

_Str = "Optional[Union[list[str], Series[str], str]]"

EmissionsDataset = Literal["asset-full-extract", "asset-units"]


class EmissionsPerformanceAnalytics:
    """
    Emissions Performance Analytics data.

    Greenhouse gas emissions (scope 1, 2 and 3), emissions intensity and
    ownership/working-interest attributes for emitting facilities.

    Includes
    --------
    ``get_asset_emissions()`` asset-level emissions by emitter and year (full extract).

    ``get_asset_units()`` emissions intensity units reported per sector.

    ``get_unique_values()`` discover valid filter values for any of the datasets.
    """

    _dataset_to_path: Dict[str, str] = {
        "asset-full-extract": "/emissions/emissions-performance-analytics/asset/v1/full-extract",
        "asset-units": "/emissions/emissions-performance-analytics/asset/v1/units",
    }

    def get_asset_emissions(
        self,
        *,
        emissions_intensity_unit: Union[list[str], Series[str], str],
        sector_name: _Str = None,
        sub_sector_name: _Str = None,
        continent: _Str = None,
        region: _Str = None,
        subregion: _Str = None,
        basin_name: _Str = None,
        emitter_name: _Str = None,
        owner_company_name: _Str = None,
        operator_company_name: _Str = None,
        reporting_owner_company_name: _Str = None,
        reporting_operator_company_name: _Str = None,
        primary_product_category: _Str = None,
        primary_product_detail: _Str = None,
        year: Optional[Union[int, list[int]]] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        emitter_status: _Str = None,
        emitter_situation: _Str = None,
        emissions_datasource: _Str = None,
        emissions_datasource_type: _Str = None,
        emissions_reported_estimated: _Str = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve asset-level greenhouse gas emissions by emitter and year.

        Parameters
        ----------
        emissions_intensity_unit : list[str] | Series[str] | str
            Unit of the emissions intensity, e.g. ``"kgCO2e/boe"``. Required by
            the API. The unit differs by sector, so intensities are only
            comparable within a unit; see ``get_asset_units()`` for the valid
            sector/unit pairs.
        sector_name : list[str] | Series[str] | str, optional
            Business sector the facility operates in, e.g. ``"Upstream"``.
        sub_sector_name : list[str] | Series[str] | str, optional
            Facility type or sub-classification, e.g. ``"Liquid Asset"``.
        continent : list[str] | Series[str] | str, optional
            Continent of the emitting facility, e.g. ``"Asia"``.
        region : list[str] | Series[str] | str, optional
            Country or broader geographic region of the facility, e.g. ``"Russia"``.
        subregion : list[str] | Series[str] | str, optional
            State, province or smaller geographic subregion of the facility.
        basin_name : list[str] | Series[str] | str, optional
            Province or large geologic area, e.g. ``"Nepa-Botuoba Basin"``.
        emitter_name : list[str] | Series[str] | str, optional
            Name of the facility that emits greenhouse gases.
        owner_company_name : list[str] | Series[str] | str, optional
            Company holding an equity interest in the facility.
        operator_company_name : list[str] | Series[str] | str, optional
            Company operating the facility.
        reporting_owner_company_name : list[str] | Series[str] | str, optional
            Reporting-level owner company of the facility.
        reporting_operator_company_name : list[str] | Series[str] | str, optional
            Reporting-level operator company of the facility.
        primary_product_category : list[str] | Series[str] | str, optional
            Main product type of the facility, e.g. ``"Liquid"``.
        primary_product_detail : list[str] | Series[str] | str, optional
            Specific product sub-category of the facility, e.g. ``"Oil"``.
        year : int | list[int], optional
            Year the emissions are reported for. Data spans 2010-2030; future
            years appear to be projections (all sampled 2030 rows are ``"Estimated"``).
        year_gt, year_gte, year_lt, year_lte : int, optional
            Filter by ``year`` with the given comparison.
        emitter_status : list[str] | Series[str] | str, optional
            Operating status of the facility, e.g. ``"Producing"``.
        emitter_situation : list[str] | Series[str] | str, optional
            ``"Onshore"`` or ``"Offshore"`` for upstream assets.
        emissions_datasource : list[str] | Series[str] | str, optional
            Data source used to calculate the emissions, e.g. ``"S&P Global - Vantage"``.
        emissions_datasource_type : list[str] | Series[str] | str, optional
            Whether the emissions are sourced internally (S&P Global) or externally.
        emissions_reported_estimated : list[str] | Series[str] | str, optional
            Whether the emissions figure is ``"Reported"`` or ``"Estimated"``.
        filter_exp : str, optional
            Additional filter expression. Use this for columns without a
            dedicated parameter, e.g. ``netScope12Co2eTons``.
        page : int, optional
            Page number, by default 1.
        page_size : int, optional
            Number of rows per page, by default 1000 (the API maximum).
        raw : bool, optional
            Return the raw `requests.Response`, by default false.
        paginate : bool, optional
            Retrieve all available pages, by default false.

        Returns
        -------
        DataFrame | Response
            Asset emissions as a DataFrame, or the raw response when `raw=True`.

        Examples
        --------
        >>> ci.EmissionsPerformanceAnalytics().get_asset_emissions(
        ...     emissions_intensity_unit="kgCO2e/boe", region="Russia", year_gte=2020
        ... )
        """
        filter_params: List[str] = [
            list_to_filter("emissionsIntensityUnit", emissions_intensity_unit),
            list_to_filter("sectorName", sector_name),
            list_to_filter("subSectorName", sub_sector_name),
            list_to_filter("continent", continent),
            list_to_filter("region", region),
            list_to_filter("subregion", subregion),
            list_to_filter("basinName", basin_name),
            list_to_filter("emitterName", emitter_name),
            list_to_filter("ownerCompanyName", owner_company_name),
            list_to_filter("operatorCompanyName", operator_company_name),
            list_to_filter("reportingOwnerCompanyName", reporting_owner_company_name),
            list_to_filter("reportingOperatorCompanyName", reporting_operator_company_name),
            list_to_filter("primaryProductCategory", primary_product_category),
            list_to_filter("primaryProductDetail", primary_product_detail),
            list_to_filter("year", year),
            list_to_filter("emitterStatus", emitter_status),
            list_to_filter("emitterSituation", emitter_situation),
            list_to_filter("emissionsDatasource", emissions_datasource),
            list_to_filter("emissionsDatasourceType", emissions_datasource_type),
            list_to_filter("emissionsReportedEstimated", emissions_reported_estimated),
        ]
        for op, val in ((">", year_gt), (">=", year_gte), ("<", year_lt), ("<=", year_lte)):
            if val is not None:
                filter_params.append(f'year {op} "{val}"')
        filter_params = [value for value in filter_params if value != ""]
        filter_exp = self._join_filters(filter_params, filter_exp)

        return get_data(
            path=self._dataset_to_path["asset-full-extract"],
            params={
                "page": page,
                "pageSize": page_size,
                "filter": filter_exp,
            },
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_asset_units(
        self,
        *,
        sector_name: _Str = None,
        emissions_intensity_unit: _Str = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve the emissions intensity units used for each sector.

        Parameters
        ----------
        sector_name : list[str] | Series[str] | str, optional
            Business sector, e.g. ``"Upstream"``.
        emissions_intensity_unit : list[str] | Series[str] | str, optional
            Standardized unit of emissions intensity, e.g. ``"kgCO2e/boe"``.
        filter_exp : str, optional
            Additional filter expression. Use this for columns without a
            dedicated parameter, e.g. ``isActive``.
        page : int, optional
            Page number, by default 1.
        page_size : int, optional
            Number of rows per page, by default 1000 (the API maximum).
        raw : bool, optional
            Return the raw `requests.Response`, by default false.
        paginate : bool, optional
            Retrieve all available pages, by default false.

        Returns
        -------
        DataFrame | Response
            Sector units as a DataFrame, or the raw response when `raw=True`.

        Examples
        --------
        >>> ci.EmissionsPerformanceAnalytics().get_asset_units(sector_name="Upstream")
        """
        filter_params: List[str] = [
            list_to_filter("sectorName", sector_name),
            list_to_filter("emissionsIntensityUnit", emissions_intensity_unit),
        ]
        filter_params = [value for value in filter_params if value != ""]
        filter_exp = self._join_filters(filter_params, filter_exp)

        return get_data(
            path=self._dataset_to_path["asset-units"],
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
        dataset: EmissionsDataset,
        columns: Optional[Union[list[str], str]],
        filter_exp: Optional[str] = None,
    ) -> DataFrame:
        """
        Get unique values (or combinations) of columns in a dataset.

        Use this to discover valid filter values before calling a ``get_*`` method.

        Parameters
        ----------
        dataset : {"asset-full-extract", "asset-units"}
            Dataset to query.
        columns : list[str] | str
            API (camelCase) column names, e.g. ``["sectorName", "emissionsIntensityUnit"]``.
        filter_exp : str, optional
            Filter expression limiting the rows considered. For
            ``"asset-full-extract"`` it must include ``emissionsIntensityUnit``.

        Returns
        -------
        DataFrame
            Unique combinations of the requested columns.

        Examples
        --------
        >>> ci.EmissionsPerformanceAnalytics().get_unique_values(
        ...     "asset-full-extract",
        ...     ["sectorName", "subSectorName"],
        ...     filter_exp='emissionsIntensityUnit: "kgCO2e/boe"',
        ... )
        """
        if dataset not in self._dataset_to_path:
            valid = ", ".join(self._dataset_to_path)
            raise ValueError(f"dataset '{dataset}' not found. Valid datasets: {valid}")
        col_value = ", ".join(columns) if isinstance(columns, list) else columns or ""
        params: Dict[str, Union[str, int]] = {"GroupBy": col_value, "pageSize": 1000}
        if filter_exp is not None:
            params["filter"] = filter_exp

        return get_data(
            self._dataset_to_path[dataset],
            params,
            self._convert_to_df_agg,
            paginate=True,
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
    def _parse_dates(df: DataFrame) -> DataFrame:
        # `errors="coerce"` turns out-of-bounds sentinel timestamps into NaT.
        if "lastModifiedDatetime" in df.columns:
            df["lastModifiedDatetime"] = pd.to_datetime(
                df["lastModifiedDatetime"], format="ISO8601", errors="coerce"
            )
        return df

    @classmethod
    def _convert_to_df_agg(cls, resp: Response) -> DataFrame:
        return cls._parse_dates(pd.json_normalize(resp.json()["aggResultValue"]))  # type: ignore

    @classmethod
    def _convert_to_df(cls, resp: Response) -> DataFrame:
        return cls._parse_dates(pd.json_normalize(resp.json()["results"]))  # type: ignore
