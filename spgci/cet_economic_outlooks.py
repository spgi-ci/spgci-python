from __future__ import annotations
from datetime import date
from typing import Literal, Optional, Union
import pandas as pd
from pandas import DataFrame, Series
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter

EconomicOutlooksDataset = Literal[
    "all-investments",
    "solarpv-capex-and-investment",
    "onshorewind-capex-and-investment",
    "offshorewind-capex-and-investment",
    "energystorage-capex-and-investment",
    "electrolyzer-capex-and-investment",
    "solarpv-capex",
    "onshorewind-capex",
    "offshorewind-capex",
    "energystorage-capex",
    "electrolyzer-capex",
    "battery-cost",
    "levelized-cost",
]


class CetEconomicOutlooks:
    """Client for CET economic outlook datasets."""

    _dataset_to_path = {
        "all-investments": "analytics/cet/economic-outlooks/v1/all-investments",
        "solarpv-capex-and-investment": "analytics/cet/economic-outlooks/v1/solarpv-capex-and-investment",
        "onshorewind-capex-and-investment": "analytics/cet/economic-outlooks/v1/onshorewind-capex-and-investment",
        "offshorewind-capex-and-investment": "analytics/cet/economic-outlooks/v1/offshorewind-capex-and-investment",
        "energystorage-capex-and-investment": "analytics/cet/economic-outlooks/v1/energystorage-capex-and-investment",
        "electrolyzer-capex-and-investment": "analytics/cet/economic-outlooks/v1/electrolyzer-capex-and-investment",
        "solarpv-capex": "analytics/cet/economic-outlooks/v1/solarpv-capex",
        "onshorewind-capex": "analytics/cet/economic-outlooks/v1/onshorewind-capex",
        "offshorewind-capex": "analytics/cet/economic-outlooks/v1/offshorewind-capex",
        "energystorage-capex": "analytics/cet/economic-outlooks/v1/energystorage-capex",
        "electrolyzer-capex": "analytics/cet/economic-outlooks/v1/electrolyzer-capex",
        "battery-cost": "analytics/cet/economic-outlooks/v1/battery-cost",
        "levelized-cost": "analytics/cet/economic-outlooks/v1/levelized-cost",
    }

    def get_unique_values(
        self,
        dataset: EconomicOutlooksDataset,
        columns: Union[list[str], str],
        filter_exp: Optional[str] = None,
    ) -> DataFrame:
        """Return unique values or combinations for API camelCase columns."""
        group_by = ", ".join(columns) if isinstance(columns, list) else columns
        if not group_by.strip():
            raise ValueError("columns must contain at least one column name")
        params = {"GroupBy": group_by, "pageSize": 5000}
        if filter_exp is not None:
            params["filter"] = filter_exp
        return get_data(
            path=self._dataset_to_path[dataset],
            params=params,
            df_fn=self._convert_unique_values_to_df,
            paginate=True,
        )

    def get_all_investments(
        self,
        *,
        vintage_additions: Optional[date] = None,
        vintage_additions_lt: Optional[date] = None,
        vintage_additions_lte: Optional[date] = None,
        vintage_additions_gt: Optional[date] = None,
        vintage_additions_gte: Optional[date] = None,
        vintage_capex: Optional[date] = None,
        vintage_capex_lt: Optional[date] = None,
        vintage_capex_lte: Optional[date] = None,
        vintage_capex_gt: Optional[date] = None,
        vintage_capex_gte: Optional[date] = None,
        technology_main_major: Optional[Union[list[str], Series[str], str]] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        technology_type: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        concept_detailed: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        metric: Optional[Union[list[str], Series[str], str]] = None,
        metric_detailed: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the all-investments dataset.

        Parameters
        ----------
        vintage_additions: Optional[date]
            Publication date of the capacity additions data vintage.
        vintage_additions_lt: Optional[date]
            Filter records where vintage additions is less than the supplied date.
        vintage_additions_lte: Optional[date]
            Filter records where vintage additions is less than or equal to the supplied date.
        vintage_additions_gt: Optional[date]
            Filter records where vintage additions is greater than the supplied date.
        vintage_additions_gte: Optional[date]
            Filter records where vintage additions is greater than or equal to the supplied date.
        vintage_capex: Optional[date]
            Publication date of the CAPEX data vintage.
        vintage_capex_lt: Optional[date]
            Filter records where vintage capex is less than the supplied date.
        vintage_capex_lte: Optional[date]
            Filter records where vintage capex is less than or equal to the supplied date.
        vintage_capex_gt: Optional[date]
            Filter records where vintage capex is greater than the supplied date.
        vintage_capex_gte: Optional[date]
            Filter records where vintage capex is greater than or equal to the supplied date.
        technology_main_major: Optional[Union[list[str], Series[str], str]]
            Top-level technology category associated with the reported data.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        technology_type: Optional[Union[list[str], Series[str], str]]
            Deployment scale or system classification associated with the technology.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        concept_detailed: Optional[Union[list[str], Series[str], str]]
            Concept detailed associated with the reported data.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        component_major: Optional[Union[list[str], Series[str], str]]
            Component major associated with the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Component minor associated with the reported data.
        metric: Optional[Union[list[str], Series[str], str]]
            Unit or denomination in which the reported value is measured.
        metric_detailed: Optional[Union[list[str], Series[str], str]]
            Metric detailed associated with the reported data.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        filter_exp: Optional[str]
            Optional filter expression appended to filters generated from the other arguments.
        page: int
            Page number to retrieve.
        page_size: int
            Maximum number of records to retrieve per page.
        raw: bool
            When True, return the raw requests Response instead of a DataFrame.
        paginate: bool
            When True, retrieve all available pages.

        Returns
        -------
        DataFrame or Response
            A normalized pandas DataFrame, or the raw response when ``raw=True``."""
        filter_params: list[str] = []
        filter_params.append(list_to_filter("vintageAdditions", vintage_additions))
        if vintage_additions_lt is not None:
            filter_params.append(f'vintageAdditions < "{vintage_additions_lt}"')
        if vintage_additions_lte is not None:
            filter_params.append(f'vintageAdditions <= "{vintage_additions_lte}"')
        if vintage_additions_gt is not None:
            filter_params.append(f'vintageAdditions > "{vintage_additions_gt}"')
        if vintage_additions_gte is not None:
            filter_params.append(f'vintageAdditions >= "{vintage_additions_gte}"')
        filter_params.append(list_to_filter("vintageCapex", vintage_capex))
        if vintage_capex_lt is not None:
            filter_params.append(f'vintageCapex < "{vintage_capex_lt}"')
        if vintage_capex_lte is not None:
            filter_params.append(f'vintageCapex <= "{vintage_capex_lte}"')
        if vintage_capex_gt is not None:
            filter_params.append(f'vintageCapex > "{vintage_capex_gt}"')
        if vintage_capex_gte is not None:
            filter_params.append(f'vintageCapex >= "{vintage_capex_gte}"')
        filter_params.append(
            list_to_filter("technologyMainMajor", technology_main_major)
        )
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params.append(list_to_filter("technologyType", technology_type))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("conceptDetailed", concept_detailed))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params.append(list_to_filter("metric", metric))
        filter_params.append(list_to_filter("metricDetailed", metric_detailed))
        filter_params.append(list_to_filter("value", value))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/economic-outlooks/v1/all-investments",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_solarpv_capex_and_investment(
        self,
        *,
        vintage_additions: Optional[date] = None,
        vintage_additions_lt: Optional[date] = None,
        vintage_additions_lte: Optional[date] = None,
        vintage_additions_gt: Optional[date] = None,
        vintage_additions_gte: Optional[date] = None,
        vintage_capex: Optional[date] = None,
        vintage_capex_lt: Optional[date] = None,
        vintage_capex_lte: Optional[date] = None,
        vintage_capex_gt: Optional[date] = None,
        vintage_capex_gte: Optional[date] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        technology_type: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        siting_minor: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        concept_detailed: Optional[Union[list[str], Series[str], str]] = None,
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        metric: Optional[Union[list[str], Series[str], str]] = None,
        metric_detailed: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the solarpv-capex-and-investment dataset.

        Parameters
        ----------
        vintage_additions: Optional[date]
            Publication date of the capacity additions data vintage.
        vintage_additions_lt: Optional[date]
            Filter records where vintage additions is less than the supplied date.
        vintage_additions_lte: Optional[date]
            Filter records where vintage additions is less than or equal to the supplied date.
        vintage_additions_gt: Optional[date]
            Filter records where vintage additions is greater than the supplied date.
        vintage_additions_gte: Optional[date]
            Filter records where vintage additions is greater than or equal to the supplied date.
        vintage_capex: Optional[date]
            Publication date of the CAPEX data vintage.
        vintage_capex_lt: Optional[date]
            Filter records where vintage capex is less than the supplied date.
        vintage_capex_lte: Optional[date]
            Filter records where vintage capex is less than or equal to the supplied date.
        vintage_capex_gt: Optional[date]
            Filter records where vintage capex is greater than the supplied date.
        vintage_capex_gte: Optional[date]
            Filter records where vintage capex is greater than or equal to the supplied date.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        technology_type: Optional[Union[list[str], Series[str], str]]
            Deployment scale or system classification associated with the technology.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        siting_minor: Optional[Union[list[str], Series[str], str]]
            Siting minor associated with the reported data.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        concept_detailed: Optional[Union[list[str], Series[str], str]]
            Concept detailed associated with the reported data.
        component_major: Optional[Union[list[str], Series[str], str]]
            Component major associated with the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Component minor associated with the reported data.
        metric: Optional[Union[list[str], Series[str], str]]
            Unit or denomination in which the reported value is measured.
        metric_detailed: Optional[Union[list[str], Series[str], str]]
            Metric detailed associated with the reported data.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        filter_exp: Optional[str]
            Optional filter expression appended to filters generated from the other arguments.
        page: int
            Page number to retrieve.
        page_size: int
            Maximum number of records to retrieve per page.
        raw: bool
            When True, return the raw requests Response instead of a DataFrame.
        paginate: bool
            When True, retrieve all available pages.

        Returns
        -------
        DataFrame or Response
            A normalized pandas DataFrame, or the raw response when ``raw=True``."""
        filter_params: list[str] = []
        filter_params.append(list_to_filter("vintageAdditions", vintage_additions))
        if vintage_additions_lt is not None:
            filter_params.append(f'vintageAdditions < "{vintage_additions_lt}"')
        if vintage_additions_lte is not None:
            filter_params.append(f'vintageAdditions <= "{vintage_additions_lte}"')
        if vintage_additions_gt is not None:
            filter_params.append(f'vintageAdditions > "{vintage_additions_gt}"')
        if vintage_additions_gte is not None:
            filter_params.append(f'vintageAdditions >= "{vintage_additions_gte}"')
        filter_params.append(list_to_filter("vintageCapex", vintage_capex))
        if vintage_capex_lt is not None:
            filter_params.append(f'vintageCapex < "{vintage_capex_lt}"')
        if vintage_capex_lte is not None:
            filter_params.append(f'vintageCapex <= "{vintage_capex_lte}"')
        if vintage_capex_gt is not None:
            filter_params.append(f'vintageCapex > "{vintage_capex_gt}"')
        if vintage_capex_gte is not None:
            filter_params.append(f'vintageCapex >= "{vintage_capex_gte}"')
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params.append(list_to_filter("technologyType", technology_type))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("sitingMinor", siting_minor))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("conceptDetailed", concept_detailed))
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params.append(list_to_filter("metric", metric))
        filter_params.append(list_to_filter("metricDetailed", metric_detailed))
        filter_params.append(list_to_filter("value", value))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/economic-outlooks/v1/solarpv-capex-and-investment",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_onshorewind_capex_and_investment(
        self,
        *,
        vintage_additions: Optional[date] = None,
        vintage_additions_lt: Optional[date] = None,
        vintage_additions_lte: Optional[date] = None,
        vintage_additions_gt: Optional[date] = None,
        vintage_additions_gte: Optional[date] = None,
        vintage_capex: Optional[date] = None,
        vintage_capex_lt: Optional[date] = None,
        vintage_capex_lte: Optional[date] = None,
        vintage_capex_gt: Optional[date] = None,
        vintage_capex_gte: Optional[date] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        technology_type: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        concept_detailed: Optional[Union[list[str], Series[str], str]] = None,
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        metric: Optional[Union[list[str], Series[str], str]] = None,
        metric_detailed: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the onshorewind-capex-and-investment dataset.

        Parameters
        ----------
        vintage_additions: Optional[date]
            Publication date of the capacity additions data vintage.
        vintage_additions_lt: Optional[date]
            Filter records where vintage additions is less than the supplied date.
        vintage_additions_lte: Optional[date]
            Filter records where vintage additions is less than or equal to the supplied date.
        vintage_additions_gt: Optional[date]
            Filter records where vintage additions is greater than the supplied date.
        vintage_additions_gte: Optional[date]
            Filter records where vintage additions is greater than or equal to the supplied date.
        vintage_capex: Optional[date]
            Publication date of the CAPEX data vintage.
        vintage_capex_lt: Optional[date]
            Filter records where vintage capex is less than the supplied date.
        vintage_capex_lte: Optional[date]
            Filter records where vintage capex is less than or equal to the supplied date.
        vintage_capex_gt: Optional[date]
            Filter records where vintage capex is greater than the supplied date.
        vintage_capex_gte: Optional[date]
            Filter records where vintage capex is greater than or equal to the supplied date.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        technology_type: Optional[Union[list[str], Series[str], str]]
            Deployment scale or system classification associated with the technology.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        concept_detailed: Optional[Union[list[str], Series[str], str]]
            Concept detailed associated with the reported data.
        component_major: Optional[Union[list[str], Series[str], str]]
            Component major associated with the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Component minor associated with the reported data.
        metric: Optional[Union[list[str], Series[str], str]]
            Unit or denomination in which the reported value is measured.
        metric_detailed: Optional[Union[list[str], Series[str], str]]
            Metric detailed associated with the reported data.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        filter_exp: Optional[str]
            Optional filter expression appended to filters generated from the other arguments.
        page: int
            Page number to retrieve.
        page_size: int
            Maximum number of records to retrieve per page.
        raw: bool
            When True, return the raw requests Response instead of a DataFrame.
        paginate: bool
            When True, retrieve all available pages.

        Returns
        -------
        DataFrame or Response
            A normalized pandas DataFrame, or the raw response when ``raw=True``."""
        filter_params: list[str] = []
        filter_params.append(list_to_filter("vintageAdditions", vintage_additions))
        if vintage_additions_lt is not None:
            filter_params.append(f'vintageAdditions < "{vintage_additions_lt}"')
        if vintage_additions_lte is not None:
            filter_params.append(f'vintageAdditions <= "{vintage_additions_lte}"')
        if vintage_additions_gt is not None:
            filter_params.append(f'vintageAdditions > "{vintage_additions_gt}"')
        if vintage_additions_gte is not None:
            filter_params.append(f'vintageAdditions >= "{vintage_additions_gte}"')
        filter_params.append(list_to_filter("vintageCapex", vintage_capex))
        if vintage_capex_lt is not None:
            filter_params.append(f'vintageCapex < "{vintage_capex_lt}"')
        if vintage_capex_lte is not None:
            filter_params.append(f'vintageCapex <= "{vintage_capex_lte}"')
        if vintage_capex_gt is not None:
            filter_params.append(f'vintageCapex > "{vintage_capex_gt}"')
        if vintage_capex_gte is not None:
            filter_params.append(f'vintageCapex >= "{vintage_capex_gte}"')
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params.append(list_to_filter("technologyType", technology_type))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("conceptDetailed", concept_detailed))
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params.append(list_to_filter("metric", metric))
        filter_params.append(list_to_filter("metricDetailed", metric_detailed))
        filter_params.append(list_to_filter("value", value))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/economic-outlooks/v1/onshorewind-capex-and-investment",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_offshorewind_capex_and_investment(
        self,
        *,
        vintage_additions: Optional[date] = None,
        vintage_additions_lt: Optional[date] = None,
        vintage_additions_lte: Optional[date] = None,
        vintage_additions_gt: Optional[date] = None,
        vintage_additions_gte: Optional[date] = None,
        vintage_capex: Optional[date] = None,
        vintage_capex_lt: Optional[date] = None,
        vintage_capex_lte: Optional[date] = None,
        vintage_capex_gt: Optional[date] = None,
        vintage_capex_gte: Optional[date] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        technology_type: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        foundation_type: Optional[Union[list[str], Series[str], str]] = None,
        concept_detailed: Optional[Union[list[str], Series[str], str]] = None,
        component_major2: Optional[Union[list[str], Series[str], str]] = None,
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        metric: Optional[Union[list[str], Series[str], str]] = None,
        metric_detailed: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the offshorewind-capex-and-investment dataset.

        Parameters
        ----------
        vintage_additions: Optional[date]
            Publication date of the capacity additions data vintage.
        vintage_additions_lt: Optional[date]
            Filter records where vintage additions is less than the supplied date.
        vintage_additions_lte: Optional[date]
            Filter records where vintage additions is less than or equal to the supplied date.
        vintage_additions_gt: Optional[date]
            Filter records where vintage additions is greater than the supplied date.
        vintage_additions_gte: Optional[date]
            Filter records where vintage additions is greater than or equal to the supplied date.
        vintage_capex: Optional[date]
            Publication date of the CAPEX data vintage.
        vintage_capex_lt: Optional[date]
            Filter records where vintage capex is less than the supplied date.
        vintage_capex_lte: Optional[date]
            Filter records where vintage capex is less than or equal to the supplied date.
        vintage_capex_gt: Optional[date]
            Filter records where vintage capex is greater than the supplied date.
        vintage_capex_gte: Optional[date]
            Filter records where vintage capex is greater than or equal to the supplied date.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        technology_type: Optional[Union[list[str], Series[str], str]]
            Deployment scale or system classification associated with the technology.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        foundation_type: Optional[Union[list[str], Series[str], str]]
            Foundation type associated with the reported data.
        concept_detailed: Optional[Union[list[str], Series[str], str]]
            Concept detailed associated with the reported data.
        component_major2: Optional[Union[list[str], Series[str], str]]
            Component major2 associated with the reported data.
        component_major: Optional[Union[list[str], Series[str], str]]
            Component major associated with the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Component minor associated with the reported data.
        metric: Optional[Union[list[str], Series[str], str]]
            Unit or denomination in which the reported value is measured.
        metric_detailed: Optional[Union[list[str], Series[str], str]]
            Metric detailed associated with the reported data.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        filter_exp: Optional[str]
            Optional filter expression appended to filters generated from the other arguments.
        page: int
            Page number to retrieve.
        page_size: int
            Maximum number of records to retrieve per page.
        raw: bool
            When True, return the raw requests Response instead of a DataFrame.
        paginate: bool
            When True, retrieve all available pages.

        Returns
        -------
        DataFrame or Response
            A normalized pandas DataFrame, or the raw response when ``raw=True``."""
        filter_params: list[str] = []
        filter_params.append(list_to_filter("vintageAdditions", vintage_additions))
        if vintage_additions_lt is not None:
            filter_params.append(f'vintageAdditions < "{vintage_additions_lt}"')
        if vintage_additions_lte is not None:
            filter_params.append(f'vintageAdditions <= "{vintage_additions_lte}"')
        if vintage_additions_gt is not None:
            filter_params.append(f'vintageAdditions > "{vintage_additions_gt}"')
        if vintage_additions_gte is not None:
            filter_params.append(f'vintageAdditions >= "{vintage_additions_gte}"')
        filter_params.append(list_to_filter("vintageCapex", vintage_capex))
        if vintage_capex_lt is not None:
            filter_params.append(f'vintageCapex < "{vintage_capex_lt}"')
        if vintage_capex_lte is not None:
            filter_params.append(f'vintageCapex <= "{vintage_capex_lte}"')
        if vintage_capex_gt is not None:
            filter_params.append(f'vintageCapex > "{vintage_capex_gt}"')
        if vintage_capex_gte is not None:
            filter_params.append(f'vintageCapex >= "{vintage_capex_gte}"')
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params.append(list_to_filter("technologyType", technology_type))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("foundationType", foundation_type))
        filter_params.append(list_to_filter("conceptDetailed", concept_detailed))
        filter_params.append(list_to_filter("componentMajor2", component_major2))
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params.append(list_to_filter("metric", metric))
        filter_params.append(list_to_filter("metricDetailed", metric_detailed))
        filter_params.append(list_to_filter("value", value))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/economic-outlooks/v1/offshorewind-capex-and-investment",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_energystorage_capex_and_investment(
        self,
        *,
        vintage_additions: Optional[date] = None,
        vintage_additions_lt: Optional[date] = None,
        vintage_additions_lte: Optional[date] = None,
        vintage_additions_gt: Optional[date] = None,
        vintage_additions_gte: Optional[date] = None,
        vintage_capex: Optional[date] = None,
        vintage_capex_lt: Optional[date] = None,
        vintage_capex_lte: Optional[date] = None,
        vintage_capex_gt: Optional[date] = None,
        vintage_capex_gte: Optional[date] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        technology_type: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        concept_detailed: Optional[Union[list[str], Series[str], str]] = None,
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        metric: Optional[Union[list[str], Series[str], str]] = None,
        metric_detailed: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        siting: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        duration: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the energystorage-capex-and-investment dataset.

        Parameters
        ----------
        vintage_additions: Optional[date]
            Publication date of the capacity additions data vintage.
        vintage_additions_lt: Optional[date]
            Filter records where vintage additions is less than the supplied date.
        vintage_additions_lte: Optional[date]
            Filter records where vintage additions is less than or equal to the supplied date.
        vintage_additions_gt: Optional[date]
            Filter records where vintage additions is greater than the supplied date.
        vintage_additions_gte: Optional[date]
            Filter records where vintage additions is greater than or equal to the supplied date.
        vintage_capex: Optional[date]
            Publication date of the CAPEX data vintage.
        vintage_capex_lt: Optional[date]
            Filter records where vintage capex is less than the supplied date.
        vintage_capex_lte: Optional[date]
            Filter records where vintage capex is less than or equal to the supplied date.
        vintage_capex_gt: Optional[date]
            Filter records where vintage capex is greater than the supplied date.
        vintage_capex_gte: Optional[date]
            Filter records where vintage capex is greater than or equal to the supplied date.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        technology_type: Optional[Union[list[str], Series[str], str]]
            Deployment scale or system classification associated with the technology.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        concept_detailed: Optional[Union[list[str], Series[str], str]]
            Concept detailed associated with the reported data.
        component_major: Optional[Union[list[str], Series[str], str]]
            Component major associated with the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Component minor associated with the reported data.
        metric: Optional[Union[list[str], Series[str], str]]
            Unit or denomination in which the reported value is measured.
        metric_detailed: Optional[Union[list[str], Series[str], str]]
            Metric detailed associated with the reported data.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        siting: Optional[Union[list[str], Series[str], str]]
            Siting associated with the reported data.
        technology: Optional[Union[list[str], Series[str], str]]
            Technology associated with the reported data.
        duration: Optional[Union[list[str], Series[str], str]]
            Duration associated with the reported data.
        filter_exp: Optional[str]
            Optional filter expression appended to filters generated from the other arguments.
        page: int
            Page number to retrieve.
        page_size: int
            Maximum number of records to retrieve per page.
        raw: bool
            When True, return the raw requests Response instead of a DataFrame.
        paginate: bool
            When True, retrieve all available pages.

        Returns
        -------
        DataFrame or Response
            A normalized pandas DataFrame, or the raw response when ``raw=True``."""
        filter_params: list[str] = []
        filter_params.append(list_to_filter("vintageAdditions", vintage_additions))
        if vintage_additions_lt is not None:
            filter_params.append(f'vintageAdditions < "{vintage_additions_lt}"')
        if vintage_additions_lte is not None:
            filter_params.append(f'vintageAdditions <= "{vintage_additions_lte}"')
        if vintage_additions_gt is not None:
            filter_params.append(f'vintageAdditions > "{vintage_additions_gt}"')
        if vintage_additions_gte is not None:
            filter_params.append(f'vintageAdditions >= "{vintage_additions_gte}"')
        filter_params.append(list_to_filter("vintageCapex", vintage_capex))
        if vintage_capex_lt is not None:
            filter_params.append(f'vintageCapex < "{vintage_capex_lt}"')
        if vintage_capex_lte is not None:
            filter_params.append(f'vintageCapex <= "{vintage_capex_lte}"')
        if vintage_capex_gt is not None:
            filter_params.append(f'vintageCapex > "{vintage_capex_gt}"')
        if vintage_capex_gte is not None:
            filter_params.append(f'vintageCapex >= "{vintage_capex_gte}"')
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params.append(list_to_filter("technologyType", technology_type))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("conceptDetailed", concept_detailed))
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params.append(list_to_filter("metric", metric))
        filter_params.append(list_to_filter("metricDetailed", metric_detailed))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("siting", siting))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("duration", duration))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/economic-outlooks/v1/energystorage-capex-and-investment",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_electrolyzer_capex_and_investment(
        self,
        *,
        vintage_additions: Optional[date] = None,
        vintage_additions_lt: Optional[date] = None,
        vintage_additions_lte: Optional[date] = None,
        vintage_additions_gt: Optional[date] = None,
        vintage_additions_gte: Optional[date] = None,
        vintage_capex: Optional[date] = None,
        vintage_capex_lt: Optional[date] = None,
        vintage_capex_lte: Optional[date] = None,
        vintage_capex_gt: Optional[date] = None,
        vintage_capex_gte: Optional[date] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        technology_type: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        concept_detailed: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        metric: Optional[Union[list[str], Series[str], str]] = None,
        metric_detailed: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        input_source_major: Optional[Union[list[str], Series[str], str]] = None,
        input_source_minor: Optional[Union[list[str], Series[str], str]] = None,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        assumption_technology: Optional[Union[list[str], Series[str], str]] = None,
        assumption_system_size: Optional[Union[list[str], Series[str], str]] = None,
        assumption_module_size: Optional[Union[list[str], Series[str], str]] = None,
        assumption_stack_size: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the electrolyzer-capex-and-investment dataset.

        Parameters
        ----------
        vintage_additions: Optional[date]
            Publication date of the capacity additions data vintage.
        vintage_additions_lt: Optional[date]
            Filter records where vintage additions is less than the supplied date.
        vintage_additions_lte: Optional[date]
            Filter records where vintage additions is less than or equal to the supplied date.
        vintage_additions_gt: Optional[date]
            Filter records where vintage additions is greater than the supplied date.
        vintage_additions_gte: Optional[date]
            Filter records where vintage additions is greater than or equal to the supplied date.
        vintage_capex: Optional[date]
            Publication date of the CAPEX data vintage.
        vintage_capex_lt: Optional[date]
            Filter records where vintage capex is less than the supplied date.
        vintage_capex_lte: Optional[date]
            Filter records where vintage capex is less than or equal to the supplied date.
        vintage_capex_gt: Optional[date]
            Filter records where vintage capex is greater than the supplied date.
        vintage_capex_gte: Optional[date]
            Filter records where vintage capex is greater than or equal to the supplied date.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        technology_type: Optional[Union[list[str], Series[str], str]]
            Deployment scale or system classification associated with the technology.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        concept_detailed: Optional[Union[list[str], Series[str], str]]
            Concept detailed associated with the reported data.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        component_major: Optional[Union[list[str], Series[str], str]]
            Component major associated with the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Component minor associated with the reported data.
        metric: Optional[Union[list[str], Series[str], str]]
            Unit or denomination in which the reported value is measured.
        metric_detailed: Optional[Union[list[str], Series[str], str]]
            Metric detailed associated with the reported data.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        input_source_major: Optional[Union[list[str], Series[str], str]]
            Input source major associated with the reported data.
        input_source_minor: Optional[Union[list[str], Series[str], str]]
            Input source minor associated with the reported data.
        scenario: Optional[Union[list[str], Series[str], str]]
            Scenario associated with the reported data.
        assumption_technology: Optional[Union[list[str], Series[str], str]]
            Assumption technology associated with the reported data.
        assumption_system_size: Optional[Union[list[str], Series[str], str]]
            Assumption system size associated with the reported data.
        assumption_module_size: Optional[Union[list[str], Series[str], str]]
            Assumption module size associated with the reported data.
        assumption_stack_size: Optional[Union[list[str], Series[str], str]]
            Assumption stack size associated with the reported data.
        filter_exp: Optional[str]
            Optional filter expression appended to filters generated from the other arguments.
        page: int
            Page number to retrieve.
        page_size: int
            Maximum number of records to retrieve per page.
        raw: bool
            When True, return the raw requests Response instead of a DataFrame.
        paginate: bool
            When True, retrieve all available pages.

        Returns
        -------
        DataFrame or Response
            A normalized pandas DataFrame, or the raw response when ``raw=True``."""
        filter_params: list[str] = []
        filter_params.append(list_to_filter("vintageAdditions", vintage_additions))
        if vintage_additions_lt is not None:
            filter_params.append(f'vintageAdditions < "{vintage_additions_lt}"')
        if vintage_additions_lte is not None:
            filter_params.append(f'vintageAdditions <= "{vintage_additions_lte}"')
        if vintage_additions_gt is not None:
            filter_params.append(f'vintageAdditions > "{vintage_additions_gt}"')
        if vintage_additions_gte is not None:
            filter_params.append(f'vintageAdditions >= "{vintage_additions_gte}"')
        filter_params.append(list_to_filter("vintageCapex", vintage_capex))
        if vintage_capex_lt is not None:
            filter_params.append(f'vintageCapex < "{vintage_capex_lt}"')
        if vintage_capex_lte is not None:
            filter_params.append(f'vintageCapex <= "{vintage_capex_lte}"')
        if vintage_capex_gt is not None:
            filter_params.append(f'vintageCapex > "{vintage_capex_gt}"')
        if vintage_capex_gte is not None:
            filter_params.append(f'vintageCapex >= "{vintage_capex_gte}"')
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params.append(list_to_filter("technologyType", technology_type))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("conceptDetailed", concept_detailed))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params.append(list_to_filter("metric", metric))
        filter_params.append(list_to_filter("metricDetailed", metric_detailed))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("inputSourceMajor", input_source_major))
        filter_params.append(list_to_filter("inputSourceMinor", input_source_minor))
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(
            list_to_filter("assumptionTechnology", assumption_technology)
        )
        filter_params.append(
            list_to_filter("assumptionSystemSize", assumption_system_size)
        )
        filter_params.append(
            list_to_filter("assumptionModuleSize", assumption_module_size)
        )
        filter_params.append(
            list_to_filter("assumptionStackSize", assumption_stack_size)
        )
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/economic-outlooks/v1/electrolyzer-capex-and-investment",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_solarpv_capex(
        self,
        *,
        vintage: Optional[date] = None,
        vintage_lt: Optional[date] = None,
        vintage_lte: Optional[date] = None,
        vintage_gt: Optional[date] = None,
        vintage_gte: Optional[date] = None,
        vintage_rank: Optional[Union[list[str], Series[str], str]] = None,
        last_updated: Optional[date] = None,
        last_updated_lt: Optional[date] = None,
        last_updated_lte: Optional[date] = None,
        last_updated_gt: Optional[date] = None,
        last_updated_gte: Optional[date] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        technology_type: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        currency_detailed: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        siting_minor: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the solarpv-capex dataset.

        Parameters
        ----------
        vintage: Optional[date]
            Publication date of the data vintage.
        vintage_lt: Optional[date]
            Filter records where vintage is less than the supplied date.
        vintage_lte: Optional[date]
            Filter records where vintage is less than or equal to the supplied date.
        vintage_gt: Optional[date]
            Filter records where vintage is greater than the supplied date.
        vintage_gte: Optional[date]
            Filter records where vintage is greater than or equal to the supplied date.
        vintage_rank: Optional[Union[list[str], Series[str], str]]
            Rank of the data vintage, where 1 represents the latest available release.
        last_updated: Optional[date]
            Date when the record was last updated.
        last_updated_lt: Optional[date]
            Filter records where last updated is less than the supplied date.
        last_updated_lte: Optional[date]
            Filter records where last updated is less than or equal to the supplied date.
        last_updated_gt: Optional[date]
            Filter records where last updated is greater than the supplied date.
        last_updated_gte: Optional[date]
            Filter records where last updated is greater than or equal to the supplied date.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        technology_type: Optional[Union[list[str], Series[str], str]]
            Deployment scale or system classification associated with the technology.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        currency: Optional[Union[list[str], Series[str], str]]
            Currency associated with the reported data.
        currency_detailed: Optional[Union[list[str], Series[str], str]]
            Currency detailed associated with the reported data.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        component_major: Optional[Union[list[str], Series[str], str]]
            Component major associated with the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Component minor associated with the reported data.
        siting_minor: Optional[Union[list[str], Series[str], str]]
            Siting minor associated with the reported data.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        filter_exp: Optional[str]
            Optional filter expression appended to filters generated from the other arguments.
        page: int
            Page number to retrieve.
        page_size: int
            Maximum number of records to retrieve per page.
        raw: bool
            When True, return the raw requests Response instead of a DataFrame.
        paginate: bool
            When True, retrieve all available pages.

        Returns
        -------
        DataFrame or Response
            A normalized pandas DataFrame, or the raw response when ``raw=True``."""
        filter_params: list[str] = []
        filter_params.append(list_to_filter("vintage", vintage))
        if vintage_lt is not None:
            filter_params.append(f'vintage < "{vintage_lt}"')
        if vintage_lte is not None:
            filter_params.append(f'vintage <= "{vintage_lte}"')
        if vintage_gt is not None:
            filter_params.append(f'vintage > "{vintage_gt}"')
        if vintage_gte is not None:
            filter_params.append(f'vintage >= "{vintage_gte}"')
        filter_params.append(list_to_filter("vintageRank", vintage_rank))
        filter_params.append(list_to_filter("lastUpdated", last_updated))
        if last_updated_lt is not None:
            filter_params.append(f'lastUpdated < "{last_updated_lt}"')
        if last_updated_lte is not None:
            filter_params.append(f'lastUpdated <= "{last_updated_lte}"')
        if last_updated_gt is not None:
            filter_params.append(f'lastUpdated > "{last_updated_gt}"')
        if last_updated_gte is not None:
            filter_params.append(f'lastUpdated >= "{last_updated_gte}"')
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params.append(list_to_filter("technologyType", technology_type))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("currencyDetailed", currency_detailed))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params.append(list_to_filter("sitingMinor", siting_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/economic-outlooks/v1/solarpv-capex",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_onshorewind_capex(
        self,
        *,
        vintage: Optional[date] = None,
        vintage_lt: Optional[date] = None,
        vintage_lte: Optional[date] = None,
        vintage_gt: Optional[date] = None,
        vintage_gte: Optional[date] = None,
        last_updated: Optional[date] = None,
        last_updated_lt: Optional[date] = None,
        last_updated_lte: Optional[date] = None,
        last_updated_gt: Optional[date] = None,
        last_updated_gte: Optional[date] = None,
        vintage_rank: Optional[Union[list[str], Series[str], str]] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        technology_type: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        currency_detailed: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the onshorewind-capex dataset.

        Parameters
        ----------
        vintage: Optional[date]
            Publication date of the data vintage.
        vintage_lt: Optional[date]
            Filter records where vintage is less than the supplied date.
        vintage_lte: Optional[date]
            Filter records where vintage is less than or equal to the supplied date.
        vintage_gt: Optional[date]
            Filter records where vintage is greater than the supplied date.
        vintage_gte: Optional[date]
            Filter records where vintage is greater than or equal to the supplied date.
        last_updated: Optional[date]
            Date when the record was last updated.
        last_updated_lt: Optional[date]
            Filter records where last updated is less than the supplied date.
        last_updated_lte: Optional[date]
            Filter records where last updated is less than or equal to the supplied date.
        last_updated_gt: Optional[date]
            Filter records where last updated is greater than the supplied date.
        last_updated_gte: Optional[date]
            Filter records where last updated is greater than or equal to the supplied date.
        vintage_rank: Optional[Union[list[str], Series[str], str]]
            Rank of the data vintage, where 1 represents the latest available release.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        technology_type: Optional[Union[list[str], Series[str], str]]
            Deployment scale or system classification associated with the technology.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        currency: Optional[Union[list[str], Series[str], str]]
            Currency associated with the reported data.
        currency_detailed: Optional[Union[list[str], Series[str], str]]
            Currency detailed associated with the reported data.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        component_major: Optional[Union[list[str], Series[str], str]]
            Component major associated with the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Component minor associated with the reported data.
        filter_exp: Optional[str]
            Optional filter expression appended to filters generated from the other arguments.
        page: int
            Page number to retrieve.
        page_size: int
            Maximum number of records to retrieve per page.
        raw: bool
            When True, return the raw requests Response instead of a DataFrame.
        paginate: bool
            When True, retrieve all available pages.

        Returns
        -------
        DataFrame or Response
            A normalized pandas DataFrame, or the raw response when ``raw=True``."""
        filter_params: list[str] = []
        filter_params.append(list_to_filter("vintage", vintage))
        if vintage_lt is not None:
            filter_params.append(f'vintage < "{vintage_lt}"')
        if vintage_lte is not None:
            filter_params.append(f'vintage <= "{vintage_lte}"')
        if vintage_gt is not None:
            filter_params.append(f'vintage > "{vintage_gt}"')
        if vintage_gte is not None:
            filter_params.append(f'vintage >= "{vintage_gte}"')
        filter_params.append(list_to_filter("lastUpdated", last_updated))
        if last_updated_lt is not None:
            filter_params.append(f'lastUpdated < "{last_updated_lt}"')
        if last_updated_lte is not None:
            filter_params.append(f'lastUpdated <= "{last_updated_lte}"')
        if last_updated_gt is not None:
            filter_params.append(f'lastUpdated > "{last_updated_gt}"')
        if last_updated_gte is not None:
            filter_params.append(f'lastUpdated >= "{last_updated_gte}"')
        filter_params.append(list_to_filter("vintageRank", vintage_rank))
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params.append(list_to_filter("technologyType", technology_type))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("currencyDetailed", currency_detailed))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/economic-outlooks/v1/onshorewind-capex",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_offshorewind_capex(
        self,
        *,
        vintage: Optional[date] = None,
        vintage_lt: Optional[date] = None,
        vintage_lte: Optional[date] = None,
        vintage_gt: Optional[date] = None,
        vintage_gte: Optional[date] = None,
        last_updated: Optional[date] = None,
        last_updated_lt: Optional[date] = None,
        last_updated_lte: Optional[date] = None,
        last_updated_gt: Optional[date] = None,
        last_updated_gte: Optional[date] = None,
        vintage_rank: Optional[Union[list[str], Series[str], str]] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        technology_type: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        currency_detailed: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        component_major2: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the offshorewind-capex dataset.

        Parameters
        ----------
        vintage: Optional[date]
            Publication date of the data vintage.
        vintage_lt: Optional[date]
            Filter records where vintage is less than the supplied date.
        vintage_lte: Optional[date]
            Filter records where vintage is less than or equal to the supplied date.
        vintage_gt: Optional[date]
            Filter records where vintage is greater than the supplied date.
        vintage_gte: Optional[date]
            Filter records where vintage is greater than or equal to the supplied date.
        last_updated: Optional[date]
            Date when the record was last updated.
        last_updated_lt: Optional[date]
            Filter records where last updated is less than the supplied date.
        last_updated_lte: Optional[date]
            Filter records where last updated is less than or equal to the supplied date.
        last_updated_gt: Optional[date]
            Filter records where last updated is greater than the supplied date.
        last_updated_gte: Optional[date]
            Filter records where last updated is greater than or equal to the supplied date.
        vintage_rank: Optional[Union[list[str], Series[str], str]]
            Rank of the data vintage, where 1 represents the latest available release.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        technology_type: Optional[Union[list[str], Series[str], str]]
            Deployment scale or system classification associated with the technology.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        currency: Optional[Union[list[str], Series[str], str]]
            Currency associated with the reported data.
        currency_detailed: Optional[Union[list[str], Series[str], str]]
            Currency detailed associated with the reported data.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        component_major: Optional[Union[list[str], Series[str], str]]
            Component major associated with the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Component minor associated with the reported data.
        component_major2: Optional[Union[list[str], Series[str], str]]
            Component major2 associated with the reported data.
        technology: Optional[Union[list[str], Series[str], str]]
            Technology associated with the reported data.
        filter_exp: Optional[str]
            Optional filter expression appended to filters generated from the other arguments.
        page: int
            Page number to retrieve.
        page_size: int
            Maximum number of records to retrieve per page.
        raw: bool
            When True, return the raw requests Response instead of a DataFrame.
        paginate: bool
            When True, retrieve all available pages.

        Returns
        -------
        DataFrame or Response
            A normalized pandas DataFrame, or the raw response when ``raw=True``."""
        filter_params: list[str] = []
        filter_params.append(list_to_filter("vintage", vintage))
        if vintage_lt is not None:
            filter_params.append(f'vintage < "{vintage_lt}"')
        if vintage_lte is not None:
            filter_params.append(f'vintage <= "{vintage_lte}"')
        if vintage_gt is not None:
            filter_params.append(f'vintage > "{vintage_gt}"')
        if vintage_gte is not None:
            filter_params.append(f'vintage >= "{vintage_gte}"')
        filter_params.append(list_to_filter("lastUpdated", last_updated))
        if last_updated_lt is not None:
            filter_params.append(f'lastUpdated < "{last_updated_lt}"')
        if last_updated_lte is not None:
            filter_params.append(f'lastUpdated <= "{last_updated_lte}"')
        if last_updated_gt is not None:
            filter_params.append(f'lastUpdated > "{last_updated_gt}"')
        if last_updated_gte is not None:
            filter_params.append(f'lastUpdated >= "{last_updated_gte}"')
        filter_params.append(list_to_filter("vintageRank", vintage_rank))
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params.append(list_to_filter("technologyType", technology_type))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("currencyDetailed", currency_detailed))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params.append(list_to_filter("componentMajor2", component_major2))
        filter_params.append(list_to_filter("technology", technology))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/economic-outlooks/v1/offshorewind-capex",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_energystorage_capex(
        self,
        *,
        vintage_rank: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        vintage: Optional[date] = None,
        vintage_lt: Optional[date] = None,
        vintage_lte: Optional[date] = None,
        vintage_gt: Optional[date] = None,
        vintage_gte: Optional[date] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        technology_type: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        currency_detailed: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        component_minor2: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        technology_detailed: Optional[Union[list[str], Series[str], str]] = None,
        siting: Optional[Union[list[str], Series[str], str]] = None,
        duration: Optional[Union[list[str], Series[str], str]] = None,
        duration_specific: Optional[Union[list[str], Series[str], str]] = None,
        publish: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the energystorage-capex dataset.

        Parameters
        ----------
        vintage_rank: Optional[Union[list[str], Series[str], str]]
            Rank of the data vintage, where 1 represents the latest available release.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        vintage: Optional[date]
            Publication date of the data vintage.
        vintage_lt: Optional[date]
            Filter records where vintage is less than the supplied date.
        vintage_lte: Optional[date]
            Filter records where vintage is less than or equal to the supplied date.
        vintage_gt: Optional[date]
            Filter records where vintage is greater than the supplied date.
        vintage_gte: Optional[date]
            Filter records where vintage is greater than or equal to the supplied date.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        technology_type: Optional[Union[list[str], Series[str], str]]
            Deployment scale or system classification associated with the technology.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        currency: Optional[Union[list[str], Series[str], str]]
            Currency associated with the reported data.
        currency_detailed: Optional[Union[list[str], Series[str], str]]
            Currency detailed associated with the reported data.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        component_major: Optional[Union[list[str], Series[str], str]]
            Component major associated with the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Component minor associated with the reported data.
        component_minor2: Optional[Union[list[str], Series[str], str]]
            Component minor2 associated with the reported data.
        technology: Optional[Union[list[str], Series[str], str]]
            Technology associated with the reported data.
        technology_detailed: Optional[Union[list[str], Series[str], str]]
            Technology detailed associated with the reported data.
        siting: Optional[Union[list[str], Series[str], str]]
            Siting associated with the reported data.
        duration: Optional[Union[list[str], Series[str], str]]
            Duration associated with the reported data.
        duration_specific: Optional[Union[list[str], Series[str], str]]
            Duration specific associated with the reported data.
        publish: Optional[Union[list[str], Series[str], str]]
            Publish associated with the reported data.
        filter_exp: Optional[str]
            Optional filter expression appended to filters generated from the other arguments.
        page: int
            Page number to retrieve.
        page_size: int
            Maximum number of records to retrieve per page.
        raw: bool
            When True, return the raw requests Response instead of a DataFrame.
        paginate: bool
            When True, retrieve all available pages.

        Returns
        -------
        DataFrame or Response
            A normalized pandas DataFrame, or the raw response when ``raw=True``."""
        filter_params: list[str] = []
        filter_params.append(list_to_filter("vintageRank", vintage_rank))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("vintage", vintage))
        if vintage_lt is not None:
            filter_params.append(f'vintage < "{vintage_lt}"')
        if vintage_lte is not None:
            filter_params.append(f'vintage <= "{vintage_lte}"')
        if vintage_gt is not None:
            filter_params.append(f'vintage > "{vintage_gt}"')
        if vintage_gte is not None:
            filter_params.append(f'vintage >= "{vintage_gte}"')
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params.append(list_to_filter("technologyType", technology_type))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("currencyDetailed", currency_detailed))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params.append(list_to_filter("componentMinor2", component_minor2))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyDetailed", technology_detailed))
        filter_params.append(list_to_filter("siting", siting))
        filter_params.append(list_to_filter("duration", duration))
        filter_params.append(list_to_filter("durationSpecific", duration_specific))
        filter_params.append(list_to_filter("publish", publish))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/economic-outlooks/v1/energystorage-capex",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_electrolyzer_capex(
        self,
        *,
        vintage: Optional[date] = None,
        vintage_lt: Optional[date] = None,
        vintage_lte: Optional[date] = None,
        vintage_gt: Optional[date] = None,
        vintage_gte: Optional[date] = None,
        last_updated: Optional[date] = None,
        last_updated_lt: Optional[date] = None,
        last_updated_lte: Optional[date] = None,
        last_updated_gt: Optional[date] = None,
        last_updated_gte: Optional[date] = None,
        vintage_rank: Optional[Union[list[str], Series[str], str]] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        technology_type: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        currency_detailed: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        system_size: Optional[Union[list[str], Series[str], str]] = None,
        module_size: Optional[Union[list[str], Series[str], str]] = None,
        stack_size: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the electrolyzer-capex dataset.

        Parameters
        ----------
        vintage: Optional[date]
            Publication date of the data vintage.
        vintage_lt: Optional[date]
            Filter records where vintage is less than the supplied date.
        vintage_lte: Optional[date]
            Filter records where vintage is less than or equal to the supplied date.
        vintage_gt: Optional[date]
            Filter records where vintage is greater than the supplied date.
        vintage_gte: Optional[date]
            Filter records where vintage is greater than or equal to the supplied date.
        last_updated: Optional[date]
            Date when the record was last updated.
        last_updated_lt: Optional[date]
            Filter records where last updated is less than the supplied date.
        last_updated_lte: Optional[date]
            Filter records where last updated is less than or equal to the supplied date.
        last_updated_gt: Optional[date]
            Filter records where last updated is greater than the supplied date.
        last_updated_gte: Optional[date]
            Filter records where last updated is greater than or equal to the supplied date.
        vintage_rank: Optional[Union[list[str], Series[str], str]]
            Rank of the data vintage, where 1 represents the latest available release.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        technology_type: Optional[Union[list[str], Series[str], str]]
            Deployment scale or system classification associated with the technology.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        currency: Optional[Union[list[str], Series[str], str]]
            Currency associated with the reported data.
        currency_detailed: Optional[Union[list[str], Series[str], str]]
            Currency detailed associated with the reported data.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        component_major: Optional[Union[list[str], Series[str], str]]
            Component major associated with the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Component minor associated with the reported data.
        technology: Optional[Union[list[str], Series[str], str]]
            Technology associated with the reported data.
        system_size: Optional[Union[list[str], Series[str], str]]
            System size associated with the reported data.
        module_size: Optional[Union[list[str], Series[str], str]]
            Module size associated with the reported data.
        stack_size: Optional[Union[list[str], Series[str], str]]
            Stack size associated with the reported data.
        filter_exp: Optional[str]
            Optional filter expression appended to filters generated from the other arguments.
        page: int
            Page number to retrieve.
        page_size: int
            Maximum number of records to retrieve per page.
        raw: bool
            When True, return the raw requests Response instead of a DataFrame.
        paginate: bool
            When True, retrieve all available pages.

        Returns
        -------
        DataFrame or Response
            A normalized pandas DataFrame, or the raw response when ``raw=True``."""
        filter_params: list[str] = []
        filter_params.append(list_to_filter("vintage", vintage))
        if vintage_lt is not None:
            filter_params.append(f'vintage < "{vintage_lt}"')
        if vintage_lte is not None:
            filter_params.append(f'vintage <= "{vintage_lte}"')
        if vintage_gt is not None:
            filter_params.append(f'vintage > "{vintage_gt}"')
        if vintage_gte is not None:
            filter_params.append(f'vintage >= "{vintage_gte}"')
        filter_params.append(list_to_filter("lastUpdated", last_updated))
        if last_updated_lt is not None:
            filter_params.append(f'lastUpdated < "{last_updated_lt}"')
        if last_updated_lte is not None:
            filter_params.append(f'lastUpdated <= "{last_updated_lte}"')
        if last_updated_gt is not None:
            filter_params.append(f'lastUpdated > "{last_updated_gt}"')
        if last_updated_gte is not None:
            filter_params.append(f'lastUpdated >= "{last_updated_gte}"')
        filter_params.append(list_to_filter("vintageRank", vintage_rank))
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params.append(list_to_filter("technologyType", technology_type))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("currencyDetailed", currency_detailed))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("systemSize", system_size))
        filter_params.append(list_to_filter("moduleSize", module_size))
        filter_params.append(list_to_filter("stackSize", stack_size))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/economic-outlooks/v1/electrolyzer-capex",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_battery_cost(
        self,
        *,
        vintage: Optional[date] = None,
        vintage_lt: Optional[date] = None,
        vintage_lte: Optional[date] = None,
        vintage_gt: Optional[date] = None,
        vintage_gte: Optional[date] = None,
        last_updated: Optional[date] = None,
        last_updated_lt: Optional[date] = None,
        last_updated_lte: Optional[date] = None,
        last_updated_gt: Optional[date] = None,
        last_updated_gte: Optional[date] = None,
        vintage_rank: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        production_geography: Optional[Union[list[str], Series[str], str]] = None,
        sales_geography: Optional[Union[list[str], Series[str], str]] = None,
        component1: Optional[Union[list[str], Series[str], str]] = None,
        component2: Optional[Union[list[str], Series[str], str]] = None,
        component3: Optional[Union[list[str], Series[str], str]] = None,
        component4: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        currency_detailed: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the battery-cost dataset.

        Parameters
        ----------
        vintage: Optional[date]
            Publication date of the data vintage.
        vintage_lt: Optional[date]
            Filter records where vintage is less than the supplied date.
        vintage_lte: Optional[date]
            Filter records where vintage is less than or equal to the supplied date.
        vintage_gt: Optional[date]
            Filter records where vintage is greater than the supplied date.
        vintage_gte: Optional[date]
            Filter records where vintage is greater than or equal to the supplied date.
        last_updated: Optional[date]
            Date when the record was last updated.
        last_updated_lt: Optional[date]
            Filter records where last updated is less than the supplied date.
        last_updated_lte: Optional[date]
            Filter records where last updated is less than or equal to the supplied date.
        last_updated_gt: Optional[date]
            Filter records where last updated is greater than the supplied date.
        last_updated_gte: Optional[date]
            Filter records where last updated is greater than or equal to the supplied date.
        vintage_rank: Optional[Union[list[str], Series[str], str]]
            Rank of the data vintage, where 1 represents the latest available release.
        technology: Optional[Union[list[str], Series[str], str]]
            Technology associated with the reported data.
        production_geography: Optional[Union[list[str], Series[str], str]]
            Production geography associated with the reported data.
        sales_geography: Optional[Union[list[str], Series[str], str]]
            Sales geography associated with the reported data.
        component1: Optional[Union[list[str], Series[str], str]]
            Component1 associated with the reported data.
        component2: Optional[Union[list[str], Series[str], str]]
            Component2 associated with the reported data.
        component3: Optional[Union[list[str], Series[str], str]]
            Component3 associated with the reported data.
        component4: Optional[Union[list[str], Series[str], str]]
            Component4 associated with the reported data.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        currency: Optional[Union[list[str], Series[str], str]]
            Currency associated with the reported data.
        currency_detailed: Optional[Union[list[str], Series[str], str]]
            Currency detailed associated with the reported data.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        filter_exp: Optional[str]
            Optional filter expression appended to filters generated from the other arguments.
        page: int
            Page number to retrieve.
        page_size: int
            Maximum number of records to retrieve per page.
        raw: bool
            When True, return the raw requests Response instead of a DataFrame.
        paginate: bool
            When True, retrieve all available pages.

        Returns
        -------
        DataFrame or Response
            A normalized pandas DataFrame, or the raw response when ``raw=True``."""
        filter_params: list[str] = []
        filter_params.append(list_to_filter("vintage", vintage))
        if vintage_lt is not None:
            filter_params.append(f'vintage < "{vintage_lt}"')
        if vintage_lte is not None:
            filter_params.append(f'vintage <= "{vintage_lte}"')
        if vintage_gt is not None:
            filter_params.append(f'vintage > "{vintage_gt}"')
        if vintage_gte is not None:
            filter_params.append(f'vintage >= "{vintage_gte}"')
        filter_params.append(list_to_filter("lastUpdated", last_updated))
        if last_updated_lt is not None:
            filter_params.append(f'lastUpdated < "{last_updated_lt}"')
        if last_updated_lte is not None:
            filter_params.append(f'lastUpdated <= "{last_updated_lte}"')
        if last_updated_gt is not None:
            filter_params.append(f'lastUpdated > "{last_updated_gt}"')
        if last_updated_gte is not None:
            filter_params.append(f'lastUpdated >= "{last_updated_gte}"')
        filter_params.append(list_to_filter("vintageRank", vintage_rank))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(
            list_to_filter("productionGeography", production_geography)
        )
        filter_params.append(list_to_filter("salesGeography", sales_geography))
        filter_params.append(list_to_filter("component1", component1))
        filter_params.append(list_to_filter("component2", component2))
        filter_params.append(list_to_filter("component3", component3))
        filter_params.append(list_to_filter("component4", component4))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("currencyDetailed", currency_detailed))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/economic-outlooks/v1/battery-cost",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_levelized_cost(
        self,
        *,
        vintage_rank: Optional[Union[list[str], Series[str], str]] = None,
        vintage: Optional[date] = None,
        vintage_lt: Optional[date] = None,
        vintage_lte: Optional[date] = None,
        vintage_gt: Optional[date] = None,
        vintage_gte: Optional[date] = None,
        last_updated: Optional[date] = None,
        last_updated_lt: Optional[date] = None,
        last_updated_lte: Optional[date] = None,
        last_updated_gt: Optional[date] = None,
        last_updated_gte: Optional[date] = None,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        hybrid_technology: Optional[Union[list[str], Series[str], str]] = None,
        storage_duration_hours: Optional[Union[list[str], Series[str], str]] = None,
        siting_major: Optional[Union[list[str], Series[str], str]] = None,
        siting_minor: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        concept_role: Optional[Union[list[str], Series[str], str]] = None,
        concept_description: Optional[Union[list[str], Series[str], str]] = None,
        levelized_metric: Optional[Union[list[str], Series[str], str]] = None,
        assumptions: Optional[Union[list[str], Series[str], str]] = None,
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        currency_detailed: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the levelized-cost dataset.

        Parameters
        ----------
        vintage_rank: Optional[Union[list[str], Series[str], str]]
            Rank of the data vintage, where 1 represents the latest available release.
        vintage: Optional[date]
            Publication date of the data vintage.
        vintage_lt: Optional[date]
            Filter records where vintage is less than the supplied date.
        vintage_lte: Optional[date]
            Filter records where vintage is less than or equal to the supplied date.
        vintage_gt: Optional[date]
            Filter records where vintage is greater than the supplied date.
        vintage_gte: Optional[date]
            Filter records where vintage is greater than or equal to the supplied date.
        last_updated: Optional[date]
            Date when the record was last updated.
        last_updated_lt: Optional[date]
            Filter records where last updated is less than the supplied date.
        last_updated_lte: Optional[date]
            Filter records where last updated is less than or equal to the supplied date.
        last_updated_gt: Optional[date]
            Filter records where last updated is greater than the supplied date.
        last_updated_gte: Optional[date]
            Filter records where last updated is greater than or equal to the supplied date.
        scenario: Optional[Union[list[str], Series[str], str]]
            Scenario associated with the reported data.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        hybrid_technology: Optional[Union[list[str], Series[str], str]]
            Hybrid technology associated with the reported data.
        storage_duration_hours: Optional[Union[list[str], Series[str], str]]
            Storage duration hours associated with the reported data.
        siting_major: Optional[Union[list[str], Series[str], str]]
            Siting major associated with the reported data.
        siting_minor: Optional[Union[list[str], Series[str], str]]
            Siting minor associated with the reported data.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        concept_role: Optional[Union[list[str], Series[str], str]]
            Concept role associated with the reported data.
        concept_description: Optional[Union[list[str], Series[str], str]]
            Concept description associated with the reported data.
        levelized_metric: Optional[Union[list[str], Series[str], str]]
            Levelized metric associated with the reported data.
        assumptions: Optional[Union[list[str], Series[str], str]]
            Assumptions associated with the reported data.
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported value is measured.
        currency: Optional[Union[list[str], Series[str], str]]
            Currency associated with the reported data.
        currency_detailed: Optional[Union[list[str], Series[str], str]]
            Currency detailed associated with the reported data.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        filter_exp: Optional[str]
            Optional filter expression appended to filters generated from the other arguments.
        page: int
            Page number to retrieve.
        page_size: int
            Maximum number of records to retrieve per page.
        raw: bool
            When True, return the raw requests Response instead of a DataFrame.
        paginate: bool
            When True, retrieve all available pages.

        Returns
        -------
        DataFrame or Response
            A normalized pandas DataFrame, or the raw response when ``raw=True``."""
        filter_params: list[str] = []
        filter_params.append(list_to_filter("vintageRank", vintage_rank))
        filter_params.append(list_to_filter("vintage", vintage))
        if vintage_lt is not None:
            filter_params.append(f'vintage < "{vintage_lt}"')
        if vintage_lte is not None:
            filter_params.append(f'vintage <= "{vintage_lte}"')
        if vintage_gt is not None:
            filter_params.append(f'vintage > "{vintage_gt}"')
        if vintage_gte is not None:
            filter_params.append(f'vintage >= "{vintage_gte}"')
        filter_params.append(list_to_filter("lastUpdated", last_updated))
        if last_updated_lt is not None:
            filter_params.append(f'lastUpdated < "{last_updated_lt}"')
        if last_updated_lte is not None:
            filter_params.append(f'lastUpdated <= "{last_updated_lte}"')
        if last_updated_gt is not None:
            filter_params.append(f'lastUpdated > "{last_updated_gt}"')
        if last_updated_gte is not None:
            filter_params.append(f'lastUpdated >= "{last_updated_gte}"')
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("hybridTechnology", hybrid_technology))
        filter_params.append(
            list_to_filter("storageDurationHours", storage_duration_hours)
        )
        filter_params.append(list_to_filter("sitingMajor", siting_major))
        filter_params.append(list_to_filter("sitingMinor", siting_minor))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("conceptRole", concept_role))
        filter_params.append(list_to_filter("conceptDescription", concept_description))
        filter_params.append(list_to_filter("levelizedMetric", levelized_metric))
        filter_params.append(list_to_filter("assumptions", assumptions))
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("currencyDetailed", currency_detailed))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/economic-outlooks/v1/levelized-cost",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    @staticmethod
    def _convert_unique_values_to_df(resp: Response) -> DataFrame:
        return CetEconomicOutlooks._normalize(resp, "aggResultValue")

    @staticmethod
    def _convert_to_df(resp: Response) -> DataFrame:
        return CetEconomicOutlooks._normalize(resp, "results")

    @staticmethod
    def _normalize(resp: Response, key: str) -> DataFrame:
        df = pd.json_normalize(resp.json()[key])
        for column in ["vintageAdditions", "vintageCapex", "vintage", "lastUpdated"]:
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column], utc=True, format="ISO8601", errors="coerce"
                )
        return df
