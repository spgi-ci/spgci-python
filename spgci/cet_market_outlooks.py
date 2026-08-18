from __future__ import annotations
from datetime import date
from typing import Literal, Optional, Union
import pandas as pd
from pandas import DataFrame, Series
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter

MarketOutlooksDataset = Literal[
    "all-installs",
    "energystorage-installs-applications",
    "wind-installs",
    "solarpv-installs-quarterly",
    "solarpv-installs-annual",
    "hydrogen-supply-demand",
    "energystorage-installs",
    "electrolyzer-installs",
    "ccus-installs-technology",
    "ccus-installs-co2src",
    "battery-demand",
]


class CetMarketOutlooks:
    """Client for CET market outlook datasets."""

    _dataset_to_path = {
        "all-installs": "analytics/cet/market-outlooks/v1/all-installs",
        "energystorage-installs-applications": "analytics/cet/market-outlooks/v1/energystorage-installs-applications",
        "wind-installs": "analytics/cet/market-outlooks/v1/wind-installs",
        "solarpv-installs-quarterly": "analytics/cet/market-outlooks/v1/solarpv-installs-quarterly",
        "solarpv-installs-annual": "analytics/cet/market-outlooks/v1/solarpv-installs-annual",
        "hydrogen-supply-demand": "analytics/cet/market-outlooks/v1/hydrogen-supply-demand",
        "energystorage-installs": "analytics/cet/market-outlooks/v1/energystorage-installs",
        "electrolyzer-installs": "analytics/cet/market-outlooks/v1/electrolyzer-installs",
        "ccus-installs-technology": "analytics/cet/market-outlooks/v1/ccus-installs-technology",
        "ccus-installs-co2src": "analytics/cet/market-outlooks/v1/ccus-installs-co2src",
        "battery-demand": "analytics/cet/market-outlooks/v1/battery-demand",
    }

    def get_unique_values(
        self,
        dataset: MarketOutlooksDataset,
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

    def get_all_installs(
        self,
        *,
        vintage: Optional[date] = None,
        vintage_lt: Optional[date] = None,
        vintage_lte: Optional[date] = None,
        vintage_gt: Optional[date] = None,
        vintage_gte: Optional[date] = None,
        technology_main_major: Optional[Union[list[str], Series[str], str]] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        technology_type: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        metric: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        siting_major: Optional[Union[list[str], Series[str], str]] = None,
        siting_minor: Optional[Union[list[str], Series[str], str]] = None,
        siting: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the all-installs dataset.

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
        technology_main_major: Optional[Union[list[str], Series[str], str]]
            Top-level technology category associated with the reported data.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        technology_type: Optional[Union[list[str], Series[str], str]]
            Deployment scale or system classification associated with the technology.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        metric: Optional[Union[list[str], Series[str], str]]
            Unit or denomination in which the reported value is measured.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        siting_major: Optional[Union[list[str], Series[str], str]]
            Siting major associated with the reported data.
        siting_minor: Optional[Union[list[str], Series[str], str]]
            Siting minor associated with the reported data.
        siting: Optional[Union[list[str], Series[str], str]]
            Siting associated with the reported data.
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
        filter_params.append(
            list_to_filter("technologyMainMajor", technology_main_major)
        )
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params.append(list_to_filter("technologyType", technology_type))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("metric", metric))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("sitingMajor", siting_major))
        filter_params.append(list_to_filter("sitingMinor", siting_minor))
        filter_params.append(list_to_filter("siting", siting))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/market-outlooks/v1/all-installs",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_energystorage_installs_applications(
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
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        siting_major: Optional[Union[list[str], Series[str], str]] = None,
        siting_minor: Optional[Union[list[str], Series[str], str]] = None,
        duration: Optional[Union[list[str], Series[str], str]] = None,
        application: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the energystorage-installs-applications dataset.

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
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported value is measured.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        siting_major: Optional[Union[list[str], Series[str], str]]
            Siting major associated with the reported data.
        siting_minor: Optional[Union[list[str], Series[str], str]]
            Siting minor associated with the reported data.
        duration: Optional[Union[list[str], Series[str], str]]
            Duration associated with the reported data.
        application: Optional[Union[list[str], Series[str], str]]
            Application associated with the reported data.
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
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("sitingMajor", siting_major))
        filter_params.append(list_to_filter("sitingMinor", siting_minor))
        filter_params.append(list_to_filter("duration", duration))
        filter_params.append(list_to_filter("application", application))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/market-outlooks/v1/energystorage-installs-applications",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_wind_installs(
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
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        foundation_type: Optional[Union[list[str], Series[str], str]] = None,
        siting_major: Optional[Union[list[str], Series[str], str]] = None,
        siting_minor: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the wind-installs dataset.

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
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported value is measured.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        foundation_type: Optional[Union[list[str], Series[str], str]]
            Foundation type associated with the reported data.
        siting_major: Optional[Union[list[str], Series[str], str]]
            Siting major associated with the reported data.
        siting_minor: Optional[Union[list[str], Series[str], str]]
            Siting minor associated with the reported data.
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
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("foundationType", foundation_type))
        filter_params.append(list_to_filter("sitingMajor", siting_major))
        filter_params.append(list_to_filter("sitingMinor", siting_minor))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/market-outlooks/v1/wind-installs",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_solarpv_installs_quarterly(
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
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        gc_og: Optional[Union[list[str], Series[str], str]] = None,
        system_type_major: Optional[Union[list[str], Series[str], str]] = None,
        system_type_minor: Optional[Union[list[str], Series[str], str]] = None,
        installation_type: Optional[Union[list[str], Series[str], str]] = None,
        quarter: Optional[Union[list[str], Series[str], str]] = None,
        quarter_year: Optional[Union[list[str], Series[str], str]] = None,
        size_major: Optional[Union[list[str], Series[str], str]] = None,
        size_minor: Optional[Union[list[str], Series[str], str]] = None,
        siting_major: Optional[Union[list[str], Series[str], str]] = None,
        siting_minor: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the solarpv-installs-quarterly dataset.

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
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported value is measured.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        gc_og: Optional[Union[list[str], Series[str], str]]
            Gc og associated with the reported data.
        system_type_major: Optional[Union[list[str], Series[str], str]]
            System type major associated with the reported data.
        system_type_minor: Optional[Union[list[str], Series[str], str]]
            System type minor associated with the reported data.
        installation_type: Optional[Union[list[str], Series[str], str]]
            Installation type associated with the reported data.
        quarter: Optional[Union[list[str], Series[str], str]]
            Quarter associated with the reported data.
        quarter_year: Optional[Union[list[str], Series[str], str]]
            Quarter year associated with the reported data.
        size_major: Optional[Union[list[str], Series[str], str]]
            Size major associated with the reported data.
        size_minor: Optional[Union[list[str], Series[str], str]]
            Size minor associated with the reported data.
        siting_major: Optional[Union[list[str], Series[str], str]]
            Siting major associated with the reported data.
        siting_minor: Optional[Union[list[str], Series[str], str]]
            Siting minor associated with the reported data.
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
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("gcOg", gc_og))
        filter_params.append(list_to_filter("systemTypeMajor", system_type_major))
        filter_params.append(list_to_filter("systemTypeMinor", system_type_minor))
        filter_params.append(list_to_filter("installationType", installation_type))
        filter_params.append(list_to_filter("quarter", quarter))
        filter_params.append(list_to_filter("quarterYear", quarter_year))
        filter_params.append(list_to_filter("sizeMajor", size_major))
        filter_params.append(list_to_filter("sizeMinor", size_minor))
        filter_params.append(list_to_filter("sitingMajor", siting_major))
        filter_params.append(list_to_filter("sitingMinor", siting_minor))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/market-outlooks/v1/solarpv-installs-quarterly",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_solarpv_installs_annual(
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
        technology_type: Optional[Union[list[str], Series[str], str]] = None,
        technology_main: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        gc_og: Optional[Union[list[str], Series[str], str]] = None,
        system_type_major: Optional[Union[list[str], Series[str], str]] = None,
        system_type_minor: Optional[Union[list[str], Series[str], str]] = None,
        installation_type: Optional[Union[list[str], Series[str], str]] = None,
        size_minor: Optional[Union[list[str], Series[str], str]] = None,
        size_major: Optional[Union[list[str], Series[str], str]] = None,
        siting_major: Optional[Union[list[str], Series[str], str]] = None,
        siting_minor: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the solarpv-installs-annual dataset.

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
        technology_type: Optional[Union[list[str], Series[str], str]]
            Deployment scale or system classification associated with the technology.
        technology_main: Optional[Union[list[str], Series[str], str]]
            Technology category associated with the reported data.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported value is measured.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        gc_og: Optional[Union[list[str], Series[str], str]]
            Gc og associated with the reported data.
        system_type_major: Optional[Union[list[str], Series[str], str]]
            System type major associated with the reported data.
        system_type_minor: Optional[Union[list[str], Series[str], str]]
            System type minor associated with the reported data.
        installation_type: Optional[Union[list[str], Series[str], str]]
            Installation type associated with the reported data.
        size_minor: Optional[Union[list[str], Series[str], str]]
            Size minor associated with the reported data.
        size_major: Optional[Union[list[str], Series[str], str]]
            Size major associated with the reported data.
        siting_major: Optional[Union[list[str], Series[str], str]]
            Siting major associated with the reported data.
        siting_minor: Optional[Union[list[str], Series[str], str]]
            Siting minor associated with the reported data.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
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
        filter_params.append(list_to_filter("technologyType", technology_type))
        filter_params.append(list_to_filter("technologyMain", technology_main))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("gcOg", gc_og))
        filter_params.append(list_to_filter("systemTypeMajor", system_type_major))
        filter_params.append(list_to_filter("systemTypeMinor", system_type_minor))
        filter_params.append(list_to_filter("installationType", installation_type))
        filter_params.append(list_to_filter("sizeMinor", size_minor))
        filter_params.append(list_to_filter("sizeMajor", size_major))
        filter_params.append(list_to_filter("sitingMajor", siting_major))
        filter_params.append(list_to_filter("sitingMinor", siting_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("value", value))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/market-outlooks/v1/solarpv-installs-annual",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_hydrogen_supply_demand(
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
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        parameter: Optional[Union[list[str], Series[str], str]] = None,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the hydrogen-supply-demand dataset.

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
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported value is measured.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        parameter: Optional[Union[list[str], Series[str], str]]
            Parameter associated with the reported data.
        scenario: Optional[Union[list[str], Series[str], str]]
            Scenario associated with the reported data.
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
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("parameter", parameter))
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/market-outlooks/v1/hydrogen-supply-demand",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_energystorage_installs(
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
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        siting_major: Optional[Union[list[str], Series[str], str]] = None,
        siting_minor: Optional[Union[list[str], Series[str], str]] = None,
        siting: Optional[Union[list[str], Series[str], str]] = None,
        duration: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the energystorage-installs dataset.

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
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported value is measured.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        siting_major: Optional[Union[list[str], Series[str], str]]
            Siting major associated with the reported data.
        siting_minor: Optional[Union[list[str], Series[str], str]]
            Siting minor associated with the reported data.
        siting: Optional[Union[list[str], Series[str], str]]
            Siting associated with the reported data.
        duration: Optional[Union[list[str], Series[str], str]]
            Duration associated with the reported data.
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
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("sitingMajor", siting_major))
        filter_params.append(list_to_filter("sitingMinor", siting_minor))
        filter_params.append(list_to_filter("siting", siting))
        filter_params.append(list_to_filter("duration", duration))
        filter_params.append(list_to_filter("technology", technology))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/market-outlooks/v1/energystorage-installs",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_electrolyzer_installs(
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
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        input_source_major: Optional[Union[list[str], Series[str], str]] = None,
        input_source_minor: Optional[Union[list[str], Series[str], str]] = None,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        siting_major: Optional[Union[list[str], Series[str], str]] = None,
        siting_minor: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the electrolyzer-installs dataset.

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
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported value is measured.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        input_source_major: Optional[Union[list[str], Series[str], str]]
            Input source major associated with the reported data.
        input_source_minor: Optional[Union[list[str], Series[str], str]]
            Input source minor associated with the reported data.
        scenario: Optional[Union[list[str], Series[str], str]]
            Scenario associated with the reported data.
        siting_major: Optional[Union[list[str], Series[str], str]]
            Siting major associated with the reported data.
        siting_minor: Optional[Union[list[str], Series[str], str]]
            Siting minor associated with the reported data.
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
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("inputSourceMajor", input_source_major))
        filter_params.append(list_to_filter("inputSourceMinor", input_source_minor))
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("sitingMajor", siting_major))
        filter_params.append(list_to_filter("sitingMinor", siting_minor))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/market-outlooks/v1/electrolyzer-installs",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_ccus_installs_technology(
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
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        industry: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the ccus-installs-technology dataset.

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
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported geography.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported value is measured.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        industry: Optional[Union[list[str], Series[str], str]]
            Industry associated with the reported data.
        technology: Optional[Union[list[str], Series[str], str]]
            Technology associated with the reported data.
        scenario: Optional[Union[list[str], Series[str], str]]
            Scenario associated with the reported data.
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
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("industry", industry))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/market-outlooks/v1/ccus-installs-technology",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_ccus_installs_co2src(
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
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        industry: Optional[Union[list[str], Series[str], str]] = None,
        co2_source: Optional[Union[list[str], Series[str], str]] = None,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the ccus-installs-co2src dataset.

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
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported value is measured.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            Reported numeric value.
        industry: Optional[Union[list[str], Series[str], str]]
            Industry associated with the reported data.
        co2_source: Optional[Union[list[str], Series[str], str]]
            Co2 source associated with the reported data.
        scenario: Optional[Union[list[str], Series[str], str]]
            Scenario associated with the reported data.
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
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("industry", industry))
        filter_params.append(list_to_filter("co2Source", co2_source))
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/market-outlooks/v1/ccus-installs-co2src",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_battery_demand(
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
        geography: Optional[Union[list[str], Series[str], str]] = None,
        application_major: Optional[Union[list[str], Series[str], str]] = None,
        application_minor: Optional[Union[list[str], Series[str], str]] = None,
        cathode_major: Optional[Union[list[str], Series[str], str]] = None,
        cathode_minor: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the battery-demand dataset.

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
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        application_major: Optional[Union[list[str], Series[str], str]]
            Application major associated with the reported data.
        application_minor: Optional[Union[list[str], Series[str], str]]
            Application minor associated with the reported data.
        cathode_major: Optional[Union[list[str], Series[str], str]]
            Cathode major associated with the reported data.
        cathode_minor: Optional[Union[list[str], Series[str], str]]
            Cathode minor associated with the reported data.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported value.
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported value is measured.
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
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("applicationMajor", application_major))
        filter_params.append(list_to_filter("applicationMinor", application_minor))
        filter_params.append(list_to_filter("cathodeMajor", cathode_major))
        filter_params.append(list_to_filter("cathodeMinor", cathode_minor))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/market-outlooks/v1/battery-demand",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    @staticmethod
    def _convert_unique_values_to_df(resp: Response) -> DataFrame:
        return CetMarketOutlooks._normalize(resp, "aggResultValue")

    @staticmethod
    def _convert_to_df(resp: Response) -> DataFrame:
        return CetMarketOutlooks._normalize(resp, "results")

    @staticmethod
    def _normalize(resp: Response, key: str) -> DataFrame:
        df = pd.json_normalize(resp.json()[key])
        for column in ["vintage", "lastUpdated"]:
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column], utc=True, format="ISO8601", errors="coerce"
                )
        return df
