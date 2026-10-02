from __future__ import annotations
from datetime import date
from typing import Literal, Optional, Union
import pandas as pd
from pandas import DataFrame, Series
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter

PoliciesDataset = Literal[
    "announcements",
    "targets",
    "policies",
]


class CetPolicies:
    """Client for CET policy datasets."""

    _dataset_to_path = {
        "announcements": "analytics/cet/policy/v1/announcements",
        "targets": "analytics/cet/policy/v1/targets",
        "policies": "analytics/cet/policy/v1/policies",
    }

    def get_unique_values(
        self,
        dataset: PoliciesDataset,
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

    def get_announcements(
        self,
        *,
        headline: Optional[Union[list[str], Series[str], str]] = None,
        announcement_id: Optional[Union[list[str], Series[str], str]] = None,
        announcement_date: Optional[date] = None,
        announcement_date_lt: Optional[date] = None,
        announcement_date_lte: Optional[date] = None,
        announcement_date_gt: Optional[date] = None,
        announcement_date_gte: Optional[date] = None,
        status: Optional[Union[list[str], Series[str], str]] = None,
        announcement_type: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        geography_display: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        state_province: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        policy_name_full: Optional[Union[list[str], Series[str], str]] = None,
        policy_id: Optional[Union[list[str], Series[str], str]] = None,
        details_insight: Optional[Union[list[str], Series[str], str]] = None,
        external_url: Optional[Union[list[str], Series[str], str]] = None,
        connect_url: Optional[Union[list[str], Series[str], str]] = None,
        core_url: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the announcements dataset.

        Parameters
        ----------
        headline: Optional[Union[list[str], Series[str], str]]
            A brief description of the announcement.
        announcement_id: Optional[Union[list[str], Series[str], str]]
            Unique identifier for each announcement.
        announcement_date: Optional[date]
            Date that the announcement was made.
        announcement_date_lt: Optional[date]
            Filter records where announcement date is less than the supplied date.
        announcement_date_lte: Optional[date]
            Filter records where announcement date is less than or equal to the supplied date.
        announcement_date_gt: Optional[date]
            Filter records where announcement date is greater than the supplied date.
        announcement_date_gte: Optional[date]
            Filter records where announcement date is greater than or equal to the supplied date.
        status: Optional[Union[list[str], Series[str], str]]
            Is the announcement confirmed or proposed.
        announcement_type: Optional[Union[list[str], Series[str], str]]
            Categorization of the announcement.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or sovereign territory to which the announcement applies.
        geography_display: Optional[Union[list[str], Series[str], str]]
            If appropriate, a more simplified geography for display purposes.
        region_major: Optional[Union[list[str], Series[str], str]]
            High-level macro-region grouping of the reported geography.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Subregional grouping of the reported geography.
        state_province: Optional[Union[list[str], Series[str], str]]
            State or province that is impacted by the announcement.
        technology: Optional[Union[list[str], Series[str], str]]
            Technologies that are impacted by the announcement.
        policy_name_full: Optional[Union[list[str], Series[str], str]]
            Name of any policies that are associated with the announcement.
        policy_id: Optional[Union[list[str], Series[str], str]]
            Unique identifier for any policy associated with the announcement.
        details_insight: Optional[Union[list[str], Series[str], str]]
            A description of the details of the announcement.
        external_url: Optional[Union[list[str], Series[str], str]]
            Link to any relevant publicly available source.
        connect_url: Optional[Union[list[str], Series[str], str]]
            Link to any relevant content on Connect.
        core_url: Optional[Union[list[str], Series[str], str]]
            Link to any relevant content on CORE.
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
        filter_params.append(list_to_filter("headline", headline))
        filter_params.append(list_to_filter("announcementId", announcement_id))
        filter_params.append(list_to_filter("announcementDate", announcement_date))
        if announcement_date_lt is not None:
            filter_params.append(f'announcementDate < "{announcement_date_lt}"')
        if announcement_date_lte is not None:
            filter_params.append(f'announcementDate <= "{announcement_date_lte}"')
        if announcement_date_gt is not None:
            filter_params.append(f'announcementDate > "{announcement_date_gt}"')
        if announcement_date_gte is not None:
            filter_params.append(f'announcementDate >= "{announcement_date_gte}"')
        filter_params.append(list_to_filter("status", status))
        filter_params.append(list_to_filter("announcementType", announcement_type))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("geographyDisplay", geography_display))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("policyNameFull", policy_name_full))
        filter_params.append(list_to_filter("policyId", policy_id))
        filter_params.append(list_to_filter("detailsInsight", details_insight))
        filter_params.append(list_to_filter("externalUrl", external_url))
        filter_params.append(list_to_filter("connectUrl", connect_url))
        filter_params.append(list_to_filter("coreUrl", core_url))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/policy/v1/announcements",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_targets(
        self,
        *,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        target_name_full: Optional[Union[list[str], Series[str], str]] = None,
        target_id: Optional[Union[list[str], Series[str], str]] = None,
        target_type: Optional[Union[list[str], Series[str], str]] = None,
        status: Optional[Union[list[str], Series[str], str]] = None,
        target_metric: Optional[Union[list[str], Series[str], str]] = None,
        announced_date: Optional[date] = None,
        announced_date_lt: Optional[date] = None,
        announced_date_lte: Optional[date] = None,
        announced_date_gt: Optional[date] = None,
        announced_date_gte: Optional[date] = None,
        final_target_date: Optional[date] = None,
        final_target_date_lt: Optional[date] = None,
        final_target_date_lte: Optional[date] = None,
        final_target_date_gt: Optional[date] = None,
        final_target_date_gte: Optional[date] = None,
        final_target_year: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        technology_major: Optional[Union[list[str], Series[str], str]] = None,
        state_province: Optional[Union[list[str], Series[str], str]] = None,
        geography_display: Optional[Union[list[str], Series[str], str]] = None,
        policy_name_full: Optional[Union[list[str], Series[str], str]] = None,
        policy_id: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the targets dataset.

        Parameters
        ----------
        geography: Optional[Union[list[str], Series[str], str]]
            Country or sovereign territory to which the target applies.
        target_name_full: Optional[Union[list[str], Series[str], str]]
            Full name of the target.
        target_id: Optional[Union[list[str], Series[str], str]]
            Unique ID for each target.
        target_type: Optional[Union[list[str], Series[str], str]]
            Categorization of the target.
        status: Optional[Union[list[str], Series[str], str]]
            Is the target confirmed or proposed.
        target_metric: Optional[Union[list[str], Series[str], str]]
            The unit used to measure the target.
        announced_date: Optional[date]
            Date that the target was announced.
        announced_date_lt: Optional[date]
            Filter records where announced date is less than the supplied date.
        announced_date_lte: Optional[date]
            Filter records where announced date is less than or equal to the supplied date.
        announced_date_gt: Optional[date]
            Filter records where announced date is greater than the supplied date.
        announced_date_gte: Optional[date]
            Filter records where announced date is greater than or equal to the supplied date.
        final_target_date: Optional[date]
            Major categorization of the target's final date.
        final_target_date_lt: Optional[date]
            Filter records where final target date is less than the supplied date.
        final_target_date_lte: Optional[date]
            Filter records where final target date is less than or equal to the supplied date.
        final_target_date_gt: Optional[date]
            Filter records where final target date is greater than the supplied date.
        final_target_date_gte: Optional[date]
            Filter records where final target date is greater than or equal to the supplied date.
        final_target_year: Optional[Union[list[str], Series[str], str]]
            Year by which the target is to be achieved.
        technology: Optional[Union[list[str], Series[str], str]]
            Technologies that include the target.
        technology_major: Optional[Union[list[str], Series[str], str]]
            Country or sovereign territory associated with the technology grouping.
        state_province: Optional[Union[list[str], Series[str], str]]
            State or province that is impacted by the target.
        geography_display: Optional[Union[list[str], Series[str], str]]
            If appropriate, a more simplified geography for display purposes.
        policy_name_full: Optional[Union[list[str], Series[str], str]]
            Name of any policy which established the target.
        policy_id: Optional[Union[list[str], Series[str], str]]
            Unique identifier for any policy associated with the target.
        year: Optional[Union[list[str], Series[str], str]]
            The year by which the target applies.
        value: Optional[Union[list[str], Series[str], str]]
            The value of the target.
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
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("targetNameFull", target_name_full))
        filter_params.append(list_to_filter("targetId", target_id))
        filter_params.append(list_to_filter("targetType", target_type))
        filter_params.append(list_to_filter("status", status))
        filter_params.append(list_to_filter("targetMetric", target_metric))
        filter_params.append(list_to_filter("announcedDate", announced_date))
        if announced_date_lt is not None:
            filter_params.append(f'announcedDate < "{announced_date_lt}"')
        if announced_date_lte is not None:
            filter_params.append(f'announcedDate <= "{announced_date_lte}"')
        if announced_date_gt is not None:
            filter_params.append(f'announcedDate > "{announced_date_gt}"')
        if announced_date_gte is not None:
            filter_params.append(f'announcedDate >= "{announced_date_gte}"')
        filter_params.append(list_to_filter("finalTargetDate", final_target_date))
        if final_target_date_lt is not None:
            filter_params.append(f'finalTargetDate < "{final_target_date_lt}"')
        if final_target_date_lte is not None:
            filter_params.append(f'finalTargetDate <= "{final_target_date_lte}"')
        if final_target_date_gt is not None:
            filter_params.append(f'finalTargetDate > "{final_target_date_gt}"')
        if final_target_date_gte is not None:
            filter_params.append(f'finalTargetDate >= "{final_target_date_gte}"')
        filter_params.append(list_to_filter("finalTargetYear", final_target_year))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("geographyDisplay", geography_display))
        filter_params.append(list_to_filter("policyNameFull", policy_name_full))
        filter_params.append(list_to_filter("policyId", policy_id))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/policy/v1/targets",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_policies(
        self,
        *,
        policy_name_full: Optional[Union[list[str], Series[str], str]] = None,
        policy_id: Optional[Union[list[str], Series[str], str]] = None,
        policy_name: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        state_province: Optional[Union[list[str], Series[str], str]] = None,
        geography_display: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        policy_start_date: Optional[date] = None,
        policy_start_date_lt: Optional[date] = None,
        policy_start_date_lte: Optional[date] = None,
        policy_start_date_gt: Optional[date] = None,
        policy_start_date_gte: Optional[date] = None,
        policy_end_date: Optional[date] = None,
        policy_end_date_lt: Optional[date] = None,
        policy_end_date_lte: Optional[date] = None,
        policy_end_date_gt: Optional[date] = None,
        policy_end_date_gte: Optional[date] = None,
        target_name_full: Optional[Union[list[str], Series[str], str]] = None,
        target_id: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the policies dataset.

        Parameters
        ----------
        policy_name_full: Optional[Union[list[str], Series[str], str]]
            Full name of the policy.
        policy_id: Optional[Union[list[str], Series[str], str]]
            Unique identifier for each policy.
        policy_name: Optional[Union[list[str], Series[str], str]]
            Name of the policy.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or sovereign territory to which the policy applies.
        state_province: Optional[Union[list[str], Series[str], str]]
            State or province that the policy applies to.
        geography_display: Optional[Union[list[str], Series[str], str]]
            If appropriate, a more simplified geography for display purposes.
        technology: Optional[Union[list[str], Series[str], str]]
            Technologies to which the policy applies.
        policy_start_date: Optional[date]
            Date that any policy commitments began.
        policy_start_date_lt: Optional[date]
            Filter records where policy start date is less than the supplied date.
        policy_start_date_lte: Optional[date]
            Filter records where policy start date is less than or equal to the supplied date.
        policy_start_date_gt: Optional[date]
            Filter records where policy start date is greater than the supplied date.
        policy_start_date_gte: Optional[date]
            Filter records where policy start date is greater than or equal to the supplied date.
        policy_end_date: Optional[date]
            Date that the policy ends.
        policy_end_date_lt: Optional[date]
            Filter records where policy end date is less than the supplied date.
        policy_end_date_lte: Optional[date]
            Filter records where policy end date is less than or equal to the supplied date.
        policy_end_date_gt: Optional[date]
            Filter records where policy end date is greater than the supplied date.
        policy_end_date_gte: Optional[date]
            Filter records where policy end date is greater than or equal to the supplied date.
        target_name_full: Optional[Union[list[str], Series[str], str]]
            Name of any target which the policy supports.
        target_id: Optional[Union[list[str], Series[str], str]]
            Unique ID associated with any target which the policy supports.
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
        filter_params.append(list_to_filter("policyNameFull", policy_name_full))
        filter_params.append(list_to_filter("policyId", policy_id))
        filter_params.append(list_to_filter("policyName", policy_name))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("geographyDisplay", geography_display))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("policyStartDate", policy_start_date))
        if policy_start_date_lt is not None:
            filter_params.append(f'policyStartDate < "{policy_start_date_lt}"')
        if policy_start_date_lte is not None:
            filter_params.append(f'policyStartDate <= "{policy_start_date_lte}"')
        if policy_start_date_gt is not None:
            filter_params.append(f'policyStartDate > "{policy_start_date_gt}"')
        if policy_start_date_gte is not None:
            filter_params.append(f'policyStartDate >= "{policy_start_date_gte}"')
        filter_params.append(list_to_filter("policyEndDate", policy_end_date))
        if policy_end_date_lt is not None:
            filter_params.append(f'policyEndDate < "{policy_end_date_lt}"')
        if policy_end_date_lte is not None:
            filter_params.append(f'policyEndDate <= "{policy_end_date_lte}"')
        if policy_end_date_gt is not None:
            filter_params.append(f'policyEndDate > "{policy_end_date_gt}"')
        if policy_end_date_gte is not None:
            filter_params.append(f'policyEndDate >= "{policy_end_date_gte}"')
        filter_params.append(list_to_filter("targetNameFull", target_name_full))
        filter_params.append(list_to_filter("targetId", target_id))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/policy/v1/policies",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    @staticmethod
    def _convert_unique_values_to_df(resp: Response) -> DataFrame:
        return CetPolicies._normalize(resp, "aggResultValue")

    @staticmethod
    def _convert_to_df(resp: Response) -> DataFrame:
        return CetPolicies._normalize(resp, "results")

    @staticmethod
    def _normalize(resp: Response, key: str) -> DataFrame:
        df = pd.json_normalize(resp.json()[key])
        for column in [
            "announcementDate",
            "announcedDate",
            "finalTargetDate",
            "policyStartDate",
            "policyEndDate",
        ]:
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column], utc=True, format="ISO8601", errors="coerce"
                )
        return df
