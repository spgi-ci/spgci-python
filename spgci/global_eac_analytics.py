from __future__ import annotations

from datetime import date
from typing import List, Literal, Optional, Union

import pandas as pd
from pandas import DataFrame, Series
from requests import Response

from spgci.api_client import get_data
from spgci.utilities import list_to_filter


GlobalEacDataset = Literal[
    "outlooks",
    "issues",
    "redemptions",
    "devices",
]


class GlobalEacAnalytics:
    """Client for global Energy Attribute Certificate analytics datasets."""

    _dataset_to_path = {
        "outlooks": "analytics/cet/eacs/v1/outlooks",
        "issues": "analytics/cet/eacs/v1/issues",
        "redemptions": "analytics/cet/eacs/v1/redemptions",
        "devices": "analytics/cet/eacs/v1/devices",
    }

    def get_unique_values(
        self,
        dataset: GlobalEacDataset,
        columns: Union[list[str], str],
        filter_exp: Optional[str] = None,
    ) -> DataFrame:
        """
        Get unique values or unique combinations for selected columns.

        Parameters
        ----------
        dataset: GlobalEacDataset
            Dataset from which to retrieve unique values.
        columns: Union[list[str], str]
            One API camelCase column name or a list of column names.
        filter_exp: Optional[str], optional
            Optional filter expression applied before grouping, by default None.

        Returns
        -------
        DataFrame
            Unique values or unique combinations for the requested columns.

        Examples
        --------
        >>> eac.get_unique_values("outlooks", "country")
        >>> eac.get_unique_values("issues", ["countryName", "technology"])
        """
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

    def get_outlooks(
        self,
        *,
        vintage: Optional[date] = None,
        vintage_lt: Optional[date] = None,
        vintage_lte: Optional[date] = None,
        vintage_gt: Optional[date] = None,
        vintage_gte: Optional[date] = None,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        state_province: Optional[Union[list[str], Series[str], str]] = None,
        certificate_name: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        certificate_class: Optional[Union[list[str], Series[str], str]] = None,
        unit: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        currency_details: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        series_type: Optional[Union[list[str], Series[str], str]] = None,
        scenario_description: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        API containing data points relating to EAC outlooks.

        Parameters
        ----------
        vintage: Optional[date], optional
            The publication or release date of the dataset or model run., by default None
        vintage_gt: Optional[date], optional
            Filter by `vintage > x`, by default None
        vintage_gte: Optional[date], optional
            Filter by `vintage >= x`, by default None
        vintage_lt: Optional[date], optional
            Filter by `vintage < x`, by default None
        vintage_lte: Optional[date], optional
            Filter by `vintage <= x`, by default None
        scenario: Optional[Union[list[str], Series[str], str]]
            Forecast or modeling scenario under which the data is projected., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Country or sovereign territory where the certificate is issued., by default None
        region_major: Optional[Union[list[str], Series[str], str]]
            High-level macro-region associated with the geography., by default None
        state_province: Optional[Union[list[str], Series[str], str]]
            State, province, administrative division, or power market zone., by default None
        certificate_name: Optional[Union[list[str], Series[str], str]]
            Official name or type of renewable energy certificate., by default None
        concept: Optional[Union[list[str], Series[str], str]]
            Analytical concept or metric category being reported., by default None
        certificate_class: Optional[Union[list[str], Series[str], str]]
            Classification or tier of the certificate., by default None
        unit: Optional[Union[list[str], Series[str], str]]
            Unit of measurement for the reported value., by default None
        currency: Optional[Union[list[str], Series[str], str]]
            Currency code for monetary values., by default None
        currency_details: Optional[Union[list[str], Series[str], str]]
            Nominal or real currency basis and base year., by default None
        technology: Optional[Union[list[str], Series[str], str]]
            Renewable energy technology associated with the certificate., by default None
        series_type: Optional[Union[list[str], Series[str], str]]
            Type of data series, such as historical, forecast, or actual., by default None
        scenario_description: Optional[Union[list[str], Series[str], str]]
            Description of the scenario and its assumptions., by default None
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year to which the data point applies., by default None
        value: Optional[Union[list[str], Series[str], str]]
            Numerical value reported for the metric., by default None
        filter_exp: Optional[str], optional
            Optional filter expression, by default None
        page: int, optional
            Page number, by default 1
        page_size: int, optional
            Records per page, by default 5000
        raw: bool, optional
            Return the raw response instead of a DataFrame, by default False
        paginate: bool, optional
            Retrieve all available pages, by default False

        Returns
        -------
        DataFrame or Response
            A normalized DataFrame, or the raw response when ``raw=True``.
        """
        filter_params: List[str] = []
        filter_params.append(list_to_filter("vintage", vintage))
        if vintage_gt is not None:
            filter_params.append(f'vintage > "{vintage_gt}"')
        if vintage_gte is not None:
            filter_params.append(f'vintage >= "{vintage_gte}"')
        if vintage_lt is not None:
            filter_params.append(f'vintage < "{vintage_lt}"')
        if vintage_lte is not None:
            filter_params.append(f'vintage <= "{vintage_lte}"')
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("certificateName", certificate_name))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("certificateClass", certificate_class))
        filter_params.append(list_to_filter("unit", unit))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("currencyDetails", currency_details))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("seriesType", series_type))
        filter_params.append(list_to_filter("scenarioDescription", scenario_description))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path=self._dataset_to_path["outlooks"],
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_issues(
        self,
        *,
        issue_date: Optional[date] = None,
        issue_date_lt: Optional[date] = None,
        issue_date_lte: Optional[date] = None,
        issue_date_gt: Optional[date] = None,
        issue_date_gte: Optional[date] = None,
        commissioning_date: Optional[date] = None,
        commissioning_date_lt: Optional[date] = None,
        commissioning_date_lte: Optional[date] = None,
        commissioning_date_gt: Optional[date] = None,
        commissioning_date_gte: Optional[date] = None,
        vintage: Optional[Union[list[str], Series[str], str]] = None,
        total_volume_issued: Optional[Union[list[str], Series[str], str]] = None,
        volume_issued_uom: Optional[Union[list[str], Series[str], str]] = None,
        issuer: Optional[Union[list[str], Series[str], str]] = None,
        total_capacity_mw: Optional[Union[list[str], Series[str], str]] = None,
        capacity_mw_uom: Optional[Union[list[str], Series[str], str]] = None,
        country_name: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        API containing data points relating to I-REC issues.

        Parameters
        ----------
        issue_date: Optional[date], optional
            Date on which I-RECs were issued., by default None
        issue_date_gt: Optional[date], optional
            Filter by `issue_date > x`, by default None
        issue_date_gte: Optional[date], optional
            Filter by `issue_date >= x`, by default None
        issue_date_lt: Optional[date], optional
            Filter by `issue_date < x`, by default None
        issue_date_lte: Optional[date], optional
            Filter by `issue_date <= x`, by default None
        commissioning_date: Optional[date], optional
            Commissioning date of the issuing facility., by default None
        commissioning_date_gt: Optional[date], optional
            Filter by `commissioning_date > x`, by default None
        commissioning_date_gte: Optional[date], optional
            Filter by `commissioning_date >= x`, by default None
        commissioning_date_lt: Optional[date], optional
            Filter by `commissioning_date < x`, by default None
        commissioning_date_lte: Optional[date], optional
            Filter by `commissioning_date <= x`, by default None
        vintage: Optional[Union[list[str], Series[str], str]]
            Year of electricity production to which the I-RECs relate., by default None
        total_volume_issued: Optional[Union[list[str], Series[str], str]]
            Number of certificates issued, where one I-REC equals one MWh., by default None
        volume_issued_uom: Optional[Union[list[str], Series[str], str]]
            Unit of certificates issued., by default None
        issuer: Optional[Union[list[str], Series[str], str]]
            Body issuing the certificates., by default None
        total_capacity_mw: Optional[Union[list[str], Series[str], str]]
            Capacity of the issuing facility., by default None
        capacity_mw_uom: Optional[Union[list[str], Series[str], str]]
            Unit of the facility capacity., by default None
        country_name: Optional[Union[list[str], Series[str], str]]
            Country where the facility operates., by default None
        region: Optional[Union[list[str], Series[str], str]]
            Region where the facility operates., by default None
        technology: Optional[Union[list[str], Series[str], str]]
            Technology type of the issuing facility., by default None
        filter_exp: Optional[str], optional
            Optional filter expression, by default None
        page: int, optional
            Page number, by default 1
        page_size: int, optional
            Records per page, by default 5000
        raw: bool, optional
            Return the raw response instead of a DataFrame, by default False
        paginate: bool, optional
            Retrieve all available pages, by default False

        Returns
        -------
        DataFrame or Response
            A normalized DataFrame, or the raw response when ``raw=True``.
        """
        filter_params: List[str] = []
        filter_params.append(list_to_filter("issueDate", issue_date))
        if issue_date_gt is not None:
            filter_params.append(f'issueDate > "{issue_date_gt}"')
        if issue_date_gte is not None:
            filter_params.append(f'issueDate >= "{issue_date_gte}"')
        if issue_date_lt is not None:
            filter_params.append(f'issueDate < "{issue_date_lt}"')
        if issue_date_lte is not None:
            filter_params.append(f'issueDate <= "{issue_date_lte}"')
        filter_params.append(list_to_filter("commissioningDate", commissioning_date))
        if commissioning_date_gt is not None:
            filter_params.append(f'commissioningDate > "{commissioning_date_gt}"')
        if commissioning_date_gte is not None:
            filter_params.append(f'commissioningDate >= "{commissioning_date_gte}"')
        if commissioning_date_lt is not None:
            filter_params.append(f'commissioningDate < "{commissioning_date_lt}"')
        if commissioning_date_lte is not None:
            filter_params.append(f'commissioningDate <= "{commissioning_date_lte}"')
        filter_params.append(list_to_filter("vintage", vintage))
        filter_params.append(list_to_filter("totalVolumeIssued", total_volume_issued))
        filter_params.append(list_to_filter("volumeIssuedUom", volume_issued_uom))
        filter_params.append(list_to_filter("issuer", issuer))
        filter_params.append(list_to_filter("totalCapacityMw", total_capacity_mw))
        filter_params.append(list_to_filter("capacityMwUom", capacity_mw_uom))
        filter_params.append(list_to_filter("countryName", country_name))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("technology", technology))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path=self._dataset_to_path["issues"],
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_redemptions(
        self,
        *,
        redemption_date: Optional[date] = None,
        redemption_date_lt: Optional[date] = None,
        redemption_date_lte: Optional[date] = None,
        redemption_date_gt: Optional[date] = None,
        redemption_date_gte: Optional[date] = None,
        total_volume_redeemed: Optional[Union[list[str], Series[str], str]] = None,
        volume_redeemed_uom: Optional[Union[list[str], Series[str], str]] = None,
        vintage: Optional[Union[list[str], Series[str], str]] = None,
        country_name_beneficiary: Optional[Union[list[str], Series[str], str]] = None,
        region_beneficiary: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        issue_country: Optional[Union[list[str], Series[str], str]] = None,
        issue_region: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        API containing data points relating to I-REC redemptions.

        Parameters
        ----------
        redemption_date: Optional[date], optional
            Date on which certificates were redeemed., by default None
        redemption_date_gt: Optional[date], optional
            Filter by `redemption_date > x`, by default None
        redemption_date_gte: Optional[date], optional
            Filter by `redemption_date >= x`, by default None
        redemption_date_lt: Optional[date], optional
            Filter by `redemption_date < x`, by default None
        redemption_date_lte: Optional[date], optional
            Filter by `redemption_date <= x`, by default None
        total_volume_redeemed: Optional[Union[list[str], Series[str], str]]
            Volume of certificates redeemed., by default None
        volume_redeemed_uom: Optional[Union[list[str], Series[str], str]]
            Unit of certificates redeemed., by default None
        vintage: Optional[Union[list[str], Series[str], str]]
            Vintage of the redeemed certificates., by default None
        country_name_beneficiary: Optional[Union[list[str], Series[str], str]]
            Country where the certificates were redeemed., by default None
        region_beneficiary: Optional[Union[list[str], Series[str], str]]
            Region where the certificates were redeemed., by default None
        technology: Optional[Union[list[str], Series[str], str]]
            Technology of the redeemed certificates., by default None
        issue_country: Optional[Union[list[str], Series[str], str]]
            Country where the certificates were issued., by default None
        issue_region: Optional[Union[list[str], Series[str], str]]
            Region where the certificates were issued., by default None
        filter_exp: Optional[str], optional
            Optional filter expression, by default None
        page: int, optional
            Page number, by default 1
        page_size: int, optional
            Records per page, by default 5000
        raw: bool, optional
            Return the raw response instead of a DataFrame, by default False
        paginate: bool, optional
            Retrieve all available pages, by default False

        Returns
        -------
        DataFrame or Response
            A normalized DataFrame, or the raw response when ``raw=True``.
        """
        filter_params: List[str] = []
        filter_params.append(list_to_filter("redemptionDate", redemption_date))
        if redemption_date_gt is not None:
            filter_params.append(f'redemptionDate > "{redemption_date_gt}"')
        if redemption_date_gte is not None:
            filter_params.append(f'redemptionDate >= "{redemption_date_gte}"')
        if redemption_date_lt is not None:
            filter_params.append(f'redemptionDate < "{redemption_date_lt}"')
        if redemption_date_lte is not None:
            filter_params.append(f'redemptionDate <= "{redemption_date_lte}"')
        filter_params.append(list_to_filter("totalVolumeRedeemed", total_volume_redeemed))
        filter_params.append(list_to_filter("volumeRedeemedUom", volume_redeemed_uom))
        filter_params.append(list_to_filter("vintage", vintage))
        filter_params.append(list_to_filter("countryNameBeneficiary", country_name_beneficiary))
        filter_params.append(list_to_filter("regionBeneficiary", region_beneficiary))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("issueCountry", issue_country))
        filter_params.append(list_to_filter("issueRegion", issue_region))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path=self._dataset_to_path["redemptions"],
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_devices(
        self,
        *,
        registration_date: Optional[date] = None,
        registration_date_lt: Optional[date] = None,
        registration_date_lte: Optional[date] = None,
        registration_date_gt: Optional[date] = None,
        registration_date_gte: Optional[date] = None,
        commissioning_date: Optional[date] = None,
        commissioning_date_lt: Optional[date] = None,
        commissioning_date_lte: Optional[date] = None,
        commissioning_date_gt: Optional[date] = None,
        commissioning_date_gte: Optional[date] = None,
        country_name: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        issuer: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        total_capacity_mw: Optional[Union[list[str], Series[str], str]] = None,
        capacity_mw_uom: Optional[Union[list[str], Series[str], str]] = None,
        active_status: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        API containing data points relating to I-REC devices.

        Parameters
        ----------
        registration_date: Optional[date], optional
            Registration date of the facility., by default None
        registration_date_gt: Optional[date], optional
            Filter by `registration_date > x`, by default None
        registration_date_gte: Optional[date], optional
            Filter by `registration_date >= x`, by default None
        registration_date_lt: Optional[date], optional
            Filter by `registration_date < x`, by default None
        registration_date_lte: Optional[date], optional
            Filter by `registration_date <= x`, by default None
        commissioning_date: Optional[date], optional
            Commissioning date of the facility., by default None
        commissioning_date_gt: Optional[date], optional
            Filter by `commissioning_date > x`, by default None
        commissioning_date_gte: Optional[date], optional
            Filter by `commissioning_date >= x`, by default None
        commissioning_date_lt: Optional[date], optional
            Filter by `commissioning_date < x`, by default None
        commissioning_date_lte: Optional[date], optional
            Filter by `commissioning_date <= x`, by default None
        country_name: Optional[Union[list[str], Series[str], str]]
            Country where the facility operates., by default None
        region: Optional[Union[list[str], Series[str], str]]
            Region where the facility operates., by default None
        issuer: Optional[Union[list[str], Series[str], str]]
            Body issuing I-RECs for the facility., by default None
        technology: Optional[Union[list[str], Series[str], str]]
            Technology of the facility., by default None
        total_capacity_mw: Optional[Union[list[str], Series[str], str]]
            Capacity of the facility., by default None
        capacity_mw_uom: Optional[Union[list[str], Series[str], str]]
            Unit in which facility capacity is measured., by default None
        active_status: Optional[Union[list[str], Series[str], str]]
            Whether the facility is active., by default None
        filter_exp: Optional[str], optional
            Optional filter expression, by default None
        page: int, optional
            Page number, by default 1
        page_size: int, optional
            Records per page, by default 5000
        raw: bool, optional
            Return the raw response instead of a DataFrame, by default False
        paginate: bool, optional
            Retrieve all available pages, by default False

        Returns
        -------
        DataFrame or Response
            A normalized DataFrame, or the raw response when ``raw=True``.
        """
        filter_params: List[str] = []
        filter_params.append(list_to_filter("registrationDate", registration_date))
        if registration_date_gt is not None:
            filter_params.append(f'registrationDate > "{registration_date_gt}"')
        if registration_date_gte is not None:
            filter_params.append(f'registrationDate >= "{registration_date_gte}"')
        if registration_date_lt is not None:
            filter_params.append(f'registrationDate < "{registration_date_lt}"')
        if registration_date_lte is not None:
            filter_params.append(f'registrationDate <= "{registration_date_lte}"')
        filter_params.append(list_to_filter("commissioningDate", commissioning_date))
        if commissioning_date_gt is not None:
            filter_params.append(f'commissioningDate > "{commissioning_date_gt}"')
        if commissioning_date_gte is not None:
            filter_params.append(f'commissioningDate >= "{commissioning_date_gte}"')
        if commissioning_date_lt is not None:
            filter_params.append(f'commissioningDate < "{commissioning_date_lt}"')
        if commissioning_date_lte is not None:
            filter_params.append(f'commissioningDate <= "{commissioning_date_lte}"')
        filter_params.append(list_to_filter("countryName", country_name))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("issuer", issuer))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("totalCapacityMw", total_capacity_mw))
        filter_params.append(list_to_filter("capacityMwUom", capacity_mw_uom))
        filter_params.append(list_to_filter("activeStatus", active_status))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        return get_data(
            path=self._dataset_to_path["devices"],
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    @staticmethod
    def _convert_unique_values_to_df(resp: Response) -> DataFrame:
        return GlobalEacAnalytics._normalize(resp, "aggResultValue")

    @staticmethod
    def _convert_to_df(resp: Response) -> DataFrame:
        return GlobalEacAnalytics._normalize(resp, "results")

    @staticmethod
    def _normalize(resp: Response, key: str) -> DataFrame:
        df = pd.json_normalize(resp.json()[key])
        date_columns = [
            "vintage",
            "issueDate",
            "commissioningDate",
            "redemptionDate",
            "registrationDate",
        ]
        for column in date_columns:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column])
        return df
