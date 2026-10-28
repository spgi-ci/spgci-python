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
from typing import Dict, List, Literal, Optional, Union

import pandas as pd
from pandas import DataFrame, Series
from requests import Response

from spgci.api_client import get_data
from spgci.utilities import convert_date_to_filter_exp, list_to_filter

_Str = "Optional[Union[list[str], Series[str], str]]"

DckbDataset = Literal["mmf-us", "mmf-global", "cms", "geospatial"]


class DataCenterKnowledgeBase:
    """
    Data Center Knowledge Base (DCKB) data.

    Includes
    --------
    ``get_mmf_us()`` US market model forecast: PUE and power metrics by state.

    ``get_mmf_global()`` global market model forecast: capacity, space, racks and
    power estimates by reporting/partner region.

    ``get_cms()`` data center facility details (periodic CMS-to-ADW data) with
    power, space, rack, redundancy and certification attributes.

    ``get_geospatial_map()`` data center facility details with coordinates (latitude,
    longitude, WKT point) for mapping.

    ``get_unique_values()`` discover valid filter values for any of the datasets.
    """

    _dataset_to_path: Dict[str, str] = {
        "mmf-us": "/dckb/v1/mmf/us",
        "mmf-global": "/dckb/v2/mmf/global",
        "cms": "/dckb/v1/cms",
        "geospatial": "/dckbgeospatial/map",
    }

    # ------------------------------------------------------------------ MMF US
    def get_mmf_us(
        self,
        *,
        reporting_region: _Str = None,
        report_for_date: Optional[Union[date, list[date], Series[date]]] = None,
        report_for_date_gt: Optional[date] = None,
        report_for_date_gte: Optional[date] = None,
        report_for_date_lt: Optional[date] = None,
        report_for_date_lte: Optional[date] = None,
        frequency: _Str = None,
        value: Optional[Union[float, list[float]]] = None,
        state: _Str = None,
        division: _Str = None,
        dc_owner: _Str = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve US data center market model forecast data (PUE and power metrics).

        Parameters
        ----------
        reporting_region : list[str] | Series[str] | str, optional
            Reporting region, e.g. ``"Alabama"``.
        report_for_date : date | list[date] | Series[date], optional
            Date the value applies to.
        report_for_date_gt, report_for_date_gte, report_for_date_lt, report_for_date_lte : date, optional
            Filter by ``report_for_date`` with the given comparison.
        frequency : list[str] | Series[str] | str, optional
            Frequency of the data, e.g. ``"Yearly"``.
        value : float | list[float], optional
            PUE value.
        state : list[str] | Series[str] | str, optional
            State abbreviation, e.g. ``"AK"``.
        division : list[str] | Series[str] | str, optional
            Census division, e.g. ``"Mountain"``.
        dc_owner : list[str] | Series[str] | str, optional
            Data center owner type, e.g. ``"Crypto Mining"``.
        filter_exp : str, optional
            Additional filter expression. Use this for columns without a
            dedicated parameter, e.g. ``netItPowerMwOriginal``.
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
            US market model forecast data as a DataFrame, or the raw response when `raw=True`.

        Examples
        --------
        >>> ci.DataCenterKnowledgeBase().get_mmf_us(state="AK", report_for_date_gte=date(2024, 1, 1))
        """
        filter_params: List[str] = [
            list_to_filter("reportingRegion", reporting_region),
            list_to_filter("reportForDate", report_for_date),
            list_to_filter("frequency", frequency),
            list_to_filter("value", value),
            list_to_filter("stateOriginal", state),
            list_to_filter("divisionOriginal", division),
            list_to_filter("dcOwnerOriginal", dc_owner),
        ]
        filter_params = convert_date_to_filter_exp(
            "reportForDate",
            report_for_date_gt,
            report_for_date_gte,
            report_for_date_lt,
            report_for_date_lte,
            filter_params,
        )
        filter_params = [value for value in filter_params if value != ""]
        filter_exp = self._join_filters(filter_params, filter_exp)

        return get_data(
            path=self._dataset_to_path["mmf-us"],
            params={
                "page": page,
                "pageSize": page_size,
                "filter": filter_exp,
            },
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    # --------------------------------------------------------------- MMF Global
    def get_mmf_global(
        self,
        *,
        reporting_region: _Str = None,
        partner_region: _Str = None,
        report_for_date: Optional[Union[date, list[date], Series[date]]] = None,
        report_for_date_gt: Optional[date] = None,
        report_for_date_gte: Optional[date] = None,
        report_for_date_lt: Optional[date] = None,
        report_for_date_lte: Optional[date] = None,
        frequency: _Str = None,
        value: Optional[Union[float, list[float]]] = None,
        dc_owner: _Str = None,
        dc_type: _Str = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve global data center market model forecast data.

        Parameters
        ----------
        reporting_region : list[str] | Series[str] | str, optional
            Reporting region, e.g. ``"Europe"``.
        partner_region : list[str] | Series[str] | str, optional
            Partner region, e.g. ``"Argentina"``.
        report_for_date : date | list[date] | Series[date], optional
            Date the value applies to.
        report_for_date_gt, report_for_date_gte, report_for_date_lt, report_for_date_lte : date, optional
            Filter by ``report_for_date`` with the given comparison.
        frequency : list[str] | Series[str] | str, optional
            Frequency of the data, e.g. ``"Yearly"``.
        value : float | list[float], optional
            Value of the metric.
        dc_owner : list[str] | Series[str] | str, optional
            Data center owner type, e.g. ``"Leased DC"``.
        dc_type : list[str] | Series[str] | str, optional
            Data center type, e.g. ``"Cryptocurrency"``.
        filter_exp : str, optional
            Additional filter expression. Use this for columns without a
            dedicated parameter, e.g. ``netItPowerMwOriginal``.
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
            Global market model forecast data as a DataFrame, or the raw response when `raw=True`.

        Examples
        --------
        >>> ci.DataCenterKnowledgeBase().get_mmf_global(reporting_region="Europe", report_for_date_gte=date(2024, 1, 1))
        """
        filter_params: List[str] = [
            list_to_filter("reportingRegion", reporting_region),
            list_to_filter("partnerRegion", partner_region),
            list_to_filter("reportForDate", report_for_date),
            list_to_filter("frequency", frequency),
            list_to_filter("value", value),
            list_to_filter("dcOwnerOriginal", dc_owner),
            list_to_filter("dcTypeOriginal", dc_type),
        ]
        filter_params = convert_date_to_filter_exp(
            "reportForDate",
            report_for_date_gt,
            report_for_date_gte,
            report_for_date_lt,
            report_for_date_lte,
            filter_params,
        )
        filter_params = [value for value in filter_params if value != ""]
        filter_exp = self._join_filters(filter_params, filter_exp)

        return get_data(
            path=self._dataset_to_path["mmf-global"],
            params={
                "page": page,
                "pageSize": page_size,
                "filter": filter_exp,
            },
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    # --------------------------------------------------------------------- CMS
    def get_cms(
        self,
        *,
        reporting_region: _Str = None,
        partner_region: _Str = None,
        report_for_date: Optional[Union[date, list[date], Series[date]]] = None,
        report_for_date_gt: Optional[date] = None,
        report_for_date_gte: Optional[date] = None,
        report_for_date_lt: Optional[date] = None,
        report_for_date_lte: Optional[date] = None,
        frequency: _Str = None,
        value: Optional[Union[float, list[float]]] = None,
        company: _Str = None,
        datacenter_name: _Str = None,
        datacenter_type: _Str = None,
        provider_type: _Str = None,
        facility_owner: _Str = None,
        operational_status: _Str = None,
        single_multi_tenant: _Str = None,
        provider_hq_country: _Str = None,
        provider_hq_city: _Str = None,
        sub_region: _Str = None,
        state: _Str = None,
        city: _Str = None,
        msa: _Str = None,
        service_types: _Str = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve data center facility details (CMS) with PUE, power, space and rack attributes.

        Parameters
        ----------
        reporting_region : list[str] | Series[str] | str, optional
            Reporting region, e.g. ``"APAC"``.
        partner_region : list[str] | Series[str] | str, optional
            Partner region (country of the facility), e.g. ``"Australia"``.
        report_for_date : date | list[date] | Series[date], optional
            Date the record applies to.
        report_for_date_gt, report_for_date_gte, report_for_date_lt, report_for_date_lte : date, optional
            Filter by ``report_for_date`` with the given comparison.
        frequency : list[str] | Series[str] | str, optional
            Frequency of the data, e.g. ``"Quarterly"``.
        value : float | list[float], optional
            PUE value.
        company : list[str] | Series[str] | str, optional
            Company name, e.g. ``"Telstra Group Limited"``.
        datacenter_name : list[str] | Series[str] | str, optional
            Data center name, e.g. ``"SYD3"``.
        datacenter_type : list[str] | Series[str] | str, optional
            Data center type, e.g. ``"Retail"``.
        provider_type : list[str] | Series[str] | str, optional
            Provider type, e.g. ``"Retail"``.
        facility_owner : list[str] | Series[str] | str, optional
            Facility owner.
        operational_status : list[str] | Series[str] | str, optional
            Operational status, e.g. ``"Operational"``.
        single_multi_tenant : list[str] | Series[str] | str, optional
            ``"Single"`` or ``"Multi"`` tenant.
        provider_hq_country : list[str] | Series[str] | str, optional
            Country of the provider headquarters.
        provider_hq_city : list[str] | Series[str] | str, optional
            City of the provider headquarters.
        sub_region : list[str] | Series[str] | str, optional
            Sub region of the facility, e.g. ``"Asia-Pacific"``.
        state : list[str] | Series[str] | str, optional
            State of the facility.
        city : list[str] | Series[str] | str, optional
            City of the facility.
        msa : list[str] | Series[str] | str, optional
            Metropolitan statistical area of the facility.
        service_types : list[str] | Series[str] | str, optional
            Service types offered, e.g. ``"Colocation"``.
        filter_exp : str, optional
            Additional filter expression. Use this for columns without a
            dedicated parameter, e.g. ``totalItPowerKwOriginal``.
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
            Facility details as a DataFrame, or the raw response when `raw=True`.

        Examples
        --------
        >>> ci.DataCenterKnowledgeBase().get_cms(partner_region="Australia", city="Sydney")
        """
        filter_params: List[str] = [
            list_to_filter("reportingRegion", reporting_region),
            list_to_filter("partnerRegion", partner_region),
            list_to_filter("reportForDate", report_for_date),
            list_to_filter("frequency", frequency),
            list_to_filter("pUE", value),
            list_to_filter("companyOriginal", company),
            list_to_filter("datacenterNameOriginal", datacenter_name),
            list_to_filter("datacenterTypeOriginal", datacenter_type),
            list_to_filter("providerTypeOriginal", provider_type),
            list_to_filter("facilityOwnerOriginal", facility_owner),
            list_to_filter("operationalStatusOriginal", operational_status),
            list_to_filter("singleMultiTenantOriginal", single_multi_tenant),
            list_to_filter("providerHqCountryOriginal", provider_hq_country),
            list_to_filter("providerHqCityOriginal", provider_hq_city),
            list_to_filter("subRegionOriginal", sub_region),
            list_to_filter("stateOriginal", state),
            list_to_filter("cityOriginal", city),
            list_to_filter("msaOriginal", msa),
            list_to_filter("serviceTypesOriginal", service_types),
        ]
        filter_params = convert_date_to_filter_exp(
            "reportForDate",
            report_for_date_gt,
            report_for_date_gte,
            report_for_date_lt,
            report_for_date_lte,
            filter_params,
        )
        filter_params = [value for value in filter_params if value != ""]
        filter_exp = self._join_filters(filter_params, filter_exp)

        return get_data(
            path=self._dataset_to_path["cms"],
            params={
                "page": page,
                "pageSize": page_size,
                "filter": filter_exp,
            },
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    # --------------------------------------------------------------- Geospatial
    def get_geospatial_map(
        self,
        *,
        datacenter_id: Optional[Union[int, list[int]]] = None,
        year: _Str = None,
        quarter: _Str = None,
        company: _Str = None,
        datacenter_name: _Str = None,
        datacenter_type: _Str = None,
        provider_type: _Str = None,
        facility_owner: _Str = None,
        operational_status: _Str = None,
        single_multi_tenant: _Str = None,
        region: _Str = None,
        sub_region: _Str = None,
        country: _Str = None,
        state: _Str = None,
        city: _Str = None,
        msa: _Str = None,
        provider_hq_country: _Str = None,
        pue: Optional[Union[float, list[float]]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Retrieve data center facility details with coordinates for mapping.

        Parameters
        ----------
        datacenter_id : int | list[int], optional
            Data center identifier.
        year : list[str] | Series[str] | str, optional
            Reporting year, e.g. ``"2026"``.
        quarter : list[str] | Series[str] | str, optional
            Reporting quarter, e.g. ``"1"``.
        company : list[str] | Series[str] | str, optional
            Company name, e.g. ``"Meta"``.
        datacenter_name : list[str] | Series[str] | str, optional
            Data center name.
        datacenter_type : list[str] | Series[str] | str, optional
            Data center type, e.g. ``"Hyperscale"``.
        provider_type : list[str] | Series[str] | str, optional
            Provider type, e.g. ``"Hyperscale"``.
        facility_owner : list[str] | Series[str] | str, optional
            Facility owner.
        operational_status : list[str] | Series[str] | str, optional
            Operational status, e.g. ``"Operational"``.
        single_multi_tenant : list[str] | Series[str] | str, optional
            ``"Single"`` or ``"Multi"`` tenant.
        region : list[str] | Series[str] | str, optional
            Region, e.g. ``"EMEA"``.
        sub_region : list[str] | Series[str] | str, optional
            Sub region, e.g. ``"Europe"``.
        country : list[str] | Series[str] | str, optional
            Country, e.g. ``"Ireland"``.
        state : list[str] | Series[str] | str, optional
            State or province.
        city : list[str] | Series[str] | str, optional
            City.
        msa : list[str] | Series[str] | str, optional
            Metropolitan statistical area.
        provider_hq_country : list[str] | Series[str] | str, optional
            Country of the provider headquarters.
        pue : float | list[float], optional
            Power usage effectiveness.
        filter_exp : str, optional
            Additional filter expression. Use this for columns without a
            dedicated parameter, e.g. ``totalItPowerKw``.
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
            Facility details as a DataFrame, or the raw response when `raw=True`.

        Examples
        --------
        >>> ci.DataCenterKnowledgeBase().get_geospatial_map(country="Ireland", company="Meta")
        """
        filter_params: List[str] = [
            self._id_filter("datacenterId", datacenter_id),
            list_to_filter("year", year),
            list_to_filter("quarter", quarter),
            list_to_filter("company", company),
            list_to_filter("datacenterName", datacenter_name),
            list_to_filter("dataCenterType", datacenter_type),
            list_to_filter("providerType", provider_type),
            list_to_filter("facilityOwner", facility_owner),
            list_to_filter("operationalStatus", operational_status),
            list_to_filter("singleMultiTenant", single_multi_tenant),
            list_to_filter("region", region),
            list_to_filter("subRegion", sub_region),
            list_to_filter("country", country),
            list_to_filter("state", state),
            list_to_filter("city", city),
            list_to_filter("msa", msa),
            list_to_filter("providerHqCountry", provider_hq_country),
            list_to_filter("pue", pue),
        ]
        filter_params = [value for value in filter_params if value != ""]
        filter_exp = self._join_filters(filter_params, filter_exp)

        return get_data(
            path=self._dataset_to_path["geospatial"],
            params={
                "page": page,
                "pageSize": page_size,
                "filter": filter_exp,
            },
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    # ------------------------------------------------------------ Unique values
    def get_unique_values(
        self,
        dataset: DckbDataset,
        columns: Optional[Union[list[str], str]],
        filter_exp: Optional[str] = None,
    ) -> DataFrame:
        """
        Get unique values (or combinations) of columns in a dataset.

        Use this to discover valid filter values before calling a ``get_*`` method.

        Parameters
        ----------
        dataset : {"mmf-us", "mmf-global", "cms", "geospatial"}
            Dataset to query.
        columns : list[str] | str
            API (camelCase) column names, e.g. ``["reportingRegion", "frequency"]``.
        filter_exp : str, optional
            Filter expression limiting the rows considered.

        Returns
        -------
        DataFrame
            Unique combinations of the requested columns.

        Examples
        --------
        >>> ci.DataCenterKnowledgeBase().get_unique_values("mmf-us", ["stateOriginal", "divisionOriginal"])
        """
        if dataset not in self._dataset_to_path:
            valid = ", ".join(self._dataset_to_path)
            raise ValueError(f"dataset '{dataset}' not found. Valid datasets: {valid}")
        col_value = ", ".join(columns) if isinstance(columns, list) else columns or ""
        params: Dict[str, Union[str, int]] = {"GroupBy": col_value, "pageSize": 5000}
        if filter_exp is not None:
            params["filter"] = filter_exp

        return get_data(
            self._dataset_to_path[dataset],
            params,
            self._convert_to_df_agg,
            paginate=True,
        )

    # ----------------------------------------------------------------- Helpers
    @staticmethod
    def _id_filter(
        column: str, ids: Optional[Union[int, list[int], Series[int]]]
    ) -> str:
        """
        Build a filter for a numeric ID column.

        The API silently ignores an unquoted ``in (1,2)`` list on ID columns
        and returns every row, so the IDs are sent as quoted strings.
        """
        if ids is None:
            return ""
        values = [ids] if isinstance(ids, int) else list(ids)
        if not values:
            return ""
        quoted = ",".join(f'"{i}"' for i in values)
        return f"{column} in ({quoted})"

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
        for column in (
            "reportForDate",
            "sysLastModifiedDatetime",
            "sysValidFrom",
            "sysValidTo",
            "sysCollectionDate",
            "sysCreatedDatetime",
        ):
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column], format="ISO8601", errors="coerce"
                )
        return df

    @classmethod
    def _convert_to_df_agg(cls, resp: Response) -> DataFrame:
        return cls._parse_dates(pd.json_normalize(resp.json()["aggResultValue"]))  # type: ignore

    @classmethod
    def _convert_to_df(cls, resp: Response) -> DataFrame:
        df = cls._parse_dates(pd.json_normalize(resp.json()["results"]))  # type: ignore

        # The API returns coordinates as strings.
        for column in ("latitude", "longitude"):
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")

        return df
