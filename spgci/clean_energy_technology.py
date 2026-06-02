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
from datetime import date, datetime
import pandas as pd


class CleanEnergyTechnology:
    _endpoint = "api/v1/"
    
    _cet_projects_onshore_wind_endpoint = "/onshore-wind"
    _cet_projects_offshore_wind_endpoint = "/offshore-wind"
    _cet_projects_energy_storage_endpoint = "/energy-storage"
    _cet_projects_solar_pv_endpoint = "/pv"
    _cet_projects_geothermal_endpoint = "/geothermal"
    _cet_projects_small_modular_reactor_endpoint = "/smr"
    _cet_projects_concentrated_solar_power_endpoint = "/csp"
    _cet_projects_carbon_capture_utilization_storage_endpoint = "/ccus"
    _cet_projects_hydrogen_endpoint = "/hydrogen"
    _cet_projects_bioenergy_endpoint = "/bioenergy"
    _cet_projects_all_clean_power_endpoint = "/all-clean-power"
    _cet_projects_tenders_endpoint = "/tenders"
    _cet_projects_offtake_contracts_endpoint = "/offtake-contracts"
    _cet_projects_companies_endpoint = "/companies"
    _cet_projects_blue_hydrogen_endpoint = "/blue-hydrogen"

    _cet_supplychain_port_wind_offshore_mv_endpoint = "/port-wind-offshore"
    _cet_supplychain_wind_turbine_orders_mv_endpoint = "/wind-turbine-orders"
    _cet_supplychain_battery_orders_mv_endpoint = "/battery-orders"
    _cet_supplychain_electrolyzer_orders_mv_endpoint = "/electrolyzer-orders"

    _datasets = Literal[
        "onshore-wind",
        "offshore-wind",
        "energy-storage",
        "pv",
        "geothermal",
        "smr",
        "csp",
        "ccus",
        "hydrogen",
        "bioenergy",
        "all-clean-power",
        "tenders",
        "offtake-contracts",
        "companies",
        "blue-hydrogen",
        "port-wind-offshore",
        "wind-turbine-orders",
        "battery-orders",
        "electrolyzer-orders",
    ]


    def get_onshore_wind(
        self,
        *,
        project_name: Optional[Union[list, str]] = None,
        project_phase: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        is_hybrid_project: Optional[bool] = None,
        is_tendered_project: Optional[bool] = None,
        tender_name: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        city_county: Optional[Union[list, str]] = None,
        project_status: Optional[Union[list, str]] = None,
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
        API containing data points relating to onshore wind power plants.

        Parameters
        ----------
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_phase : Optional[Union[list, str]], optional
            filter by projectPhase, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        is_hybrid_project : Optional[bool], optional
            filter by isHybridProject, by default None
        is_tendered_project : Optional[bool], optional
            filter by isTenderedProject, by default None
        tender_name : Optional[Union[list, str]], optional
            filter by tenderName, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        city_county : Optional[Union[list, str]], optional
            filter by cityCounty, by default None
        project_status : Optional[Union[list, str]], optional
            filter by projectStatus, by default None
        modified_date : Optional[datetime], optional
            filter by ``modifiedTime = x``, by default None
        modified_date_gt : Optional[datetime], optional
            filter by ``modifiedTime > x``, by default None
        modified_date_gte : Optional[datetime], optional
            filter by ``modifiedTime >= x``, by default None
        modified_date_lt : Optional[datetime], optional
            filter by ``modifiedTime < x``, by default None
        modified_date_lte : Optional[datetime], optional
            filter by ``modifiedTime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        page : int, optional
            page number, by default 1
        page_size : int, optional
            page size, by default 5000
        raw : bool, optional
            return raw response, by default False
        paginate : bool, optional
            auto-paginate, by default False
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectPhase", project_phase))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("isHybridProject", is_hybrid_project))
        filter_params.append(list_to_filter("isTenderedProject", is_tendered_project))
        filter_params.append(list_to_filter("tenderName", tender_name))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("cityCounty", city_county))
        filter_params.append(list_to_filter("projectStatus", project_status))
        filter_params.append(list_to_filter("modifiedTime", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/onshore-wind",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_offshore_wind(
        self,
        *,
        project_name: Optional[Union[list, str]] = None,
        project_phase: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        is_hybrid_project: Optional[bool] = None,
        is_tendered_project: Optional[bool] = None,
        tender_name: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        city_county: Optional[Union[list, str]] = None,
        project_status: Optional[Union[list, str]] = None,
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
        API containing data points relating to offshore wind power plants.

        Parameters
        ----------
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_phase : Optional[Union[list, str]], optional
            filter by projectPhase, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        is_hybrid_project : Optional[bool], optional
            filter by isHybridProject, by default None
        is_tendered_project : Optional[bool], optional
            filter by isTenderedProject, by default None
        tender_name : Optional[Union[list, str]], optional
            filter by tenderName, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        city_county : Optional[Union[list, str]], optional
            filter by cityCounty, by default None
        project_status : Optional[Union[list, str]], optional
            filter by projectStatus, by default None
        modified_date : Optional[datetime], optional
            filter by ``modifiedTime = x``, by default None
        modified_date_gt : Optional[datetime], optional
            filter by ``modifiedTime > x``, by default None
        modified_date_gte : Optional[datetime], optional
            filter by ``modifiedTime >= x``, by default None
        modified_date_lt : Optional[datetime], optional
            filter by ``modifiedTime < x``, by default None
        modified_date_lte : Optional[datetime], optional
            filter by ``modifiedTime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        page : int, optional
            page number, by default 1
        page_size : int, optional
            page size, by default 5000
        raw : bool, optional
            return raw response, by default False
        paginate : bool, optional
            auto-paginate, by default False
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectPhase", project_phase))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("isHybridProject", is_hybrid_project))
        filter_params.append(list_to_filter("isTenderedProject", is_tendered_project))
        filter_params.append(list_to_filter("tenderName", tender_name))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("cityCounty", city_county))
        filter_params.append(list_to_filter("projectStatus", project_status))
        filter_params.append(list_to_filter("modifiedTime", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/offshore-wind",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_energy_storage(
        self,
        *,
        project_name: Optional[Union[list, str]] = None,
        project_phase: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        is_hybrid_project: Optional[bool] = None,
        is_tendered_project: Optional[bool] = None,
        tender_name: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        city_county: Optional[Union[list, str]] = None,
        ess_project_technology_minor: Optional[Union[list, str]] = None,
        es_project_technology_major: Optional[Union[list, str]] = None,
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
        API containing data points relating to energy storage plants.

        Parameters
        ----------
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_phase : Optional[Union[list, str]], optional
            filter by projectPhase, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        is_hybrid_project : Optional[bool], optional
            filter by isHybridProject, by default None
        is_tendered_project : Optional[bool], optional
            filter by isTenderedProject, by default None
        tender_name : Optional[Union[list, str]], optional
            filter by tenderName, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        city_county : Optional[Union[list, str]], optional
            filter by cityCounty, by default None
        ess_project_technology_minor : Optional[Union[list, str]], optional
            filter by eSSProjectTechnologyMinor, by default None
        es_project_technology_major : Optional[Union[list, str]], optional
            filter by eSProjectTechnologyMajor, by default None
        modified_date : Optional[datetime], optional
            filter by ``modifiedTime = x``, by default None
        modified_date_gt : Optional[datetime], optional
            filter by ``modifiedTime > x``, by default None
        modified_date_gte : Optional[datetime], optional
            filter by ``modifiedTime >= x``, by default None
        modified_date_lt : Optional[datetime], optional
            filter by ``modifiedTime < x``, by default None
        modified_date_lte : Optional[datetime], optional
            filter by ``modifiedTime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        page : int, optional
            page number, by default 1
        page_size : int, optional
            page size, by default 5000
        raw : bool, optional
            return raw response, by default False
        paginate : bool, optional
            auto-paginate, by default False
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectPhase", project_phase))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("isHybridProject", is_hybrid_project))
        filter_params.append(list_to_filter("isTenderedProject", is_tendered_project))
        filter_params.append(list_to_filter("tenderName", tender_name))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("cityCounty", city_county))
        filter_params.append(list_to_filter("eSSProjectTechnologyMinor", ess_project_technology_minor))
        filter_params.append(list_to_filter("eSProjectTechnologyMajor", es_project_technology_major))
        filter_params.append(list_to_filter("modifiedTime", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/energy-storage",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_pv(
        self,
        *,
        project_name: Optional[Union[list, str]] = None,
        project_phase: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        is_hybrid_project: Optional[bool] = None,
        is_tendered_project: Optional[bool] = None,
        tender_name: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        city_county: Optional[Union[list, str]] = None,
        project_status: Optional[Union[list, str]] = None,
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
        API containing data points relating to solar PV power plants.

        Parameters
        ----------
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_phase : Optional[Union[list, str]], optional
            filter by projectPhase, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        is_hybrid_project : Optional[bool], optional
            filter by isHybridProject, by default None
        is_tendered_project : Optional[bool], optional
            filter by isTenderedProject, by default None
        tender_name : Optional[Union[list, str]], optional
            filter by tenderName, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        city_county : Optional[Union[list, str]], optional
            filter by cityCounty, by default None
        project_status : Optional[Union[list, str]], optional
            filter by projectStatus, by default None
        modified_date : Optional[datetime], optional
            filter by ``modifiedTime = x``, by default None
        modified_date_gt : Optional[datetime], optional
            filter by ``modifiedTime > x``, by default None
        modified_date_gte : Optional[datetime], optional
            filter by ``modifiedTime >= x``, by default None
        modified_date_lt : Optional[datetime], optional
            filter by ``modifiedTime < x``, by default None
        modified_date_lte : Optional[datetime], optional
            filter by ``modifiedTime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        page : int, optional
            page number, by default 1
        page_size : int, optional
            page size, by default 5000
        raw : bool, optional
            return raw response, by default False
        paginate : bool, optional
            auto-paginate, by default False
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectPhase", project_phase))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("isHybridProject", is_hybrid_project))
        filter_params.append(list_to_filter("isTenderedProject", is_tendered_project))
        filter_params.append(list_to_filter("tenderName", tender_name))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("cityCounty", city_county))
        filter_params.append(list_to_filter("projectStatus", project_status))
        filter_params.append(list_to_filter("modifiedTime", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/pv",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_geothermal(
        self,
        *,
        project_name: Optional[Union[list, str]] = None,
        project_phase: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        is_hybrid_project: Optional[bool] = None,
        is_tendered_project: Optional[bool] = None,
        tender_name: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        city_county: Optional[Union[list, str]] = None,
        project_status: Optional[Union[list, str]] = None,
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
        API containing data points relating to geothermal power plants.

        Parameters
        ----------
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_phase : Optional[Union[list, str]], optional
            filter by projectPhase, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        is_hybrid_project : Optional[bool], optional
            filter by isHybridProject, by default None
        is_tendered_project : Optional[bool], optional
            filter by isTenderedProject, by default None
        tender_name : Optional[Union[list, str]], optional
            filter by tenderName, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        city_county : Optional[Union[list, str]], optional
            filter by cityCounty, by default None
        project_status : Optional[Union[list, str]], optional
            filter by projectStatus, by default None
        modified_date : Optional[datetime], optional
            filter by ``modifiedTime = x``, by default None
        modified_date_gt : Optional[datetime], optional
            filter by ``modifiedTime > x``, by default None
        modified_date_gte : Optional[datetime], optional
            filter by ``modifiedTime >= x``, by default None
        modified_date_lt : Optional[datetime], optional
            filter by ``modifiedTime < x``, by default None
        modified_date_lte : Optional[datetime], optional
            filter by ``modifiedTime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        page : int, optional
            page number, by default 1
        page_size : int, optional
            page size, by default 5000
        raw : bool, optional
            return raw response, by default False
        paginate : bool, optional
            auto-paginate, by default False
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectPhase", project_phase))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("isHybridProject", is_hybrid_project))
        filter_params.append(list_to_filter("isTenderedProject", is_tendered_project))
        filter_params.append(list_to_filter("tenderName", tender_name))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("cityCounty", city_county))
        filter_params.append(list_to_filter("projectStatus", project_status))
        filter_params.append(list_to_filter("modifiedTime", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/geothermal",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_smr(
        self,
        *,
        project_name: Optional[Union[list, str]] = None,
        project_phase: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        is_hybrid_project: Optional[bool] = None,
        is_tendered_project: Optional[bool] = None,
        tender_name: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        city_county: Optional[Union[list, str]] = None,
        project_status: Optional[Union[list, str]] = None,
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
        API containing data points relating to small modular reactor power plants.

        Parameters
        ----------
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_phase : Optional[Union[list, str]], optional
            filter by projectPhase, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        is_hybrid_project : Optional[bool], optional
            filter by isHybridProject, by default None
        is_tendered_project : Optional[bool], optional
            filter by isTenderedProject, by default None
        tender_name : Optional[Union[list, str]], optional
            filter by tenderName, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        city_county : Optional[Union[list, str]], optional
            filter by cityCounty, by default None
        project_status : Optional[Union[list, str]], optional
            filter by projectStatus, by default None
        modified_date : Optional[datetime], optional
            filter by ``modifiedTime = x``, by default None
        modified_date_gt : Optional[datetime], optional
            filter by ``modifiedTime > x``, by default None
        modified_date_gte : Optional[datetime], optional
            filter by ``modifiedTime >= x``, by default None
        modified_date_lt : Optional[datetime], optional
            filter by ``modifiedTime < x``, by default None
        modified_date_lte : Optional[datetime], optional
            filter by ``modifiedTime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        page : int, optional
            page number, by default 1
        page_size : int, optional
            page size, by default 5000
        raw : bool, optional
            return raw response, by default False
        paginate : bool, optional
            auto-paginate, by default False
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectPhase", project_phase))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("isHybridProject", is_hybrid_project))
        filter_params.append(list_to_filter("isTenderedProject", is_tendered_project))
        filter_params.append(list_to_filter("tenderName", tender_name))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("cityCounty", city_county))
        filter_params.append(list_to_filter("projectStatus", project_status))
        filter_params.append(list_to_filter("modifiedTime", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/smr",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_csp(
        self,
        *,
        project_name: Optional[Union[list, str]] = None,
        project_phase: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        is_hybrid_project: Optional[bool] = None,
        is_blue_hydrogen_project: Optional[bool] = None,
        is_tendered_project: Optional[bool] = None,
        tender_name: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        city_county: Optional[Union[list, str]] = None,
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
        API containing data points relating to concentrating solar power plants.

        Parameters
        ----------
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_phase : Optional[Union[list, str]], optional
            filter by projectPhase, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        is_hybrid_project : Optional[bool], optional
            filter by isHybridProject, by default None
        is_blue_hydrogen_project : Optional[bool], optional
            filter by isBlueHydrogenProject, by default None
        is_tendered_project : Optional[bool], optional
            filter by isTenderedProject, by default None
        tender_name : Optional[Union[list, str]], optional
            filter by tenderName, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        city_county : Optional[Union[list, str]], optional
            filter by cityCounty, by default None
        modified_date : Optional[datetime], optional
            filter by ``modifiedTime = x``, by default None
        modified_date_gt : Optional[datetime], optional
            filter by ``modifiedTime > x``, by default None
        modified_date_gte : Optional[datetime], optional
            filter by ``modifiedTime >= x``, by default None
        modified_date_lt : Optional[datetime], optional
            filter by ``modifiedTime < x``, by default None
        modified_date_lte : Optional[datetime], optional
            filter by ``modifiedTime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectPhase", project_phase))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("isHybridProject", is_hybrid_project))
        filter_params.append(list_to_filter("isBlueHydrogenProject", is_blue_hydrogen_project))
        filter_params.append(list_to_filter("isTenderedProject", is_tendered_project))
        filter_params.append(list_to_filter("tenderName", tender_name))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("cityCounty", city_county))
        filter_params.append(list_to_filter("modifiedTime", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/csp",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_ccus(
        self,
        *,
        project_name: Optional[Union[list, str]] = None,
        project_phase: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        is_hybrid_project: Optional[bool] = None,
        is_blue_hydrogen_project: Optional[bool] = None,
        is_tendered_project: Optional[bool] = None,
        tender_name: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        city_county: Optional[Union[list, str]] = None,
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
        API containing data points relating to carbon capture utilization and storage plants.

        Parameters
        ----------
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_phase : Optional[Union[list, str]], optional
            filter by projectPhase, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        is_hybrid_project : Optional[bool], optional
            filter by isHybridProject, by default None
        is_blue_hydrogen_project : Optional[bool], optional
            filter by isBlueHydrogenProject, by default None
        is_tendered_project : Optional[bool], optional
            filter by isTenderedProject, by default None
        tender_name : Optional[Union[list, str]], optional
            filter by tenderName, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        city_county : Optional[Union[list, str]], optional
            filter by cityCounty, by default None
        modified_date : Optional[datetime], optional
            filter by ``modifiedTime = x``, by default None
        modified_date_gt : Optional[datetime], optional
            filter by ``modifiedTime > x``, by default None
        modified_date_gte : Optional[datetime], optional
            filter by ``modifiedTime >= x``, by default None
        modified_date_lt : Optional[datetime], optional
            filter by ``modifiedTime < x``, by default None
        modified_date_lte : Optional[datetime], optional
            filter by ``modifiedTime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectPhase", project_phase))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("isHybridProject", is_hybrid_project))
        filter_params.append(list_to_filter("isBlueHydrogenProject", is_blue_hydrogen_project))
        filter_params.append(list_to_filter("isTenderedProject", is_tendered_project))
        filter_params.append(list_to_filter("tenderName", tender_name))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("cityCounty", city_county))
        filter_params.append(list_to_filter("modifiedTime", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/ccus",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_hydrogen(
        self,
        *,
        project_name: Optional[Union[list, str]] = None,
        project_phase: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        is_hybrid_project: Optional[bool] = None,
        is_tendered_project: Optional[bool] = None,
        tender_name: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
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
        API containing data points relating to hydrogen production plants.

        Parameters
        ----------
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_phase : Optional[Union[list, str]], optional
            filter by projectPhase, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        is_hybrid_project : Optional[bool], optional
            filter by isHybridProject, by default None
        is_tendered_project : Optional[bool], optional
            filter by isTenderedProject, by default None
        tender_name : Optional[Union[list, str]], optional
            filter by tenderName, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        modified_date : Optional[datetime], optional
            filter by ``modifiedTime = x``, by default None
        modified_date_gt : Optional[datetime], optional
            filter by ``modifiedTime > x``, by default None
        modified_date_gte : Optional[datetime], optional
            filter by ``modifiedTime >= x``, by default None
        modified_date_lt : Optional[datetime], optional
            filter by ``modifiedTime < x``, by default None
        modified_date_lte : Optional[datetime], optional
            filter by ``modifiedTime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectPhase", project_phase))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("isHybridProject", is_hybrid_project))
        filter_params.append(list_to_filter("isTenderedProject", is_tendered_project))
        filter_params.append(list_to_filter("tenderName", tender_name))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("modifiedTime", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/hydrogen",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_bioenergy(
        self,
        *,
        project_name: Optional[Union[list, str]] = None,
        project_phase: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        bioenergy_project_output_product: Optional[Union[list, str]] = None,
        is_hybrid_project: Optional[bool] = None,
        is_tendered_project: Optional[bool] = None,
        tender_name: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        city_county: Optional[Union[list, str]] = None,
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
        API containing data points relating to bioenergy production plants.

        Parameters
        ----------
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_phase : Optional[Union[list, str]], optional
            filter by projectPhase, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        bioenergy_project_output_product : Optional[Union[list, str]], optional
            filter by bioenergyProjectOutputProduct, by default None
        is_hybrid_project : Optional[bool], optional
            filter by isHybridProject, by default None
        is_tendered_project : Optional[bool], optional
            filter by isTenderedProject, by default None
        tender_name : Optional[Union[list, str]], optional
            filter by tenderName, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        city_county : Optional[Union[list, str]], optional
            filter by cityCounty, by default None
        modified_date : Optional[datetime], optional
            filter by ``modifiedTime = x``, by default None
        modified_date_gt : Optional[datetime], optional
            filter by ``modifiedTime > x``, by default None
        modified_date_gte : Optional[datetime], optional
            filter by ``modifiedTime >= x``, by default None
        modified_date_lt : Optional[datetime], optional
            filter by ``modifiedTime < x``, by default None
        modified_date_lte : Optional[datetime], optional
            filter by ``modifiedTime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        page : int, optional
            page number, by default 1
        page_size : int, optional
            page size, by default 5000
        raw : bool, optional
            return raw response, by default False
        paginate : bool, optional
            auto-paginate, by default False
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectPhase", project_phase))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("bioenergyProjectOutputProduct", bioenergy_project_output_product))
        filter_params.append(list_to_filter("isHybridProject", is_hybrid_project))
        filter_params.append(list_to_filter("isTenderedProject", is_tendered_project))
        filter_params.append(list_to_filter("tenderName", tender_name))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("cityCounty", city_county))
        filter_params.append(list_to_filter("modifiedTime", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/bioenergy",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_companies(
        self,
        *,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        project_status: Optional[Union[list, str]] = None,
        project_status_major: Optional[Union[list, str]] = None,
        company: Optional[Union[list, str]] = None,
        year_online: Optional[int] = None,
        date_online: Optional[date] = None,
        date_online_lt: Optional[date] = None,
        date_online_lte: Optional[date] = None,
        date_online_gt: Optional[date] = None,
        date_online_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        API containing data points relating to companies involved in clean energy projects.

        Parameters
        ----------
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        project_status : Optional[Union[list, str]], optional
            filter by projectStatus, by default None
        project_status_major : Optional[Union[list, str]], optional
            filter by projectStatusMajor, by default None
        company : Optional[Union[list, str]], optional
            filter by company, by default None
        year_online : Optional[int], optional
            filter by yearOnline, by default None
        date_online : Optional[date], optional
            filter by ``dateOnline = x``, by default None
        date_online_gt : Optional[date], optional
            filter by ``dateOnline > x``, by default None
        date_online_gte : Optional[date], optional
            filter by ``dateOnline >= x``, by default None
        date_online_lt : Optional[date], optional
            filter by ``dateOnline < x``, by default None
        date_online_lte : Optional[date], optional
            filter by ``dateOnline <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("projectStatus", project_status))
        filter_params.append(list_to_filter("projectStatusMajor", project_status_major))
        filter_params.append(list_to_filter("company", company))
        filter_params.append(list_to_filter("yearOnline", year_online))
        filter_params.append(list_to_filter("dateOnline", date_online))
        if date_online_gt is not None:
            filter_params.append(f'dateOnline > "{date_online_gt}"')
        if date_online_gte is not None:
            filter_params.append(f'dateOnline >= "{date_online_gte}"')
        if date_online_lt is not None:
            filter_params.append(f'dateOnline < "{date_online_lt}"')
        if date_online_lte is not None:
            filter_params.append(f'dateOnline <= "{date_online_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/companies",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_blue_hydrogen(
        self,
        *,
        record_id: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        ammonia_end_use_detail: Optional[Union[list, str]] = None,
        ammonia_operator: Optional[Union[list, str]] = None,
        ammonia_primary_owner: Optional[Union[list, str]] = None,
        ccus_project_application: Optional[Union[list, str]] = None,
        ccus_project_capture_technology: Optional[Union[list, str]] = None,
        ccus_project_carbon_use: Optional[Union[list, str]] = None,
        ccus_hub_name: Optional[Union[list, str]] = None,
        year_online: Optional[int] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        API containing data points relating to blue hydrogen production plants.

        Parameters
        ----------
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        ammonia_end_use_detail : Optional[Union[list, str]], optional
            filter by ammoniaEndUseDetail, by default None
        ammonia_operator : Optional[Union[list, str]], optional
            filter by ammoniaOperator, by default None
        ammonia_primary_owner : Optional[Union[list, str]], optional
            filter by ammoniaPrimaryOwner, by default None
        ccus_project_application : Optional[Union[list, str]], optional
            filter by ccusProjectApplication, by default None
        ccus_project_capture_technology : Optional[Union[list, str]], optional
            filter by ccusProjectCaptureTechnology, by default None
        ccus_project_carbon_use : Optional[Union[list, str]], optional
            filter by ccusProjectCarbonUse, by default None
        ccus_hub_name : Optional[Union[list, str]], optional
            filter by ccusHubName, by default None
        year_online : Optional[int], optional
            filter by yearOnline, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("ammoniaEndUseDetail", ammonia_end_use_detail))
        filter_params.append(list_to_filter("ammoniaOperator", ammonia_operator))
        filter_params.append(list_to_filter("ammoniaPrimaryOwner", ammonia_primary_owner))
        filter_params.append(list_to_filter("ccusProjectApplication", ccus_project_application))
        filter_params.append(list_to_filter("ccusProjectCaptureTechnology", ccus_project_capture_technology))
        filter_params.append(list_to_filter("ccusProjectCarbonUse", ccus_project_carbon_use))
        filter_params.append(list_to_filter("ccusHubName", ccus_hub_name))
        filter_params.append(list_to_filter("yearOnline", year_online))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/blue-hydrogen",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_all_clean_power(
        self,
        *,
        project_name: Optional[Union[list, str]] = None,
        project_phase: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        is_hybrid_project: Optional[bool] = None,
        is_tendered_project: Optional[bool] = None,
        tender_name: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        city_county: Optional[Union[list, str]] = None,
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
        API containing data points relating to all clean power projects.

        Parameters
        ----------
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_phase : Optional[Union[list, str]], optional
            filter by projectPhase, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        is_hybrid_project : Optional[bool], optional
            filter by isHybridProject, by default None
        is_tendered_project : Optional[bool], optional
            filter by isTenderedProject, by default None
        tender_name : Optional[Union[list, str]], optional
            filter by tenderName, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        city_county : Optional[Union[list, str]], optional
            filter by cityCounty, by default None
        modified_date : Optional[datetime], optional
            filter by ``modifiedTime = x``, by default None
        modified_date_gt : Optional[datetime], optional
            filter by ``modifiedTime > x``, by default None
        modified_date_gte : Optional[datetime], optional
            filter by ``modifiedTime >= x``, by default None
        modified_date_lt : Optional[datetime], optional
            filter by ``modifiedTime < x``, by default None
        modified_date_lte : Optional[datetime], optional
            filter by ``modifiedTime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectPhase", project_phase))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("isHybridProject", is_hybrid_project))
        filter_params.append(list_to_filter("isTenderedProject", is_tendered_project))
        filter_params.append(list_to_filter("tenderName", tender_name))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("cityCounty", city_county))
        filter_params.append(list_to_filter("modifiedTime", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/all-clean-power",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_tenders(
        self,
        *,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        is_hybrid_tender: Optional[bool] = None,
        tender_includes_storage: Optional[bool] = None,
        tender_jurisdiction: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        city_county: Optional[Union[list, str]] = None,
        tender_status: Optional[Union[list, str]] = None,
        offtake_renumeration_type: Optional[Union[list, str]] = None,
        offtake_remuneration_type_major: Optional[Union[list, str]] = None,
        tender_date_announced: Optional[date] = None,
        tender_date_announced_lt: Optional[date] = None,
        tender_date_announced_lte: Optional[date] = None,
        tender_date_announced_gt: Optional[date] = None,
        tender_date_announced_gte: Optional[date] = None,
        tender_date_closed: Optional[date] = None,
        tender_date_closed_lt: Optional[date] = None,
        tender_date_closed_lte: Optional[date] = None,
        tender_date_closed_gt: Optional[date] = None,
        tender_date_closed_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        API containing data points relating to clean energy project tenders.

        Parameters
        ----------
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        is_hybrid_tender : Optional[bool], optional
            filter by isHybridTender, by default None
        tender_includes_storage : Optional[bool], optional
            filter by tenderIncludesStorage, by default None
        tender_jurisdiction : Optional[Union[list, str]], optional
            filter by tenderJurisdiction, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        city_county : Optional[Union[list, str]], optional
            filter by cityCounty, by default None
        tender_status : Optional[Union[list, str]], optional
            filter by tenderStatus, by default None
        offtake_renumeration_type : Optional[Union[list, str]], optional
            filter by offtakeRenumerationType, by default None
        offtake_remuneration_type_major : Optional[Union[list, str]], optional
            filter by offtakeRemunerationTypeMajor, by default None
        tender_date_announced : Optional[date], optional
            filter by ``tenderDateAnnounced = x``, by default None
        tender_date_announced_gt : Optional[date], optional
            filter by ``tenderDateAnnounced > x``, by default None
        tender_date_announced_gte : Optional[date], optional
            filter by ``tenderDateAnnounced >= x``, by default None
        tender_date_announced_lt : Optional[date], optional
            filter by ``tenderDateAnnounced < x``, by default None
        tender_date_announced_lte : Optional[date], optional
            filter by ``tenderDateAnnounced <= x``, by default None
        tender_date_closed : Optional[date], optional
            filter by ``tenderDateClosed = x``, by default None
        tender_date_closed_gt : Optional[date], optional
            filter by ``tenderDateClosed > x``, by default None
        tender_date_closed_gte : Optional[date], optional
            filter by ``tenderDateClosed >= x``, by default None
        tender_date_closed_lt : Optional[date], optional
            filter by ``tenderDateClosed < x``, by default None
        tender_date_closed_lte : Optional[date], optional
            filter by ``tenderDateClosed <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("isHybridTender", is_hybrid_tender))
        filter_params.append(list_to_filter("tenderIncludesStorage", tender_includes_storage))
        filter_params.append(list_to_filter("tenderJurisdiction", tender_jurisdiction))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("cityCounty", city_county))
        filter_params.append(list_to_filter("tenderStatus", tender_status))
        filter_params.append(list_to_filter("offtakeRenumerationType", offtake_renumeration_type))
        filter_params.append(list_to_filter("offtakeRemunerationTypeMajor", offtake_remuneration_type_major))
        filter_params.append(list_to_filter("tenderDateAnnounced", tender_date_announced))
        if tender_date_announced_gt is not None:
            filter_params.append(f'tenderDateAnnounced > "{tender_date_announced_gt}"')
        if tender_date_announced_gte is not None:
            filter_params.append(f'tenderDateAnnounced >= "{tender_date_announced_gte}"')
        if tender_date_announced_lt is not None:
            filter_params.append(f'tenderDateAnnounced < "{tender_date_announced_lt}"')
        if tender_date_announced_lte is not None:
            filter_params.append(f'tenderDateAnnounced <= "{tender_date_announced_lte}"')
        filter_params.append(list_to_filter("tenderDateClosed", tender_date_closed))
        if tender_date_closed_gt is not None:
            filter_params.append(f'tenderDateClosed > "{tender_date_closed_gt}"')
        if tender_date_closed_gte is not None:
            filter_params.append(f'tenderDateClosed >= "{tender_date_closed_gte}"')
        if tender_date_closed_lt is not None:
            filter_params.append(f'tenderDateClosed < "{tender_date_closed_lt}"')
        if tender_date_closed_lte is not None:
            filter_params.append(f'tenderDateClosed <= "{tender_date_closed_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/tenders",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_offtake_contracts(
        self,
        *,
        record_id: Optional[Union[list, str]] = None,
        project_name: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        offtake_renumeration_type: Optional[Union[list, str]] = None,
        offtake_remuneration_type_major: Optional[Union[list, str]] = None,
        is_offtake_agreement_tendered: Optional[bool] = None,
        tender_name: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        iso_rto_region: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        city_county: Optional[Union[list, str]] = None,
        offtake_contract_sign_date: Optional[date] = None,
        offtake_contract_sign_date_lt: Optional[date] = None,
        offtake_contract_sign_date_lte: Optional[date] = None,
        offtake_contract_sign_date_gt: Optional[date] = None,
        offtake_contract_sign_date_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        API containing data points relating to clean energy offtake contracts.

        Parameters
        ----------
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        offtake_renumeration_type : Optional[Union[list, str]], optional
            filter by offtakeRenumerationType, by default None
        offtake_remuneration_type_major : Optional[Union[list, str]], optional
            filter by offtakeRemunerationTypeMajor, by default None
        is_offtake_agreement_tendered : Optional[bool], optional
            filter by offtakeAgreementTendered, by default None
        tender_name : Optional[Union[list, str]], optional
            filter by tenderName, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        iso_rto_region : Optional[Union[list, str]], optional
            filter by isoRtoRegion, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        city_county : Optional[Union[list, str]], optional
            filter by cityCounty, by default None
        offtake_contract_sign_date : Optional[date], optional
            filter by ``offtakeContractSignDate = x``, by default None
        offtake_contract_sign_date_gt : Optional[date], optional
            filter by ``offtakeContractSignDate > x``, by default None
        offtake_contract_sign_date_gte : Optional[date], optional
            filter by ``offtakeContractSignDate >= x``, by default None
        offtake_contract_sign_date_lt : Optional[date], optional
            filter by ``offtakeContractSignDate < x``, by default None
        offtake_contract_sign_date_lte : Optional[date], optional
            filter by ``offtakeContractSignDate <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("offtakeRenumerationType", offtake_renumeration_type))
        filter_params.append(list_to_filter("offtakeRemunerationTypeMajor", offtake_remuneration_type_major))
        filter_params.append(list_to_filter("offtakeAgreementTendered", is_offtake_agreement_tendered))
        filter_params.append(list_to_filter("tenderName", tender_name))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("isoRtoRegion", iso_rto_region))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("cityCounty", city_county))
        filter_params.append(list_to_filter("offtakeContractSignDate", offtake_contract_sign_date))
        if offtake_contract_sign_date_gt is not None:
            filter_params.append(f'offtakeContractSignDate > "{offtake_contract_sign_date_gt}"')
        if offtake_contract_sign_date_gte is not None:
            filter_params.append(f'offtakeContractSignDate >= "{offtake_contract_sign_date_gte}"')
        if offtake_contract_sign_date_lt is not None:
            filter_params.append(f'offtakeContractSignDate < "{offtake_contract_sign_date_lt}"')
        if offtake_contract_sign_date_lte is not None:
            filter_params.append(f'offtakeContractSignDate <= "{offtake_contract_sign_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/projects/v1/offtake-contracts",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_port_wind_offshore(
        self,
        *,
        offshore_wind_port_name: Optional[Union[list, str]] = None,
        offshore_wind_port_name_native: Optional[Union[list, str]] = None,
        offshore_wind_port_phase: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        technology_major: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        region_minor: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        city_county: Optional[Union[list, str]] = None,
        offshore_wind_port_location: Optional[Union[list, str]] = None,
        port_status: Optional[Union[list, str]] = None,
        port_status_major: Optional[Union[list, str]] = None,
        port_capacity_uom: Optional[Union[list, str]] = None,
        year_announced: Optional[int] = None,
        year_online: Optional[int] = None,
        year_offline: Optional[int] = None,
        is_floating_offshore_wind_capable: Optional[bool] = None,
        offshore_wind_port_waterbody: Optional[Union[list, str]] = None,
        offshore_wind_port_size_class: Optional[Union[list, str]] = None,
        offshore_wind_port_service_geography: Optional[Union[list, str]] = None,
        operator: Optional[Union[list, str]] = None,
        operator_tier_level: Optional[Union[list, str]] = None,
        operator_hq: Optional[Union[list, str]] = None,
        primary_owner: Optional[Union[list, str]] = None,
        other_owner: Optional[Union[list, str]] = None,
        legacy_owner: Optional[Union[list, str]] = None,
        offshore_wind_port_customer: Optional[Union[list, str]] = None,
        date_announced: Optional[date] = None,
        date_announced_lt: Optional[date] = None,
        date_announced_lte: Optional[date] = None,
        date_announced_gt: Optional[date] = None,
        date_announced_gte: Optional[date] = None,
        date_construction: Optional[date] = None,
        date_construction_lt: Optional[date] = None,
        date_construction_lte: Optional[date] = None,
        date_construction_gt: Optional[date] = None,
        date_construction_gte: Optional[date] = None,
        date_online: Optional[date] = None,
        date_online_lt: Optional[date] = None,
        date_online_lte: Optional[date] = None,
        date_online_gt: Optional[date] = None,
        date_online_gte: Optional[date] = None,
        date_offline: Optional[date] = None,
        date_offline_lt: Optional[date] = None,
        date_offline_lte: Optional[date] = None,
        date_offline_gt: Optional[date] = None,
        date_offline_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        API containing data points relating to offshore wind ports.

        Parameters
        ----------
        offshore_wind_port_name : Optional[Union[list, str]], optional
            filter by offshoreWindPortName, by default None
        offshore_wind_port_name_native : Optional[Union[list, str]], optional
            filter by offshoreWindPortNameNative, by default None
        offshore_wind_port_phase : Optional[Union[list, str]], optional
            filter by offshoreWindPortPhase, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        technology_major : Optional[Union[list, str]], optional
            filter by technologyMajor, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        region_minor : Optional[Union[list, str]], optional
            filter by regionMinor, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        city_county : Optional[Union[list, str]], optional
            filter by cityCounty, by default None
        offshore_wind_port_location : Optional[Union[list, str]], optional
            filter by offshoreWindPortLocation, by default None
        port_status : Optional[Union[list, str]], optional
            filter by portStatus, by default None
        port_status_major : Optional[Union[list, str]], optional
            filter by portStatusMajor, by default None
        port_capacity_uom : Optional[Union[list, str]], optional
            filter by portCapacityUOM, by default None
        year_announced : Optional[int], optional
            filter by yearAnnounced, by default None
        year_online : Optional[int], optional
            filter by yearOnline, by default None
        year_offline : Optional[int], optional
            filter by yearOffline, by default None
        is_floating_offshore_wind_capable : Optional[bool], optional
            filter by isFloatingOffshoreWindCapable, by default None
        offshore_wind_port_waterbody : Optional[Union[list, str]], optional
            filter by offshoreWindPortWaterbody, by default None
        offshore_wind_port_size_class : Optional[Union[list, str]], optional
            filter by offshoreWindPortSizeClass, by default None
        offshore_wind_port_service_geography : Optional[Union[list, str]], optional
            filter by offshoreWindPortServiceGeography, by default None
        operator : Optional[Union[list, str]], optional
            filter by operator, by default None
        operator_tier_level : Optional[Union[list, str]], optional
            filter by operatorTierLevel, by default None
        operator_hq : Optional[Union[list, str]], optional
            filter by operatorHQ, by default None
        primary_owner : Optional[Union[list, str]], optional
            filter by primaryOwner, by default None
        other_owner : Optional[Union[list, str]], optional
            filter by otherOwner, by default None
        legacy_owner : Optional[Union[list, str]], optional
            filter by legacyOwner, by default None
        offshore_wind_port_customer : Optional[Union[list, str]], optional
            filter by offshoreWindPortCustomer, by default None
        date_announced : Optional[date], optional
            filter by ``dateAnnounced = x``, by default None
        date_announced_gt : Optional[date], optional
            filter by ``dateAnnounced > x``, by default None
        date_announced_gte : Optional[date], optional
            filter by ``dateAnnounced >= x``, by default None
        date_announced_lt : Optional[date], optional
            filter by ``dateAnnounced < x``, by default None
        date_announced_lte : Optional[date], optional
            filter by ``dateAnnounced <= x``, by default None
        date_construction : Optional[date], optional
            filter by ``dateConstruction = x``, by default None
        date_construction_gt : Optional[date], optional
            filter by ``dateConstruction > x``, by default None
        date_construction_gte : Optional[date], optional
            filter by ``dateConstruction >= x``, by default None
        date_construction_lt : Optional[date], optional
            filter by ``dateConstruction < x``, by default None
        date_construction_lte : Optional[date], optional
            filter by ``dateConstruction <= x``, by default None
        date_online : Optional[date], optional
            filter by ``dateOnline = x``, by default None
        date_online_gt : Optional[date], optional
            filter by ``dateOnline > x``, by default None
        date_online_gte : Optional[date], optional
            filter by ``dateOnline >= x``, by default None
        date_online_lt : Optional[date], optional
            filter by ``dateOnline < x``, by default None
        date_online_lte : Optional[date], optional
            filter by ``dateOnline <= x``, by default None
        date_offline : Optional[date], optional
            filter by ``dateOffline = x``, by default None
        date_offline_gt : Optional[date], optional
            filter by ``dateOffline > x``, by default None
        date_offline_gte : Optional[date], optional
            filter by ``dateOffline >= x``, by default None
        date_offline_lt : Optional[date], optional
            filter by ``dateOffline < x``, by default None
        date_offline_lte : Optional[date], optional
            filter by ``dateOffline <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        page : int, optional
            page number, by default 1
        page_size : int, optional
            page size, by default 5000
        raw : bool, optional
            return raw response, by default False
        paginate : bool, optional
            auto-paginate, by default False
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("offshoreWindPortName", offshore_wind_port_name))
        filter_params.append(list_to_filter("offshoreWindPortNameNative", offshore_wind_port_name_native))
        filter_params.append(list_to_filter("offshoreWindPortPhase", offshore_wind_port_phase))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("technologyMajor", technology_major))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("cityCounty", city_county))
        filter_params.append(list_to_filter("offshoreWindPortLocation", offshore_wind_port_location))
        filter_params.append(list_to_filter("portStatus", port_status))
        filter_params.append(list_to_filter("portStatusMajor", port_status_major))
        filter_params.append(list_to_filter("portCapacityUOM", port_capacity_uom))
        filter_params.append(list_to_filter("yearAnnounced", year_announced))
        filter_params.append(list_to_filter("yearOnline", year_online))
        filter_params.append(list_to_filter("yearOffline", year_offline))
        filter_params.append(list_to_filter("isFloatingOffshoreWindCapable", is_floating_offshore_wind_capable))
        filter_params.append(list_to_filter("offshoreWindPortWaterbody", offshore_wind_port_waterbody))
        filter_params.append(list_to_filter("offshoreWindPortSizeClass", offshore_wind_port_size_class))
        filter_params.append(list_to_filter("offshoreWindPortServiceGeography", offshore_wind_port_service_geography))
        filter_params.append(list_to_filter("operator", operator))
        filter_params.append(list_to_filter("operatorTierLevel", operator_tier_level))
        filter_params.append(list_to_filter("operatorHQ", operator_hq))
        filter_params.append(list_to_filter("primaryOwner", primary_owner))
        filter_params.append(list_to_filter("otherOwner", other_owner))
        filter_params.append(list_to_filter("legacyOwner", legacy_owner))
        filter_params.append(list_to_filter("offshoreWindPortCustomer", offshore_wind_port_customer))
        filter_params.append(list_to_filter("dateAnnounced", date_announced))
        if date_announced_gt is not None:
            filter_params.append(f'dateAnnounced > "{date_announced_gt}"')
        if date_announced_gte is not None:
            filter_params.append(f'dateAnnounced >= "{date_announced_gte}"')
        if date_announced_lt is not None:
            filter_params.append(f'dateAnnounced < "{date_announced_lt}"')
        if date_announced_lte is not None:
            filter_params.append(f'dateAnnounced <= "{date_announced_lte}"')
        filter_params.append(list_to_filter("dateConstruction", date_construction))
        if date_construction_gt is not None:
            filter_params.append(f'dateConstruction > "{date_construction_gt}"')
        if date_construction_gte is not None:
            filter_params.append(f'dateConstruction >= "{date_construction_gte}"')
        if date_construction_lt is not None:
            filter_params.append(f'dateConstruction < "{date_construction_lt}"')
        if date_construction_lte is not None:
            filter_params.append(f'dateConstruction <= "{date_construction_lte}"')
        filter_params.append(list_to_filter("dateOnline", date_online))
        if date_online_gt is not None:
            filter_params.append(f'dateOnline > "{date_online_gt}"')
        if date_online_gte is not None:
            filter_params.append(f'dateOnline >= "{date_online_gte}"')
        if date_online_lt is not None:
            filter_params.append(f'dateOnline < "{date_online_lt}"')
        if date_online_lte is not None:
            filter_params.append(f'dateOnline <= "{date_online_lte}"')
        filter_params.append(list_to_filter("dateOffline", date_offline))
        if date_offline_gt is not None:
            filter_params.append(f'dateOffline > "{date_offline_gt}"')
        if date_offline_gte is not None:
            filter_params.append(f'dateOffline >= "{date_offline_gte}"')
        if date_offline_lt is not None:
            filter_params.append(f'dateOffline < "{date_offline_lt}"')
        if date_offline_lte is not None:
            filter_params.append(f'dateOffline <= "{date_offline_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/supplychain/v1/port-wind-offshore",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_wind_turbine_orders(
        self,
        *,
        wind_turbine_order_name: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        order_type: Optional[Union[list, str]] = None,
        order_major_type: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        project_name: Optional[Union[list, str]] = None,
        project_name_native: Optional[Union[list, str]] = None,
        order_customer: Optional[Union[list, str]] = None,
        wind_turbine_supplier_hq: Optional[Union[list, str]] = None,
        wind_turbine_supplier: Optional[Union[list, str]] = None,
        order_capacity_uom: Optional[Union[list, str]] = None,
        wind_turbine_configuration: Optional[Union[list, str]] = None,
        wind_turbine_model: Optional[Union[list, str]] = None,
        wind_turbine_platform: Optional[Union[list, str]] = None,
        wind_turbine_nominal_rating_segment: Optional[Union[list, str]] = None,
        wind_turbine_rotor_diameter_segment: Optional[Union[list, str]] = None,
        wind_turbine_specific_power_segment: Optional[Union[list, str]] = None,
        wind_turbine_iec_class: Optional[Union[list, str]] = None,
        wind_turbine_drivetrain_type: Optional[Union[list, str]] = None,
        wind_turbine_generator_type: Optional[Union[list, str]] = None,
        oandm_package: Optional[Union[list, str]] = None,
        year_announced: Optional[int] = None,
        half_year_announced: Optional[Union[list, str]] = None,
        quarter_announced: Optional[Union[list, str]] = None,
        year_expected_wind_turbine_shipment: Optional[int] = None,
        year_expected_wind_turbine_commissioning: Optional[int] = None,
        date_announced: Optional[date] = None,
        date_announced_lt: Optional[date] = None,
        date_announced_lte: Optional[date] = None,
        date_announced_gt: Optional[date] = None,
        date_announced_gte: Optional[date] = None,
        date_expected_wind_turbine_shipment: Optional[date] = None,
        date_expected_wind_turbine_shipment_lt: Optional[date] = None,
        date_expected_wind_turbine_shipment_lte: Optional[date] = None,
        date_expected_wind_turbine_shipment_gt: Optional[date] = None,
        date_expected_wind_turbine_shipment_gte: Optional[date] = None,
        date_expected_wind_turbine_commissioning: Optional[date] = None,
        date_expected_wind_turbine_commissioning_lt: Optional[date] = None,
        date_expected_wind_turbine_commissioning_lte: Optional[date] = None,
        date_expected_wind_turbine_commissioning_gt: Optional[date] = None,
        date_expected_wind_turbine_commissioning_gte: Optional[date] = None,
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
        API containing data points relating to wind turbine orders.

        Parameters
        ----------
        wind_turbine_order_name : Optional[Union[list, str]], optional
            filter by windTurbineOrderName, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        order_type : Optional[Union[list, str]], optional
            filter by orderType, by default None
        order_major_type : Optional[Union[list, str]], optional
            filter by orderMajorType, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_name_native : Optional[Union[list, str]], optional
            filter by projectNameNative, by default None
        order_customer : Optional[Union[list, str]], optional
            filter by orderCustomer, by default None
        wind_turbine_supplier_hq : Optional[Union[list, str]], optional
            filter by windTurbineSupplierHQ, by default None
        wind_turbine_supplier : Optional[Union[list, str]], optional
            filter by windTurbineSupplier, by default None
        order_capacity_uom : Optional[Union[list, str]], optional
            filter by orderCapacityUOM, by default None
        wind_turbine_configuration : Optional[Union[list, str]], optional
            filter by windTurbineConfiguration, by default None
        wind_turbine_model : Optional[Union[list, str]], optional
            filter by windTurbineModel, by default None
        wind_turbine_platform : Optional[Union[list, str]], optional
            filter by windTurbinePlatform, by default None
        wind_turbine_nominal_rating_segment : Optional[Union[list, str]], optional
            filter by windTurbineNominalRatingSegment, by default None
        wind_turbine_rotor_diameter_segment : Optional[Union[list, str]], optional
            filter by windTurbineRotorDiameterSegment, by default None
        wind_turbine_specific_power_segment : Optional[Union[list, str]], optional
            filter by windTurbineSpecificPowerSegment, by default None
        wind_turbine_iec_class : Optional[Union[list, str]], optional
            filter by windTurbineIecClass, by default None
        wind_turbine_drivetrain_type : Optional[Union[list, str]], optional
            filter by windTurbineDrivetrainType, by default None
        wind_turbine_generator_type : Optional[Union[list, str]], optional
            filter by windTurbineGeneratorType, by default None
        oandm_package : Optional[Union[list, str]], optional
            filter by oandMPackage, by default None
        year_announced : Optional[int], optional
            filter by yearAnnounced, by default None
        half_year_announced : Optional[Union[list, str]], optional
            filter by halfYearAnnounced, by default None
        quarter_announced : Optional[Union[list, str]], optional
            filter by quarterAnnounced, by default None
        year_expected_wind_turbine_shipment : Optional[int], optional
            filter by yearExpectedWindTurbineShipment, by default None
        year_expected_wind_turbine_commissioning : Optional[int], optional
            filter by yearExpectedWindTurbineCommissioning, by default None
        date_announced : Optional[date], optional
            filter by ``dateAnnounced = x``, by default None
        date_announced_gt : Optional[date], optional
            filter by ``dateAnnounced > x``, by default None
        date_announced_gte : Optional[date], optional
            filter by ``dateAnnounced >= x``, by default None
        date_announced_lt : Optional[date], optional
            filter by ``dateAnnounced < x``, by default None
        date_announced_lte : Optional[date], optional
            filter by ``dateAnnounced <= x``, by default None
        date_expected_wind_turbine_shipment : Optional[date], optional
            filter by ``dateExpectedWindTurbineShipment = x``, by default None
        date_expected_wind_turbine_shipment_gt : Optional[date], optional
            filter by ``dateExpectedWindTurbineShipment > x``, by default None
        date_expected_wind_turbine_shipment_gte : Optional[date], optional
            filter by ``dateExpectedWindTurbineShipment >= x``, by default None
        date_expected_wind_turbine_shipment_lt : Optional[date], optional
            filter by ``dateExpectedWindTurbineShipment < x``, by default None
        date_expected_wind_turbine_shipment_lte : Optional[date], optional
            filter by ``dateExpectedWindTurbineShipment <= x``, by default None
        date_expected_wind_turbine_commissioning : Optional[date], optional
            filter by ``dateExpectedWindTurbineCommissioning = x``, by default None
        date_expected_wind_turbine_commissioning_gt : Optional[date], optional
            filter by ``dateExpectedWindTurbineCommissioning > x``, by default None
        date_expected_wind_turbine_commissioning_gte : Optional[date], optional
            filter by ``dateExpectedWindTurbineCommissioning >= x``, by default None
        date_expected_wind_turbine_commissioning_lt : Optional[date], optional
            filter by ``dateExpectedWindTurbineCommissioning < x``, by default None
        date_expected_wind_turbine_commissioning_lte : Optional[date], optional
            filter by ``dateExpectedWindTurbineCommissioning <= x``, by default None
        modified_date : Optional[datetime], optional
            filter by ``modifiedTime = x``, by default None
        modified_date_gt : Optional[datetime], optional
            filter by ``modifiedTime > x``, by default None
        modified_date_gte : Optional[datetime], optional
            filter by ``modifiedTime >= x``, by default None
        modified_date_lt : Optional[datetime], optional
            filter by ``modifiedTime < x``, by default None
        modified_date_lte : Optional[datetime], optional
            filter by ``modifiedTime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        page : int, optional
            page number, by default 1
        page_size : int, optional
            page size, by default 5000
        raw : bool, optional
            return raw response, by default False
        paginate : bool, optional
            auto-paginate, by default False
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("windTurbineOrderName", wind_turbine_order_name))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("orderType", order_type))
        filter_params.append(list_to_filter("orderMajorType", order_major_type))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectNameNative", project_name_native))
        filter_params.append(list_to_filter("orderCustomer", order_customer))
        filter_params.append(list_to_filter("windTurbineSupplierHQ", wind_turbine_supplier_hq))
        filter_params.append(list_to_filter("windTurbineSupplier", wind_turbine_supplier))
        filter_params.append(list_to_filter("orderCapacityUOM", order_capacity_uom))
        filter_params.append(list_to_filter("windTurbineConfiguration", wind_turbine_configuration))
        filter_params.append(list_to_filter("windTurbineModel", wind_turbine_model))
        filter_params.append(list_to_filter("windTurbinePlatform", wind_turbine_platform))
        filter_params.append(list_to_filter("windTurbineNominalRatingSegment", wind_turbine_nominal_rating_segment))
        filter_params.append(list_to_filter("windTurbineRotorDiameterSegment", wind_turbine_rotor_diameter_segment))
        filter_params.append(list_to_filter("windTurbineSpecificPowerSegment", wind_turbine_specific_power_segment))
        filter_params.append(list_to_filter("windTurbineIecClass", wind_turbine_iec_class))
        filter_params.append(list_to_filter("windTurbineDrivetrainType", wind_turbine_drivetrain_type))
        filter_params.append(list_to_filter("windTurbineGeneratorType", wind_turbine_generator_type))
        filter_params.append(list_to_filter("oandMPackage", oandm_package))
        filter_params.append(list_to_filter("yearAnnounced", year_announced))
        filter_params.append(list_to_filter("halfYearAnnounced", half_year_announced))
        filter_params.append(list_to_filter("quarterAnnounced", quarter_announced))
        filter_params.append(list_to_filter("yearExpectedWindTurbineShipment", year_expected_wind_turbine_shipment))
        filter_params.append(list_to_filter("yearExpectedWindTurbineCommissioning", year_expected_wind_turbine_commissioning))
        filter_params.append(list_to_filter("dateAnnounced", date_announced))
        if date_announced_gt is not None:
            filter_params.append(f'dateAnnounced > "{date_announced_gt}"')
        if date_announced_gte is not None:
            filter_params.append(f'dateAnnounced >= "{date_announced_gte}"')
        if date_announced_lt is not None:
            filter_params.append(f'dateAnnounced < "{date_announced_lt}"')
        if date_announced_lte is not None:
            filter_params.append(f'dateAnnounced <= "{date_announced_lte}"')
        filter_params.append(list_to_filter("dateExpectedWindTurbineShipment", date_expected_wind_turbine_shipment))
        if date_expected_wind_turbine_shipment_gt is not None:
            filter_params.append(f'dateExpectedWindTurbineShipment > "{date_expected_wind_turbine_shipment_gt}"')
        if date_expected_wind_turbine_shipment_gte is not None:
            filter_params.append(f'dateExpectedWindTurbineShipment >= "{date_expected_wind_turbine_shipment_gte}"')
        if date_expected_wind_turbine_shipment_lt is not None:
            filter_params.append(f'dateExpectedWindTurbineShipment < "{date_expected_wind_turbine_shipment_lt}"')
        if date_expected_wind_turbine_shipment_lte is not None:
            filter_params.append(f'dateExpectedWindTurbineShipment <= "{date_expected_wind_turbine_shipment_lte}"')
        filter_params.append(list_to_filter("dateExpectedWindTurbineCommissioning", date_expected_wind_turbine_commissioning))
        if date_expected_wind_turbine_commissioning_gt is not None:
            filter_params.append(f'dateExpectedWindTurbineCommissioning > "{date_expected_wind_turbine_commissioning_gt}"')
        if date_expected_wind_turbine_commissioning_gte is not None:
            filter_params.append(f'dateExpectedWindTurbineCommissioning >= "{date_expected_wind_turbine_commissioning_gte}"')
        if date_expected_wind_turbine_commissioning_lt is not None:
            filter_params.append(f'dateExpectedWindTurbineCommissioning < "{date_expected_wind_turbine_commissioning_lt}"')
        if date_expected_wind_turbine_commissioning_lte is not None:
            filter_params.append(f'dateExpectedWindTurbineCommissioning <= "{date_expected_wind_turbine_commissioning_lte}"')
        filter_params.append(list_to_filter("modifiedTime", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/supplychain/v1/wind-turbine-orders",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_battery_orders(
        self,
        *,
        battery_order_name: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        order_type: Optional[Union[list, str]] = None,
        order_major_type: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        project_name: Optional[Union[list, str]] = None,
        project_name_native: Optional[Union[list, str]] = None,
        order_customer: Optional[Union[list, str]] = None,
        order_customer_hq: Optional[Union[list, str]] = None,
        battery_order_customer_type: Optional[Union[list, str]] = None,
        battery_supplier: Optional[Union[list, str]] = None,
        battery_supplier_hq: Optional[Union[list, str]] = None,
        battery_supplier_type: Optional[Union[list, str]] = None,
        battery_product_type_supplied: Optional[Union[list, str]] = None,
        battery_product_supplied: Optional[Union[list, str]] = None,
        battery_cell_technology: Optional[Union[list, str]] = None,
        battery_cell_technology_major: Optional[Union[list, str]] = None,
        battery_manufacturing_location: Optional[Union[list, str]] = None,
        battery_manufacturing_factory: Optional[Union[list, str]] = None,
        order_capacity_uom: Optional[Union[list, str]] = None,
        year_announced: Optional[int] = None,
        half_year_announced: Optional[Union[list, str]] = None,
        quarter_announced: Optional[Union[list, str]] = None,
        battery_shipment_start_year: Optional[int] = None,
        battery_shipment_end_year: Optional[int] = None,
        expected_project_commissioning_year: Optional[int] = None,
        date_announced: Optional[date] = None,
        date_announced_lt: Optional[date] = None,
        date_announced_lte: Optional[date] = None,
        date_announced_gt: Optional[date] = None,
        date_announced_gte: Optional[date] = None,
        battery_shipment_start_date: Optional[date] = None,
        battery_shipment_start_date_lt: Optional[date] = None,
        battery_shipment_start_date_lte: Optional[date] = None,
        battery_shipment_start_date_gt: Optional[date] = None,
        battery_shipment_start_date_gte: Optional[date] = None,
        battery_shipment_end_date: Optional[date] = None,
        battery_shipment_end_date_lt: Optional[date] = None,
        battery_shipment_end_date_lte: Optional[date] = None,
        battery_shipment_end_date_gt: Optional[date] = None,
        battery_shipment_end_date_gte: Optional[date] = None,
        expected_project_commissioning_date: Optional[date] = None,
        expected_project_commissioning_date_lt: Optional[date] = None,
        expected_project_commissioning_date_lte: Optional[date] = None,
        expected_project_commissioning_date_gt: Optional[date] = None,
        expected_project_commissioning_date_gte: Optional[date] = None,
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
        API containing data points relating to battery (energy storage) orders.

        Parameters
        ----------
        battery_order_name : Optional[Union[list, str]], optional
            filter by batteryOrderName, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordID, by default None
        order_type : Optional[Union[list, str]], optional
            filter by orderType, by default None
        order_major_type : Optional[Union[list, str]], optional
            filter by orderMajorType, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_name_native : Optional[Union[list, str]], optional
            filter by projectNameNative, by default None
        order_customer : Optional[Union[list, str]], optional
            filter by orderCustomer, by default None
        order_customer_hq : Optional[Union[list, str]], optional
            filter by orderCustomerHQ, by default None
        battery_order_customer_type : Optional[Union[list, str]], optional
            filter by batteryOrderCustomerType, by default None
        battery_supplier : Optional[Union[list, str]], optional
            filter by batterySupplier, by default None
        battery_supplier_hq : Optional[Union[list, str]], optional
            filter by batterySupplierHQ, by default None
        battery_supplier_type : Optional[Union[list, str]], optional
            filter by batterySupplierType, by default None
        battery_product_type_supplied : Optional[Union[list, str]], optional
            filter by batteryProductTypeSupplied, by default None
        battery_product_supplied : Optional[Union[list, str]], optional
            filter by batteryProductSupplied, by default None
        battery_cell_technology : Optional[Union[list, str]], optional
            filter by batteryCellTechnology, by default None
        battery_cell_technology_major : Optional[Union[list, str]], optional
            filter by batteryCellTechnologyMajor, by default None
        battery_manufacturing_location : Optional[Union[list, str]], optional
            filter by batteryManufacturingLocation, by default None
        battery_manufacturing_factory : Optional[Union[list, str]], optional
            filter by batteryManufacturingFactory, by default None
        order_capacity_uom : Optional[Union[list, str]], optional
            filter by orderCapacityUOM, by default None
        year_announced : Optional[int], optional
            filter by yearAnnounced, by default None
        half_year_announced : Optional[Union[list, str]], optional
            filter by halfYearAnnounced, by default None
        quarter_announced : Optional[Union[list, str]], optional
            filter by quarterAnnounced, by default None
        battery_shipment_start_year : Optional[int], optional
            filter by batteryShipmentStartYear, by default None
        battery_shipment_end_year : Optional[int], optional
            filter by batteryShipmentEndYear, by default None
        expected_project_commissioning_year : Optional[int], optional
            filter by expectedProjectCommissioningYear, by default None
        date_announced : Optional[date], optional
            filter by ``dateAnnounced = x``, by default None
        date_announced_gt : Optional[date], optional
            filter by ``dateAnnounced > x``, by default None
        date_announced_gte : Optional[date], optional
            filter by ``dateAnnounced >= x``, by default None
        date_announced_lt : Optional[date], optional
            filter by ``dateAnnounced < x``, by default None
        date_announced_lte : Optional[date], optional
            filter by ``dateAnnounced <= x``, by default None
        battery_shipment_start_date : Optional[date], optional
            filter by ``batteryShipmentStartDate = x``, by default None
        battery_shipment_start_date_gt : Optional[date], optional
            filter by ``batteryShipmentStartDate > x``, by default None
        battery_shipment_start_date_gte : Optional[date], optional
            filter by ``batteryShipmentStartDate >= x``, by default None
        battery_shipment_start_date_lt : Optional[date], optional
            filter by ``batteryShipmentStartDate < x``, by default None
        battery_shipment_start_date_lte : Optional[date], optional
            filter by ``batteryShipmentStartDate <= x``, by default None
        battery_shipment_end_date : Optional[date], optional
            filter by ``batteryShipmentEndDate = x``, by default None
        battery_shipment_end_date_gt : Optional[date], optional
            filter by ``batteryShipmentEndDate > x``, by default None
        battery_shipment_end_date_gte : Optional[date], optional
            filter by ``batteryShipmentEndDate >= x``, by default None
        battery_shipment_end_date_lt : Optional[date], optional
            filter by ``batteryShipmentEndDate < x``, by default None
        battery_shipment_end_date_lte : Optional[date], optional
            filter by ``batteryShipmentEndDate <= x``, by default None
        expected_project_commissioning_date : Optional[date], optional
            filter by ``expectedProjectCommissioningDate = x``, by default None
        expected_project_commissioning_date_gt : Optional[date], optional
            filter by ``expectedProjectCommissioningDate > x``, by default None
        expected_project_commissioning_date_gte : Optional[date], optional
            filter by ``expectedProjectCommissioningDate >= x``, by default None
        expected_project_commissioning_date_lt : Optional[date], optional
            filter by ``expectedProjectCommissioningDate < x``, by default None
        expected_project_commissioning_date_lte : Optional[date], optional
            filter by ``expectedProjectCommissioningDate <= x``, by default None
        modified_date : Optional[datetime], optional
            filter by ``modifiedTime = x``, by default None
        modified_date_gt : Optional[datetime], optional
            filter by ``modifiedTime > x``, by default None
        modified_date_gte : Optional[datetime], optional
            filter by ``modifiedTime >= x``, by default None
        modified_date_lt : Optional[datetime], optional
            filter by ``modifiedTime < x``, by default None
        modified_date_lte : Optional[datetime], optional
            filter by ``modifiedTime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        page : int, optional
            page number, by default 1
        page_size : int, optional
            page size, by default 5000
        raw : bool, optional
            return raw response, by default False
        paginate : bool, optional
            auto-paginate, by default False
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("batteryOrderName", battery_order_name))
        filter_params.append(list_to_filter("recordID", record_id))
        filter_params.append(list_to_filter("orderType", order_type))
        filter_params.append(list_to_filter("orderMajorType", order_major_type))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectNameNative", project_name_native))
        filter_params.append(list_to_filter("orderCustomer", order_customer))
        filter_params.append(list_to_filter("orderCustomerHQ", order_customer_hq))
        filter_params.append(list_to_filter("batteryOrderCustomerType", battery_order_customer_type))
        filter_params.append(list_to_filter("batterySupplier", battery_supplier))
        filter_params.append(list_to_filter("batterySupplierHQ", battery_supplier_hq))
        filter_params.append(list_to_filter("batterySupplierType", battery_supplier_type))
        filter_params.append(list_to_filter("batteryProductTypeSupplied", battery_product_type_supplied))
        filter_params.append(list_to_filter("batteryProductSupplied", battery_product_supplied))
        filter_params.append(list_to_filter("batteryCellTechnology", battery_cell_technology))
        filter_params.append(list_to_filter("batteryCellTechnologyMajor", battery_cell_technology_major))
        filter_params.append(list_to_filter("batteryManufacturingLocation", battery_manufacturing_location))
        filter_params.append(list_to_filter("batteryManufacturingFactory", battery_manufacturing_factory))
        filter_params.append(list_to_filter("orderCapacityUOM", order_capacity_uom))
        filter_params.append(list_to_filter("yearAnnounced", year_announced))
        filter_params.append(list_to_filter("halfYearAnnounced", half_year_announced))
        filter_params.append(list_to_filter("quarterAnnounced", quarter_announced))
        filter_params.append(list_to_filter("batteryShipmentStartYear", battery_shipment_start_year))
        filter_params.append(list_to_filter("batteryShipmentEndYear", battery_shipment_end_year))
        filter_params.append(list_to_filter("expectedProjectCommissioningYear", expected_project_commissioning_year))
        filter_params.append(list_to_filter("dateAnnounced", date_announced))
        if date_announced_gt is not None:
            filter_params.append(f'dateAnnounced > "{date_announced_gt}"')
        if date_announced_gte is not None:
            filter_params.append(f'dateAnnounced >= "{date_announced_gte}"')
        if date_announced_lt is not None:
            filter_params.append(f'dateAnnounced < "{date_announced_lt}"')
        if date_announced_lte is not None:
            filter_params.append(f'dateAnnounced <= "{date_announced_lte}"')
        filter_params.append(list_to_filter("batteryShipmentStartDate", battery_shipment_start_date))
        if battery_shipment_start_date_gt is not None:
            filter_params.append(f'batteryShipmentStartDate > "{battery_shipment_start_date_gt}"')
        if battery_shipment_start_date_gte is not None:
            filter_params.append(f'batteryShipmentStartDate >= "{battery_shipment_start_date_gte}"')
        if battery_shipment_start_date_lt is not None:
            filter_params.append(f'batteryShipmentStartDate < "{battery_shipment_start_date_lt}"')
        if battery_shipment_start_date_lte is not None:
            filter_params.append(f'batteryShipmentStartDate <= "{battery_shipment_start_date_lte}"')
        filter_params.append(list_to_filter("batteryShipmentEndDate", battery_shipment_end_date))
        if battery_shipment_end_date_gt is not None:
            filter_params.append(f'batteryShipmentEndDate > "{battery_shipment_end_date_gt}"')
        if battery_shipment_end_date_gte is not None:
            filter_params.append(f'batteryShipmentEndDate >= "{battery_shipment_end_date_gte}"')
        if battery_shipment_end_date_lt is not None:
            filter_params.append(f'batteryShipmentEndDate < "{battery_shipment_end_date_lt}"')
        if battery_shipment_end_date_lte is not None:
            filter_params.append(f'batteryShipmentEndDate <= "{battery_shipment_end_date_lte}"')
        filter_params.append(list_to_filter("expectedProjectCommissioningDate", expected_project_commissioning_date))
        if expected_project_commissioning_date_gt is not None:
            filter_params.append(f'expectedProjectCommissioningDate > "{expected_project_commissioning_date_gt}"')
        if expected_project_commissioning_date_gte is not None:
            filter_params.append(f'expectedProjectCommissioningDate >= "{expected_project_commissioning_date_gte}"')
        if expected_project_commissioning_date_lt is not None:
            filter_params.append(f'expectedProjectCommissioningDate < "{expected_project_commissioning_date_lt}"')
        if expected_project_commissioning_date_lte is not None:
            filter_params.append(f'expectedProjectCommissioningDate <= "{expected_project_commissioning_date_lte}"')
        filter_params.append(list_to_filter("modifiedTime", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/supplychain/v1/battery-orders",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_electrolyzer_orders(
        self,
        *,
        electrolyzer_order_name: Optional[Union[list, str]] = None,
        record_id: Optional[Union[list, str]] = None,
        order_type: Optional[Union[list, str]] = None,
        order_major_type: Optional[Union[list, str]] = None,
        technology: Optional[Union[list, str]] = None,
        geography: Optional[Union[list, str]] = None,
        region_major: Optional[Union[list, str]] = None,
        state_province: Optional[Union[list, str]] = None,
        project_name: Optional[Union[list, str]] = None,
        project_name_native: Optional[Union[list, str]] = None,
        includes_power_supply: Optional[Union[list, str]] = None,
        includes_separation: Optional[Union[list, str]] = None,
        includes_purification: Optional[Union[list, str]] = None,
        hydrogen_project_name: Optional[Union[list, str]] = None,
        hydrogen_project_record_id: Optional[Union[list, str]] = None,
        order_customer: Optional[Union[list, str]] = None,
        order_customer_head_quarters: Optional[Union[list, str]] = None,
        year_announced: Optional[int] = None,
        half_year_announced: Optional[Union[list, str]] = None,
        quarter_announced: Optional[Union[list, str]] = None,
        date_announced: Optional[date] = None,
        date_announced_lt: Optional[date] = None,
        date_announced_lte: Optional[date] = None,
        date_announced_gt: Optional[date] = None,
        date_announced_gte: Optional[date] = None,
        modified_datetime: Optional[datetime] = None,
        modified_datetime_lt: Optional[datetime] = None,
        modified_datetime_lte: Optional[datetime] = None,
        modified_datetime_gt: Optional[datetime] = None,
        modified_datetime_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        API containing data points relating to electrolyzer supply orders.

        Parameters
        ----------
        electrolyzer_order_name : Optional[Union[list, str]], optional
            filter by electrolyzerOrderName, by default None
        record_id : Optional[Union[list, str]], optional
            filter by recordId, by default None
        order_type : Optional[Union[list, str]], optional
            filter by orderType, by default None
        order_major_type : Optional[Union[list, str]], optional
            filter by orderMajorType, by default None
        technology : Optional[Union[list, str]], optional
            filter by technology, by default None
        geography : Optional[Union[list, str]], optional
            filter by geography, by default None
        region_major : Optional[Union[list, str]], optional
            filter by regionMajor, by default None
        state_province : Optional[Union[list, str]], optional
            filter by stateProvince, by default None
        project_name : Optional[Union[list, str]], optional
            filter by projectName, by default None
        project_name_native : Optional[Union[list, str]], optional
            filter by projectNameNative, by default None
        includes_power_supply : Optional[Union[list, str]], optional
            filter by includesPowerSupply, by default None
        includes_separation : Optional[Union[list, str]], optional
            filter by includesSeparation, by default None
        includes_purification : Optional[Union[list, str]], optional
            filter by includesPurification, by default None
        hydrogen_project_name : Optional[Union[list, str]], optional
            filter by hydrogenProjectName, by default None
        hydrogen_project_record_id : Optional[Union[list, str]], optional
            filter by hydrogenProjectRecordId, by default None
        order_customer : Optional[Union[list, str]], optional
            filter by orderCustomer, by default None
        order_customer_head_quarters : Optional[Union[list, str]], optional
            filter by orderCustomerHeadQuarters, by default None
        year_announced : Optional[int], optional
            filter by yearAnnounced, by default None
        half_year_announced : Optional[Union[list, str]], optional
            filter by halfYearAnnounced, by default None
        quarter_announced : Optional[Union[list, str]], optional
            filter by quarterAnnounced, by default None
        date_announced : Optional[date], optional
            filter by ``dateAnnounced = x``, by default None
        date_announced_gt : Optional[date], optional
            filter by ``dateAnnounced > x``, by default None
        date_announced_gte : Optional[date], optional
            filter by ``dateAnnounced >= x``, by default None
        date_announced_lt : Optional[date], optional
            filter by ``dateAnnounced < x``, by default None
        date_announced_lte : Optional[date], optional
            filter by ``dateAnnounced <= x``, by default None
        modified_datetime : Optional[datetime], optional
            filter by ``modifiedDatetime = x``, by default None
        modified_datetime_gt : Optional[datetime], optional
            filter by ``modifiedDatetime > x``, by default None
        modified_datetime_gte : Optional[datetime], optional
            filter by ``modifiedDatetime >= x``, by default None
        modified_datetime_lt : Optional[datetime], optional
            filter by ``modifiedDatetime < x``, by default None
        modified_datetime_lte : Optional[datetime], optional
            filter by ``modifiedDatetime <= x``, by default None
        filter_exp : Optional[str], optional
            pass-thru filter expression, by default None
        page : int, optional
            page number, by default 1
        page_size : int, optional
            page size, by default 5000
        raw : bool, optional
            return raw response, by default False
        paginate : bool, optional
            auto-paginate, by default False
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("electrolyzerOrderName", electrolyzer_order_name))
        filter_params.append(list_to_filter("recordId", record_id))
        filter_params.append(list_to_filter("orderType", order_type))
        filter_params.append(list_to_filter("orderMajorType", order_major_type))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectNameNative", project_name_native))
        filter_params.append(list_to_filter("includesPowerSupply", includes_power_supply))
        filter_params.append(list_to_filter("includesSeparation", includes_separation))
        filter_params.append(list_to_filter("includesPurification", includes_purification))
        filter_params.append(list_to_filter("hydrogenProjectName", hydrogen_project_name))
        filter_params.append(list_to_filter("hydrogenProjectRecordId", hydrogen_project_record_id))
        filter_params.append(list_to_filter("orderCustomer", order_customer))
        filter_params.append(list_to_filter("orderCustomerHeadQuarters", order_customer_head_quarters))
        filter_params.append(list_to_filter("yearAnnounced", year_announced))
        filter_params.append(list_to_filter("halfYearAnnounced", half_year_announced))
        filter_params.append(list_to_filter("quarterAnnounced", quarter_announced))
        filter_params.append(list_to_filter("dateAnnounced", date_announced))
        if date_announced_gt is not None:
            filter_params.append(f'dateAnnounced > "{date_announced_gt}"')
        if date_announced_gte is not None:
            filter_params.append(f'dateAnnounced >= "{date_announced_gte}"')
        if date_announced_lt is not None:
            filter_params.append(f'dateAnnounced < "{date_announced_lt}"')
        if date_announced_lte is not None:
            filter_params.append(f'dateAnnounced <= "{date_announced_lte}"')
        filter_params.append(list_to_filter("modifiedDatetime", modified_datetime))
        if modified_datetime_gt is not None:
            filter_params.append(f'modifiedDatetime > "{modified_datetime_gt}"')
        if modified_datetime_gte is not None:
            filter_params.append(f'modifiedDatetime >= "{modified_datetime_gte}"')
        if modified_datetime_lt is not None:
            filter_params.append(f'modifiedDatetime < "{modified_datetime_lt}"')
        if modified_datetime_lte is not None:
            filter_params.append(f'modifiedDatetime <= "{modified_datetime_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/cet/supplychain/v1/electrolyzer-orders",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_unique_values(
        self,
        dataset: _datasets,
        columns: Optional[Union[list, str]],
        filter_exp: Optional[str] = None,
    ) -> DataFrame:
        """
        Get unique values for specified columns in a dataset, optionally filtered by an expression.

        Args:
            dataset (str): The CET dataset name:
                - "onshore-wind", "offshore-wind", "energy-storage", "pv", "geothermal"
                - "smr", "csp", "ccus", "hydrogen", "bioenergy", "all-clean-power"
                - "tenders", "offtake-contracts", "companies", "blue-hydrogen"
                - "port-wind-offshore", "wind-turbine-orders", "battery-orders", "electrolyzer-orders"
            columns (list[str] or str): Column names to get unique values for.
            filter_exp (str, optional): Filter expression to limit results.

        Returns:
            pd.DataFrame: DataFrame with unique combinations of the specified columns.

        Example:
            >>> cet.get_unique_values("onshore-wind", "geography")
            >>> cet.get_unique_values("battery-orders", "technology")
        """
        dataset_to_path = {
            "onshore-wind": "/analytics/cet/projects/v1/onshore-wind",
            "offshore-wind": "/analytics/cet/projects/v1/offshore-wind",
            "energy-storage": "/analytics/cet/projects/v1/energy-storage",
            "pv": "/analytics/cet/projects/v1/pv",
            "geothermal": "/analytics/cet/projects/v1/geothermal",
            "smr": "/analytics/cet/projects/v1/smr",
            "csp": "/analytics/cet/projects/v1/csp",
            "ccus": "/analytics/cet/projects/v1/ccus",
            "hydrogen": "/analytics/cet/projects/v1/hydrogen",
            "bioenergy": "/analytics/cet/projects/v1/bioenergy",
            "all-clean-power": "/analytics/cet/projects/v1/all-clean-power",
            "tenders": "/analytics/cet/projects/v1/tenders",
            "offtake-contracts": "/analytics/cet/projects/v1/offtake-contracts",
            "companies": "/analytics/cet/projects/v1/companies",
            "blue-hydrogen": "/analytics/cet/projects/v1/blue-hydrogen",
            "port-wind-offshore": "/analytics/cet/supplychain/v1/port-wind-offshore",
            "wind-turbine-orders": "/analytics/cet/supplychain/v1/wind-turbine-orders",
            "battery-orders": "/analytics/cet/supplychain/v1/battery-orders",
            "electrolyzer-orders": "/analytics/cet/supplychain/v1/electrolyzer-orders",
        }

        if dataset not in dataset_to_path:
            valid = "\n".join(dataset_to_path.keys())
            print(f"Dataset '{dataset}' not found. Valid Datasets:\n", valid)
            raise ValueError(f"dataset '{dataset}' not found")

        path = dataset_to_path[dataset]
        col_value = ", ".join(columns) if isinstance(columns, list) else columns or ""
        params = {"groupBy": col_value, "pageSize": 5000}

        if filter_exp is not None:
            params.update({"filter": filter_exp})

        def to_df(resp: Response):
            j = resp.json()
            return DataFrame(j["aggResultValue"])

        return get_data(path, params, to_df, paginate=True)

    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
        j = resp.json()
        df = pd.json_normalize(j["results"])  # type: ignore

        # Policy dates
        if "policyStartDate" in df.columns:
            df["policyStartDate"] = pd.to_datetime(df["policyStartDate"])  # type: ignore

        if "policyEndDate" in df.columns:
            df["policyEndDate"] = pd.to_datetime(df["policyEndDate"])  # type: ignore

        if "announcedDate" in df.columns:
            df["announcedDate"] = pd.to_datetime(df["announcedDate"])  # type: ignore

        if "targetDate" in df.columns:
            df["targetDate"] = pd.to_datetime(df["targetDate"])  # type: ignore

        if "announcementDate" in df.columns:
            df["announcementDate"] = pd.to_datetime(df["announcementDate"])  # type: ignore

        # Project dates
        if "dateAnnounced" in df.columns:
            df["dateAnnounced"] = pd.to_datetime(df["dateAnnounced"])  # type: ignore

        if "datePermittingCompletion" in df.columns:
            df["datePermittingCompletion"] = pd.to_datetime(df["datePermittingCompletion"])  # type: ignore

        if "dateFinanced" in df.columns:
            df["dateFinanced"] = pd.to_datetime(df["dateFinanced"])  # type: ignore

        if "dateConstruction" in df.columns:
            df["dateConstruction"] = pd.to_datetime(df["dateConstruction"])  # type: ignore

        if "dateCompleted" in df.columns:
            df["dateCompleted"] = pd.to_datetime(df["dateCompleted"])  # type: ignore

        if "dateOnline" in df.columns:
            df["dateOnline"] = pd.to_datetime(df["dateOnline"])  # type: ignore

        if "dateOffline" in df.columns:
            df["dateOffline"] = pd.to_datetime(df["dateOffline"])  # type: ignore

        if "oandMContractStartDate" in df.columns:
            df["oandMContractStartDate"] = pd.to_datetime(df["oandMContractStartDate"])  # type: ignore

        if "oandMContractEndDate" in df.columns:
            df["oandMContractEndDate"] = pd.to_datetime(df["oandMContractEndDate"])  # type: ignore

        if "createdTime" in df.columns:
            df["createdTime"] = pd.to_datetime(df["createdTime"])  # type: ignore

        if "modifiedTime" in df.columns:
            df["modifiedTime"] = pd.to_datetime(df["modifiedTime"])  # type: ignore

        if "dateGeoProjectExplorationStart" in df.columns:
            df["dateGeoProjectExplorationStart"] = pd.to_datetime(df["dateGeoProjectExplorationStart"])  # type: ignore

        if "dateGeoProjectExplorationEnd" in df.columns:
            df["dateGeoProjectExplorationEnd"] = pd.to_datetime(df["dateGeoProjectExplorationEnd"])  # type: ignore

        if "dateCompletedAfterFeasibility" in df.columns:
            df["dateCompletedAfterFeasibility"] = pd.to_datetime(df["dateCompletedAfterFeasibility"])  # type: ignore

        if "tenderDateAnnounced" in df.columns:
            df["tenderDateAnnounced"] = pd.to_datetime(df["tenderDateAnnounced"])  # type: ignore

        if "tenderDateOpened" in df.columns:
            df["tenderDateOpened"] = pd.to_datetime(df["tenderDateOpened"])  # type: ignore

        if "tenderDateClosed" in df.columns:
            df["tenderDateClosed"] = pd.to_datetime(df["tenderDateClosed"])  # type: ignore

        if "tenderProjectDateOnlineDeadline" in df.columns:
            df["tenderProjectDateOnlineDeadline"] = pd.to_datetime(df["tenderProjectDateOnlineDeadline"])  # type: ignore

        if "offtakeContractSignDate" in df.columns:
            df["offtakeContractSignDate"] = pd.to_datetime(df["offtakeContractSignDate"])  # type: ignore

        if "offtakeContractEndDate" in df.columns:
            df["offtakeContractEndDate"] = pd.to_datetime(df["offtakeContractEndDate"])  # type: ignore

        if "dateConstructionEstimated" in df.columns:
            df["dateConstructionEstimated"] = pd.to_datetime(df["dateConstructionEstimated"])  # type: ignore

        if "ccusOandMContractEndDate" in df.columns:
            df["ccusOandMContractEndDate"] = pd.to_datetime(df["ccusOandMContractEndDate"])  # type: ignore

        if "hydrogenOandMContractEndDate" in df.columns:
            df["hydrogenOandMContractEndDate"] = pd.to_datetime(df["hydrogenOandMContractEndDate"])  # type: ignore

        if "ccusOandMContractStartDate" in df.columns:
            df["ccusOandMContractStartDate"] = pd.to_datetime(df["ccusOandMContractStartDate"])  # type: ignore

        if "hydrogenOandMContractStartDate" in df.columns:
            df["hydrogenOandMContractStartDate"] = pd.to_datetime(df["hydrogenOandMContractStartDate"])  # type: ignore

        # Supply chain dates
        if "dateExpectedWindTurbineShipment" in df.columns:
            df["dateExpectedWindTurbineShipment"] = pd.to_datetime(df["dateExpectedWindTurbineShipment"])  # type: ignore

        if "dateExpectedWindTurbineCommissioning" in df.columns:
            df["dateExpectedWindTurbineCommissioning"] = pd.to_datetime(df["dateExpectedWindTurbineCommissioning"])  # type: ignore

        if "batteryShipmentStartDate" in df.columns:
            df["batteryShipmentStartDate"] = pd.to_datetime(df["batteryShipmentStartDate"])  # type: ignore

        if "batteryShipmentEndDate" in df.columns:
            df["batteryShipmentEndDate"] = pd.to_datetime(df["batteryShipmentEndDate"])  # type: ignore

        if "modifiedDatetime" in df.columns:
            df["modifiedDatetime"] = pd.to_datetime(df["modifiedDatetime"])  # type: ignore

        if "expectedProjectCommissioningDate" in df.columns:
            df["expectedProjectCommissioningDate"] = pd.to_datetime(df["expectedProjectCommissioningDate"])  # type: ignore

        return df