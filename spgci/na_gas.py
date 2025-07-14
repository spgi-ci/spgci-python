# Copyright 2025 S&P Global Commodity Insights

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
from requests import Response
from pandas import DataFrame, Series
import pandas as pd
from typing import Union, Optional, List
from spgci.api_client import get_data, Paginator
from spgci.utilities import list_to_filter
from enum import Enum
from datetime import datetime, date


class NANaturalGasAnalytics:
    """
    North America Natural Gas Pipeline.

    Includes
    --------
    ``get_pipelines()`` to get the list of pipelines and their associated properties such as county, facility type, drn number, etc; for the North America Natural Gas Pipeline dataset.
    ``get_pipeline_flows()`` to get north america natural gas pipeline flow data. Returns 48 hours of history by default.

    """

    _path = "analytics/natural-gas/north-america/supply-demand/v1/"

    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
        j = resp.json()
        # df = pd.json_normalize(j["results"], record_path=["data"], meta="symbol")  # type: ignore
        df = pd.json_normalize(j["results"])  # type: ignore

        if "gasDate" in df.columns:
            df["gasDate"] = pd.to_datetime(df["gasDate"])  # type: ignore
        if "validFrom" in df.columns:
            df["validFrom"] = pd.to_datetime(df["validFrom"])  # type: ignore
        if "validTo" in df.columns:
            df["validTo"] = pd.to_datetime(df["validTo"], errors="ignore")  # type: ignore
        if "createDate" in df.columns:
            df["createDate"] = pd.to_datetime(df["createDate"])  # type: ignore
        if "modifiedDate" in df.columns:
            df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])  # type: ignore

        return df

    class PointType(Enum):
        """Point Type"""

        Point = "point"
        Segment = "segment"

    class FacilityType(Enum):
        """Facility Type"""

        LDC = "LDC"
        Constraint = "Constraint"
        Compressor = "Compressor"
        Production = "Production"
        GatheringCompany = "Gathering Company"
        Interconnect = "Interconnect"
        Lateral = "Lateral"
        ProcessingPlant = "Processing Plant"
        LNG = "LNG"
        End_User = "End User"
        StorageParkLoan = "Storage Park/Loan"
        IntraStateConnect = "IntraState Connect"
        GasTreatmentPlants = "Gas Treatment Plants"
        Demand = "Demand"
        Municipal = "Municipal"
        PowerPlant = "Power Plant"
        PipelineAdministration = "Pipeline Administration"
        FieldFuel = "Field Fuel"
        EnhancedOilRecoveryCogen = "Enhanced Oil Recovery/Cogen"

    @staticmethod
    def _paginate(resp: Response) -> Paginator:
        j = resp.json()
        count = j["metadata"]["count"]
        size = j["metadata"]["pageSize"]

        remainder = count % size
        quotient = count // size
        total_pages = quotient + (1 if remainder > 0 else 0)

        if total_pages <= 1:
            return Paginator(False, "page", 1)

        return Paginator(True, "page", total_pages=total_pages)

    def get_pipeline_flows(
        self,
        *,
        pipeline_id: Optional[int] = None,
        component_id: Optional[int] = None,
        gasdate: Optional[Union[date, list[date], "Series[date]"]] = None,
        gasdate_gt: Optional[date] = None,
        gasdate_gte: Optional[date] = None,
        gasdate_lt: Optional[date] = None,
        gasdate_lte: Optional[date] = None,
        nomination_cycle: Optional[Union[str, list[str], "Series[str]"]] = None,
        location_type: Optional[Union[str, list[str], "Series[str]"]] = None,
        scheduled_volume: Optional[float] = None,
        actual_volume: Optional[float] = None,
        utilization: Optional[float] = None,
        design_capacity: Optional[float] = None,
        operational_capacity: Optional[float] = None,
        actual_capacity: Optional[float] = None,
        operationally_available: Optional[float] = None,
        flow_dir: Optional[Union[str, list[str], "Series[str]"]] = None,
        interruptible_flow: Optional[Union[str, list[str], "Series[str]"]] = None,
        valid_from: Optional[datetime] = None,
        valid_to: Optional[datetime] = None,
        data_active: Optional[bool] = None,
        data_source: Optional[str] = None,
        create_date: Optional[datetime] = None,
        modified_date: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 2000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[Response, DataFrame]:
        """
        Analytics data for North America Natural Gas Pipelines .

        Parameters
        ----------
        pipeline_id : int, optional
            filter by pipe_line_id, by default None
        component_id : int, optional
            filter by component_id, by default None
        gasdate : Optional[Union[str, list[datetime], Series[datetime]]], optional
            filter by gasdate, by default None
        gasdate_gt : Optional[datetime], optional
            filter by ``gasdate > x``, by default None
        gasdate_gte : Optional[datetime], optional
            filter by ``gasdate >= x``, by default None
        gasdate_lt : Optional[datetime], optional
            filter by ``gasdate < x``, by default None
        gasdate_lte : Optional[datetime], optional
            filter by ``gasdate <= x``, by default None
        nomination_cycle: Optional[Union[str, list[str], "Series[str]"]] , optional
            filter by nomination_cycle, by default None
        location_type: Optional[Union[str, list[str], "Series[str]"]] , optional
            filter by location_type, by default None
        scheduled_volume: Optional[float] , optional
            filter by scheduled_volume, by default None
        actual_volume: Optional[float] , optional
            filter by actual_volume, by default None
        utilization: Optional[float] , optional
            filter by utilization, by default None
        design_capacity: Optional[float] , optional
            filter by design_capacity, by default None
        operational_capacity: Optional[float] , optional
            filter by operational_capacity, by default None
        actual_capacity: Optional[float] , optional
            filter by actual_capacity, by default None
        operationally_available: Optional[float] , optional
            filter by operationally_available, by default None,
        flow_dir: Optional[Union[str, list[str], "Series[str]"]] , optional
            filter by flow_dir, by default None
        interruptible_flow: Optional[Union[str, list[str], "Series[str]"]] , optional
            filter by interruptible_flow, by default None
        valid_from: Optional[datetime] , optional
            filter by valid_from, by default None
        valid_to: Optional[datetime], optional
            filter by valid_to, by default None
        data_active: Optional[bool] , optional
            filter by data_active, by default None
        data_source: Optional[str], optional
            filter by data_source, by default None
        create_date: Optional[datetime], optional
            filter by create_date, by default None
        modified_date: Optional[datetime], optional
            filter by modified_date, by default None
        filter_exp : Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 2000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
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
        **Get Pipeline flow data by date and nomination cycle**
        >>> d = date(2023, 7, 1)
        >>> ci.NANaturalGasAnalytics().get_pipeline_flows(gasdate=d, nomination_cycle="I2")

        **Using Enum**
        >>> ng = ci.NANaturalGasAnalytics().get_pipeline_flows(faciltyType=ci.NANaturalGasAnalytics().FacilityType.LNG)
        """
        endpoint_path = "pipeline-flow-data"

        filter_param: List[str] = []

        filter_param.append(list_to_filter("pipelineId", pipeline_id))
        filter_param.append(list_to_filter("componentId", component_id))
        filter_param.append(list_to_filter("gasDate", gasdate))
        filter_param.append(list_to_filter("nominationCycle", nomination_cycle))
        filter_param.append(list_to_filter("locationType", location_type))
        filter_param.append(list_to_filter("scheduledVolume", scheduled_volume))
        filter_param.append(list_to_filter("actualVolume", actual_volume))
        filter_param.append(list_to_filter("utilization", utilization))
        filter_param.append(list_to_filter("designCapacity", design_capacity))
        filter_param.append(list_to_filter("operationalCapacity", operational_capacity))
        filter_param.append(list_to_filter("actualCapacity", actual_capacity))
        filter_param.append(
            list_to_filter("operationallyAvailable", operationally_available)
        )
        filter_param.append(list_to_filter("flowDir", flow_dir))
        filter_param.append(list_to_filter("interruptibleFlow", interruptible_flow))
        filter_param.append(list_to_filter("validFrom", valid_from))
        filter_param.append(list_to_filter("validTo", valid_to))
        filter_param.append(list_to_filter("dataActive", data_active))
        filter_param.append(list_to_filter("dataSource", data_source))
        filter_param.append(list_to_filter("createDate", create_date))
        filter_param.append(list_to_filter("modifiedDate", modified_date))

        if gasdate_gt:
            filter_param.append(f'gasDate > "{gasdate_gt}"')
        if gasdate_gte:
            filter_param.append(f'gasDate >= "{gasdate_gte}"')
        if gasdate_lt:
            filter_param.append(f'gasDate < "{gasdate_lt}"')
        if gasdate_lte:
            filter_param.append(f'gasDate <= "{gasdate_lte}"')

        filter_param = [fp for fp in filter_param if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_param)
        elif len(filter_param) > 0:
            filter_exp = " AND ".join(filter_param) + " AND (" + filter_exp + ")"

        params = {
            "pageSize": page_size,
            "filter": filter_exp,
            "page": page,
        }
        return get_data(
            path=f"{self._path}{endpoint_path}",
            df_fn=self._convert_to_df,
            params=params,
            paginate=paginate,
            paginate_fn=self._paginate,
            raw=raw,
        )

    @staticmethod
    def _search_to_df(resp: Response) -> pd.DataFrame:
        j = resp.json()
        df = pd.DataFrame(j["results"])

        # if len(df) > 0:
        #     cols = ["symbol", "description"]
        #     df: pd.DataFrame = df[cols + [x for x in df.columns if x not in cols]]  # type: ignore

        return df

    @staticmethod
    def _ref_paginate(resp: Response) -> Paginator:
        j = resp.json()
        count = j["metadata"]["count"]
        size = j["metadata"]["pageSize"]

        remainder = count % size
        quotient = count // size
        total_pages = quotient + (1 if remainder > 0 else 0)

        if total_pages <= 1:
            return Paginator(False, "page", 1)

        return Paginator(True, "page", total_pages=total_pages)

    def get_pipelines(
        self,
        *,
        q: Optional[str] = None,
        pipeline_id: Optional[Union[list[int], "Series[int]", int]] = None,
        pipeline_name: Optional[Union[list[str], "Series[str]", str]] = None,
        point_type: Optional[Union[list[str], "Series[str]", str]] = None,
        point_name: Optional[Union[list[str], "Series[str]", str]] = None,
        drn_number: Optional[Union[list[str], "Series[str]", str]] = None,
        county: Optional[Union[list[str], "Series[str]", str]] = None,
        state: Optional[Union[list[str], "Series[str]", str]] = None,
        country: Optional[Union[list[str], "Series[str]", str]] = None,
        connecting_party_name: Optional[Union[list[str], "Series[str]", str]] = None,
        facility_type: Optional[Union[list[str], "Series[str]", str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 2000,
        paginate: bool = False,
        raw: bool = False,
    ) -> Union[pd.DataFrame, Response]:
        """
        Fetch Pipelines from the NANaturalGasPipeline API.

        Parameters
        ----------
        q : Optional[str], optional
            filter across fields using free text search, by default None
        pipeline_id : Optional[Union[list[int], Series[int], int]], optional
            filter by pipelineId, by default None
        pipeline_name : Optional[Union[list[str], Series[str], str]], optional
            filter by pipelineName, by default None
        point_type : Optional[Union[list[str], Series[str], str]], optional
            filter by pointName, by default None
        point_name : Optional[Union[list[str], Series[str], str]], optional
            filter by pointName, by default None
        drn_number : Optional[Union[list[str], Series[str], str]], optional
            filter by drnNumber, by default None
        county : Optional[Union[list[str], Series[str], str]], optional
            filter by county, by default None
        state : Optional[Union[list[str], Series[str], str]], optional
            filter by state, by default None
        country : Optional[Union[list[str], Series[str], str]], optional
            filter by country, by default None
        connecting_party_name : Optional[Union[list[str], Series[str], str]], optional
            filter by connectingPartyName, by default None
        facility_type : Optional[Union[list[str], Series[str], str]], optional
            filter by facilityType, by default None
        filter_exp : Optional[str], optional
            pass-thru ``filter`` query param to use a handcrafted filter expression, by default None
        page : int, optional
            pass-thru ``page`` query param to request a particular page of results, by default 1
        page_size : int, optional
            pass-thru ``pageSize`` query param to request a particular page size, by default 2000
        paginate : bool, optional
            whether to auto-paginate the response, by default False
        raw : bool, optional
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
        **Using String**
        >>> ci.NANaturalGasAnalytics().get_pipelines(point_type="Segment")

        **Using List**
        >>> ci.NANaturalGasAnalytics().get_pipelines(state=["WA", "CO"])

        """
        endpoint_path = "pipeline-reference-data"

        filter_params: List[str] = []

        filter_params.append(list_to_filter("pipelineName", pipeline_name))
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("pointType", point_type))
        filter_params.append(list_to_filter("pointName", point_name))
        filter_params.append(list_to_filter("drnNumber", drn_number))
        filter_params.append(list_to_filter("county", county))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(
            list_to_filter("connectingPartyName", connecting_party_name)
        )
        filter_params.append(list_to_filter("facilityType", facility_type))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {
            "q": q,
            "filter": filter_exp,
            "page": page,
            "pageSize": page_size,
        }
        return get_data(
            path=f"{self._path}{endpoint_path}",
            df_fn=self._search_to_df,
            paginate_fn=self._ref_paginate,
            params=params,
            paginate=paginate,
            raw=raw,
        )
