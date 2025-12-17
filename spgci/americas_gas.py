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
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date, datetime
import pandas as pd
from dateutil.parser import parse
from typing import Literal

class AmericasGas:

    _datasets = Literal[
        "reference-data-pipeline-flows",
        "reference-data-geography",
        "pipeline-flows", 
        "modeled-demand-actual",
        "natural-gas-production",
        "population-weighted-weather",
        "outlook-production-play",
        "outlook-marketbalances-prices",
        "pipeline-storage-projects",
        "pipeline-profiles-data",
        "modeled-demand-forecast",
        "no-notice-flow-data",
        "gas-quality-data",
        "notices-data",
        "tariff-rate-data",
        "index-of-customer-data",
        "production-oil-data",
        "facility-flow-data",
        "market-balances-data",
        "regional-summaries-flowdata",
        "storage-data",
        "pipeline-flows-essentials-delta",
        "pipeline-flows-essentials-history"
    ]
    
    def get_unique_values(
        self,
        dataset: _datasets,
        columns: Optional[Union[list[str], str]],
        **filters
    ) -> DataFrame:
        """
        Returns unique values for the specified columns in a given Americas Gas dataset.

        Parameters
        ----------
        dataset : _datasets
            The dataset to query.
        columns : list[str] or str
            The column(s) to get unique values for.

        Returns
        -------
        DataFrame
            DataFrame of unique values for the specified columns.
            Examples
        --------
        >>> na.get_unique_values("pipeline-flows-essentials-history", "locationType", pipelineId=1)
        >>> na.get_unique_values("pipeline-flows", ["flowDate", "pipelineId"], flowDate="2025-11-26")
        """
        dataset_to_path = {
            "reference-data-pipeline-flows": "/analytics/gas/na-gas/v1/reference-data/pipeline-flows",
            "reference-data-geography": "/analytics/gas/na-gas/v1/reference-data/geography",
            "pipeline-flows": "/analytics/gas/na-gas/v1/pipeline-flows",
            "modeled-demand-actual": "/analytics/gas/na-gas/v1/modeled-demand-actual",
            "natural-gas-production": "/analytics/gas/na-gas/v1/natural-gas-production",
            "population-weighted-weather": "/analytics/gas/na-gas/v1/population-weighted-weather",
            "outlook-production-play": "/analytics/gas/na-gas/v1/outlook-production-play",
            "outlook-marketbalances-prices": "/analytics/gas/na-gas/v1/outlook-marketbalances-prices",
            "pipeline-storage-projects": "/analytics/gas/na-gas/v1/pipeline-storage-projects",
            "pipeline-profiles-data": "/analytics/gas/na-gas/v1/pipeline-profiles-data",
            "modeled-demand-forecast": "/analytics/gas/na-gas/v1/modeled-demand-forecast",
            "no-notice-flow-data": "/analytics/gas/na-gas/v1/no-notice-flow-data",
            "gas-quality-data": "/analytics/gas/na-gas/v1/gas-quality-data",
            "notices-data": "/analytics/gas/na-gas/v1/notices-data",
            "tariff-rate-data": "/analytics/gas/na-gas/v1/tariff-rate-data",
            "index-of-customer-data" : "/analytics/gas/na-gas/v1/index-of-customer-data",
            "production-oil-data" : "/analytics/gas/na-gas/v1/production-oil-data",
            "facility-flow-data" : "/analytics/gas/na-gas/v1/facility-flow-data",
            "market-balances-data" : "/analytics/gas/na-gas/v1/market-balances-data",
            "regional-summaries-flow-data" : "/analytics/gas/na-gas/v1/regional-summaries-flow-data",
            "storage-data" : "/analytics/gas/na-gas/v1/storage-data",
            "pipeline-flows-essentials-delta": "/analytics/gas/na-gas/v1/pipeline-flows-essentials-delta",
            "pipeline-flows-essentials-history": "/analytics/gas/na-gas/v1/pipeline-flows-essentials-history"
            
        }

        if dataset not in dataset_to_path:
            valid = "\n".join(dataset_to_path.keys())
            raise ValueError(
                f"Dataset '{dataset}' not found. Valid datasets:\n{valid}"
            )

        path = dataset_to_path[dataset]
        col_value = ", ".join(columns) if isinstance(columns, list) else columns or ""
        params = {"GroupBy": col_value, "pageSize": 5000}
        if filters:
            from .utilities import build_filter_expression_with_strategy
            params["filter"] = build_filter_expression_with_strategy(filters, strategy="platts")

        def to_df(resp: Response):
            j = resp.json()
            df = pd.json_normalize(j["aggResultValue"])
            columns_dt = ["lastModifiedDate", "flowDate", "forecastDate", "postingDatetime",
            "createDate", "measurementDate", "effectiveDate", "endDate",
            "validFrom", "validTo", "dateEffective", "dateRetire", "dateIssued",
            "date", "contractStartDate", "contractEndDate", "inServiceDate",
            "projectCreatedDate", "projectUpdatedDate", "preFileDate",
            "projectFileDate", "projectApprovalDate", "componentCreateDate"]

            for c in columns_dt:
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c], utc=True, format="ISO8601", errors="coerce")
            return df

        return get_data(path, params, to_df, paginate=True)
    

    def get_reference_data_geography(
        self,
        *,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        gulfcoast_substate: Optional[Union[list[str], Series[str], str]] = None,
        gulfcoast_substate_id: Optional[Union[list[str], Series[str], str]] = None,
        state_abbreviation: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        state_id: Optional[Union[list[str], Series[str], str]] = None,
        county: Optional[Union[list[str], Series[str], str]] = None,
        county_id: Optional[Union[list[str], Series[str], str]] = None,
        producing_area: Optional[Union[list[str], Series[str], str]] = None,
        producing_area_id: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        A relationship table expressing the hierarchy from county and state up through subregion, region and domain levels such as the US lower 48, Canada and Mexico.

        Parameters
        ----------

         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         gulfcoast_substate: Optional[Union[list[str], Series[str], str]]
             The name of substate region or special area within the Gulf Coast region., by default None
         gulfcoast_substate_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic substate regions within the Gulf Coast area., by default None
         state_abbreviation: Optional[Union[list[str], Series[str], str]]
             Abbreviation for a state or province within country., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The political boundaries that define a state or province within country., by default None
         state_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the state or province utilizes legacy Bentek IDs., by default None
         county: Optional[Union[list[str], Series[str], str]]
             The political boundaries of a defined county within a US state in which a meter or point resides., by default None
         county_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the county within a US state in which a meter or point resides, utilizes legacy Bentek IDs., by default None
         producing_area: Optional[Union[list[str], Series[str], str]]
             Defined aggregation of counties within a state that is a best fit representation of prominent oil and gas plays and basins., by default None
         producing_area_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for a defined Producing Area utilizes legacy PointLogic IDs., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("gulfcoastSubstate", gulfcoast_substate))
        filter_params.append(
            list_to_filter("gulfcoastSubstateId", gulfcoast_substate_id)
        )
        filter_params.append(list_to_filter("stateAbbreviation", state_abbreviation))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("stateId", state_id))
        filter_params.append(list_to_filter("county", county))
        filter_params.append(list_to_filter("countyId", county_id))
        filter_params.append(list_to_filter("producingArea", producing_area))
        filter_params.append(list_to_filter("producingAreaId", producing_area_id))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/reference-data/geography",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_reference_data_pipeline_flows(
        self,
        *,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        legacy_point_logic_point_id: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        legacy_point_logic_lci_id: Optional[Union[list[str], Series[str], str]] = None,
        legacy_bentek_point_id: Optional[Union[list[str], Series[str], str]] = None,
        component_point_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_operator_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_operator_name: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_name: Optional[Union[list[str], Series[str], str]] = None,
        point_name: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_primary: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_id_primary: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_id_secondary: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_secondary: Optional[Union[list[str], Series[str], str]] = None,
        flow_direction: Optional[Union[list[str], Series[str], str]] = None,
        flow_direction_code: Optional[Union[list[str], Series[str], str]] = None,
        flow_direction_id: Optional[Union[list[str], Series[str], str]] = None,
        zone: Optional[Union[list[str], Series[str], str]] = None,
        company_id: Optional[Union[list[str], Series[str], str]] = None,
        connecting_party: Optional[Union[list[str], Series[str], str]] = None,
        loc_prop: Optional[Union[list[str], Series[str], str]] = None,
        point_is_active: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        state_id: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        county_id: Optional[Union[list[str], Series[str], str]] = None,
        county: Optional[Union[list[str], Series[str], str]] = None,
        producing_area: Optional[Union[list[str], Series[str], str]] = None,
        producing_area_id: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        A single source for pipeline metadata that captures 35 different attributes. Mimics pipeline flows dataset with the removal of volume metric time series data.

        Parameters
        ----------

         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         legacy_point_logic_point_id: Optional[Union[list[str], Series[str], str]]
             Point ID for a meter used by the legacy PointLogic service., by default None
         legacy_point_logic_lci_id: Optional[Union[list[str], Series[str], str]]
             Alternative point ID for a meter used by the legacy PointLogic service., by default None
         legacy_bentek_point_id: Optional[Union[list[str], Series[str], str]]
             Point ID for a meter used by the legacy Bentek service., by default None
         component_point_id: Optional[Union[list[str], Series[str], str]]
             Point ID for a meter used by the Americas Gas service., by default None
         pipeline_operator_id: Optional[Union[list[str], Series[str], str]]
             ID associated with the common parent owner or operator of a pipeline system., by default None
         pipeline_operator_name: Optional[Union[list[str], Series[str], str]]
             The name of the common parent owner or operator of a pipeline system., by default None
         pipeline_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a pipeline system, utilizes legacy Bentek pipeline ids when applicable., by default None
         pipeline_name: Optional[Union[list[str], Series[str], str]]
             The display name of a pipeline system, utilizes legacy Bentek names when applicable., by default None
         point_name: Optional[Union[list[str], Series[str], str]]
             The display name of a meter or point, utilizes legacy Bentek point name when applicable., by default None
         meter_type_primary: Optional[Union[list[str], Series[str], str]]
             The primary type of classification and purpose of a meter or point, utilizes legacy PointLogic definitions., by default None
         meter_type_id_primary: Optional[Union[list[str], Series[str], str]]
             An ID for the primary type of classification and purpose of a meter or point, utilizes legacy PointLogic definitions and ID., by default None
         meter_type_id_secondary: Optional[Union[list[str], Series[str], str]]
             A secondary type classification and purpose of a meter or point, meant to provide an extra level of detail and utilizes legacy Bentek ids., by default None
         meter_type_secondary: Optional[Union[list[str], Series[str], str]]
             A secondary type classification and purpose of a meter or point, meant to provide an extra level of detail, utilizes legacy Bentek definitions., by default None
         flow_direction: Optional[Union[list[str], Series[str], str]]
             Flow direction indicates the orientation of a point such as receipt, delivery, bi-directional or the reported flow direction of segment or compressor. Attribute is sourced from legacy PointLogic. Flow direction is primary to the similar and secondary attribute of Location Type., by default None
         flow_direction_code: Optional[Union[list[str], Series[str], str]]
             A one letter code for Flow Direction such as ‘R’ for receipt or ‘D’ for delivery. Attribute is sourced from legacy PointLogic. Flow direction is primary to the similar and secondary attribute of Location Type., by default None
         flow_direction_id: Optional[Union[list[str], Series[str], str]]
             Flow direction identification number for the orientation of a point such as receipt, delivery, bi-directional or the reported flow direction of segment or compressor. Attribute is sourced from legacy PointLogic. Flow direction is primary to the similar and secondary attribute of Location Type., by default None
         zone: Optional[Union[list[str], Series[str], str]]
             A designation for where on the pipeline system the point is located, corresponding to a pipeline’s operational and market design. Zonal information is sourced from the legacy Bentek service., by default None
         company_id: Optional[Union[list[str], Series[str], str]]
             An ID sourced from the legacy Bentek service used to identify the connecting business or company name of a meter., by default None
         connecting_party: Optional[Union[list[str], Series[str], str]]
             The downstream connecting business name of a meter as reported by the pipeline, utilizes legacy Bentek service., by default None
         loc_prop: Optional[Union[list[str], Series[str], str]]
             The location propriety code reported by the pipeline for a specific meter., by default None
         point_is_active: Optional[Union[list[str], Series[str], str]]
             A true or false return if a point or meter is active and in use within the Americas Gas service., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         state_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the state or province utilizes legacy Bentek IDs., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The political boundaries that define a state or province within country., by default None
         county_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the county within a US state in which a meter or point resides, utilizes legacy Bentek IDs., by default None
         county: Optional[Union[list[str], Series[str], str]]
             The political boundaries of a defined county within a US state in which a meter or point resides., by default None
         producing_area: Optional[Union[list[str], Series[str], str]]
             Defined aggregation of counties within a state that is a best fit representation of prominent oil and gas plays and basins., by default None
         producing_area_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for a defined Producing Area utilizes legacy PointLogic IDs., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(
            list_to_filter("legacyPointLogicPointId", legacy_point_logic_point_id)
        )
        filter_params.append(
            list_to_filter("legacyPointLogicLciId", legacy_point_logic_lci_id)
        )
        filter_params.append(
            list_to_filter("legacyBentekPointId", legacy_bentek_point_id)
        )
        filter_params.append(list_to_filter("componentPointId", component_point_id))
        filter_params.append(list_to_filter("pipelineOperatorId", pipeline_operator_id))
        filter_params.append(
            list_to_filter("pipelineOperatorName", pipeline_operator_name)
        )
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("pipelineName", pipeline_name))
        filter_params.append(list_to_filter("pointName", point_name))
        filter_params.append(list_to_filter("meterTypePrimary", meter_type_primary))
        filter_params.append(
            list_to_filter("meterTypeIdPrimary", meter_type_id_primary)
        )
        filter_params.append(
            list_to_filter("meterTypeIdSecondary", meter_type_id_secondary)
        )
        filter_params.append(list_to_filter("meterTypeSecondary", meter_type_secondary))
        filter_params.append(list_to_filter("flowDirection", flow_direction))
        filter_params.append(list_to_filter("flowDirectionCode", flow_direction_code))
        filter_params.append(list_to_filter("flowDirectionId", flow_direction_id))
        filter_params.append(list_to_filter("zone", zone))
        filter_params.append(list_to_filter("companyId", company_id))
        filter_params.append(list_to_filter("connectingParty", connecting_party))
        filter_params.append(list_to_filter("locProp", loc_prop))
        filter_params.append(list_to_filter("pointIsActive", point_is_active))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("stateId", state_id))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("countyId", county_id))
        filter_params.append(list_to_filter("county", county))
        filter_params.append(list_to_filter("producingArea", producing_area))
        filter_params.append(list_to_filter("producingAreaId", producing_area_id))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/reference-data/pipeline-flows",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_pipeline_flows(
        self,
        *,
        flow_date: Optional[date] = None,
        flow_date_lt: Optional[date] = None,
        flow_date_lte: Optional[date] = None,
        flow_date_gt: Optional[date] = None,
        flow_date_gte: Optional[date] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        nomination_cycle: Optional[Union[list[str], Series[str], str]] = None,
        legacy_point_logic_point_id: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        legacy_point_logic_lci_id: Optional[Union[list[str], Series[str], str]] = None,
        legacy_bentek_point_id: Optional[Union[list[str], Series[str], str]] = None,
        component_point_id: Optional[Union[list[str], Series[str], str]] = None,
        component_create_date: Optional[datetime] = None,
        component_create_date_lt: Optional[datetime] = None,
        component_create_date_lte: Optional[datetime] = None,
        component_create_date_gt: Optional[datetime] = None,
        component_create_date_gte: Optional[datetime] = None,
        pipeline_operator_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_operator_name: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_name: Optional[Union[list[str], Series[str], str]] = None,
        point_name: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_primary: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_id_primary: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_id_secondary: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_secondary: Optional[Union[list[str], Series[str], str]] = None,
        location_type_code: Optional[Union[list[str], Series[str], str]] = None,
        location_description: Optional[Union[list[str], Series[str], str]] = None,
        location_type_id: Optional[Union[list[str], Series[str], str]] = None,
        flow_direction: Optional[Union[list[str], Series[str], str]] = None,
        flow_direction_code: Optional[Union[list[str], Series[str], str]] = None,
        flow_direction_id: Optional[Union[list[str], Series[str], str]] = None,
        zone: Optional[Union[list[str], Series[str], str]] = None,
        company_id: Optional[Union[list[str], Series[str], str]] = None,
        connecting_party: Optional[Union[list[str], Series[str], str]] = None,
        loc_prop: Optional[Union[list[str], Series[str], str]] = None,
        point_is_active: Optional[Union[list[bool], Series[bool], bool]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        state_id: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        county_id: Optional[Union[list[str], Series[str], str]] = None,
        county: Optional[Union[list[str], Series[str], str]] = None,
        producing_area: Optional[Union[list[str], Series[str], str]] = None,
        producing_area_id: Optional[Union[list[str], Series[str], str]] = None,
        design_capacity: Optional[str] = None,
        design_capacity_lt: Optional[str] = None,
        design_capacity_lte: Optional[str] = None,
        design_capacity_gt: Optional[str] = None,
        design_capacity_gte: Optional[str] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        operational_capacity: Optional[str] = None,
        operational_capacity_lt: Optional[str] = None,
        operational_capacity_lte: Optional[str] = None,
        operational_capacity_gt: Optional[str] = None,
        operational_capacity_gte: Optional[str] = None,
        scheduled_volume: Optional[str] = None,
        scheduled_volume_lt: Optional[str] = None,
        scheduled_volume_lte: Optional[str] = None,
        scheduled_volume_gt: Optional[str] = None,
        scheduled_volume_gte: Optional[str] = None,
        utilization: Optional[str] = None,
        utilization_lt: Optional[str] = None,
        utilization_lte: Optional[str] = None,
        utilization_gt: Optional[str] = None,
        utilization_gte: Optional[str] = None,
        best_available: Optional[Union[list[bool], Series[bool], bool]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Daily natural gas pipeline flows in the US lower 48, Canada and Mexico that is accompanied with an array of metadata attributes.

        Parameters
        ----------

         flow_date: Optional[date], optional
             The calendar date or gas day the activity occurred., by default None
         flow_date_gt: Optional[date], optional
             filter by `flow_date > x`, by default None
         flow_date_gte: Optional[date], optional
             filter by `flow_date >= x`, by default None
         flow_date_lt: Optional[date], optional
             filter by `flow_date < x`, by default None
         flow_date_lte: Optional[date], optional
             filter by `flow_date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Daily, Weekly, Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Daily, Weekly, Monthly, Seasonal, Annual. Weekly date frequencies are based on the defined EIA storage week of Friday-Thursday. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         nomination_cycle: Optional[Union[list[str], Series[str], str]]
             Standard NAESB defined nomination cycles for timely (T), evening (E), intraday 1 (I1), intraday 2 (I2) or intraday 3 (I3)., by default None
         legacy_point_logic_point_id: Optional[Union[list[str], Series[str], str]]
             Point ID for a meter used by the legacy PointLogic service., by default None
         legacy_point_logic_lci_id: Optional[Union[list[str], Series[str], str]]
             Alternative point ID for a meter used by the legacy PointLogic service., by default None
         legacy_bentek_point_id: Optional[Union[list[str], Series[str], str]]
             Point ID for a meter used by the legacy Bentek service., by default None
         component_point_id: Optional[Union[list[str], Series[str], str]]
             Point ID for a meter used by the Americas Gas service., by default None
         component_create_date: Optional[datetime], optional
             The date and time stamp of when the record was created., by default None
         component_create_date_gt: Optional[datetime], optional
             filter by `component_create_date > x`, by default None
         component_create_date_gte: Optional[datetime], optional
             filter by `component_create_date >= x`, by default None
         component_create_date_lt: Optional[datetime], optional
             filter by `component_create_date < x`, by default None
         component_create_date_lte: Optional[datetime], optional
             filter by `component_create_date <= x`, by default None
         pipeline_operator_id: Optional[Union[list[str], Series[str], str]]
             ID associated with the common parent owner or operator of a pipeline system., by default None
         pipeline_operator_name: Optional[Union[list[str], Series[str], str]]
             The name of the common parent owner or operator of a pipeline system., by default None
         pipeline_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a pipeline system, utilizes legacy Bentek pipeline ids when applicable., by default None
         pipeline_name: Optional[Union[list[str], Series[str], str]]
             The display name of a pipeline system, utilizes legacy Bentek names when applicable., by default None
         point_name: Optional[Union[list[str], Series[str], str]]
             The display name of a meter or point, utilizes legacy Bentek point name when applicable., by default None
         meter_type_primary: Optional[Union[list[str], Series[str], str]]
             The primary type of classification and purpose of a meter or point, utilizes legacy PointLogic definitions., by default None
         meter_type_id_primary: Optional[Union[list[str], Series[str], str]]
             An ID for the primary type of classification and purpose of a meter or point, utilizes legacy PointLogic definitions and ID., by default None
         meter_type_id_secondary: Optional[Union[list[str], Series[str], str]]
             A secondary type classification and purpose of a meter or point, meant to provide an extra level of detail and utilizes legacy Bentek ids., by default None
         meter_type_secondary: Optional[Union[list[str], Series[str], str]]
             A secondary type classification and purpose of a meter or point, meant to provide an extra level of detail, utilizes legacy Bentek definitions., by default None
         location_type_code: Optional[Union[list[str], Series[str], str]]
             Location type code is a one letter abbreviation of the location description. Location types are sourced from legacy Bentek. These are similar to Flow Direction but serve as a secondary attribute., by default None
         location_description: Optional[Union[list[str], Series[str], str]]
             Location types are sourced from legacy Bentek. These are similar to Flow Direction but serve as a secondary attribute., by default None
         location_type_id: Optional[Union[list[str], Series[str], str]]
             An ID for the location type sourced from legacy Bentek. These are similar to Flow Direction but serve as a secondary attribute., by default None
         flow_direction: Optional[Union[list[str], Series[str], str]]
             Flow direction indicates the orientation of a point such as receipt, delivery, bi-directional or the reported flow direction of segment or compressor. Attribute is sourced from legacy PointLogic. Flow direction is primary to the similar and secondary attribute of Location Type., by default None
         flow_direction_code: Optional[Union[list[str], Series[str], str]]
             A one letter code for Flow Direction such as ‘R’ for receipt or ‘D’ for delivery. Attribute is sourced from legacy PointLogic. Flow direction is primary to the similar and secondary attribute of Location Type., by default None
         flow_direction_id: Optional[Union[list[str], Series[str], str]]
             Flow direction identification number for the orientation of a point such as receipt, delivery, bi-directional or the reported flow direction of segment or compressor. Attribute is sourced from legacy PointLogic. Flow direction is primary to the similar and secondary attribute of Location Type., by default None
         zone: Optional[Union[list[str], Series[str], str]]
             A designation for where on the pipeline system the point is located, corresponding to a pipeline’s operational and market design. Zonal information is sourced from the legacy Bentek service., by default None
         company_id: Optional[Union[list[str], Series[str], str]]
             An ID sourced from the legacy Bentek service used to identify the connecting business or company name of a meter., by default None
         connecting_party: Optional[Union[list[str], Series[str], str]]
             The downstream connecting business name of a meter as reported by the pipeline, utilizes legacy Bentek service., by default None
         loc_prop: Optional[Union[list[str], Series[str], str]]
             The location propriety code reported by the pipeline for a specific meter., by default None
         point_is_active: Optional[Union[list[bool], Series[bool], bool]]
             A true or false return if a point or meter is active and in use within the Americas Gas service., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         state_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the state or province utilizes legacy Bentek IDs., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The political boundaries that define a state or province within country., by default None
         county_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the county within a US state in which a meter or point resides, utilizes legacy Bentek IDs., by default None
         county: Optional[Union[list[str], Series[str], str]]
             The political boundaries of a defined county within a US state in which a meter or point resides., by default None
         producing_area: Optional[Union[list[str], Series[str], str]]
             Defined aggregation of counties within a state that is a best fit representation of prominent oil and gas plays and basins., by default None
         producing_area_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for a defined Producing Area utilizes legacy PointLogic IDs., by default None
         design_capacity: Optional[str], optional
             The volumetric max that a given meter, segment or compressor can receive or deliver as reported by the pipeline., by default None
         design_capacity_gt: Optional[str], optional
             filter by `design_capacity > x`, by default None
         design_capacity_gte: Optional[str], optional
             filter by `design_capacity >= x`, by default None
         design_capacity_lt: Optional[str], optional
             filter by `design_capacity < x`, by default None
         design_capacity_lte: Optional[str], optional
             filter by `design_capacity <= x`, by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         operational_capacity: Optional[str], optional
             The pipeline’s reported daily operational capacity for a specific meter, segment or compressor can receive or deliver., by default None
         operational_capacity_gt: Optional[str], optional
             filter by `operational_capacity > x`, by default None
         operational_capacity_gte: Optional[str], optional
             filter by `operational_capacity >= x`, by default None
         operational_capacity_lt: Optional[str], optional
             filter by `operational_capacity < x`, by default None
         operational_capacity_lte: Optional[str], optional
             filter by `operational_capacity <= x`, by default None
         scheduled_volume: Optional[str], optional
             Scheduled volume as reported by the pipeline for a specific meter, segment or compressor., by default None
         scheduled_volume_gt: Optional[str], optional
             filter by `scheduled_volume > x`, by default None
         scheduled_volume_gte: Optional[str], optional
             filter by `scheduled_volume >= x`, by default None
         scheduled_volume_lt: Optional[str], optional
             filter by `scheduled_volume < x`, by default None
         scheduled_volume_lte: Optional[str], optional
             filter by `scheduled_volume <= x`, by default None
         utilization: Optional[str], optional
             Utilization rate in decimal form or in percentage terms. Utilization for a given meter is calculated by dividing the scheduled volume by its operational capacity., by default None
         utilization_gt: Optional[str], optional
             filter by `utilization > x`, by default None
         utilization_gte: Optional[str], optional
             filter by `utilization >= x`, by default None
         utilization_lt: Optional[str], optional
             filter by `utilization < x`, by default None
         utilization_lte: Optional[str], optional
             filter by `utilization <= x`, by default None
         best_available: Optional[Union[list[bool], Series[bool], bool]]
             Best Available refers to the most recent or final nomination cycle for a specific meter on a given flow date. It is indicated as either true or false, with true signifying that the nomination cycle in question is the Best Available. This designation helps in identifying the most current data for operational decisions and reporting purposes., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("flowDate", flow_date))
        if flow_date_gt is not None:
            filter_params.append(f'flowDate > "{flow_date_gt}"')
        if flow_date_gte is not None:
            filter_params.append(f'flowDate >= "{flow_date_gte}"')
        if flow_date_lt is not None:
            filter_params.append(f'flowDate < "{flow_date_lt}"')
        if flow_date_lte is not None:
            filter_params.append(f'flowDate <= "{flow_date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(list_to_filter("nominationCycle", nomination_cycle))
        filter_params.append(
            list_to_filter("legacyPointLogicPointId", legacy_point_logic_point_id)
        )
        filter_params.append(
            list_to_filter("legacyPointLogicLciId", legacy_point_logic_lci_id)
        )
        filter_params.append(
            list_to_filter("legacyBentekPointId", legacy_bentek_point_id)
        )
        filter_params.append(list_to_filter("componentPointId", component_point_id))
        filter_params.append(
            list_to_filter("componentCreateDate", component_create_date)
        )
        if component_create_date_gt is not None:
            filter_params.append(f'componentCreateDate > "{component_create_date_gt}"')
        if component_create_date_gte is not None:
            filter_params.append(
                f'componentCreateDate >= "{component_create_date_gte}"'
            )
        if component_create_date_lt is not None:
            filter_params.append(f'componentCreateDate < "{component_create_date_lt}"')
        if component_create_date_lte is not None:
            filter_params.append(
                f'componentCreateDate <= "{component_create_date_lte}"'
            )
        filter_params.append(list_to_filter("pipelineOperatorId", pipeline_operator_id))
        filter_params.append(
            list_to_filter("pipelineOperatorName", pipeline_operator_name)
        )
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("pipelineName", pipeline_name))
        filter_params.append(list_to_filter("pointName", point_name))
        filter_params.append(list_to_filter("meterTypePrimary", meter_type_primary))
        filter_params.append(
            list_to_filter("meterTypeIdPrimary", meter_type_id_primary)
        )
        filter_params.append(
            list_to_filter("meterTypeIdSecondary", meter_type_id_secondary)
        )
        filter_params.append(list_to_filter("meterTypeSecondary", meter_type_secondary))
        filter_params.append(list_to_filter("locationTypeCode", location_type_code))
        filter_params.append(
            list_to_filter("locationDescription", location_description)
        )
        filter_params.append(list_to_filter("locationTypeId", location_type_id))
        filter_params.append(list_to_filter("flowDirection", flow_direction))
        filter_params.append(list_to_filter("flowDirectionCode", flow_direction_code))
        filter_params.append(list_to_filter("flowDirectionId", flow_direction_id))
        filter_params.append(list_to_filter("zone", zone))
        filter_params.append(list_to_filter("companyId", company_id))
        filter_params.append(list_to_filter("connectingParty", connecting_party))
        filter_params.append(list_to_filter("locProp", loc_prop))
        filter_params.append(list_to_filter("pointIsActive", point_is_active))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("stateId", state_id))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("countyId", county_id))
        filter_params.append(list_to_filter("county", county))
        filter_params.append(list_to_filter("producingArea", producing_area))
        filter_params.append(list_to_filter("producingAreaId", producing_area_id))
        filter_params.append(list_to_filter("designCapacity", design_capacity))
        if design_capacity_gt is not None:
            filter_params.append(f'designCapacity > "{design_capacity_gt}"')
        if design_capacity_gte is not None:
            filter_params.append(f'designCapacity >= "{design_capacity_gte}"')
        if design_capacity_lt is not None:
            filter_params.append(f'designCapacity < "{design_capacity_lt}"')
        if design_capacity_lte is not None:
            filter_params.append(f'designCapacity <= "{design_capacity_lte}"')
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(
            list_to_filter("operationalCapacity", operational_capacity)
        )
        if operational_capacity_gt is not None:
            filter_params.append(f'operationalCapacity > "{operational_capacity_gt}"')
        if operational_capacity_gte is not None:
            filter_params.append(f'operationalCapacity >= "{operational_capacity_gte}"')
        if operational_capacity_lt is not None:
            filter_params.append(f'operationalCapacity < "{operational_capacity_lt}"')
        if operational_capacity_lte is not None:
            filter_params.append(f'operationalCapacity <= "{operational_capacity_lte}"')
        filter_params.append(list_to_filter("scheduledVolume", scheduled_volume))
        if scheduled_volume_gt is not None:
            filter_params.append(f'scheduledVolume > "{scheduled_volume_gt}"')
        if scheduled_volume_gte is not None:
            filter_params.append(f'scheduledVolume >= "{scheduled_volume_gte}"')
        if scheduled_volume_lt is not None:
            filter_params.append(f'scheduledVolume < "{scheduled_volume_lt}"')
        if scheduled_volume_lte is not None:
            filter_params.append(f'scheduledVolume <= "{scheduled_volume_lte}"')
        filter_params.append(list_to_filter("utilization", utilization))
        if utilization_gt is not None:
            filter_params.append(f'utilization > "{utilization_gt}"')
        if utilization_gte is not None:
            filter_params.append(f'utilization >= "{utilization_gte}"')
        if utilization_lt is not None:
            filter_params.append(f'utilization < "{utilization_lt}"')
        if utilization_lte is not None:
            filter_params.append(f'utilization <= "{utilization_lte}"')
        filter_params.append(list_to_filter("bestAvailable", best_available))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/pipeline-flows",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    
    
    def get_pipeline_flows_essentials_delta(
        self,
        *,
        day: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        month: Optional[Union[list[str], Series[str], str]] = None,
        reason: Optional[Union[list[str], Series[str], str]] = None,
        queue_id: Optional[Union[list[str], Series[str], str]] = None,
        valid_to: Optional[datetime] = None,
        valid_to_lt: Optional[datetime] = None,
        valid_to_lte: Optional[datetime] = None,
        valid_to_gt: Optional[datetime] = None,
        valid_to_gte: Optional[datetime] = None,
        flow_date: Optional[date] = None,
        flow_date_lt: Optional[date] = None,
        flow_date_lte: Optional[date] = None,
        flow_date_gt: Optional[date] = None,
        flow_date_gte: Optional[date] = None,
        valid_from: Optional[datetime] = None,
        data_active: Optional[Union[list[bool], Series[bool], bool]] = None,
        data_source: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_id: Optional[Union[list[str], Series[str], str]] = None,
        component_id: Optional[Union[list[str], Series[str], str]] = None,
        utilization: Optional[str] = None,
        utilization_lt: Optional[str] = None,
        utilization_lte: Optional[str] = None,
        utilization_gt: Optional[str] = None,
        utilization_gte: Optional[str] = None,
        location_type: Optional[Union[list[str], Series[str], str]] = None,
        design_capacity: Optional[str] = None,
        design_capacity_lt: Optional[str] = None,
        design_capacity_lte: Optional[str] = None,
        design_capacity_gt: Optional[str] = None,
        design_capacity_gte: Optional[str] = None,
        nomination_cycle: Optional[Union[list[str], Series[str], str]] = None,
        scheduled_volume: Optional[str] = None,
        scheduled_volume_lt: Optional[str] = None,
        scheduled_volume_lte: Optional[str] = None,
        scheduled_volume_gt: Optional[str] = None,
        scheduled_volume_gte: Optional[str] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        interruptible_flow: Optional[Union[list[str], Series[str], str]] = None,
        component_create_date: Optional[datetime] = None,
        component_create_date_lt: Optional[datetime] = None,
        component_create_date_lte: Optional[datetime] = None,
        component_create_date_gt: Optional[datetime] = None,
        component_create_date_gte: Optional[datetime] = None,
        operational_capacity: Optional[str] = None,
        operational_capacity_lt: Optional[str] = None,
        operational_capacity_lte: Optional[str] = None,
        operational_capacity_gt: Optional[str] = None,
        operational_capacity_gte: Optional[str] = None,
        operationally_available: Optional[str] = None,
        operationally_available_lt: Optional[str] = None,
        operationally_available_lte: Optional[str] = None,
        operationally_available_gt: Optional[str] = None,
        operationally_available_gte: Optional[str] = None,
        field: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Query the Pipeline Flows Essentials Delta endpoint.
        """

        filter_params: List[str] = []

        filter_params.append(list_to_filter("day", day))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("month", month))
        filter_params.append(list_to_filter("reason", reason))
        filter_params.append(list_to_filter("queueId", queue_id))
        filter_params.append(list_to_filter("dataActive", data_active))
        filter_params.append(list_to_filter("dataSource", data_source))
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("componentId", component_id))
        filter_params.append(list_to_filter("locationType", location_type))
        filter_params.append(list_to_filter("nominationCycle", nomination_cycle))
        filter_params.append(list_to_filter("interruptibleFlow", interruptible_flow))

        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')

        filter_params.append(list_to_filter("flowDate", flow_date))
        if flow_date_gt is not None:
            filter_params.append(f'flowDate > "{flow_date_gt}"')
        if flow_date_gte is not None:
            filter_params.append(f'flowDate >= "{flow_date_gte}"')
        if flow_date_lt is not None:
            filter_params.append(f'flowDate < "{flow_date_lt}"')
        if flow_date_lte is not None:
            filter_params.append(f'flowDate <= "{flow_date_lte}"')

        filter_params.append(list_to_filter("validFrom", valid_from))

        filter_params.append(list_to_filter("componentCreateDate", component_create_date))
        if component_create_date_gt is not None:
            filter_params.append(f'componentCreateDate > "{component_create_date_gt}"')
        if component_create_date_gte is not None:
            filter_params.append(f'componentCreateDate >= "{component_create_date_gte}"')
        if component_create_date_lt is not None:
            filter_params.append(f'componentCreateDate < "{component_create_date_lt}"')
        if component_create_date_lte is not None:
            filter_params.append(f'componentCreateDate <= "{component_create_date_lte}"')

        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')

        filter_params.append(list_to_filter("scheduledVolume", scheduled_volume))
        if scheduled_volume_gt is not None:
            filter_params.append(f'scheduledVolume > "{scheduled_volume_gt}"')
        if scheduled_volume_gte is not None:
            filter_params.append(f'scheduledVolume >= "{scheduled_volume_gte}"')
        if scheduled_volume_lt is not None:
            filter_params.append(f'scheduledVolume < "{scheduled_volume_lt}"')
        if scheduled_volume_lte is not None:
            filter_params.append(f'scheduledVolume <= "{scheduled_volume_lte}"')

        filter_params.append(list_to_filter("utilization", utilization))
        if utilization_gt is not None:
            filter_params.append(f'utilization > "{utilization_gt}"')
        if utilization_gte is not None:
            filter_params.append(f'utilization >= "{utilization_gte}"')
        if utilization_lt is not None:
            filter_params.append(f'utilization < "{utilization_lt}"')
        if utilization_lte is not None:
            filter_params.append(f'utilization <= "{utilization_lte}"')

        filter_params.append(list_to_filter("designCapacity", design_capacity))
        if design_capacity_gt is not None:
            filter_params.append(f'designCapacity > "{design_capacity_gt}"')
        if design_capacity_gte is not None:
            filter_params.append(f'designCapacity >= "{design_capacity_gte}"')
        if design_capacity_lt is not None:
            filter_params.append(f'designCapacity < "{design_capacity_lt}"')
        if design_capacity_lte is not None:
            filter_params.append(f'designCapacity <= "{design_capacity_lte}"')

        filter_params.append(list_to_filter("operationalCapacity", operational_capacity))
        if operational_capacity_gt is not None:
            filter_params.append(f'operationalCapacity > "{operational_capacity_gt}"')
        if operational_capacity_gte is not None:
            filter_params.append(f'operationalCapacity >= "{operational_capacity_gte}"')
        if operational_capacity_lt is not None:
            filter_params.append(f'operationalCapacity < "{operational_capacity_lt}"')
        if operational_capacity_lte is not None:
            filter_params.append(f'operationalCapacity <= "{operational_capacity_lte}"')

        filter_params.append(list_to_filter("operationallyAvailable", operationally_available))
        if operationally_available_gt is not None:
            filter_params.append(f'operationallyAvailable > "{operationally_available_gt}"')
        if operationally_available_gte is not None:
            filter_params.append(f'operationallyAvailable >= "{operationally_available_gte}"')
        if operationally_available_lt is not None:
            filter_params.append(f'operationallyAvailable < "{operationally_available_lt}"')
        if operationally_available_lte is not None:
            filter_params.append(f'operationallyAvailable <= "{operationally_available_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params) if len(filter_params) > 0 else None
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        if field is not None:
            params["field"] = ",".join(field) if isinstance(field, list) else str(field)
        if sort is not None:
            params["sort"] = sort

        response = get_data(
            path="/analytics/gas/na-gas/v1/pipeline-flows-essentials-delta",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_pipeline_flows_essentials_history(
        self,
        *,
        day: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        month: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_id: Optional[Union[list[str], Series[str], str]] = None,
        component_id: Optional[Union[list[str], Series[str], str]] = None,
        flow_date: Optional[date] = None,
        flow_date_lt: Optional[date] = None,
        flow_date_lte: Optional[date] = None,
        flow_date_gt: Optional[date] = None,
        flow_date_gte: Optional[date] = None,
        queue_id: Optional[Union[list[str], Series[str], str]] = None,
        nomination_cycle: Optional[Union[list[str], Series[str], str]] = None,
        location_type: Optional[Union[list[str], Series[str], str]] = None,
        scheduled_volume: Optional[str] = None,
        scheduled_volume_lt: Optional[str] = None,
        scheduled_volume_lte: Optional[str] = None,
        scheduled_volume_gt: Optional[str] = None,
        scheduled_volume_gte: Optional[str] = None,
        utilization: Optional[str] = None,
        utilization_lt: Optional[str] = None,
        utilization_lte: Optional[str] = None,
        utilization_gt: Optional[str] = None,
        utilization_gte: Optional[str] = None,
        design_capacity: Optional[str] = None,
        design_capacity_lt: Optional[str] = None,
        design_capacity_lte: Optional[str] = None,
        design_capacity_gt: Optional[str] = None,
        design_capacity_gte: Optional[str] = None,
        operational_capacity: Optional[str] = None,
        operational_capacity_lt: Optional[str] = None,
        operational_capacity_lte: Optional[str] = None,
        operational_capacity_gt: Optional[str] = None,
        operational_capacity_gte: Optional[str] = None,
        operationally_available: Optional[str] = None,
        operationally_available_lt: Optional[str] = None,
        operationally_available_lte: Optional[str] = None,
        operationally_available_gt: Optional[str] = None,
        operationally_available_gte: Optional[str] = None,
        interruptible_flow: Optional[Union[list[str], Series[str], str]] = None,
        data_source: Optional[Union[list[str], Series[str], str]] = None,
        data_active: Optional[Union[list[bool], Series[bool], bool]] = None,
        component_create_date: Optional[datetime] = None,
        component_create_date_lt: Optional[datetime] = None,
        component_create_date_lte: Optional[datetime] = None,
        component_create_date_gt: Optional[datetime] = None,
        component_create_date_gte: Optional[datetime] = None,
        valid_from: Optional[datetime] = None,
        valid_to: Optional[datetime] = None,
        valid_to_lt: Optional[datetime] = None,
        valid_to_lte: Optional[datetime] = None,
        valid_to_gt: Optional[datetime] = None,
        valid_to_gte: Optional[datetime] = None,
        reason: Optional[Union[list[str], Series[str], str]] = None,
        field: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Query the Pipeline Flows Essentials History endpoint.
        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("day", day))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("month", month))
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("componentId", component_id))
        filter_params.append(list_to_filter("queueId", queue_id))
        filter_params.append(list_to_filter("nominationCycle", nomination_cycle))
        filter_params.append(list_to_filter("locationType", location_type))

        filter_params.append(list_to_filter("flowDate", flow_date))
        if flow_date_gt is not None:
            filter_params.append(f'flowDate > "{flow_date_gt}"')
        if flow_date_gte is not None:
            filter_params.append(f'flowDate >= "{flow_date_gte}"')
        if flow_date_lt is not None:
            filter_params.append(f'flowDate < "{flow_date_lt}"')
        if flow_date_lte is not None:
            filter_params.append(f'flowDate <= "{flow_date_lte}"')

        filter_params.append(list_to_filter("scheduledVolume", scheduled_volume))
        if scheduled_volume_gt is not None:
            filter_params.append(f'scheduledVolume > "{scheduled_volume_gt}"')
        if scheduled_volume_gte is not None:
            filter_params.append(f'scheduledVolume >= "{scheduled_volume_gte}"')
        if scheduled_volume_lt is not None:
            filter_params.append(f'scheduledVolume < "{scheduled_volume_lt}"')
        if scheduled_volume_lte is not None:
            filter_params.append(f'scheduledVolume <= "{scheduled_volume_lte}"')

        filter_params.append(list_to_filter("utilization", utilization))
        if utilization_gt is not None:
            filter_params.append(f'utilization > "{utilization_gt}"')
        if utilization_gte is not None:
            filter_params.append(f'utilization >= "{utilization_gte}"')
        if utilization_lt is not None:
            filter_params.append(f'utilization < "{utilization_lt}"')
        if utilization_lte is not None:
            filter_params.append(f'utilization <= "{utilization_lte}"')

        filter_params.append(list_to_filter("designCapacity", design_capacity))
        if design_capacity_gt is not None:
            filter_params.append(f'designCapacity > "{design_capacity_gt}"')
        if design_capacity_gte is not None:
            filter_params.append(f'designCapacity >= "{design_capacity_gte}"')
        if design_capacity_lt is not None:
            filter_params.append(f'designCapacity < "{design_capacity_lt}"')
        if design_capacity_lte is not None:
            filter_params.append(f'designCapacity <= "{design_capacity_lte}"')

        filter_params.append(list_to_filter("operationalCapacity", operational_capacity))
        if operational_capacity_gt is not None:
            filter_params.append(f'operationalCapacity > "{operational_capacity_gt}"')
        if operational_capacity_gte is not None:
            filter_params.append(f'operationalCapacity >= "{operational_capacity_gte}"')
        if operational_capacity_lt is not None:
            filter_params.append(f'operationalCapacity < "{operational_capacity_lt}"')
        if operational_capacity_lte is not None:
            filter_params.append(f'operationalCapacity <= "{operational_capacity_lte}"')

        filter_params.append(list_to_filter("operationallyAvailable", operationally_available))
        if operationally_available_gt is not None:
            filter_params.append(f'operationallyAvailable > "{operationally_available_gt}"')
        if operationally_available_gte is not None:
            filter_params.append(f'operationallyAvailable >= "{operationally_available_gte}"')
        if operationally_available_lt is not None:
            filter_params.append(f'operationallyAvailable < "{operationally_available_lt}"')
        if operationally_available_lte is not None:
            filter_params.append(f'operationallyAvailable <= "{operationally_available_lte}"')

        filter_params.append(list_to_filter("interruptibleFlow", interruptible_flow))
        filter_params.append(list_to_filter("dataSource", data_source))
        filter_params.append(list_to_filter("dataActive", data_active))
        filter_params.append(list_to_filter("componentCreateDate", component_create_date))
        if component_create_date_gt is not None:
            filter_params.append(f'componentCreateDate > "{component_create_date_gt}"')
        if component_create_date_gte is not None:
            filter_params.append(f'componentCreateDate >= "{component_create_date_gte}"')
        if component_create_date_lt is not None:
            filter_params.append(f'componentCreateDate < "{component_create_date_lt}"')
        if component_create_date_lte is not None:
            filter_params.append(f'componentCreateDate <= "{component_create_date_lte}"')

        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')

        filter_params.append(list_to_filter("validFrom", valid_from))
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params) if len(filter_params) > 0 else None
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        if field is not None:
            params["field"] = ",".join(field) if isinstance(field, list) else str(field)
        if sort is not None:
            params["sort"] = sort

        response = get_data(
            path="/analytics/gas/na-gas/v1/pipeline-flows-essentials-history",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_modeled_demand_actual(
        self,
        *,
        flow_date: Optional[date] = None,
        flow_date_lt: Optional[date] = None,
        flow_date_lte: Optional[date] = None,
        flow_date_gt: Optional[date] = None,
        flow_date_gte: Optional[date] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        forecast_date: Optional[datetime] = None,
        forecast_date_lt: Optional[datetime] = None,
        forecast_date_lte: Optional[datetime] = None,
        forecast_date_gt: Optional[datetime] = None,
        forecast_date_gte: Optional[datetime] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        model_id: Optional[Union[list[str], Series[str], str]] = None,
        model_type: Optional[Union[list[str], Series[str], str]] = None,
        model_type_id: Optional[Union[list[str], Series[str], str]] = None,
        market_type: Optional[Union[list[str], Series[str], str]] = None,
        market_type_id: Optional[Union[list[str], Series[str], str]] = None,
        function_type: Optional[Union[list[str], Series[str], str]] = None,
        function_type_id: Optional[Union[list[str], Series[str], str]] = None,
        point_of_view: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        volume: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Daily natural gas modeled at a sector level for power, industrial, residential & commercial, fuel losses and total demand in the US lower 48 across all geographies. Canada and Mexico is included, but sector-level details may vary.

        Parameters
        ----------

         flow_date: Optional[date], optional
             The calendar date or gas day the activity occurred., by default None
         flow_date_gt: Optional[date], optional
             filter by `flow_date > x`, by default None
         flow_date_gte: Optional[date], optional
             filter by `flow_date >= x`, by default None
         flow_date_lt: Optional[date], optional
             filter by `flow_date < x`, by default None
         flow_date_lte: Optional[date], optional
             filter by `flow_date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         forecast_date: Optional[datetime], optional
             Standard Forecast Date., by default None
         forecast_date_gt: Optional[datetime], optional
             filter by `forecast_date > x`, by default None
         forecast_date_gte: Optional[datetime], optional
             filter by `forecast_date >= x`, by default None
         forecast_date_lt: Optional[datetime], optional
             filter by `forecast_date < x`, by default None
         forecast_date_lte: Optional[datetime], optional
             filter by `forecast_date <= x`, by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Daily, Weekly, Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Daily, Weekly, Monthly, Seasonal, Annual. Weekly date frequencies are based on the defined EIA storage week of Friday-Thursday. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         model_id: Optional[Union[list[str], Series[str], str]]
             Internal use, Model ID value., by default None
         model_type: Optional[Union[list[str], Series[str], str]]
             Model types can vary among supply, demand and other market fundamentals. The type describes the fundamentals the model output represents., by default None
         model_type_id: Optional[Union[list[str], Series[str], str]]
             ID associated with Model type., by default None
         market_type: Optional[Union[list[str], Series[str], str]]
             Market Type name, actual or forecast., by default None
         market_type_id: Optional[Union[list[str], Series[str], str]]
             ID associated with Market type., by default None
         function_type: Optional[Union[list[str], Series[str], str]]
             The name of the Function Type such as prediction, aggregation, allocation, ten year average., by default None
         function_type_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a Function Type such as 1 is prediction, 2 is aggregation, 3 is allocation, 4 is ten year average., by default None
         point_of_view: Optional[Union[list[str], Series[str], str]]
             Point of View for the values. Point of view based on a geographic hierarchy of country, region, subregion, or producing area., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         volume: Optional[Union[list[str], Series[str], str]]
             Volume., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("flowDate", flow_date))
        if flow_date_gt is not None:
            filter_params.append(f'flowDate > "{flow_date_gt}"')
        if flow_date_gte is not None:
            filter_params.append(f'flowDate >= "{flow_date_gte}"')
        if flow_date_lt is not None:
            filter_params.append(f'flowDate < "{flow_date_lt}"')
        if flow_date_lte is not None:
            filter_params.append(f'flowDate <= "{flow_date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("forecastDate", forecast_date))
        if forecast_date_gt is not None:
            filter_params.append(f'forecastDate > "{forecast_date_gt}"')
        if forecast_date_gte is not None:
            filter_params.append(f'forecastDate >= "{forecast_date_gte}"')
        if forecast_date_lt is not None:
            filter_params.append(f'forecastDate < "{forecast_date_lt}"')
        if forecast_date_lte is not None:
            filter_params.append(f'forecastDate <= "{forecast_date_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(list_to_filter("modelId", model_id))
        filter_params.append(list_to_filter("modelType", model_type))
        filter_params.append(list_to_filter("modelTypeId", model_type_id))
        filter_params.append(list_to_filter("marketType", market_type))
        filter_params.append(list_to_filter("marketTypeId", market_type_id))
        filter_params.append(list_to_filter("functionType", function_type))
        filter_params.append(list_to_filter("functionTypeId", function_type_id))
        filter_params.append(list_to_filter("pointOfView", point_of_view))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("volume", volume))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/modeled-demand-actual",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_natural_gas_production(
        self,
        *,
        flow_date: Optional[date] = None,
        flow_date_lt: Optional[date] = None,
        flow_date_lte: Optional[date] = None,
        flow_date_gt: Optional[date] = None,
        flow_date_gte: Optional[date] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        model_id: Optional[Union[list[str], Series[str], str]] = None,
        model_type: Optional[Union[list[str], Series[str], str]] = None,
        model_type_id: Optional[Union[list[str], Series[str], str]] = None,
        market_type: Optional[Union[list[str], Series[str], str]] = None,
        market_type_id: Optional[Union[list[str], Series[str], str]] = None,
        function_type: Optional[Union[list[str], Series[str], str]] = None,
        function_type_id: Optional[Union[list[str], Series[str], str]] = None,
        point_of_view: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        state_id: Optional[Union[list[str], Series[str], str]] = None,
        producing_area: Optional[Union[list[str], Series[str], str]] = None,
        producing_area_id: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        volume: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Daily natural gas modeled production in the US lower 48, Canada and Mexico that is accompanied with an array of metadata attributes. Model types include modeled wet gas, modeled production losses and modeled dry gas.

        Parameters
        ----------

         flow_date: date
             The calendar date or gas day the activity occurred.
         flow_date_gt: Optional[date], optional
             filter by `flow_date > x`, by default None
         flow_date_gte: Optional[date], optional
             filter by `flow_date >= x`, by default None
         flow_date_lt: Optional[date], optional
             filter by `flow_date < x`, by default None
         flow_date_lte: Optional[date], optional
             filter by `flow_date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Daily, Weekly, Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Daily, Weekly, Monthly, Seasonal, Annual. Weekly date frequencies are based on the defined EIA storage week of Friday-Thursday. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         model_id: Optional[Union[list[str], Series[str], str]]
             Internal use, Model ID value., by default None
         model_type: Optional[Union[list[str], Series[str], str]]
             Model types can vary among supply, demand and other market fundamentals. The type describes the fundamentals the model output represents., by default None
         model_type_id: Optional[Union[list[str], Series[str], str]]
             ID associated with Model type., by default None
         market_type: Optional[Union[list[str], Series[str], str]]
             Market Type name, actual or forecast., by default None
         market_type_id: Optional[Union[list[str], Series[str], str]]
             ID associated with Market type., by default None
         function_type: Optional[Union[list[str], Series[str], str]]
             The name of the Function Type such as prediction, aggregation, allocation, ten year average., by default None
         function_type_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a Function Type such as 1 is prediction, 2 is aggregation, 3 is allocation, 4 is ten year average., by default None
         point_of_view: Optional[Union[list[str], Series[str], str]]
             Point of View for the values. Point of view based on a geographic hierarchy of country, region, subregion, or producing area., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The political boundaries that define a state or province within country., by default None
         state_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the state or province utilizes legacy Bentek IDs., by default None
         producing_area: Optional[Union[list[str], Series[str], str]]
             Defined aggregation of counties within a state that is a best fit representation of prominent oil and gas plays and basins., by default None
         producing_area_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for a defined Producing Area utilizes legacy PointLogic IDs., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         volume: Optional[Union[list[str], Series[str], str]]
             Volume., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("flowDate", flow_date))
        if flow_date_gt is not None:
            filter_params.append(f'flowDate > "{flow_date_gt}"')
        if flow_date_gte is not None:
            filter_params.append(f'flowDate >= "{flow_date_gte}"')
        if flow_date_lt is not None:
            filter_params.append(f'flowDate < "{flow_date_lt}"')
        if flow_date_lte is not None:
            filter_params.append(f'flowDate <= "{flow_date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(list_to_filter("modelId", model_id))
        filter_params.append(list_to_filter("modelType", model_type))
        filter_params.append(list_to_filter("modelTypeId", model_type_id))
        filter_params.append(list_to_filter("marketType", market_type))
        filter_params.append(list_to_filter("marketTypeId", market_type_id))
        filter_params.append(list_to_filter("functionType", function_type))
        filter_params.append(list_to_filter("functionTypeId", function_type_id))
        filter_params.append(list_to_filter("pointOfView", point_of_view))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("stateId", state_id))
        filter_params.append(list_to_filter("producingArea", producing_area))
        filter_params.append(list_to_filter("producingAreaId", producing_area_id))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("volume", volume))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/natural-gas-production",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    

    def get_population_weighted_weather(
        self,
        *,
        flow_date: Optional[date] = None,
        flow_date_lt: Optional[date] = None,
        flow_date_lte: Optional[date] = None,
        flow_date_gt: Optional[date] = None,
        flow_date_gte: Optional[date] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        forecast_date: Optional[datetime] = None,
        forecast_date_lt: Optional[datetime] = None,
        forecast_date_lte: Optional[datetime] = None,
        forecast_date_gt: Optional[datetime] = None,
        forecast_date_gte: Optional[datetime] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        market_type: Optional[Union[list[str], Series[str], str]] = None,
        market_type_id: Optional[Union[list[str], Series[str], str]] = None,
        point_of_view: Optional[Union[list[str], Series[str], str]] = None,
        geography_id: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        temperature_unit_of_measure: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        weighted_temperature: Optional[str] = None,
        weighted_temperature_lt: Optional[str] = None,
        weighted_temperature_lte: Optional[str] = None,
        weighted_temperature_gt: Optional[str] = None,
        weighted_temperature_gte: Optional[str] = None,
        normal_temperature: Optional[str] = None,
        normal_temperature_lt: Optional[str] = None,
        normal_temperature_lte: Optional[str] = None,
        normal_temperature_gt: Optional[str] = None,
        normal_temperature_gte: Optional[str] = None,
        heating_degree_day: Optional[str] = None,
        heating_degree_day_lt: Optional[str] = None,
        heating_degree_day_lte: Optional[str] = None,
        heating_degree_day_gt: Optional[str] = None,
        heating_degree_day_gte: Optional[str] = None,
        cooling_degree_day: Optional[str] = None,
        cooling_degree_day_lt: Optional[str] = None,
        cooling_degree_day_lte: Optional[str] = None,
        cooling_degree_day_gt: Optional[str] = None,
        cooling_degree_day_gte: Optional[str] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Population weighted temperatures and degree days by geography within the US lower 48, Canada and Mexico.

        Parameters
        ----------

         flow_date: date, optional
             The calendar date or gas day the activity occurred.
         flow_date_gt: Optional[date], optional
             filter by `flow_date > x`, by default None
         flow_date_gte: Optional[date], optional
             filter by `flow_date >= x`, by default None
         flow_date_lt: Optional[date], optional
             filter by `flow_date < x`, by default None
         flow_date_lte: Optional[date], optional
             filter by `flow_date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         forecast_date: Optional[datetime], optional
             Standard Forecast Date., by default None
         forecast_date_gt: Optional[datetime], optional
             filter by `forecast_date > x`, by default None
         forecast_date_gte: Optional[datetime], optional
             filter by `forecast_date >= x`, by default None
         forecast_date_lt: Optional[datetime], optional
             filter by `forecast_date < x`, by default None
         forecast_date_lte: Optional[datetime], optional
             filter by `forecast_date <= x`, by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Daily, Weekly, Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Daily, Weekly, Monthly, Seasonal, Annual. Weekly date frequencies are based on the defined EIA storage week of Friday-Thursday. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         market_type: Optional[Union[list[str], Series[str], str]]
             Market Type name, actual or forecast, by default None
         market_type_id: Optional[Union[list[str], Series[str], str]]
             ID associated with Market type., by default None
         point_of_view: Optional[Union[list[str], Series[str], str]]
             Point of View for the values. Point of view based on a geographic hierarchy of country, region, subregion, or producing area., by default None
         geography_id: Optional[Union[list[str], Series[str], str]]
             Geography ID value for the point of view., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         temperature_unit_of_measure: Optional[Union[list[str], Series[str], str]]
             Temperature unit of measure., by default None
         weighted_temperature: Optional[str], optional
             Population weighted temperature value., by default None
         weighted_temperature_gt: Optional[str], optional
             filter by `weighted_temperature > x`, by default None
         weighted_temperature_gte: Optional[str], optional
             filter by `weighted_temperature >= x`, by default None
         weighted_temperature_lt: Optional[str], optional
             filter by `weighted_temperature < x`, by default None
         weighted_temperature_lte: Optional[str], optional
             filter by `weighted_temperature <= x`, by default None
         normal_temperature: Optional[str], optional
             10-year normal temperature value., by default None
         normal_temperature_gt: Optional[str], optional
             filter by `normal_temperature > x`, by default None
         normal_temperature_gte: Optional[str], optional
             filter by `normal_temperature >= x`, by default None
         normal_temperature_lt: Optional[str], optional
             filter by `normal_temperature < x`, by default None
         normal_temperature_lte: Optional[str], optional
             filter by `normal_temperature <= x`, by default None
         heating_degree_day: Optional[str], optional
             Heating Degree Day value., by default None
         heating_degree_day_gt: Optional[str], optional
             filter by `heating_degree_day > x`, by default None
         heating_degree_day_gte: Optional[str], optional
             filter by `heating_degree_day >= x`, by default None
         heating_degree_day_lt: Optional[str], optional
             filter by `heating_degree_day < x`, by default None
         heating_degree_day_lte: Optional[str], optional
             filter by `heating_degree_day <= x`, by default None
         cooling_degree_day: Optional[str], optional
             Cooling Degree Day value., by default None
         cooling_degree_day_gt: Optional[str], optional
             filter by `cooling_degree_day > x`, by default None
         cooling_degree_day_gte: Optional[str], optional
             filter by `cooling_degree_day >= x`, by default None
         cooling_degree_day_lt: Optional[str], optional
             filter by `cooling_degree_day < x`, by default None
         cooling_degree_day_lte: Optional[str], optional
             filter by `cooling_degree_day <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("flowDate", flow_date))
        if flow_date_gt is not None:
            filter_params.append(f'flowDate > "{flow_date_gt}"')
        if flow_date_gte is not None:
            filter_params.append(f'flowDate >= "{flow_date_gte}"')
        if flow_date_lt is not None:
            filter_params.append(f'flowDate < "{flow_date_lt}"')
        if flow_date_lte is not None:
            filter_params.append(f'flowDate <= "{flow_date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("forecastDate", forecast_date))
        if forecast_date_gt is not None:
            filter_params.append(f'forecastDate > "{forecast_date_gt}"')
        if forecast_date_gte is not None:
            filter_params.append(f'forecastDate >= "{forecast_date_gte}"')
        if forecast_date_lt is not None:
            filter_params.append(f'forecastDate < "{forecast_date_lt}"')
        if forecast_date_lte is not None:
            filter_params.append(f'forecastDate <= "{forecast_date_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(list_to_filter("marketType", market_type))
        filter_params.append(list_to_filter("marketTypeId", market_type_id))
        filter_params.append(list_to_filter("pointOfView", point_of_view))
        filter_params.append(list_to_filter("geographyId", geography_id))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(
            list_to_filter("temperatureUnitOfMeasure", temperature_unit_of_measure)
        )
        filter_params.append(
            list_to_filter("weightedTemperature", weighted_temperature)
        )
        if weighted_temperature_gt is not None:
            filter_params.append(f'weightedTemperature > "{weighted_temperature_gt}"')
        if weighted_temperature_gte is not None:
            filter_params.append(f'weightedTemperature >= "{weighted_temperature_gte}"')
        if weighted_temperature_lt is not None:
            filter_params.append(f'weightedTemperature < "{weighted_temperature_lt}"')
        if weighted_temperature_lte is not None:
            filter_params.append(f'weightedTemperature <= "{weighted_temperature_lte}"')
        filter_params.append(list_to_filter("normalTemperature", normal_temperature))
        if normal_temperature_gt is not None:
            filter_params.append(f'normalTemperature > "{normal_temperature_gt}"')
        if normal_temperature_gte is not None:
            filter_params.append(f'normalTemperature >= "{normal_temperature_gte}"')
        if normal_temperature_lt is not None:
            filter_params.append(f'normalTemperature < "{normal_temperature_lt}"')
        if normal_temperature_lte is not None:
            filter_params.append(f'normalTemperature <= "{normal_temperature_lte}"')
        filter_params.append(list_to_filter("heatingDegreeDay", heating_degree_day))
        if heating_degree_day_gt is not None:
            filter_params.append(f'heatingDegreeDay > "{heating_degree_day_gt}"')
        if heating_degree_day_gte is not None:
            filter_params.append(f'heatingDegreeDay >= "{heating_degree_day_gte}"')
        if heating_degree_day_lt is not None:
            filter_params.append(f'heatingDegreeDay < "{heating_degree_day_lt}"')
        if heating_degree_day_lte is not None:
            filter_params.append(f'heatingDegreeDay <= "{heating_degree_day_lte}"')
        filter_params.append(list_to_filter("coolingDegreeDay", cooling_degree_day))
        if cooling_degree_day_gt is not None:
            filter_params.append(f'coolingDegreeDay > "{cooling_degree_day_gt}"')
        if cooling_degree_day_gte is not None:
            filter_params.append(f'coolingDegreeDay >= "{cooling_degree_day_gte}"')
        if cooling_degree_day_lt is not None:
            filter_params.append(f'coolingDegreeDay < "{cooling_degree_day_lt}"')
        if cooling_degree_day_lte is not None:
            filter_params.append(f'coolingDegreeDay <= "{cooling_degree_day_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/population-weighted-weather",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    
    def get_outlook_production_play(
        self,
        *,
        date: Optional[datetime] = None,
        date_lt: Optional[datetime] = None,
        date_lte: Optional[datetime] = None,
        date_gt: Optional[datetime] = None,
        date_gte: Optional[datetime] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        vintage: Optional[Union[list[str], Series[str], str]] = None,
        vintage_type: Optional[Union[list[str], Series[str], str]] = None,
        aggregate_type: Optional[Union[list[str], Series[str], str]] = None,
        aggregate_play: Optional[Union[list[str], Series[str], str]] = None,
        subplay: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        gulfcoast_substate: Optional[Union[list[str], Series[str], str]] = None,
        gulfcoast_substate_id: Optional[Union[list[str], Series[str], str]] = None,
        state_abbreviation: Optional[Union[list[str], Series[str], str]] = None,
        state_id: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Dry natural gas production forecast across sub plays and aggregations that align to related vintages of the short and long-term outlooks.

        Parameters
        ----------

         date: Optional[datetime], optional
             The calendar date or the date when the activity occurred., by default None
         date_gt: Optional[datetime], optional
             filter by `date > x`, by default None
         date_gte: Optional[datetime], optional
             filter by `date >= x`, by default None
         date_lt: Optional[datetime], optional
             filter by `date < x`, by default None
         date_lte: Optional[datetime], optional
             filter by `date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Daily, Weekly, Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Daily, Weekly, Monthly, Seasonal, Annual. Weekly date frequencies are based on the defined EIA storage week of Friday-Thursday. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         year: Optional[Union[list[str], Series[str], str]]
             The calendar year or the year when the activity occurred., by default None
         vintage: Optional[Union[list[str], Series[str], str]]
             The year and month the short term outlook (STO) was issued.  Long term outlook (LTO) is bi-annual and expressed by year and instance., by default None
         vintage_type: Optional[Union[list[str], Series[str], str]]
             The outlook type for each vintage is either short term outlook or long term outlook. In general, short term outlooks are a five-year forecast and long term outlooks can be for up to 30 years., by default None
         aggregate_type: Optional[Union[list[str], Series[str], str]]
             An aggregation of US Lower-48 sub plays into five  high-level expressions such as associated (oil-driven plays), Haynesville, Marcellus/Utica, Gulf of Mexico and Other Dry Gas., by default None
         aggregate_play: Optional[Union[list[str], Series[str], str]]
             Aggregate Play is mid-level summary of Sub Plays, but more granular than Aggregate Type.  In some cases Aggregate Play and Sub Play will match., by default None
         subplay: Optional[Union[list[str], Series[str], str]]
             Production forecasts at a Sub Play level in its most granular expression and compliments the regional aggregation of production within the short and long term outlooks., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada or Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         gulfcoast_substate: Optional[Union[list[str], Series[str], str]]
             The name of substate region or special area within the Gulf Coast region., by default None
         gulfcoast_substate_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic substate regions within the Gulf Coast area., by default None
         state_abbreviation: Optional[Union[list[str], Series[str], str]]
             Abbreviation for a state or province within country., by default None
         state_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the state or province utilizes legacy Bentek IDs., by default None
         value: Optional[Union[list[str], Series[str], str]]
             The dry natural gas production volume expressed in a daily average format., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("vintage", vintage))
        filter_params.append(list_to_filter("vintageType", vintage_type))
        filter_params.append(list_to_filter("aggregateType", aggregate_type))
        filter_params.append(list_to_filter("aggregatePlay", aggregate_play))
        filter_params.append(list_to_filter("subplay", subplay))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("gulfcoastSubstate", gulfcoast_substate))
        filter_params.append(
            list_to_filter("gulfcoastSubstateId", gulfcoast_substate_id)
        )
        filter_params.append(list_to_filter("stateAbbreviation", state_abbreviation))
        filter_params.append(list_to_filter("stateId", state_id))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("uom", uom))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/outlook-production-play",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    

    def get_outlook_marketbalances_prices(
        self,
        *,
        vintage_type: Optional[Union[list[str], Series[str], str]] = None,
        vintage: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        date: Optional[date] = None,
        date_lt: Optional[date] = None,
        date_lte: Optional[date] = None,
        date_gt: Optional[date] = None,
        date_gte: Optional[date] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        geography_point_of_view: Optional[Union[list[str], Series[str], str]] = None,
        geography_id: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Natural gas forecast for market fundamentals and prices within Canada, United States and Mexico. Outlook periods consist of history and forecast in the short-term covering 5 years and the long-term up to 30 years.

        Parameters
        ----------

         vintage_type: Optional[Union[list[str], Series[str], str]]
             The outlook type for each vintage is either short term outlook or long term outlook. In general, short term outlooks are a five-year forecast and long term outlooks can be for up to 30 years., by default None
         vintage: Optional[Union[list[str], Series[str], str]]
             The year and month the short term outlook (STO) was issued.  Long term outlook (LTO) is bi-annual and expressed by year and instance., by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Daily, Weekly, Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Monthly, Seasonal, Annual. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         year: Optional[Union[list[str], Series[str], str]]
             The calendar year or the year when the activity occurred., by default None
         date: Optional[date], optional
             The calendar date or the date when the activity occurred., by default None
         date_gt: Optional[date], optional
             filter by `date > x`, by default None
         date_gte: Optional[date], optional
             filter by `date >= x`, by default None
         date_lt: Optional[date], optional
             filter by `date < x`, by default None
         date_lte: Optional[date], optional
             filter by `date <= x`, by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Data concept such as Supply, Demand, LNG, Storage, Flows, etc., by default None
         category: Optional[Union[list[str], Series[str], str]]
             Category that is unique within a Concept., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         geography_point_of_view: Optional[Union[list[str], Series[str], str]]
             The geography in which the data perspective is expressed., by default None
         geography_id: Optional[Union[list[str], Series[str], str]]
             Geography ID value for the point of view., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         value: Optional[Union[list[str], Series[str], str]]
             Value of the record., by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("vintageType", vintage_type))
        filter_params.append(list_to_filter("vintage", vintage))
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("date", date))
        if date_gt is not None:
            filter_params.append(f'date > "{date_gt}"')
        if date_gte is not None:
            filter_params.append(f'date >= "{date_gte}"')
        if date_lt is not None:
            filter_params.append(f'date < "{date_lt}"')
        if date_lte is not None:
            filter_params.append(f'date <= "{date_lte}"')
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(
            list_to_filter("geographyPointOfView", geography_point_of_view)
        )
        filter_params.append(list_to_filter("geographyId", geography_id))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/outlook-marketbalances-prices",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_pipeline_storage_projects(
        self,
        *,
        project_name: Optional[Union[list[str], Series[str], str]] = None,
        company_name: Optional[Union[list[str], Series[str], str]] = None,
        company_id: Optional[Union[list[str], Series[str], str]] = None,
        infrastructure_type: Optional[Union[list[str], Series[str], str]] = None,
        project_id: Optional[Union[list[str], Series[str], str]] = None,
        storage_field_name: Optional[Union[list[str], Series[str], str]] = None,
        storage_field_type: Optional[Union[list[str], Series[str], str]] = None,
        project_type: Optional[Union[list[str], Series[str], str]] = None,
        project_source: Optional[Union[list[str], Series[str], str]] = None,
        capacity: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        in_service_date: Optional[date] = None,
        in_service_date_lt: Optional[date] = None,
        in_service_date_lte: Optional[date] = None,
        in_service_date_gt: Optional[date] = None,
        in_service_date_gte: Optional[date] = None,
        in_service_year: Optional[Union[list[str], Series[str], str]] = None,
        inservice_quarter: Optional[Union[list[str], Series[str], str]] = None,
        project_created_date: Optional[date] = None,
        project_created_date_lt: Optional[date] = None,
        project_created_date_lte: Optional[date] = None,
        project_created_date_gt: Optional[date] = None,
        project_created_date_gte: Optional[date] = None,
        project_updated_date: Optional[date] = None,
        project_updated_date_lt: Optional[date] = None,
        project_updated_date_lte: Optional[date] = None,
        project_updated_date_gt: Optional[date] = None,
        project_updated_date_gte: Optional[date] = None,
        pre_file_date: Optional[date] = None,
        pre_file_date_lt: Optional[date] = None,
        pre_file_date_lte: Optional[date] = None,
        pre_file_date_gt: Optional[date] = None,
        pre_file_date_gte: Optional[date] = None,
        project_file_date: Optional[date] = None,
        project_file_date_lt: Optional[date] = None,
        project_file_date_lte: Optional[date] = None,
        project_file_date_gt: Optional[date] = None,
        project_file_date_gte: Optional[date] = None,
        project_approval_date: Optional[date] = None,
        project_approval_date_lt: Optional[date] = None,
        project_approval_date_lte: Optional[date] = None,
        project_approval_date_gt: Optional[date] = None,
        project_approval_date_gte: Optional[date] = None,
        production_enabling: Optional[Union[list[str], Series[str], str]] = None,
        play_where_production_is_enabled: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        additional_production_enabling_capacity_mmcfd: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        lng_related: Optional[Union[list[str], Series[str], str]] = None,
        function_served_by_pipeline: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        name_of_lng_facility_served: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        capacity_available_to_lng_facility_mmcfd: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        miles: Optional[Union[list[str], Series[str], str]] = None,
        authority: Optional[Union[list[str], Series[str], str]] = None,
        regulatory_document_identifier: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        project_is_active: Optional[Union[list[bool], Series[bool], bool]] = None,
        from_domain: Optional[Union[list[str], Series[str], str]] = None,
        from_domain_id: Optional[Union[list[str], Series[str], str]] = None,
        to_domain: Optional[Union[list[str], Series[str], str]] = None,
        to_domain_id: Optional[Union[list[str], Series[str], str]] = None,
        from_state_name: Optional[Union[list[str], Series[str], str]] = None,
        from_state_id: Optional[Union[list[str], Series[str], str]] = None,
        to_state_name: Optional[Union[list[str], Series[str], str]] = None,
        to_state_id: Optional[Union[list[str], Series[str], str]] = None,
        from_county: Optional[Union[list[str], Series[str], str]] = None,
        from_county_id: Optional[Union[list[str], Series[str], str]] = None,
        to_county: Optional[Union[list[str], Series[str], str]] = None,
        to_county_id: Optional[Union[list[str], Series[str], str]] = None,
        flow_path_states: Optional[Union[list[str], Series[str], str]] = None,
        region_start: Optional[Union[list[str], Series[str], str]] = None,
        region_start_id: Optional[Union[list[str], Series[str], str]] = None,
        region_end: Optional[Union[list[str], Series[str], str]] = None,
        region_end_id: Optional[Union[list[str], Series[str], str]] = None,
        sub_region_start: Optional[Union[list[str], Series[str], str]] = None,
        sub_region_start_id: Optional[Union[list[str], Series[str], str]] = None,
        sub_region_end: Optional[Union[list[str], Series[str], str]] = None,
        sub_region_end_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_project_estimated_cost_thousand_usd: Optional[
            Union[list[float], Series[float], float]
        ] = None,
        rate_usd_dth: Optional[Union[list[float], Series[float], float]] = None,
        comm_rate_usd_dth: Optional[Union[list[float], Series[float], float]] = None,
        status: Optional[Union[list[str], Series[str], str]] = None,
        common_operator: Optional[Union[list[str], Series[str], str]] = None,
        parent_company: Optional[Union[list[str], Series[str], str]] = None,
        scope: Optional[Union[list[str], Series[str], str]] = None,
        create_date: Optional[datetime] = None,
        create_date_lt: Optional[datetime] = None,
        create_date_lte: Optional[datetime] = None,
        create_date_gt: Optional[datetime] = None,
        create_date_gte: Optional[datetime] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Natural gas pipeline and storage projects that expand or enhance the infrastructure affecting the gas markets in the US, Canada or Mexico.

        Parameters
        ----------

         project_name: Optional[Union[list[str], Series[str], str]]
             The name of the project as it is displayed on Platts Connect., by default None
         company_name: Optional[Union[list[str], Series[str], str]]
             The name of the project's company or related pipeline name., by default None
         company_id: Optional[Union[list[str], Series[str], str]]
             An ID sourced from the legacy Bentek service used to identify the connecting business or company name of a meter., by default None
         infrastructure_type: Optional[Union[list[str], Series[str], str]]
             The name of the type of project- Pipeline, LNG facility or Storage., by default None
         project_id: Optional[Union[list[str], Series[str], str]]
             The integer project id for the project., by default None
         storage_field_name: Optional[Union[list[str], Series[str], str]]
             The storage field name where the project is located., by default None
         storage_field_type: Optional[Union[list[str], Series[str], str]]
             The type of storage field. When applicable, the name will align to EIA-191., by default None
         project_type: Optional[Union[list[str], Series[str], str]]
             The project type description such as New Pipeline, Expansion, Compression, Reversal, Active facility expansion among many others., by default None
         project_source: Optional[Union[list[str], Series[str], str]]
             Where information about the project was collected., by default None
         capacity: Optional[Union[list[str], Series[str], str]]
             The capacity of the project- How much gas can be transported or stored., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measure for gas- in this case MMcf or MMcf/d., by default None
         in_service_date: Optional[date], optional
             The date the project was put in service by the company., by default None
         in_service_date_gt: Optional[date], optional
             filter by `in_service_date > x`, by default None
         in_service_date_gte: Optional[date], optional
             filter by `in_service_date >= x`, by default None
         in_service_date_lt: Optional[date], optional
             filter by `in_service_date < x`, by default None
         in_service_date_lte: Optional[date], optional
             filter by `in_service_date <= x`, by default None
         in_service_year: Optional[Union[list[str], Series[str], str]]
             The year the project started service., by default None
         inservice_quarter: Optional[Union[list[str], Series[str], str]]
             The in-service quarter of the project., by default None
         project_created_date: Optional[date], optional
             The create date of the project record., by default None
         project_created_date_gt: Optional[date], optional
             filter by `project_created_date > x`, by default None
         project_created_date_gte: Optional[date], optional
             filter by `project_created_date >= x`, by default None
         project_created_date_lt: Optional[date], optional
             filter by `project_created_date < x`, by default None
         project_created_date_lte: Optional[date], optional
             filter by `project_created_date <= x`, by default None
         project_updated_date: Optional[date], optional
             Date of the last update of the project record., by default None
         project_updated_date_gt: Optional[date], optional
             filter by `project_updated_date > x`, by default None
         project_updated_date_gte: Optional[date], optional
             filter by `project_updated_date >= x`, by default None
         project_updated_date_lt: Optional[date], optional
             filter by `project_updated_date < x`, by default None
         project_updated_date_lte: Optional[date], optional
             filter by `project_updated_date <= x`, by default None
         pre_file_date: Optional[date], optional
             The date the project pre-filed with the regulatory authority., by default None
         pre_file_date_gt: Optional[date], optional
             filter by `pre_file_date > x`, by default None
         pre_file_date_gte: Optional[date], optional
             filter by `pre_file_date >= x`, by default None
         pre_file_date_lt: Optional[date], optional
             filter by `pre_file_date < x`, by default None
         pre_file_date_lte: Optional[date], optional
             filter by `pre_file_date <= x`, by default None
         project_file_date: Optional[date], optional
             The date when the project filed with the regulatory authority., by default None
         project_file_date_gt: Optional[date], optional
             filter by `project_file_date > x`, by default None
         project_file_date_gte: Optional[date], optional
             filter by `project_file_date >= x`, by default None
         project_file_date_lt: Optional[date], optional
             filter by `project_file_date < x`, by default None
         project_file_date_lte: Optional[date], optional
             filter by `project_file_date <= x`, by default None
         project_approval_date: Optional[date], optional
             The date the project was approved by the regulatory agency., by default None
         project_approval_date_gt: Optional[date], optional
             filter by `project_approval_date > x`, by default None
         project_approval_date_gte: Optional[date], optional
             filter by `project_approval_date >= x`, by default None
         project_approval_date_lt: Optional[date], optional
             filter by `project_approval_date < x`, by default None
         project_approval_date_lte: Optional[date], optional
             filter by `project_approval_date <= x`, by default None
         production_enabling: Optional[Union[list[str], Series[str], str]]
             Indicates whether a project provides an incremental production outlet connecting to downstream markets (i.e., a production-enabling project)., by default None
         play_where_production_is_enabled: Optional[Union[list[str], Series[str], str]]
             The production play where the project is located., by default None
         additional_production_enabling_capacity_mmcfd: Optional[Union[list[str], Series[str], str]]
             For projects classified as production-enabling, the size of the incremental production outlet., by default None
         lng_related: Optional[Union[list[str], Series[str], str]]
             Is this related to an LNG facility?  Yes or No., by default None
         function_served_by_pipeline: Optional[Union[list[str], Series[str], str]]
             The overall function what the project serves to do once built., by default None
         name_of_lng_facility_served: Optional[Union[list[str], Series[str], str]]
             The name of the projects primary customer., by default None
         capacity_available_to_lng_facility_mmcfd: Optional[Union[list[str], Series[str], str]]
             The available capacity to the LNG facility in MMcf/d., by default None
         miles: Optional[Union[list[str], Series[str], str]]
             The length of the project in miles., by default None
         authority: Optional[Union[list[str], Series[str], str]]
             The project regulatory authority name., by default None
         regulatory_document_identifier: Optional[Union[list[str], Series[str], str]]
             The name of the project's regulatory record ID., by default None
         project_is_active: Optional[Union[list[bool], Series[bool], bool]]
             Is the project active or not- True it should show up on Platts Connect and False it should not show up on Platts Connect., by default None
         from_domain: Optional[Union[list[str], Series[str], str]]
             The name of the country- US lower 48, Canada and Mexico where the project originates., by default None
         from_domain_id: Optional[Union[list[str], Series[str], str]]
             The integer id of the country- US lower 48, Canada and Mexico where the project originates., by default None
         to_domain: Optional[Union[list[str], Series[str], str]]
             The name of the country- US lower 48, Canada and Mexico where the project terminates., by default None
         to_domain_id: Optional[Union[list[str], Series[str], str]]
             The integer id of the country- US lower 48, Canada and Mexico where the project terminates., by default None
         from_state_name: Optional[Union[list[str], Series[str], str]]
             The name of the state where the project originates., by default None
         from_state_id: Optional[Union[list[str], Series[str], str]]
             The integer id of the state where the project originates., by default None
         to_state_name: Optional[Union[list[str], Series[str], str]]
             The name of the state where the project terminates., by default None
         to_state_id: Optional[Union[list[str], Series[str], str]]
             The integer id of the state where the project terminates., by default None
         from_county: Optional[Union[list[str], Series[str], str]]
             The name of the county where the project originates., by default None
         from_county_id: Optional[Union[list[str], Series[str], str]]
             The integer id of the county where the project originates., by default None
         to_county: Optional[Union[list[str], Series[str], str]]
             The name of the county where the project terminates., by default None
         to_county_id: Optional[Union[list[str], Series[str], str]]
             The integer id of the county where the project terminates., by default None
         flow_path_states: Optional[Union[list[str], Series[str], str]]
             The abbreviated names of the states that the project resides in or exists in as infrastructure., by default None
         region_start: Optional[Union[list[str], Series[str], str]]
             The name of the region where the project originates., by default None
         region_start_id: Optional[Union[list[str], Series[str], str]]
             The integer id of the region where the project originates., by default None
         region_end: Optional[Union[list[str], Series[str], str]]
             The name of the region where the project terminates., by default None
         region_end_id: Optional[Union[list[str], Series[str], str]]
             The integer id of the region where the project terminates., by default None
         sub_region_start: Optional[Union[list[str], Series[str], str]]
             The name of the sub-region where the project originates., by default None
         sub_region_start_id: Optional[Union[list[str], Series[str], str]]
             The integer id of the sub-region where the project originates., by default None
         sub_region_end: Optional[Union[list[str], Series[str], str]]
             The name of the sub-region where the project terminates., by default None
         sub_region_end_id: Optional[Union[list[str], Series[str], str]]
             The integer id of the sub-region where the project terminates., by default None
         pipeline_project_estimated_cost_thousand_usd: Optional[Union[list[float], Series[float], float]]
             The estimated cost of the project in thousands- US dollars., by default None
         rate_usd_dth: Optional[Union[list[float], Series[float], float]]
             Rate in US dollars per dekatherm., by default None
         comm_rate_usd_dth: Optional[Union[list[float], Series[float], float]]
             Project transportation costs classified as the commodity (usage) rate in USD per Dekatherm = commRateUSDDth Value., by default None
         status: Optional[Union[list[str], Series[str], str]]
             The status of the project., by default None
         common_operator: Optional[Union[list[str], Series[str], str]]
             The operator of the project., by default None
         parent_company: Optional[Union[list[str], Series[str], str]]
             The parent company of the project., by default None
         scope: Optional[Union[list[str], Series[str], str]]
             The project description., by default None
         create_date: Optional[datetime], optional
             The date and time stamp of when the record was created., by default None
         create_date_gt: Optional[datetime], optional
             filter by `create_date > x`, by default None
         create_date_gte: Optional[datetime], optional
             filter by `create_date >= x`, by default None
         create_date_lt: Optional[datetime], optional
             filter by `create_date < x`, by default None
         create_date_lte: Optional[datetime], optional
             filter by `create_date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("companyName", company_name))
        filter_params.append(list_to_filter("companyId", company_id))
        filter_params.append(list_to_filter("infrastructureType", infrastructure_type))
        filter_params.append(list_to_filter("projectId", project_id))
        filter_params.append(list_to_filter("storageFieldName", storage_field_name))
        filter_params.append(list_to_filter("storageFieldType", storage_field_type))
        filter_params.append(list_to_filter("projectType", project_type))
        filter_params.append(list_to_filter("projectSource", project_source))
        filter_params.append(list_to_filter("capacity", capacity))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("inServiceDate", in_service_date))
        if in_service_date_gt is not None:
            filter_params.append(f'inServiceDate > "{in_service_date_gt}"')
        if in_service_date_gte is not None:
            filter_params.append(f'inServiceDate >= "{in_service_date_gte}"')
        if in_service_date_lt is not None:
            filter_params.append(f'inServiceDate < "{in_service_date_lt}"')
        if in_service_date_lte is not None:
            filter_params.append(f'inServiceDate <= "{in_service_date_lte}"')
        filter_params.append(list_to_filter("inServiceYear", in_service_year))
        filter_params.append(list_to_filter("inserviceQuarter", inservice_quarter))
        filter_params.append(list_to_filter("projectCreatedDate", project_created_date))
        if project_created_date_gt is not None:
            filter_params.append(f'projectCreatedDate > "{project_created_date_gt}"')
        if project_created_date_gte is not None:
            filter_params.append(f'projectCreatedDate >= "{project_created_date_gte}"')
        if project_created_date_lt is not None:
            filter_params.append(f'projectCreatedDate < "{project_created_date_lt}"')
        if project_created_date_lte is not None:
            filter_params.append(f'projectCreatedDate <= "{project_created_date_lte}"')
        filter_params.append(list_to_filter("projectUpdatedDate", project_updated_date))
        if project_updated_date_gt is not None:
            filter_params.append(f'projectUpdatedDate > "{project_updated_date_gt}"')
        if project_updated_date_gte is not None:
            filter_params.append(f'projectUpdatedDate >= "{project_updated_date_gte}"')
        if project_updated_date_lt is not None:
            filter_params.append(f'projectUpdatedDate < "{project_updated_date_lt}"')
        if project_updated_date_lte is not None:
            filter_params.append(f'projectUpdatedDate <= "{project_updated_date_lte}"')
        filter_params.append(list_to_filter("preFileDate", pre_file_date))
        if pre_file_date_gt is not None:
            filter_params.append(f'preFileDate > "{pre_file_date_gt}"')
        if pre_file_date_gte is not None:
            filter_params.append(f'preFileDate >= "{pre_file_date_gte}"')
        if pre_file_date_lt is not None:
            filter_params.append(f'preFileDate < "{pre_file_date_lt}"')
        if pre_file_date_lte is not None:
            filter_params.append(f'preFileDate <= "{pre_file_date_lte}"')
        filter_params.append(list_to_filter("projectFileDate", project_file_date))
        if project_file_date_gt is not None:
            filter_params.append(f'projectFileDate > "{project_file_date_gt}"')
        if project_file_date_gte is not None:
            filter_params.append(f'projectFileDate >= "{project_file_date_gte}"')
        if project_file_date_lt is not None:
            filter_params.append(f'projectFileDate < "{project_file_date_lt}"')
        if project_file_date_lte is not None:
            filter_params.append(f'projectFileDate <= "{project_file_date_lte}"')
        filter_params.append(
            list_to_filter("projectApprovalDate", project_approval_date)
        )
        if project_approval_date_gt is not None:
            filter_params.append(f'projectApprovalDate > "{project_approval_date_gt}"')
        if project_approval_date_gte is not None:
            filter_params.append(
                f'projectApprovalDate >= "{project_approval_date_gte}"'
            )
        if project_approval_date_lt is not None:
            filter_params.append(f'projectApprovalDate < "{project_approval_date_lt}"')
        if project_approval_date_lte is not None:
            filter_params.append(
                f'projectApprovalDate <= "{project_approval_date_lte}"'
            )
        filter_params.append(list_to_filter("productionEnabling", production_enabling))
        filter_params.append(
            list_to_filter(
                "playWhereProductionIsEnabled", play_where_production_is_enabled
            )
        )
        filter_params.append(
            list_to_filter(
                "additionalProductionEnablingCapacityMmcfd",
                additional_production_enabling_capacity_mmcfd,
            )
        )
        filter_params.append(list_to_filter("lngRelated", lng_related))
        filter_params.append(
            list_to_filter("functionServedByPipeline", function_served_by_pipeline)
        )
        filter_params.append(
            list_to_filter("nameOfLngFacilityServed", name_of_lng_facility_served)
        )
        filter_params.append(
            list_to_filter(
                "capacityAvailableToLngFacilityMmcfd",
                capacity_available_to_lng_facility_mmcfd,
            )
        )
        filter_params.append(list_to_filter("miles", miles))
        filter_params.append(list_to_filter("authority", authority))
        filter_params.append(
            list_to_filter(
                "regulatoryDocumentIdentifier", regulatory_document_identifier
            )
        )
        filter_params.append(list_to_filter("projectIsActive", project_is_active))
        filter_params.append(list_to_filter("fromDomain", from_domain))
        filter_params.append(list_to_filter("fromDomainId", from_domain_id))
        filter_params.append(list_to_filter("toDomain", to_domain))
        filter_params.append(list_to_filter("toDomainId", to_domain_id))
        filter_params.append(list_to_filter("fromStateName", from_state_name))
        filter_params.append(list_to_filter("fromStateId", from_state_id))
        filter_params.append(list_to_filter("toStateName", to_state_name))
        filter_params.append(list_to_filter("toStateId", to_state_id))
        filter_params.append(list_to_filter("fromCounty", from_county))
        filter_params.append(list_to_filter("fromCountyId", from_county_id))
        filter_params.append(list_to_filter("toCounty", to_county))
        filter_params.append(list_to_filter("toCountyId", to_county_id))
        filter_params.append(list_to_filter("flowPathStates", flow_path_states))
        filter_params.append(list_to_filter("regionStart", region_start))
        filter_params.append(list_to_filter("regionStartId", region_start_id))
        filter_params.append(list_to_filter("regionEnd", region_end))
        filter_params.append(list_to_filter("regionEndId", region_end_id))
        filter_params.append(list_to_filter("subRegionStart", sub_region_start))
        filter_params.append(list_to_filter("subRegionStartId", sub_region_start_id))
        filter_params.append(list_to_filter("subRegionEnd", sub_region_end))
        filter_params.append(list_to_filter("subRegionEndId", sub_region_end_id))
        filter_params.append(
            list_to_filter(
                "pipelineProjectEstimatedCostThousandUsd",
                pipeline_project_estimated_cost_thousand_usd,
            )
        )
        filter_params.append(list_to_filter("rateUsdDth", rate_usd_dth))
        filter_params.append(list_to_filter("commRateUsdDth", comm_rate_usd_dth))
        filter_params.append(list_to_filter("status", status))
        filter_params.append(list_to_filter("commonOperator", common_operator))
        filter_params.append(list_to_filter("parentCompany", parent_company))
        filter_params.append(list_to_filter("scope", scope))
        filter_params.append(list_to_filter("createDate", create_date))
        if create_date_gt is not None:
            filter_params.append(f'createDate > "{create_date_gt}"')
        if create_date_gte is not None:
            filter_params.append(f'createDate >= "{create_date_gte}"')
        if create_date_lt is not None:
            filter_params.append(f'createDate < "{create_date_lt}"')
        if create_date_lte is not None:
            filter_params.append(f'createDate <= "{create_date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/pipeline-storage-projects",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    
    def get_pipeline_profiles_data(
        self,
        *,
        year: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_filer_name: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_name: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_id: Optional[Union[list[str], Series[str], str]] = None,
        operating_revenues_gas: Optional[str] = None,
        operating_revenues_gas_lt: Optional[str] = None,
        operating_revenues_gas_lte: Optional[str] = None,
        operating_revenues_gas_gt: Optional[str] = None,
        operating_revenues_gas_gte: Optional[str] = None,
        operating_revenues_total: Optional[str] = None,
        operating_revenues_total_lt: Optional[str] = None,
        operating_revenues_total_lte: Optional[str] = None,
        operating_revenues_total_gt: Optional[str] = None,
        operating_revenues_total_gte: Optional[str] = None,
        operation_expenses_gas: Optional[str] = None,
        operation_expenses_gas_lt: Optional[str] = None,
        operation_expenses_gas_lte: Optional[str] = None,
        operation_expenses_gas_gt: Optional[str] = None,
        operation_expenses_gas_gte: Optional[str] = None,
        operating_expenses_total: Optional[str] = None,
        operating_expenses_total_lt: Optional[str] = None,
        operating_expenses_total_lte: Optional[str] = None,
        operating_expenses_total_gt: Optional[str] = None,
        operating_expenses_total_gte: Optional[str] = None,
        maintenance_expenses_gas: Optional[str] = None,
        maintenance_expenses_gas_lt: Optional[str] = None,
        maintenance_expenses_gas_lte: Optional[str] = None,
        maintenance_expenses_gas_gt: Optional[str] = None,
        maintenance_expenses_gas_gte: Optional[str] = None,
        maintenance_expenses_total: Optional[str] = None,
        maintenance_expenses_total_lt: Optional[str] = None,
        maintenance_expenses_total_lte: Optional[str] = None,
        maintenance_expenses_total_gt: Optional[str] = None,
        maintenance_expenses_total_gte: Optional[str] = None,
        taxes_other_than_income_taxes_total: Optional[str] = None,
        taxes_other_than_income_taxes_total_lt: Optional[str] = None,
        taxes_other_than_income_taxes_total_lte: Optional[str] = None,
        taxes_other_than_income_taxes_total_gt: Optional[str] = None,
        taxes_other_than_income_taxes_total_gte: Optional[str] = None,
        utility_ebitda: Optional[str] = None,
        utility_ebitda_lt: Optional[str] = None,
        utility_ebitda_lte: Optional[str] = None,
        utility_ebitda_gt: Optional[str] = None,
        utility_ebitda_gte: Optional[str] = None,
        transmission_pipeline_length: Optional[str] = None,
        transmission_pipeline_length_lt: Optional[str] = None,
        transmission_pipeline_length_lte: Optional[str] = None,
        transmission_pipeline_length_gt: Optional[str] = None,
        transmission_pipeline_length_gte: Optional[str] = None,
        trpt_gas_for_others_transmsn_vol_mmcf: Optional[str] = None,
        trpt_gas_for_others_transmsn_vol_mmcf_lt: Optional[str] = None,
        trpt_gas_for_others_transmsn_vol_mmcf_lte: Optional[str] = None,
        trpt_gas_for_others_transmsn_vol_mmcf_gt: Optional[str] = None,
        trpt_gas_for_others_transmsn_vol_mmcf_gte: Optional[str] = None,
        land_and_rights_trans_eoy000: Optional[str] = None,
        land_and_rights_trans_eoy000_lt: Optional[str] = None,
        land_and_rights_trans_eoy000_lte: Optional[str] = None,
        land_and_rights_trans_eoy000_gt: Optional[str] = None,
        land_and_rights_trans_eoy000_gte: Optional[str] = None,
        rights_of_way_trans_eoy000: Optional[str] = None,
        rights_of_way_trans_eoy000_lt: Optional[str] = None,
        rights_of_way_trans_eoy000_lte: Optional[str] = None,
        rights_of_way_trans_eoy000_gt: Optional[str] = None,
        rights_of_way_trans_eoy000_gte: Optional[str] = None,
        struc_and_improv_tran_eoy000: Optional[str] = None,
        struc_and_improv_tran_eoy000_lt: Optional[str] = None,
        struc_and_improv_tran_eoy000_lte: Optional[str] = None,
        struc_and_improv_tran_eoy000_gt: Optional[str] = None,
        struc_and_improv_tran_eoy000_gte: Optional[str] = None,
        mains_transmission_eoy000: Optional[str] = None,
        mains_transmission_eoy000_lt: Optional[str] = None,
        mains_transmission_eoy000_lte: Optional[str] = None,
        mains_transmission_eoy000_gt: Optional[str] = None,
        mains_transmission_eoy000_gte: Optional[str] = None,
        comprstaequip_trans_eoy000: Optional[str] = None,
        comprstaequip_trans_eoy000_lt: Optional[str] = None,
        comprstaequip_trans_eoy000_lte: Optional[str] = None,
        comprstaequip_trans_eoy000_gt: Optional[str] = None,
        comprstaequip_trans_eoy000_gte: Optional[str] = None,
        meas_reg_sta_eq_trans_eoy000: Optional[str] = None,
        meas_reg_sta_eq_trans_eoy000_lt: Optional[str] = None,
        meas_reg_sta_eq_trans_eoy000_lte: Optional[str] = None,
        meas_reg_sta_eq_trans_eoy000_gt: Optional[str] = None,
        meas_reg_sta_eq_trans_eoy000_gte: Optional[str] = None,
        communication_equip_trans_eoy000: Optional[str] = None,
        communication_equip_trans_eoy000_lt: Optional[str] = None,
        communication_equip_trans_eoy000_lte: Optional[str] = None,
        communication_equip_trans_eoy000_gt: Optional[str] = None,
        communication_equip_trans_eoy000_gte: Optional[str] = None,
        total_transmission_plant_addns000: Optional[str] = None,
        total_transmission_plant_addns000_lt: Optional[str] = None,
        total_transmission_plant_addns000_lte: Optional[str] = None,
        total_transmission_plant_addns000_gt: Optional[str] = None,
        total_transmission_plant_addns000_gte: Optional[str] = None,
        total_transmission_plant_ret000: Optional[str] = None,
        total_transmission_plant_ret000_lt: Optional[str] = None,
        total_transmission_plant_ret000_lte: Optional[str] = None,
        total_transmission_plant_ret000_gt: Optional[str] = None,
        total_transmission_plant_ret000_gte: Optional[str] = None,
        total_transmission_plant_adjust000: Optional[str] = None,
        total_transmission_plant_adjust000_lt: Optional[str] = None,
        total_transmission_plant_adjust000_lte: Optional[str] = None,
        total_transmission_plant_adjust000_gt: Optional[str] = None,
        total_transmission_plant_adjust000_gte: Optional[str] = None,
        total_transmission_plant_transf000: Optional[str] = None,
        total_transmission_plant_transf000_lt: Optional[str] = None,
        total_transmission_plant_transf000_lte: Optional[str] = None,
        total_transmission_plant_transf000_gt: Optional[str] = None,
        total_transmission_plant_transf000_gte: Optional[str] = None,
        total_transmission_plant_eoy000: Optional[str] = None,
        total_transmission_plant_eoy000_lt: Optional[str] = None,
        total_transmission_plant_eoy000_lte: Optional[str] = None,
        total_transmission_plant_eoy000_gt: Optional[str] = None,
        total_transmission_plant_eoy000_gte: Optional[str] = None,
        total_gas_plant_in_service_eoy: Optional[str] = None,
        total_gas_plant_in_service_eoy_lt: Optional[str] = None,
        total_gas_plant_in_service_eoy_lte: Optional[str] = None,
        total_gas_plant_in_service_eoy_gt: Optional[str] = None,
        total_gas_plant_in_service_eoy_gte: Optional[str] = None,
        constr_wip_total: Optional[str] = None,
        constr_wip_total_lt: Optional[str] = None,
        constr_wip_total_lte: Optional[str] = None,
        constr_wip_total_gt: Optional[str] = None,
        constr_wip_total_gte: Optional[str] = None,
        total_utility_plant_total: Optional[str] = None,
        total_utility_plant_total_lt: Optional[str] = None,
        total_utility_plant_total_lte: Optional[str] = None,
        total_utility_plant_total_gt: Optional[str] = None,
        total_utility_plant_total_gte: Optional[str] = None,
        tran_op_sup_and_engineering: Optional[str] = None,
        tran_op_sup_and_engineering_lt: Optional[str] = None,
        tran_op_sup_and_engineering_lte: Optional[str] = None,
        tran_op_sup_and_engineering_gt: Optional[str] = None,
        tran_op_sup_and_engineering_gte: Optional[str] = None,
        transmiss_oper_load_dispatch: Optional[str] = None,
        transmiss_oper_load_dispatch_lt: Optional[str] = None,
        transmiss_oper_load_dispatch_lte: Optional[str] = None,
        transmiss_oper_load_dispatch_gt: Optional[str] = None,
        transmiss_oper_load_dispatch_gte: Optional[str] = None,
        oper_trans_communication_sys_exp: Optional[str] = None,
        oper_trans_communication_sys_exp_lt: Optional[str] = None,
        oper_trans_communication_sys_exp_lte: Optional[str] = None,
        oper_trans_communication_sys_exp_gt: Optional[str] = None,
        oper_trans_communication_sys_exp_gte: Optional[str] = None,
        oper_trans_compr_sta_labor_and_exp: Optional[str] = None,
        oper_trans_compr_sta_labor_and_exp_lt: Optional[str] = None,
        oper_trans_compr_sta_labor_and_exp_lte: Optional[str] = None,
        oper_trans_compr_sta_labor_and_exp_gt: Optional[str] = None,
        oper_trans_compr_sta_labor_and_exp_gte: Optional[str] = None,
        oper_trans_gas_for_compr_st_fuel: Optional[str] = None,
        oper_trans_gas_for_compr_st_fuel_lt: Optional[str] = None,
        oper_trans_gas_for_compr_st_fuel_lte: Optional[str] = None,
        oper_trans_gas_for_compr_st_fuel_gt: Optional[str] = None,
        oper_trans_gas_for_compr_st_fuel_gte: Optional[str] = None,
        oper_trans_oth_fuel_and_pwr_for_compr_st: Optional[str] = None,
        oper_trans_oth_fuel_and_pwr_for_compr_st_lt: Optional[str] = None,
        oper_trans_oth_fuel_and_pwr_for_compr_st_lte: Optional[str] = None,
        oper_trans_oth_fuel_and_pwr_for_compr_st_gt: Optional[str] = None,
        oper_trans_oth_fuel_and_pwr_for_compr_st_gte: Optional[str] = None,
        oper_trans_mains_exp: Optional[str] = None,
        oper_trans_mains_exp_lt: Optional[str] = None,
        oper_trans_mains_exp_lte: Optional[str] = None,
        oper_trans_mains_exp_gt: Optional[str] = None,
        oper_trans_mains_exp_gte: Optional[str] = None,
        oper_trans_meas_and_reg_sta_exp: Optional[str] = None,
        oper_trans_meas_and_reg_sta_exp_lt: Optional[str] = None,
        oper_trans_meas_and_reg_sta_exp_lte: Optional[str] = None,
        oper_trans_meas_and_reg_sta_exp_gt: Optional[str] = None,
        oper_trans_meas_and_reg_sta_exp_gte: Optional[str] = None,
        oper_trans_transm_and_compr_by_oth: Optional[str] = None,
        oper_trans_transm_and_compr_by_oth_lt: Optional[str] = None,
        oper_trans_transm_and_compr_by_oth_lte: Optional[str] = None,
        oper_trans_transm_and_compr_by_oth_gt: Optional[str] = None,
        oper_trans_transm_and_compr_by_oth_gte: Optional[str] = None,
        tran_op_misc_transmission_exp: Optional[str] = None,
        tran_op_misc_transmission_exp_lt: Optional[str] = None,
        tran_op_misc_transmission_exp_lte: Optional[str] = None,
        tran_op_misc_transmission_exp_gt: Optional[str] = None,
        tran_op_misc_transmission_exp_gte: Optional[str] = None,
        transmiss_oper_rents: Optional[str] = None,
        transmiss_oper_rents_lt: Optional[str] = None,
        transmiss_oper_rents_lte: Optional[str] = None,
        transmiss_oper_rents_gt: Optional[str] = None,
        transmiss_oper_rents_gte: Optional[str] = None,
        transmiss_tran_operation_exp: Optional[str] = None,
        transmiss_tran_operation_exp_lt: Optional[str] = None,
        transmiss_tran_operation_exp_lte: Optional[str] = None,
        transmiss_tran_operation_exp_gt: Optional[str] = None,
        transmiss_tran_operation_exp_gte: Optional[str] = None,
        transmiss_maint_supvsn_and_engin: Optional[str] = None,
        transmiss_maint_supvsn_and_engin_lt: Optional[str] = None,
        transmiss_maint_supvsn_and_engin_lte: Optional[str] = None,
        transmiss_maint_supvsn_and_engin_gt: Optional[str] = None,
        transmiss_maint_supvsn_and_engin_gte: Optional[str] = None,
        transmiss_maint_of_structures: Optional[str] = None,
        transmiss_maint_of_structures_lt: Optional[str] = None,
        transmiss_maint_of_structures_lte: Optional[str] = None,
        transmiss_maint_of_structures_gt: Optional[str] = None,
        transmiss_maint_of_structures_gte: Optional[str] = None,
        maint_trans_mains: Optional[str] = None,
        maint_trans_mains_lt: Optional[str] = None,
        maint_trans_mains_lte: Optional[str] = None,
        maint_trans_mains_gt: Optional[str] = None,
        maint_trans_mains_gte: Optional[str] = None,
        maint_trans_compressor_sta_equip: Optional[str] = None,
        maint_trans_compressor_sta_equip_lt: Optional[str] = None,
        maint_trans_compressor_sta_equip_lte: Optional[str] = None,
        maint_trans_compressor_sta_equip_gt: Optional[str] = None,
        maint_trans_compressor_sta_equip_gte: Optional[str] = None,
        maint_trans_meas_and_reg_sta_equip: Optional[str] = None,
        maint_trans_meas_and_reg_sta_equip_lt: Optional[str] = None,
        maint_trans_meas_and_reg_sta_equip_lte: Optional[str] = None,
        maint_trans_meas_and_reg_sta_equip_gt: Optional[str] = None,
        maint_trans_meas_and_reg_sta_equip_gte: Optional[str] = None,
        maint_trans_communication_equip: Optional[str] = None,
        maint_trans_communication_equip_lt: Optional[str] = None,
        maint_trans_communication_equip_lte: Optional[str] = None,
        maint_trans_communication_equip_gt: Optional[str] = None,
        maint_trans_communication_equip_gte: Optional[str] = None,
        transmiss_maint_of_misc_tran_plt: Optional[str] = None,
        transmiss_maint_of_misc_tran_plt_lt: Optional[str] = None,
        transmiss_maint_of_misc_tran_plt_lte: Optional[str] = None,
        transmiss_maint_of_misc_tran_plt_gt: Optional[str] = None,
        transmiss_maint_of_misc_tran_plt_gte: Optional[str] = None,
        transmiss_maint_exp: Optional[str] = None,
        transmiss_maint_exp_lt: Optional[str] = None,
        transmiss_maint_exp_lte: Optional[str] = None,
        transmiss_maint_exp_gt: Optional[str] = None,
        transmiss_maint_exp_gte: Optional[str] = None,
        transmiss_oand_mexp: Optional[str] = None,
        transmiss_oand_mexp_lt: Optional[str] = None,
        transmiss_oand_mexp_lte: Optional[str] = None,
        transmiss_oand_mexp_gt: Optional[str] = None,
        transmiss_oand_mexp_gte: Optional[str] = None,
        peak1_int_pipe_no_notice_transp: Optional[str] = None,
        peak1_int_pipe_no_notice_transp_lt: Optional[str] = None,
        peak1_int_pipe_no_notice_transp_lte: Optional[str] = None,
        peak1_int_pipe_no_notice_transp_gt: Optional[str] = None,
        peak1_int_pipe_no_notice_transp_gte: Optional[str] = None,
        peak1_oth_dth_no_notice_transport: Optional[str] = None,
        peak1_oth_dth_no_notice_transport_lt: Optional[str] = None,
        peak1_oth_dth_no_notice_transport_lte: Optional[str] = None,
        peak1_oth_dth_no_notice_transport_gt: Optional[str] = None,
        peak1_oth_dth_no_notice_transport_gte: Optional[str] = None,
        peak1_total_dth_no_notice_transp: Optional[str] = None,
        peak1_total_dth_no_notice_transp_lt: Optional[str] = None,
        peak1_total_dth_no_notice_transp_lte: Optional[str] = None,
        peak1_total_dth_no_notice_transp_gt: Optional[str] = None,
        peak1_total_dth_no_notice_transp_gte: Optional[str] = None,
        peak1_int_pipe_dth_oth_firm_transp: Optional[str] = None,
        peak1_int_pipe_dth_oth_firm_transp_lt: Optional[str] = None,
        peak1_int_pipe_dth_oth_firm_transp_lte: Optional[str] = None,
        peak1_int_pipe_dth_oth_firm_transp_gt: Optional[str] = None,
        peak1_int_pipe_dth_oth_firm_transp_gte: Optional[str] = None,
        peak1_oth_dth_other_firm_transport: Optional[str] = None,
        peak1_oth_dth_other_firm_transport_lt: Optional[str] = None,
        peak1_oth_dth_other_firm_transport_lte: Optional[str] = None,
        peak1_oth_dth_other_firm_transport_gt: Optional[str] = None,
        peak1_oth_dth_other_firm_transport_gte: Optional[str] = None,
        peak1_total_dth_oth_firm_transport: Optional[str] = None,
        peak1_total_dth_oth_firm_transport_lt: Optional[str] = None,
        peak1_total_dth_oth_firm_transport_lte: Optional[str] = None,
        peak1_total_dth_oth_firm_transport_gt: Optional[str] = None,
        peak1_total_dth_oth_firm_transport_gte: Optional[str] = None,
        peak1_int_pipe_dth_interr_transp: Optional[str] = None,
        peak1_int_pipe_dth_interr_transp_lt: Optional[str] = None,
        peak1_int_pipe_dth_interr_transp_lte: Optional[str] = None,
        peak1_int_pipe_dth_interr_transp_gt: Optional[str] = None,
        peak1_int_pipe_dth_interr_transp_gte: Optional[str] = None,
        peak1_oth_dth_interr_transport: Optional[str] = None,
        peak1_oth_dth_interr_transport_lt: Optional[str] = None,
        peak1_oth_dth_interr_transport_lte: Optional[str] = None,
        peak1_oth_dth_interr_transport_gt: Optional[str] = None,
        peak1_oth_dth_interr_transport_gte: Optional[str] = None,
        peak1_total_dth_interr_transport: Optional[str] = None,
        peak1_total_dth_interr_transport_lt: Optional[str] = None,
        peak1_total_dth_interr_transport_lte: Optional[str] = None,
        peak1_total_dth_interr_transport_gt: Optional[str] = None,
        peak1_total_dth_interr_transport_gte: Optional[str] = None,
        peak1_int_pipe_dth_oth_transp: Optional[str] = None,
        peak1_int_pipe_dth_oth_transp_lt: Optional[str] = None,
        peak1_int_pipe_dth_oth_transp_lte: Optional[str] = None,
        peak1_int_pipe_dth_oth_transp_gt: Optional[str] = None,
        peak1_int_pipe_dth_oth_transp_gte: Optional[str] = None,
        peak1_oth_dth_other_transport: Optional[str] = None,
        peak1_oth_dth_other_transport_lt: Optional[str] = None,
        peak1_oth_dth_other_transport_lte: Optional[str] = None,
        peak1_oth_dth_other_transport_gt: Optional[str] = None,
        peak1_oth_dth_other_transport_gte: Optional[str] = None,
        peak1_total_dth_oth_transport: Optional[str] = None,
        peak1_total_dth_oth_transport_lt: Optional[str] = None,
        peak1_total_dth_oth_transport_lte: Optional[str] = None,
        peak1_total_dth_oth_transport_gt: Optional[str] = None,
        peak1_total_dth_oth_transport_gte: Optional[str] = None,
        peak1_int_pipe_dth_transp: Optional[str] = None,
        peak1_int_pipe_dth_transp_lt: Optional[str] = None,
        peak1_int_pipe_dth_transp_lte: Optional[str] = None,
        peak1_int_pipe_dth_transp_gt: Optional[str] = None,
        peak1_int_pipe_dth_transp_gte: Optional[str] = None,
        peak1_oth_dth_transport: Optional[str] = None,
        peak1_oth_dth_transport_lt: Optional[str] = None,
        peak1_oth_dth_transport_lte: Optional[str] = None,
        peak1_oth_dth_transport_gt: Optional[str] = None,
        peak1_oth_dth_transport_gte: Optional[str] = None,
        peak1_total_dth_transport: Optional[str] = None,
        peak1_total_dth_transport_lt: Optional[str] = None,
        peak1_total_dth_transport_lte: Optional[str] = None,
        peak1_total_dth_transport_gt: Optional[str] = None,
        peak1_total_dth_transport_gte: Optional[str] = None,
        peak3_int_pipe_no_notice_transp: Optional[str] = None,
        peak3_int_pipe_no_notice_transp_lt: Optional[str] = None,
        peak3_int_pipe_no_notice_transp_lte: Optional[str] = None,
        peak3_int_pipe_no_notice_transp_gt: Optional[str] = None,
        peak3_int_pipe_no_notice_transp_gte: Optional[str] = None,
        peak3_oth_no_notice_transport: Optional[str] = None,
        peak3_oth_no_notice_transport_lt: Optional[str] = None,
        peak3_oth_no_notice_transport_lte: Optional[str] = None,
        peak3_oth_no_notice_transport_gt: Optional[str] = None,
        peak3_oth_no_notice_transport_gte: Optional[str] = None,
        peak3_total_no_notice_transport: Optional[str] = None,
        peak3_total_no_notice_transport_lt: Optional[str] = None,
        peak3_total_no_notice_transport_lte: Optional[str] = None,
        peak3_total_no_notice_transport_gt: Optional[str] = None,
        peak3_total_no_notice_transport_gte: Optional[str] = None,
        peak3_int_pipe_dth_oth_firm_transp: Optional[str] = None,
        peak3_int_pipe_dth_oth_firm_transp_lt: Optional[str] = None,
        peak3_int_pipe_dth_oth_firm_transp_lte: Optional[str] = None,
        peak3_int_pipe_dth_oth_firm_transp_gt: Optional[str] = None,
        peak3_int_pipe_dth_oth_firm_transp_gte: Optional[str] = None,
        peak3_oth_dth_other_firm_transport: Optional[str] = None,
        peak3_oth_dth_other_firm_transport_lt: Optional[str] = None,
        peak3_oth_dth_other_firm_transport_lte: Optional[str] = None,
        peak3_oth_dth_other_firm_transport_gt: Optional[str] = None,
        peak3_oth_dth_other_firm_transport_gte: Optional[str] = None,
        peak3_total_dth_oth_firm_transp: Optional[str] = None,
        peak3_total_dth_oth_firm_transp_lt: Optional[str] = None,
        peak3_total_dth_oth_firm_transp_lte: Optional[str] = None,
        peak3_total_dth_oth_firm_transp_gt: Optional[str] = None,
        peak3_total_dth_oth_firm_transp_gte: Optional[str] = None,
        peak3_int_pipe_dth_interr_transp: Optional[str] = None,
        peak3_int_pipe_dth_interr_transp_lt: Optional[str] = None,
        peak3_int_pipe_dth_interr_transp_lte: Optional[str] = None,
        peak3_int_pipe_dth_interr_transp_gt: Optional[str] = None,
        peak3_int_pipe_dth_interr_transp_gte: Optional[str] = None,
        peak3_oth_dth_interr_transport: Optional[str] = None,
        peak3_oth_dth_interr_transport_lt: Optional[str] = None,
        peak3_oth_dth_interr_transport_lte: Optional[str] = None,
        peak3_oth_dth_interr_transport_gt: Optional[str] = None,
        peak3_oth_dth_interr_transport_gte: Optional[str] = None,
        peak3_total_dth_interr_transport: Optional[str] = None,
        peak3_total_dth_interr_transport_lt: Optional[str] = None,
        peak3_total_dth_interr_transport_lte: Optional[str] = None,
        peak3_total_dth_interr_transport_gt: Optional[str] = None,
        peak3_total_dth_interr_transport_gte: Optional[str] = None,
        peak3_int_pipe_dth_oth_transp: Optional[str] = None,
        peak3_int_pipe_dth_oth_transp_lt: Optional[str] = None,
        peak3_int_pipe_dth_oth_transp_lte: Optional[str] = None,
        peak3_int_pipe_dth_oth_transp_gt: Optional[str] = None,
        peak3_int_pipe_dth_oth_transp_gte: Optional[str] = None,
        peak3_oth_dth_other_transport: Optional[str] = None,
        peak3_oth_dth_other_transport_lt: Optional[str] = None,
        peak3_oth_dth_other_transport_lte: Optional[str] = None,
        peak3_oth_dth_other_transport_gt: Optional[str] = None,
        peak3_oth_dth_other_transport_gte: Optional[str] = None,
        peak3_total_dth_other_transport: Optional[str] = None,
        peak3_total_dth_other_transport_lt: Optional[str] = None,
        peak3_total_dth_other_transport_lte: Optional[str] = None,
        peak3_total_dth_other_transport_gt: Optional[str] = None,
        peak3_total_dth_other_transport_gte: Optional[str] = None,
        peak3_int_pipe_dth_transp: Optional[str] = None,
        peak3_int_pipe_dth_transp_lt: Optional[str] = None,
        peak3_int_pipe_dth_transp_lte: Optional[str] = None,
        peak3_int_pipe_dth_transp_gt: Optional[str] = None,
        peak3_int_pipe_dth_transp_gte: Optional[str] = None,
        peak3_oth_dth_transport: Optional[str] = None,
        peak3_oth_dth_transport_lt: Optional[str] = None,
        peak3_oth_dth_transport_lte: Optional[str] = None,
        peak3_oth_dth_transport_gt: Optional[str] = None,
        peak3_oth_dth_transport_gte: Optional[str] = None,
        peak3_total_dth_transport: Optional[str] = None,
        peak3_total_dth_transport_lt: Optional[str] = None,
        peak3_total_dth_transport_lte: Optional[str] = None,
        peak3_total_dth_transport_gt: Optional[str] = None,
        peak3_total_dth_transport_gte: Optional[str] = None,
        gas_of_oth_recd_for_gathering: Optional[str] = None,
        gas_of_oth_recd_for_gathering_lt: Optional[str] = None,
        gas_of_oth_recd_for_gathering_lte: Optional[str] = None,
        gas_of_oth_recd_for_gathering_gt: Optional[str] = None,
        gas_of_oth_recd_for_gathering_gte: Optional[str] = None,
        reciepts: Optional[str] = None,
        reciepts_lt: Optional[str] = None,
        reciepts_lte: Optional[str] = None,
        reciepts_gt: Optional[str] = None,
        reciepts_gte: Optional[str] = None,
        deliv_of_gas_trans_or_compr_oth: Optional[str] = None,
        deliv_of_gas_trans_or_compr_oth_lt: Optional[str] = None,
        deliv_of_gas_trans_or_compr_oth_lte: Optional[str] = None,
        deliv_of_gas_trans_or_compr_oth_gt: Optional[str] = None,
        deliv_of_gas_trans_or_compr_oth_gte: Optional[str] = None,
        gas_delivered_as_imbalances: Optional[str] = None,
        gas_delivered_as_imbalances_lt: Optional[str] = None,
        gas_delivered_as_imbalances_lte: Optional[str] = None,
        gas_delivered_as_imbalances_gt: Optional[str] = None,
        gas_delivered_as_imbalances_gte: Optional[str] = None,
        gas_used_for_compressor_sta_fuel: Optional[str] = None,
        gas_used_for_compressor_sta_fuel_lt: Optional[str] = None,
        gas_used_for_compressor_sta_fuel_lte: Optional[str] = None,
        gas_used_for_compressor_sta_fuel_gt: Optional[str] = None,
        gas_used_for_compressor_sta_fuel_gte: Optional[str] = None,
        nat_gas_other_deliv: Optional[str] = None,
        nat_gas_other_deliv_lt: Optional[str] = None,
        nat_gas_other_deliv_lte: Optional[str] = None,
        nat_gas_other_deliv_gt: Optional[str] = None,
        nat_gas_other_deliv_gte: Optional[str] = None,
        total_deliveries: Optional[str] = None,
        total_deliveries_lt: Optional[str] = None,
        total_deliveries_lte: Optional[str] = None,
        total_deliveries_gt: Optional[str] = None,
        total_deliveries_gte: Optional[str] = None,
        gas_stored_boy: Optional[str] = None,
        gas_stored_boy_lt: Optional[str] = None,
        gas_stored_boy_lte: Optional[str] = None,
        gas_stored_boy_gt: Optional[str] = None,
        gas_stored_boy_gte: Optional[str] = None,
        gas_stored_gas_deliv_to_storage: Optional[str] = None,
        gas_stored_gas_deliv_to_storage_lt: Optional[str] = None,
        gas_stored_gas_deliv_to_storage_lte: Optional[str] = None,
        gas_stored_gas_deliv_to_storage_gt: Optional[str] = None,
        gas_stored_gas_deliv_to_storage_gte: Optional[str] = None,
        gas_stored_gas_withdr_from_stor: Optional[str] = None,
        gas_stored_gas_withdr_from_stor_lt: Optional[str] = None,
        gas_stored_gas_withdr_from_stor_lte: Optional[str] = None,
        gas_stored_gas_withdr_from_stor_gt: Optional[str] = None,
        gas_stored_gas_withdr_from_stor_gte: Optional[str] = None,
        gas_stored_oth_deb_or_cred_net: Optional[str] = None,
        gas_stored_oth_deb_or_cred_net_lt: Optional[str] = None,
        gas_stored_oth_deb_or_cred_net_lte: Optional[str] = None,
        gas_stored_oth_deb_or_cred_net_gt: Optional[str] = None,
        gas_stored_oth_deb_or_cred_net_gte: Optional[str] = None,
        gas_stored_eoy: Optional[str] = None,
        gas_stored_eoy_lt: Optional[str] = None,
        gas_stored_eoy_lte: Optional[str] = None,
        gas_stored_eoy_gt: Optional[str] = None,
        gas_stored_eoy_gte: Optional[str] = None,
        gas_stored_gas_volume_dth: Optional[str] = None,
        gas_stored_gas_volume_dth_lt: Optional[str] = None,
        gas_stored_gas_volume_dth_lte: Optional[str] = None,
        gas_stored_gas_volume_dth_gt: Optional[str] = None,
        gas_stored_gas_volume_dth_gte: Optional[str] = None,
        gas_stored_amount_per_dth: Optional[str] = None,
        gas_stored_amount_per_dth_lt: Optional[str] = None,
        gas_stored_amount_per_dth_lte: Optional[str] = None,
        gas_stored_amount_per_dth_gt: Optional[str] = None,
        gas_stored_amount_per_dth_gte: Optional[str] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Gas pipeline profiles with financial highlights, plant in service expenses, operation & maintenance expenses, transmission peaks and receipts & deliveries sourced from the FERC Form 2 & 2A.

        Parameters
        ----------

         year: Optional[Union[list[str], Series[str], str]]
             The year the data was reported to the US Federal Energy Regulatory Commission (FERC)., by default None
         pipeline_filer_name: Optional[Union[list[str], Series[str], str]]
             The filer name of a pipeline system., by default None
         pipeline_name: Optional[Union[list[str], Series[str], str]]
             The display name of a pipeline system, utilizes legacy Bentek names when applicable., by default None
         pipeline_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a pipeline system, utilizes legacy Bentek pipeline ids when applicable., by default None
         operating_revenues_gas: Optional[str], optional
             Operating revenues gas are the gas component of the revenues., by default None
         operating_revenues_gas_gt: Optional[str], optional
             filter by `operating_revenues_gas > x`, by default None
         operating_revenues_gas_gte: Optional[str], optional
             filter by `operating_revenues_gas >= x`, by default None
         operating_revenues_gas_lt: Optional[str], optional
             filter by `operating_revenues_gas < x`, by default None
         operating_revenues_gas_lte: Optional[str], optional
             filter by `operating_revenues_gas <= x`, by default None
         operating_revenues_total: Optional[str], optional
             Total operating revenues for the entire pipeline., by default None
         operating_revenues_total_gt: Optional[str], optional
             filter by `operating_revenues_total > x`, by default None
         operating_revenues_total_gte: Optional[str], optional
             filter by `operating_revenues_total >= x`, by default None
         operating_revenues_total_lt: Optional[str], optional
             filter by `operating_revenues_total < x`, by default None
         operating_revenues_total_lte: Optional[str], optional
             filter by `operating_revenues_total <= x`, by default None
         operation_expenses_gas: Optional[str], optional
             The operating expenses of pipeline - maintenance, supervision etc., by default None
         operation_expenses_gas_gt: Optional[str], optional
             filter by `operation_expenses_gas > x`, by default None
         operation_expenses_gas_gte: Optional[str], optional
             filter by `operation_expenses_gas >= x`, by default None
         operation_expenses_gas_lt: Optional[str], optional
             filter by `operation_expenses_gas < x`, by default None
         operation_expenses_gas_lte: Optional[str], optional
             filter by `operation_expenses_gas <= x`, by default None
         operating_expenses_total: Optional[str], optional
             The total operating expense for the pipeline., by default None
         operating_expenses_total_gt: Optional[str], optional
             filter by `operating_expenses_total > x`, by default None
         operating_expenses_total_gte: Optional[str], optional
             filter by `operating_expenses_total >= x`, by default None
         operating_expenses_total_lt: Optional[str], optional
             filter by `operating_expenses_total < x`, by default None
         operating_expenses_total_lte: Optional[str], optional
             filter by `operating_expenses_total <= x`, by default None
         maintenance_expenses_gas: Optional[str], optional
             The maintenance expenses for natural gas associated to the pipeline., by default None
         maintenance_expenses_gas_gt: Optional[str], optional
             filter by `maintenance_expenses_gas > x`, by default None
         maintenance_expenses_gas_gte: Optional[str], optional
             filter by `maintenance_expenses_gas >= x`, by default None
         maintenance_expenses_gas_lt: Optional[str], optional
             filter by `maintenance_expenses_gas < x`, by default None
         maintenance_expenses_gas_lte: Optional[str], optional
             filter by `maintenance_expenses_gas <= x`, by default None
         maintenance_expenses_total: Optional[str], optional
             The total maintenance expenses including gas and other maintenance expenses., by default None
         maintenance_expenses_total_gt: Optional[str], optional
             filter by `maintenance_expenses_total > x`, by default None
         maintenance_expenses_total_gte: Optional[str], optional
             filter by `maintenance_expenses_total >= x`, by default None
         maintenance_expenses_total_lt: Optional[str], optional
             filter by `maintenance_expenses_total < x`, by default None
         maintenance_expenses_total_lte: Optional[str], optional
             filter by `maintenance_expenses_total <= x`, by default None
         taxes_other_than_income_taxes_total: Optional[str], optional
             The total associated taxes other than income taxes for the pipeline., by default None
         taxes_other_than_income_taxes_total_gt: Optional[str], optional
             filter by `taxes_other_than_income_taxes_total > x`, by default None
         taxes_other_than_income_taxes_total_gte: Optional[str], optional
             filter by `taxes_other_than_income_taxes_total >= x`, by default None
         taxes_other_than_income_taxes_total_lt: Optional[str], optional
             filter by `taxes_other_than_income_taxes_total < x`, by default None
         taxes_other_than_income_taxes_total_lte: Optional[str], optional
             filter by `taxes_other_than_income_taxes_total <= x`, by default None
         utility_ebitda: Optional[str], optional
             The pipeline EBITDA = Earnings Before Interest, Taxes, Depreciation and Amortization., by default None
         utility_ebitda_gt: Optional[str], optional
             filter by `utility_ebitda > x`, by default None
         utility_ebitda_gte: Optional[str], optional
             filter by `utility_ebitda >= x`, by default None
         utility_ebitda_lt: Optional[str], optional
             filter by `utility_ebitda < x`, by default None
         utility_ebitda_lte: Optional[str], optional
             filter by `utility_ebitda <= x`, by default None
         transmission_pipeline_length: Optional[str], optional
             The pipeline length in miles., by default None
         transmission_pipeline_length_gt: Optional[str], optional
             filter by `transmission_pipeline_length > x`, by default None
         transmission_pipeline_length_gte: Optional[str], optional
             filter by `transmission_pipeline_length >= x`, by default None
         transmission_pipeline_length_lt: Optional[str], optional
             filter by `transmission_pipeline_length < x`, by default None
         transmission_pipeline_length_lte: Optional[str], optional
             filter by `transmission_pipeline_length <= x`, by default None
         trpt_gas_for_others_transmsn_vol_mmcf: Optional[str], optional
             Transportation of gas for others in MMcf., by default None
         trpt_gas_for_others_transmsn_vol_mmcf_gt: Optional[str], optional
             filter by `trpt_gas_for_others_transmsn_vol_mmcf > x`, by default None
         trpt_gas_for_others_transmsn_vol_mmcf_gte: Optional[str], optional
             filter by `trpt_gas_for_others_transmsn_vol_mmcf >= x`, by default None
         trpt_gas_for_others_transmsn_vol_mmcf_lt: Optional[str], optional
             filter by `trpt_gas_for_others_transmsn_vol_mmcf < x`, by default None
         trpt_gas_for_others_transmsn_vol_mmcf_lte: Optional[str], optional
             filter by `trpt_gas_for_others_transmsn_vol_mmcf <= x`, by default None
         land_and_rights_trans_eoy000: Optional[str], optional
             The land and rights of way costs associated with the transport of natural gas for the pipeline at end of year (EOY)., by default None
         land_and_rights_trans_eoy000_gt: Optional[str], optional
             filter by `land_and_rights_trans_eoy000 > x`, by default None
         land_and_rights_trans_eoy000_gte: Optional[str], optional
             filter by `land_and_rights_trans_eoy000 >= x`, by default None
         land_and_rights_trans_eoy000_lt: Optional[str], optional
             filter by `land_and_rights_trans_eoy000 < x`, by default None
         land_and_rights_trans_eoy000_lte: Optional[str], optional
             filter by `land_and_rights_trans_eoy000 <= x`, by default None
         rights_of_way_trans_eoy000: Optional[str], optional
             Specific right of way costs associated with the transport of natural gas for the pipeline at end of year (EOY)., by default None
         rights_of_way_trans_eoy000_gt: Optional[str], optional
             filter by `rights_of_way_trans_eoy000 > x`, by default None
         rights_of_way_trans_eoy000_gte: Optional[str], optional
             filter by `rights_of_way_trans_eoy000 >= x`, by default None
         rights_of_way_trans_eoy000_lt: Optional[str], optional
             filter by `rights_of_way_trans_eoy000 < x`, by default None
         rights_of_way_trans_eoy000_lte: Optional[str], optional
             filter by `rights_of_way_trans_eoy000 <= x`, by default None
         struc_and_improv_tran_eoy000: Optional[str], optional
             Specific structure and improvement costs in the transportation of natural gas for the pipeline at end of year (EOY)., by default None
         struc_and_improv_tran_eoy000_gt: Optional[str], optional
             filter by `struc_and_improv_tran_eoy000 > x`, by default None
         struc_and_improv_tran_eoy000_gte: Optional[str], optional
             filter by `struc_and_improv_tran_eoy000 >= x`, by default None
         struc_and_improv_tran_eoy000_lt: Optional[str], optional
             filter by `struc_and_improv_tran_eoy000 < x`, by default None
         struc_and_improv_tran_eoy000_lte: Optional[str], optional
             filter by `struc_and_improv_tran_eoy000 <= x`, by default None
         mains_transmission_eoy000: Optional[str], optional
             Specific maintenance costs in the transportation of natural gas for the pipeline at end of year (EOY)., by default None
         mains_transmission_eoy000_gt: Optional[str], optional
             filter by `mains_transmission_eoy000 > x`, by default None
         mains_transmission_eoy000_gte: Optional[str], optional
             filter by `mains_transmission_eoy000 >= x`, by default None
         mains_transmission_eoy000_lt: Optional[str], optional
             filter by `mains_transmission_eoy000 < x`, by default None
         mains_transmission_eoy000_lte: Optional[str], optional
             filter by `mains_transmission_eoy000 <= x`, by default None
         comprstaequip_trans_eoy000: Optional[str], optional
             Specific compressor station equipment costs accrued by the pipeline in the transportation of natural gas for the pipeline at end of year (EOY)., by default None
         comprstaequip_trans_eoy000_gt: Optional[str], optional
             filter by `comprstaequip_trans_eoy000 > x`, by default None
         comprstaequip_trans_eoy000_gte: Optional[str], optional
             filter by `comprstaequip_trans_eoy000 >= x`, by default None
         comprstaequip_trans_eoy000_lt: Optional[str], optional
             filter by `comprstaequip_trans_eoy000 < x`, by default None
         comprstaequip_trans_eoy000_lte: Optional[str], optional
             filter by `comprstaequip_trans_eoy000 <= x`, by default None
         meas_reg_sta_eq_trans_eoy000: Optional[str], optional
             Specific measuring, regulating station equipment costs accrued by the pipeline in the transportation of natural gas for the pipeline at end of year (EOY)., by default None
         meas_reg_sta_eq_trans_eoy000_gt: Optional[str], optional
             filter by `meas_reg_sta_eq_trans_eoy000 > x`, by default None
         meas_reg_sta_eq_trans_eoy000_gte: Optional[str], optional
             filter by `meas_reg_sta_eq_trans_eoy000 >= x`, by default None
         meas_reg_sta_eq_trans_eoy000_lt: Optional[str], optional
             filter by `meas_reg_sta_eq_trans_eoy000 < x`, by default None
         meas_reg_sta_eq_trans_eoy000_lte: Optional[str], optional
             filter by `meas_reg_sta_eq_trans_eoy000 <= x`, by default None
         communication_equip_trans_eoy000: Optional[str], optional
             Specific communication equipment costs accrued by the pipeline in the transportation of natural gas for the pipeline at end of year (EOY)., by default None
         communication_equip_trans_eoy000_gt: Optional[str], optional
             filter by `communication_equip_trans_eoy000 > x`, by default None
         communication_equip_trans_eoy000_gte: Optional[str], optional
             filter by `communication_equip_trans_eoy000 >= x`, by default None
         communication_equip_trans_eoy000_lt: Optional[str], optional
             filter by `communication_equip_trans_eoy000 < x`, by default None
         communication_equip_trans_eoy000_lte: Optional[str], optional
             filter by `communication_equip_trans_eoy000 <= x`, by default None
         total_transmission_plant_addns000: Optional[str], optional
             The cost of additional meters, meter installation, house regulator installations, industrial measuring and regulating station equipment, other property on customers' premises, asset retirement costs for distribution, land and land rights, structures and improvements, office furniture and equipment, transportation equipment, stores equipment, tools, shop, and garage equipment, laboratory equipment etc., by default None
         total_transmission_plant_addns000_gt: Optional[str], optional
             filter by `total_transmission_plant_addns000 > x`, by default None
         total_transmission_plant_addns000_gte: Optional[str], optional
             filter by `total_transmission_plant_addns000 >= x`, by default None
         total_transmission_plant_addns000_lt: Optional[str], optional
             filter by `total_transmission_plant_addns000 < x`, by default None
         total_transmission_plant_addns000_lte: Optional[str], optional
             filter by `total_transmission_plant_addns000 <= x`, by default None
         total_transmission_plant_ret000: Optional[str], optional
             Total pipeline asset retirement costs., by default None
         total_transmission_plant_ret000_gt: Optional[str], optional
             filter by `total_transmission_plant_ret000 > x`, by default None
         total_transmission_plant_ret000_gte: Optional[str], optional
             filter by `total_transmission_plant_ret000 >= x`, by default None
         total_transmission_plant_ret000_lt: Optional[str], optional
             filter by `total_transmission_plant_ret000 < x`, by default None
         total_transmission_plant_ret000_lte: Optional[str], optional
             filter by `total_transmission_plant_ret000 <= x`, by default None
         total_transmission_plant_adjust000: Optional[str], optional
             The cost of adjustments of meters, meter installation, house regulator installations, industrial measuring and regulating station equipment, other property on customers' premises, asset retirement costs for distribution, land and land rights, structures and improvements, office furniture and equipment, transportation equipment, stores equipment, tools, shop, and garage equipment, laboratory equipment etc., by default None
         total_transmission_plant_adjust000_gt: Optional[str], optional
             filter by `total_transmission_plant_adjust000 > x`, by default None
         total_transmission_plant_adjust000_gte: Optional[str], optional
             filter by `total_transmission_plant_adjust000 >= x`, by default None
         total_transmission_plant_adjust000_lt: Optional[str], optional
             filter by `total_transmission_plant_adjust000 < x`, by default None
         total_transmission_plant_adjust000_lte: Optional[str], optional
             filter by `total_transmission_plant_adjust000 <= x`, by default None
         total_transmission_plant_transf000: Optional[str], optional
             The transfer costs of meters, meter installation, house regulator installations, industrial measuring and regulating station equipment, other property on customers' premises, asset retirement costs for distribution, land and land rights, structures and improvements, office furniture and equipment, transportation equipment, stores equipment, tools, shop, and garage equipment, laboratory equipment etc., by default None
         total_transmission_plant_transf000_gt: Optional[str], optional
             filter by `total_transmission_plant_transf000 > x`, by default None
         total_transmission_plant_transf000_gte: Optional[str], optional
             filter by `total_transmission_plant_transf000 >= x`, by default None
         total_transmission_plant_transf000_lt: Optional[str], optional
             filter by `total_transmission_plant_transf000 < x`, by default None
         total_transmission_plant_transf000_lte: Optional[str], optional
             filter by `total_transmission_plant_transf000 <= x`, by default None
         total_transmission_plant_eoy000: Optional[str], optional
             The total costs of operating a pipeline with respect to additions, retirements, adjustments, and transfers (includes land and land rights, structures and improvements, maintenance, compressor station equipment etc., by default None
         total_transmission_plant_eoy000_gt: Optional[str], optional
             filter by `total_transmission_plant_eoy000 > x`, by default None
         total_transmission_plant_eoy000_gte: Optional[str], optional
             filter by `total_transmission_plant_eoy000 >= x`, by default None
         total_transmission_plant_eoy000_lt: Optional[str], optional
             filter by `total_transmission_plant_eoy000 < x`, by default None
         total_transmission_plant_eoy000_lte: Optional[str], optional
             filter by `total_transmission_plant_eoy000 <= x`, by default None
         total_gas_plant_in_service_eoy: Optional[str], optional
             The total costs of operating a pipeline., by default None
         total_gas_plant_in_service_eoy_gt: Optional[str], optional
             filter by `total_gas_plant_in_service_eoy > x`, by default None
         total_gas_plant_in_service_eoy_gte: Optional[str], optional
             filter by `total_gas_plant_in_service_eoy >= x`, by default None
         total_gas_plant_in_service_eoy_lt: Optional[str], optional
             filter by `total_gas_plant_in_service_eoy < x`, by default None
         total_gas_plant_in_service_eoy_lte: Optional[str], optional
             filter by `total_gas_plant_in_service_eoy <= x`, by default None
         constr_wip_total: Optional[str], optional
             Construction Work in Progress total., by default None
         constr_wip_total_gt: Optional[str], optional
             filter by `constr_wip_total > x`, by default None
         constr_wip_total_gte: Optional[str], optional
             filter by `constr_wip_total >= x`, by default None
         constr_wip_total_lt: Optional[str], optional
             filter by `constr_wip_total < x`, by default None
         constr_wip_total_lte: Optional[str], optional
             filter by `constr_wip_total <= x`, by default None
         total_utility_plant_total: Optional[str], optional
             All costs associated in operating a pipeline + construction work in progress expenses., by default None
         total_utility_plant_total_gt: Optional[str], optional
             filter by `total_utility_plant_total > x`, by default None
         total_utility_plant_total_gte: Optional[str], optional
             filter by `total_utility_plant_total >= x`, by default None
         total_utility_plant_total_lt: Optional[str], optional
             filter by `total_utility_plant_total < x`, by default None
         total_utility_plant_total_lte: Optional[str], optional
             filter by `total_utility_plant_total <= x`, by default None
         tran_op_sup_and_engineering: Optional[str], optional
             Transmission facility operation, supervision and engineering costs., by default None
         tran_op_sup_and_engineering_gt: Optional[str], optional
             filter by `tran_op_sup_and_engineering > x`, by default None
         tran_op_sup_and_engineering_gte: Optional[str], optional
             filter by `tran_op_sup_and_engineering >= x`, by default None
         tran_op_sup_and_engineering_lt: Optional[str], optional
             filter by `tran_op_sup_and_engineering < x`, by default None
         tran_op_sup_and_engineering_lte: Optional[str], optional
             filter by `tran_op_sup_and_engineering <= x`, by default None
         transmiss_oper_load_dispatch: Optional[str], optional
             Transmission facility operation system control and load dispatching costs., by default None
         transmiss_oper_load_dispatch_gt: Optional[str], optional
             filter by `transmiss_oper_load_dispatch > x`, by default None
         transmiss_oper_load_dispatch_gte: Optional[str], optional
             filter by `transmiss_oper_load_dispatch >= x`, by default None
         transmiss_oper_load_dispatch_lt: Optional[str], optional
             filter by `transmiss_oper_load_dispatch < x`, by default None
         transmiss_oper_load_dispatch_lte: Optional[str], optional
             filter by `transmiss_oper_load_dispatch <= x`, by default None
         oper_trans_communication_sys_exp: Optional[str], optional
             Communication system expenses at the transmission facility., by default None
         oper_trans_communication_sys_exp_gt: Optional[str], optional
             filter by `oper_trans_communication_sys_exp > x`, by default None
         oper_trans_communication_sys_exp_gte: Optional[str], optional
             filter by `oper_trans_communication_sys_exp >= x`, by default None
         oper_trans_communication_sys_exp_lt: Optional[str], optional
             filter by `oper_trans_communication_sys_exp < x`, by default None
         oper_trans_communication_sys_exp_lte: Optional[str], optional
             filter by `oper_trans_communication_sys_exp <= x`, by default None
         oper_trans_compr_sta_labor_and_exp: Optional[str], optional
             Compressor station labor and expenses., by default None
         oper_trans_compr_sta_labor_and_exp_gt: Optional[str], optional
             filter by `oper_trans_compr_sta_labor_and_exp > x`, by default None
         oper_trans_compr_sta_labor_and_exp_gte: Optional[str], optional
             filter by `oper_trans_compr_sta_labor_and_exp >= x`, by default None
         oper_trans_compr_sta_labor_and_exp_lt: Optional[str], optional
             filter by `oper_trans_compr_sta_labor_and_exp < x`, by default None
         oper_trans_compr_sta_labor_and_exp_lte: Optional[str], optional
             filter by `oper_trans_compr_sta_labor_and_exp <= x`, by default None
         oper_trans_gas_for_compr_st_fuel: Optional[str], optional
             Gas for compressor station fuel expenses., by default None
         oper_trans_gas_for_compr_st_fuel_gt: Optional[str], optional
             filter by `oper_trans_gas_for_compr_st_fuel > x`, by default None
         oper_trans_gas_for_compr_st_fuel_gte: Optional[str], optional
             filter by `oper_trans_gas_for_compr_st_fuel >= x`, by default None
         oper_trans_gas_for_compr_st_fuel_lt: Optional[str], optional
             filter by `oper_trans_gas_for_compr_st_fuel < x`, by default None
         oper_trans_gas_for_compr_st_fuel_lte: Optional[str], optional
             filter by `oper_trans_gas_for_compr_st_fuel <= x`, by default None
         oper_trans_oth_fuel_and_pwr_for_compr_st: Optional[str], optional
             Other fuel and power expenses at compressor stations, by default None
         oper_trans_oth_fuel_and_pwr_for_compr_st_gt: Optional[str], optional
             filter by `oper_trans_oth_fuel_and_pwr_for_compr_st > x`, by default None
         oper_trans_oth_fuel_and_pwr_for_compr_st_gte: Optional[str], optional
             filter by `oper_trans_oth_fuel_and_pwr_for_compr_st >= x`, by default None
         oper_trans_oth_fuel_and_pwr_for_compr_st_lt: Optional[str], optional
             filter by `oper_trans_oth_fuel_and_pwr_for_compr_st < x`, by default None
         oper_trans_oth_fuel_and_pwr_for_compr_st_lte: Optional[str], optional
             filter by `oper_trans_oth_fuel_and_pwr_for_compr_st <= x`, by default None
         oper_trans_mains_exp: Optional[str], optional
             Transmission facility mains expenses., by default None
         oper_trans_mains_exp_gt: Optional[str], optional
             filter by `oper_trans_mains_exp > x`, by default None
         oper_trans_mains_exp_gte: Optional[str], optional
             filter by `oper_trans_mains_exp >= x`, by default None
         oper_trans_mains_exp_lt: Optional[str], optional
             filter by `oper_trans_mains_exp < x`, by default None
         oper_trans_mains_exp_lte: Optional[str], optional
             filter by `oper_trans_mains_exp <= x`, by default None
         oper_trans_meas_and_reg_sta_exp: Optional[str], optional
             Transmission facility Measuring and Regulating Station Expenses., by default None
         oper_trans_meas_and_reg_sta_exp_gt: Optional[str], optional
             filter by `oper_trans_meas_and_reg_sta_exp > x`, by default None
         oper_trans_meas_and_reg_sta_exp_gte: Optional[str], optional
             filter by `oper_trans_meas_and_reg_sta_exp >= x`, by default None
         oper_trans_meas_and_reg_sta_exp_lt: Optional[str], optional
             filter by `oper_trans_meas_and_reg_sta_exp < x`, by default None
         oper_trans_meas_and_reg_sta_exp_lte: Optional[str], optional
             filter by `oper_trans_meas_and_reg_sta_exp <= x`, by default None
         oper_trans_transm_and_compr_by_oth: Optional[str], optional
             Expenses associate with the transmission and compression of gas by others., by default None
         oper_trans_transm_and_compr_by_oth_gt: Optional[str], optional
             filter by `oper_trans_transm_and_compr_by_oth > x`, by default None
         oper_trans_transm_and_compr_by_oth_gte: Optional[str], optional
             filter by `oper_trans_transm_and_compr_by_oth >= x`, by default None
         oper_trans_transm_and_compr_by_oth_lt: Optional[str], optional
             filter by `oper_trans_transm_and_compr_by_oth < x`, by default None
         oper_trans_transm_and_compr_by_oth_lte: Optional[str], optional
             filter by `oper_trans_transm_and_compr_by_oth <= x`, by default None
         tran_op_misc_transmission_exp: Optional[str], optional
             Miscellaneous expenses associated with the transmission of gas., by default None
         tran_op_misc_transmission_exp_gt: Optional[str], optional
             filter by `tran_op_misc_transmission_exp > x`, by default None
         tran_op_misc_transmission_exp_gte: Optional[str], optional
             filter by `tran_op_misc_transmission_exp >= x`, by default None
         tran_op_misc_transmission_exp_lt: Optional[str], optional
             filter by `tran_op_misc_transmission_exp < x`, by default None
         tran_op_misc_transmission_exp_lte: Optional[str], optional
             filter by `tran_op_misc_transmission_exp <= x`, by default None
         transmiss_oper_rents: Optional[str], optional
             Rent expenses associated with the transmission of gas., by default None
         transmiss_oper_rents_gt: Optional[str], optional
             filter by `transmiss_oper_rents > x`, by default None
         transmiss_oper_rents_gte: Optional[str], optional
             filter by `transmiss_oper_rents >= x`, by default None
         transmiss_oper_rents_lt: Optional[str], optional
             filter by `transmiss_oper_rents < x`, by default None
         transmiss_oper_rents_lte: Optional[str], optional
             filter by `transmiss_oper_rents <= x`, by default None
         transmiss_tran_operation_exp: Optional[str], optional
             The total transmission expenses: operation supervision and engineering, system control and load dispatching, communication system expenses, compressor station labor and expenses, gas for compressor station fuel, other fuel and power for compressor stations, mains expenses, measuring and regulating station expenses, transmission and compression of gas by others, other expenses and rents., by default None
         transmiss_tran_operation_exp_gt: Optional[str], optional
             filter by `transmiss_tran_operation_exp > x`, by default None
         transmiss_tran_operation_exp_gte: Optional[str], optional
             filter by `transmiss_tran_operation_exp >= x`, by default None
         transmiss_tran_operation_exp_lt: Optional[str], optional
             filter by `transmiss_tran_operation_exp < x`, by default None
         transmiss_tran_operation_exp_lte: Optional[str], optional
             filter by `transmiss_tran_operation_exp <= x`, by default None
         transmiss_maint_supvsn_and_engin: Optional[str], optional
             Operation, supervision and engineering maintenance expenses., by default None
         transmiss_maint_supvsn_and_engin_gt: Optional[str], optional
             filter by `transmiss_maint_supvsn_and_engin > x`, by default None
         transmiss_maint_supvsn_and_engin_gte: Optional[str], optional
             filter by `transmiss_maint_supvsn_and_engin >= x`, by default None
         transmiss_maint_supvsn_and_engin_lt: Optional[str], optional
             filter by `transmiss_maint_supvsn_and_engin < x`, by default None
         transmiss_maint_supvsn_and_engin_lte: Optional[str], optional
             filter by `transmiss_maint_supvsn_and_engin <= x`, by default None
         transmiss_maint_of_structures: Optional[str], optional
             Maintenance of structures and improvements expenses., by default None
         transmiss_maint_of_structures_gt: Optional[str], optional
             filter by `transmiss_maint_of_structures > x`, by default None
         transmiss_maint_of_structures_gte: Optional[str], optional
             filter by `transmiss_maint_of_structures >= x`, by default None
         transmiss_maint_of_structures_lt: Optional[str], optional
             filter by `transmiss_maint_of_structures < x`, by default None
         transmiss_maint_of_structures_lte: Optional[str], optional
             filter by `transmiss_maint_of_structures <= x`, by default None
         maint_trans_mains: Optional[str], optional
             Maintenance of mains expenses., by default None
         maint_trans_mains_gt: Optional[str], optional
             filter by `maint_trans_mains > x`, by default None
         maint_trans_mains_gte: Optional[str], optional
             filter by `maint_trans_mains >= x`, by default None
         maint_trans_mains_lt: Optional[str], optional
             filter by `maint_trans_mains < x`, by default None
         maint_trans_mains_lte: Optional[str], optional
             filter by `maint_trans_mains <= x`, by default None
         maint_trans_compressor_sta_equip: Optional[str], optional
             Compressor station equipment maintenance expenses., by default None
         maint_trans_compressor_sta_equip_gt: Optional[str], optional
             filter by `maint_trans_compressor_sta_equip > x`, by default None
         maint_trans_compressor_sta_equip_gte: Optional[str], optional
             filter by `maint_trans_compressor_sta_equip >= x`, by default None
         maint_trans_compressor_sta_equip_lt: Optional[str], optional
             filter by `maint_trans_compressor_sta_equip < x`, by default None
         maint_trans_compressor_sta_equip_lte: Optional[str], optional
             filter by `maint_trans_compressor_sta_equip <= x`, by default None
         maint_trans_meas_and_reg_sta_equip: Optional[str], optional
             Transmission maintenance of measuring and regulation station equipment., by default None
         maint_trans_meas_and_reg_sta_equip_gt: Optional[str], optional
             filter by `maint_trans_meas_and_reg_sta_equip > x`, by default None
         maint_trans_meas_and_reg_sta_equip_gte: Optional[str], optional
             filter by `maint_trans_meas_and_reg_sta_equip >= x`, by default None
         maint_trans_meas_and_reg_sta_equip_lt: Optional[str], optional
             filter by `maint_trans_meas_and_reg_sta_equip < x`, by default None
         maint_trans_meas_and_reg_sta_equip_lte: Optional[str], optional
             filter by `maint_trans_meas_and_reg_sta_equip <= x`, by default None
         maint_trans_communication_equip: Optional[str], optional
             Maintenance expenses associated with communication equipment., by default None
         maint_trans_communication_equip_gt: Optional[str], optional
             filter by `maint_trans_communication_equip > x`, by default None
         maint_trans_communication_equip_gte: Optional[str], optional
             filter by `maint_trans_communication_equip >= x`, by default None
         maint_trans_communication_equip_lt: Optional[str], optional
             filter by `maint_trans_communication_equip < x`, by default None
         maint_trans_communication_equip_lte: Optional[str], optional
             filter by `maint_trans_communication_equip <= x`, by default None
         transmiss_maint_of_misc_tran_plt: Optional[str], optional
             Total of other maintenance expenses., by default None
         transmiss_maint_of_misc_tran_plt_gt: Optional[str], optional
             filter by `transmiss_maint_of_misc_tran_plt > x`, by default None
         transmiss_maint_of_misc_tran_plt_gte: Optional[str], optional
             filter by `transmiss_maint_of_misc_tran_plt >= x`, by default None
         transmiss_maint_of_misc_tran_plt_lt: Optional[str], optional
             filter by `transmiss_maint_of_misc_tran_plt < x`, by default None
         transmiss_maint_of_misc_tran_plt_lte: Optional[str], optional
             filter by `transmiss_maint_of_misc_tran_plt <= x`, by default None
         transmiss_maint_exp: Optional[str], optional
             The total maintenance expenses for the pipeline., by default None
         transmiss_maint_exp_gt: Optional[str], optional
             filter by `transmiss_maint_exp > x`, by default None
         transmiss_maint_exp_gte: Optional[str], optional
             filter by `transmiss_maint_exp >= x`, by default None
         transmiss_maint_exp_lt: Optional[str], optional
             filter by `transmiss_maint_exp < x`, by default None
         transmiss_maint_exp_lte: Optional[str], optional
             filter by `transmiss_maint_exp <= x`, by default None
         transmiss_oand_mexp: Optional[str], optional
             Operation and maintenance expenses., by default None
         transmiss_oand_mexp_gt: Optional[str], optional
             filter by `transmiss_oand_mexp > x`, by default None
         transmiss_oand_mexp_gte: Optional[str], optional
             filter by `transmiss_oand_mexp >= x`, by default None
         transmiss_oand_mexp_lt: Optional[str], optional
             filter by `transmiss_oand_mexp < x`, by default None
         transmiss_oand_mexp_lte: Optional[str], optional
             filter by `transmiss_oand_mexp <= x`, by default None
         peak1_int_pipe_no_notice_transp: Optional[str], optional
             Single day peak deliveries for interstate pipelines Dth., by default None
         peak1_int_pipe_no_notice_transp_gt: Optional[str], optional
             filter by `peak1_int_pipe_no_notice_transp > x`, by default None
         peak1_int_pipe_no_notice_transp_gte: Optional[str], optional
             filter by `peak1_int_pipe_no_notice_transp >= x`, by default None
         peak1_int_pipe_no_notice_transp_lt: Optional[str], optional
             filter by `peak1_int_pipe_no_notice_transp < x`, by default None
         peak1_int_pipe_no_notice_transp_lte: Optional[str], optional
             filter by `peak1_int_pipe_no_notice_transp <= x`, by default None
         peak1_oth_dth_no_notice_transport: Optional[str], optional
             Single day peak deliveries for no notice transport for others Dth., by default None
         peak1_oth_dth_no_notice_transport_gt: Optional[str], optional
             filter by `peak1_oth_dth_no_notice_transport > x`, by default None
         peak1_oth_dth_no_notice_transport_gte: Optional[str], optional
             filter by `peak1_oth_dth_no_notice_transport >= x`, by default None
         peak1_oth_dth_no_notice_transport_lt: Optional[str], optional
             filter by `peak1_oth_dth_no_notice_transport < x`, by default None
         peak1_oth_dth_no_notice_transport_lte: Optional[str], optional
             filter by `peak1_oth_dth_no_notice_transport <= x`, by default None
         peak1_total_dth_no_notice_transp: Optional[str], optional
             Total single day peak deliveries for no notice transported volumes Dth., by default None
         peak1_total_dth_no_notice_transp_gt: Optional[str], optional
             filter by `peak1_total_dth_no_notice_transp > x`, by default None
         peak1_total_dth_no_notice_transp_gte: Optional[str], optional
             filter by `peak1_total_dth_no_notice_transp >= x`, by default None
         peak1_total_dth_no_notice_transp_lt: Optional[str], optional
             filter by `peak1_total_dth_no_notice_transp < x`, by default None
         peak1_total_dth_no_notice_transp_lte: Optional[str], optional
             filter by `peak1_total_dth_no_notice_transp <= x`, by default None
         peak1_int_pipe_dth_oth_firm_transp: Optional[str], optional
             Single peak day other firm transportation-Dth of Gas Delivered to Interstate Pipelines., by default None
         peak1_int_pipe_dth_oth_firm_transp_gt: Optional[str], optional
             filter by `peak1_int_pipe_dth_oth_firm_transp > x`, by default None
         peak1_int_pipe_dth_oth_firm_transp_gte: Optional[str], optional
             filter by `peak1_int_pipe_dth_oth_firm_transp >= x`, by default None
         peak1_int_pipe_dth_oth_firm_transp_lt: Optional[str], optional
             filter by `peak1_int_pipe_dth_oth_firm_transp < x`, by default None
         peak1_int_pipe_dth_oth_firm_transp_lte: Optional[str], optional
             filter by `peak1_int_pipe_dth_oth_firm_transp <= x`, by default None
         peak1_oth_dth_other_firm_transport: Optional[str], optional
             Single peak day other firm transportation-Dth of gas delivered to others., by default None
         peak1_oth_dth_other_firm_transport_gt: Optional[str], optional
             filter by `peak1_oth_dth_other_firm_transport > x`, by default None
         peak1_oth_dth_other_firm_transport_gte: Optional[str], optional
             filter by `peak1_oth_dth_other_firm_transport >= x`, by default None
         peak1_oth_dth_other_firm_transport_lt: Optional[str], optional
             filter by `peak1_oth_dth_other_firm_transport < x`, by default None
         peak1_oth_dth_other_firm_transport_lte: Optional[str], optional
             filter by `peak1_oth_dth_other_firm_transport <= x`, by default None
         peak1_total_dth_oth_firm_transport: Optional[str], optional
             Total single day peak deliveries for other firm transportation Dth., by default None
         peak1_total_dth_oth_firm_transport_gt: Optional[str], optional
             filter by `peak1_total_dth_oth_firm_transport > x`, by default None
         peak1_total_dth_oth_firm_transport_gte: Optional[str], optional
             filter by `peak1_total_dth_oth_firm_transport >= x`, by default None
         peak1_total_dth_oth_firm_transport_lt: Optional[str], optional
             filter by `peak1_total_dth_oth_firm_transport < x`, by default None
         peak1_total_dth_oth_firm_transport_lte: Optional[str], optional
             filter by `peak1_total_dth_oth_firm_transport <= x`, by default None
         peak1_int_pipe_dth_interr_transp: Optional[str], optional
             Peak day interruptible transportation-Dth of gas delivered to interstate pipelines., by default None
         peak1_int_pipe_dth_interr_transp_gt: Optional[str], optional
             filter by `peak1_int_pipe_dth_interr_transp > x`, by default None
         peak1_int_pipe_dth_interr_transp_gte: Optional[str], optional
             filter by `peak1_int_pipe_dth_interr_transp >= x`, by default None
         peak1_int_pipe_dth_interr_transp_lt: Optional[str], optional
             filter by `peak1_int_pipe_dth_interr_transp < x`, by default None
         peak1_int_pipe_dth_interr_transp_lte: Optional[str], optional
             filter by `peak1_int_pipe_dth_interr_transp <= x`, by default None
         peak1_oth_dth_interr_transport: Optional[str], optional
             Peak day interruptible transportation-Dth of gas delivered to others., by default None
         peak1_oth_dth_interr_transport_gt: Optional[str], optional
             filter by `peak1_oth_dth_interr_transport > x`, by default None
         peak1_oth_dth_interr_transport_gte: Optional[str], optional
             filter by `peak1_oth_dth_interr_transport >= x`, by default None
         peak1_oth_dth_interr_transport_lt: Optional[str], optional
             filter by `peak1_oth_dth_interr_transport < x`, by default None
         peak1_oth_dth_interr_transport_lte: Optional[str], optional
             filter by `peak1_oth_dth_interr_transport <= x`, by default None
         peak1_total_dth_interr_transport: Optional[str], optional
             Peak day interruptible transportation-total Dth., by default None
         peak1_total_dth_interr_transport_gt: Optional[str], optional
             filter by `peak1_total_dth_interr_transport > x`, by default None
         peak1_total_dth_interr_transport_gte: Optional[str], optional
             filter by `peak1_total_dth_interr_transport >= x`, by default None
         peak1_total_dth_interr_transport_lt: Optional[str], optional
             filter by `peak1_total_dth_interr_transport < x`, by default None
         peak1_total_dth_interr_transport_lte: Optional[str], optional
             filter by `peak1_total_dth_interr_transport <= x`, by default None
         peak1_int_pipe_dth_oth_transp: Optional[str], optional
             Other-single peak day-Dth of gas delivered to interstate pipelines., by default None
         peak1_int_pipe_dth_oth_transp_gt: Optional[str], optional
             filter by `peak1_int_pipe_dth_oth_transp > x`, by default None
         peak1_int_pipe_dth_oth_transp_gte: Optional[str], optional
             filter by `peak1_int_pipe_dth_oth_transp >= x`, by default None
         peak1_int_pipe_dth_oth_transp_lt: Optional[str], optional
             filter by `peak1_int_pipe_dth_oth_transp < x`, by default None
         peak1_int_pipe_dth_oth_transp_lte: Optional[str], optional
             filter by `peak1_int_pipe_dth_oth_transp <= x`, by default None
         peak1_oth_dth_other_transport: Optional[str], optional
             Other-single peak day-Dth of gas delivered to others., by default None
         peak1_oth_dth_other_transport_gt: Optional[str], optional
             filter by `peak1_oth_dth_other_transport > x`, by default None
         peak1_oth_dth_other_transport_gte: Optional[str], optional
             filter by `peak1_oth_dth_other_transport >= x`, by default None
         peak1_oth_dth_other_transport_lt: Optional[str], optional
             filter by `peak1_oth_dth_other_transport < x`, by default None
         peak1_oth_dth_other_transport_lte: Optional[str], optional
             filter by `peak1_oth_dth_other_transport <= x`, by default None
         peak1_total_dth_oth_transport: Optional[str], optional
             Other-single peak day total Dth., by default None
         peak1_total_dth_oth_transport_gt: Optional[str], optional
             filter by `peak1_total_dth_oth_transport > x`, by default None
         peak1_total_dth_oth_transport_gte: Optional[str], optional
             filter by `peak1_total_dth_oth_transport >= x`, by default None
         peak1_total_dth_oth_transport_lt: Optional[str], optional
             filter by `peak1_total_dth_oth_transport < x`, by default None
         peak1_total_dth_oth_transport_lte: Optional[str], optional
             filter by `peak1_total_dth_oth_transport <= x`, by default None
         peak1_int_pipe_dth_transp: Optional[str], optional
             Single peak day-Dth of gas delivered to interstate pipelines total., by default None
         peak1_int_pipe_dth_transp_gt: Optional[str], optional
             filter by `peak1_int_pipe_dth_transp > x`, by default None
         peak1_int_pipe_dth_transp_gte: Optional[str], optional
             filter by `peak1_int_pipe_dth_transp >= x`, by default None
         peak1_int_pipe_dth_transp_lt: Optional[str], optional
             filter by `peak1_int_pipe_dth_transp < x`, by default None
         peak1_int_pipe_dth_transp_lte: Optional[str], optional
             filter by `peak1_int_pipe_dth_transp <= x`, by default None
         peak1_oth_dth_transport: Optional[str], optional
             Single peak day-Dth of gas delivered to others total., by default None
         peak1_oth_dth_transport_gt: Optional[str], optional
             filter by `peak1_oth_dth_transport > x`, by default None
         peak1_oth_dth_transport_gte: Optional[str], optional
             filter by `peak1_oth_dth_transport >= x`, by default None
         peak1_oth_dth_transport_lt: Optional[str], optional
             filter by `peak1_oth_dth_transport < x`, by default None
         peak1_oth_dth_transport_lte: Optional[str], optional
             filter by `peak1_oth_dth_transport <= x`, by default None
         peak1_total_dth_transport: Optional[str], optional
             Single peak day-Dth of gas delivered total., by default None
         peak1_total_dth_transport_gt: Optional[str], optional
             filter by `peak1_total_dth_transport > x`, by default None
         peak1_total_dth_transport_gte: Optional[str], optional
             filter by `peak1_total_dth_transport >= x`, by default None
         peak1_total_dth_transport_lt: Optional[str], optional
             filter by `peak1_total_dth_transport < x`, by default None
         peak1_total_dth_transport_lte: Optional[str], optional
             filter by `peak1_total_dth_transport <= x`, by default None
         peak3_int_pipe_no_notice_transp: Optional[str], optional
             Peak 3 Day No-Notice Transportation-Dth of Gas Delivered to Interstate Pipelines., by default None
         peak3_int_pipe_no_notice_transp_gt: Optional[str], optional
             filter by `peak3_int_pipe_no_notice_transp > x`, by default None
         peak3_int_pipe_no_notice_transp_gte: Optional[str], optional
             filter by `peak3_int_pipe_no_notice_transp >= x`, by default None
         peak3_int_pipe_no_notice_transp_lt: Optional[str], optional
             filter by `peak3_int_pipe_no_notice_transp < x`, by default None
         peak3_int_pipe_no_notice_transp_lte: Optional[str], optional
             filter by `peak3_int_pipe_no_notice_transp <= x`, by default None
         peak3_oth_no_notice_transport: Optional[str], optional
             Peak 3 Day No-Notice Transportation-Dth of Gas Delivered to Others., by default None
         peak3_oth_no_notice_transport_gt: Optional[str], optional
             filter by `peak3_oth_no_notice_transport > x`, by default None
         peak3_oth_no_notice_transport_gte: Optional[str], optional
             filter by `peak3_oth_no_notice_transport >= x`, by default None
         peak3_oth_no_notice_transport_lt: Optional[str], optional
             filter by `peak3_oth_no_notice_transport < x`, by default None
         peak3_oth_no_notice_transport_lte: Optional[str], optional
             filter by `peak3_oth_no_notice_transport <= x`, by default None
         peak3_total_no_notice_transport: Optional[str], optional
             Total Peak 3 Day No-Notice Transportation Dth., by default None
         peak3_total_no_notice_transport_gt: Optional[str], optional
             filter by `peak3_total_no_notice_transport > x`, by default None
         peak3_total_no_notice_transport_gte: Optional[str], optional
             filter by `peak3_total_no_notice_transport >= x`, by default None
         peak3_total_no_notice_transport_lt: Optional[str], optional
             filter by `peak3_total_no_notice_transport < x`, by default None
         peak3_total_no_notice_transport_lte: Optional[str], optional
             filter by `peak3_total_no_notice_transport <= x`, by default None
         peak3_int_pipe_dth_oth_firm_transp: Optional[str], optional
             Peak 3 day Other Firm Transportation-Dth of Gas Delivered to Interstate Pipelines., by default None
         peak3_int_pipe_dth_oth_firm_transp_gt: Optional[str], optional
             filter by `peak3_int_pipe_dth_oth_firm_transp > x`, by default None
         peak3_int_pipe_dth_oth_firm_transp_gte: Optional[str], optional
             filter by `peak3_int_pipe_dth_oth_firm_transp >= x`, by default None
         peak3_int_pipe_dth_oth_firm_transp_lt: Optional[str], optional
             filter by `peak3_int_pipe_dth_oth_firm_transp < x`, by default None
         peak3_int_pipe_dth_oth_firm_transp_lte: Optional[str], optional
             filter by `peak3_int_pipe_dth_oth_firm_transp <= x`, by default None
         peak3_oth_dth_other_firm_transport: Optional[str], optional
             Peak 3 day Other Firm Transportation-Dth of Gas Delivered to Others., by default None
         peak3_oth_dth_other_firm_transport_gt: Optional[str], optional
             filter by `peak3_oth_dth_other_firm_transport > x`, by default None
         peak3_oth_dth_other_firm_transport_gte: Optional[str], optional
             filter by `peak3_oth_dth_other_firm_transport >= x`, by default None
         peak3_oth_dth_other_firm_transport_lt: Optional[str], optional
             filter by `peak3_oth_dth_other_firm_transport < x`, by default None
         peak3_oth_dth_other_firm_transport_lte: Optional[str], optional
             filter by `peak3_oth_dth_other_firm_transport <= x`, by default None
         peak3_total_dth_oth_firm_transp: Optional[str], optional
             Total 3 day peak other firm transportation Dth., by default None
         peak3_total_dth_oth_firm_transp_gt: Optional[str], optional
             filter by `peak3_total_dth_oth_firm_transp > x`, by default None
         peak3_total_dth_oth_firm_transp_gte: Optional[str], optional
             filter by `peak3_total_dth_oth_firm_transp >= x`, by default None
         peak3_total_dth_oth_firm_transp_lt: Optional[str], optional
             filter by `peak3_total_dth_oth_firm_transp < x`, by default None
         peak3_total_dth_oth_firm_transp_lte: Optional[str], optional
             filter by `peak3_total_dth_oth_firm_transp <= x`, by default None
         peak3_int_pipe_dth_interr_transp: Optional[str], optional
             Peak 3 day interruptible transportation-Dth of gas delivered to interstate pipelines., by default None
         peak3_int_pipe_dth_interr_transp_gt: Optional[str], optional
             filter by `peak3_int_pipe_dth_interr_transp > x`, by default None
         peak3_int_pipe_dth_interr_transp_gte: Optional[str], optional
             filter by `peak3_int_pipe_dth_interr_transp >= x`, by default None
         peak3_int_pipe_dth_interr_transp_lt: Optional[str], optional
             filter by `peak3_int_pipe_dth_interr_transp < x`, by default None
         peak3_int_pipe_dth_interr_transp_lte: Optional[str], optional
             filter by `peak3_int_pipe_dth_interr_transp <= x`, by default None
         peak3_oth_dth_interr_transport: Optional[str], optional
             Peak 3 day interruptible transportation-Dth of gas delivered to others., by default None
         peak3_oth_dth_interr_transport_gt: Optional[str], optional
             filter by `peak3_oth_dth_interr_transport > x`, by default None
         peak3_oth_dth_interr_transport_gte: Optional[str], optional
             filter by `peak3_oth_dth_interr_transport >= x`, by default None
         peak3_oth_dth_interr_transport_lt: Optional[str], optional
             filter by `peak3_oth_dth_interr_transport < x`, by default None
         peak3_oth_dth_interr_transport_lte: Optional[str], optional
             filter by `peak3_oth_dth_interr_transport <= x`, by default None
         peak3_total_dth_interr_transport: Optional[str], optional
             Peak 3 day total interruptible transportation Dth., by default None
         peak3_total_dth_interr_transport_gt: Optional[str], optional
             filter by `peak3_total_dth_interr_transport > x`, by default None
         peak3_total_dth_interr_transport_gte: Optional[str], optional
             filter by `peak3_total_dth_interr_transport >= x`, by default None
         peak3_total_dth_interr_transport_lt: Optional[str], optional
             filter by `peak3_total_dth_interr_transport < x`, by default None
         peak3_total_dth_interr_transport_lte: Optional[str], optional
             filter by `peak3_total_dth_interr_transport <= x`, by default None
         peak3_int_pipe_dth_oth_transp: Optional[str], optional
             Peak 3 day other-Dth of gas delivered to interstate pipelines., by default None
         peak3_int_pipe_dth_oth_transp_gt: Optional[str], optional
             filter by `peak3_int_pipe_dth_oth_transp > x`, by default None
         peak3_int_pipe_dth_oth_transp_gte: Optional[str], optional
             filter by `peak3_int_pipe_dth_oth_transp >= x`, by default None
         peak3_int_pipe_dth_oth_transp_lt: Optional[str], optional
             filter by `peak3_int_pipe_dth_oth_transp < x`, by default None
         peak3_int_pipe_dth_oth_transp_lte: Optional[str], optional
             filter by `peak3_int_pipe_dth_oth_transp <= x`, by default None
         peak3_oth_dth_other_transport: Optional[str], optional
             Peak 3 day other-Dth of gas delivered to others., by default None
         peak3_oth_dth_other_transport_gt: Optional[str], optional
             filter by `peak3_oth_dth_other_transport > x`, by default None
         peak3_oth_dth_other_transport_gte: Optional[str], optional
             filter by `peak3_oth_dth_other_transport >= x`, by default None
         peak3_oth_dth_other_transport_lt: Optional[str], optional
             filter by `peak3_oth_dth_other_transport < x`, by default None
         peak3_oth_dth_other_transport_lte: Optional[str], optional
             filter by `peak3_oth_dth_other_transport <= x`, by default None
         peak3_total_dth_other_transport: Optional[str], optional
             Total 3 day peak other-Dth of gas delivered to others., by default None
         peak3_total_dth_other_transport_gt: Optional[str], optional
             filter by `peak3_total_dth_other_transport > x`, by default None
         peak3_total_dth_other_transport_gte: Optional[str], optional
             filter by `peak3_total_dth_other_transport >= x`, by default None
         peak3_total_dth_other_transport_lt: Optional[str], optional
             filter by `peak3_total_dth_other_transport < x`, by default None
         peak3_total_dth_other_transport_lte: Optional[str], optional
             filter by `peak3_total_dth_other_transport <= x`, by default None
         peak3_int_pipe_dth_transp: Optional[str], optional
             Total 3 day peak-Dth of gas delivered to interstate pipelines., by default None
         peak3_int_pipe_dth_transp_gt: Optional[str], optional
             filter by `peak3_int_pipe_dth_transp > x`, by default None
         peak3_int_pipe_dth_transp_gte: Optional[str], optional
             filter by `peak3_int_pipe_dth_transp >= x`, by default None
         peak3_int_pipe_dth_transp_lt: Optional[str], optional
             filter by `peak3_int_pipe_dth_transp < x`, by default None
         peak3_int_pipe_dth_transp_lte: Optional[str], optional
             filter by `peak3_int_pipe_dth_transp <= x`, by default None
         peak3_oth_dth_transport: Optional[str], optional
             Total 3 day peak-Dth of gas delivered to others., by default None
         peak3_oth_dth_transport_gt: Optional[str], optional
             filter by `peak3_oth_dth_transport > x`, by default None
         peak3_oth_dth_transport_gte: Optional[str], optional
             filter by `peak3_oth_dth_transport >= x`, by default None
         peak3_oth_dth_transport_lt: Optional[str], optional
             filter by `peak3_oth_dth_transport < x`, by default None
         peak3_oth_dth_transport_lte: Optional[str], optional
             filter by `peak3_oth_dth_transport <= x`, by default None
         peak3_total_dth_transport: Optional[str], optional
             Total peak 3 day-Dth transport., by default None
         peak3_total_dth_transport_gt: Optional[str], optional
             filter by `peak3_total_dth_transport > x`, by default None
         peak3_total_dth_transport_gte: Optional[str], optional
             filter by `peak3_total_dth_transport >= x`, by default None
         peak3_total_dth_transport_lt: Optional[str], optional
             filter by `peak3_total_dth_transport < x`, by default None
         peak3_total_dth_transport_lte: Optional[str], optional
             filter by `peak3_total_dth_transport <= x`, by default None
         gas_of_oth_recd_for_gathering: Optional[str], optional
             Gas of others received for gathering., by default None
         gas_of_oth_recd_for_gathering_gt: Optional[str], optional
             filter by `gas_of_oth_recd_for_gathering > x`, by default None
         gas_of_oth_recd_for_gathering_gte: Optional[str], optional
             filter by `gas_of_oth_recd_for_gathering >= x`, by default None
         gas_of_oth_recd_for_gathering_lt: Optional[str], optional
             filter by `gas_of_oth_recd_for_gathering < x`, by default None
         gas_of_oth_recd_for_gathering_lte: Optional[str], optional
             filter by `gas_of_oth_recd_for_gathering <= x`, by default None
         reciepts: Optional[str], optional
             Total receipts of gas received., by default None
         reciepts_gt: Optional[str], optional
             filter by `reciepts > x`, by default None
         reciepts_gte: Optional[str], optional
             filter by `reciepts >= x`, by default None
         reciepts_lt: Optional[str], optional
             filter by `reciepts < x`, by default None
         reciepts_lte: Optional[str], optional
             filter by `reciepts <= x`, by default None
         deliv_of_gas_trans_or_compr_oth: Optional[str], optional
             Deliveries of gas transported for others., by default None
         deliv_of_gas_trans_or_compr_oth_gt: Optional[str], optional
             filter by `deliv_of_gas_trans_or_compr_oth > x`, by default None
         deliv_of_gas_trans_or_compr_oth_gte: Optional[str], optional
             filter by `deliv_of_gas_trans_or_compr_oth >= x`, by default None
         deliv_of_gas_trans_or_compr_oth_lt: Optional[str], optional
             filter by `deliv_of_gas_trans_or_compr_oth < x`, by default None
         deliv_of_gas_trans_or_compr_oth_lte: Optional[str], optional
             filter by `deliv_of_gas_trans_or_compr_oth <= x`, by default None
         gas_delivered_as_imbalances: Optional[str], optional
             Gas delivered as imbalances., by default None
         gas_delivered_as_imbalances_gt: Optional[str], optional
             filter by `gas_delivered_as_imbalances > x`, by default None
         gas_delivered_as_imbalances_gte: Optional[str], optional
             filter by `gas_delivered_as_imbalances >= x`, by default None
         gas_delivered_as_imbalances_lt: Optional[str], optional
             filter by `gas_delivered_as_imbalances < x`, by default None
         gas_delivered_as_imbalances_lte: Optional[str], optional
             filter by `gas_delivered_as_imbalances <= x`, by default None
         gas_used_for_compressor_sta_fuel: Optional[str], optional
             Gas used for compressor station fuel., by default None
         gas_used_for_compressor_sta_fuel_gt: Optional[str], optional
             filter by `gas_used_for_compressor_sta_fuel > x`, by default None
         gas_used_for_compressor_sta_fuel_gte: Optional[str], optional
             filter by `gas_used_for_compressor_sta_fuel >= x`, by default None
         gas_used_for_compressor_sta_fuel_lt: Optional[str], optional
             filter by `gas_used_for_compressor_sta_fuel < x`, by default None
         gas_used_for_compressor_sta_fuel_lte: Optional[str], optional
             filter by `gas_used_for_compressor_sta_fuel <= x`, by default None
         nat_gas_other_deliv: Optional[str], optional
             Natural gas other deliveries., by default None
         nat_gas_other_deliv_gt: Optional[str], optional
             filter by `nat_gas_other_deliv > x`, by default None
         nat_gas_other_deliv_gte: Optional[str], optional
             filter by `nat_gas_other_deliv >= x`, by default None
         nat_gas_other_deliv_lt: Optional[str], optional
             filter by `nat_gas_other_deliv < x`, by default None
         nat_gas_other_deliv_lte: Optional[str], optional
             filter by `nat_gas_other_deliv <= x`, by default None
         total_deliveries: Optional[str], optional
             Total deliveries of natural gas., by default None
         total_deliveries_gt: Optional[str], optional
             filter by `total_deliveries > x`, by default None
         total_deliveries_gte: Optional[str], optional
             filter by `total_deliveries >= x`, by default None
         total_deliveries_lt: Optional[str], optional
             filter by `total_deliveries < x`, by default None
         total_deliveries_lte: Optional[str], optional
             filter by `total_deliveries <= x`, by default None
         gas_stored_boy: Optional[str], optional
             Stored gas balance-beginning of year (BOY)., by default None
         gas_stored_boy_gt: Optional[str], optional
             filter by `gas_stored_boy > x`, by default None
         gas_stored_boy_gte: Optional[str], optional
             filter by `gas_stored_boy >= x`, by default None
         gas_stored_boy_lt: Optional[str], optional
             filter by `gas_stored_boy < x`, by default None
         gas_stored_boy_lte: Optional[str], optional
             filter by `gas_stored_boy <= x`, by default None
         gas_stored_gas_deliv_to_storage: Optional[str], optional
             Gas delivered to storage-beginning of year (BOY)., by default None
         gas_stored_gas_deliv_to_storage_gt: Optional[str], optional
             filter by `gas_stored_gas_deliv_to_storage > x`, by default None
         gas_stored_gas_deliv_to_storage_gte: Optional[str], optional
             filter by `gas_stored_gas_deliv_to_storage >= x`, by default None
         gas_stored_gas_deliv_to_storage_lt: Optional[str], optional
             filter by `gas_stored_gas_deliv_to_storage < x`, by default None
         gas_stored_gas_deliv_to_storage_lte: Optional[str], optional
             filter by `gas_stored_gas_deliv_to_storage <= x`, by default None
         gas_stored_gas_withdr_from_stor: Optional[str], optional
             Gas withdrawn from storage- beginning of year (BOY)., by default None
         gas_stored_gas_withdr_from_stor_gt: Optional[str], optional
             filter by `gas_stored_gas_withdr_from_stor > x`, by default None
         gas_stored_gas_withdr_from_stor_gte: Optional[str], optional
             filter by `gas_stored_gas_withdr_from_stor >= x`, by default None
         gas_stored_gas_withdr_from_stor_lt: Optional[str], optional
             filter by `gas_stored_gas_withdr_from_stor < x`, by default None
         gas_stored_gas_withdr_from_stor_lte: Optional[str], optional
             filter by `gas_stored_gas_withdr_from_stor <= x`, by default None
         gas_stored_oth_deb_or_cred_net: Optional[str], optional
             Gas stored other debits and credits., by default None
         gas_stored_oth_deb_or_cred_net_gt: Optional[str], optional
             filter by `gas_stored_oth_deb_or_cred_net > x`, by default None
         gas_stored_oth_deb_or_cred_net_gte: Optional[str], optional
             filter by `gas_stored_oth_deb_or_cred_net >= x`, by default None
         gas_stored_oth_deb_or_cred_net_lt: Optional[str], optional
             filter by `gas_stored_oth_deb_or_cred_net < x`, by default None
         gas_stored_oth_deb_or_cred_net_lte: Optional[str], optional
             filter by `gas_stored_oth_deb_or_cred_net <= x`, by default None
         gas_stored_eoy: Optional[str], optional
             Stored gas- end of year Dth (EOY)., by default None
         gas_stored_eoy_gt: Optional[str], optional
             filter by `gas_stored_eoy > x`, by default None
         gas_stored_eoy_gte: Optional[str], optional
             filter by `gas_stored_eoy >= x`, by default None
         gas_stored_eoy_lt: Optional[str], optional
             filter by `gas_stored_eoy < x`, by default None
         gas_stored_eoy_lte: Optional[str], optional
             filter by `gas_stored_eoy <= x`, by default None
         gas_stored_gas_volume_dth: Optional[str], optional
             Gas volume stored Dth., by default None
         gas_stored_gas_volume_dth_gt: Optional[str], optional
             filter by `gas_stored_gas_volume_dth > x`, by default None
         gas_stored_gas_volume_dth_gte: Optional[str], optional
             filter by `gas_stored_gas_volume_dth >= x`, by default None
         gas_stored_gas_volume_dth_lt: Optional[str], optional
             filter by `gas_stored_gas_volume_dth < x`, by default None
         gas_stored_gas_volume_dth_lte: Optional[str], optional
             filter by `gas_stored_gas_volume_dth <= x`, by default None
         gas_stored_amount_per_dth: Optional[str], optional
             Gas stored per Dth., by default None
         gas_stored_amount_per_dth_gt: Optional[str], optional
             filter by `gas_stored_amount_per_dth > x`, by default None
         gas_stored_amount_per_dth_gte: Optional[str], optional
             filter by `gas_stored_amount_per_dth >= x`, by default None
         gas_stored_amount_per_dth_lt: Optional[str], optional
             filter by `gas_stored_amount_per_dth < x`, by default None
         gas_stored_amount_per_dth_lte: Optional[str], optional
             filter by `gas_stored_amount_per_dth <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("pipelineFilerName", pipeline_filer_name))
        filter_params.append(list_to_filter("pipelineName", pipeline_name))
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(
            list_to_filter("operatingRevenuesGas", operating_revenues_gas)
        )
        if operating_revenues_gas_gt is not None:
            filter_params.append(
                f'operatingRevenuesGas > "{operating_revenues_gas_gt}"'
            )
        if operating_revenues_gas_gte is not None:
            filter_params.append(
                f'operatingRevenuesGas >= "{operating_revenues_gas_gte}"'
            )
        if operating_revenues_gas_lt is not None:
            filter_params.append(
                f'operatingRevenuesGas < "{operating_revenues_gas_lt}"'
            )
        if operating_revenues_gas_lte is not None:
            filter_params.append(
                f'operatingRevenuesGas <= "{operating_revenues_gas_lte}"'
            )
        filter_params.append(
            list_to_filter("operatingRevenuesTotal", operating_revenues_total)
        )
        if operating_revenues_total_gt is not None:
            filter_params.append(
                f'operatingRevenuesTotal > "{operating_revenues_total_gt}"'
            )
        if operating_revenues_total_gte is not None:
            filter_params.append(
                f'operatingRevenuesTotal >= "{operating_revenues_total_gte}"'
            )
        if operating_revenues_total_lt is not None:
            filter_params.append(
                f'operatingRevenuesTotal < "{operating_revenues_total_lt}"'
            )
        if operating_revenues_total_lte is not None:
            filter_params.append(
                f'operatingRevenuesTotal <= "{operating_revenues_total_lte}"'
            )
        filter_params.append(
            list_to_filter("operationExpensesGas", operation_expenses_gas)
        )
        if operation_expenses_gas_gt is not None:
            filter_params.append(
                f'operationExpensesGas > "{operation_expenses_gas_gt}"'
            )
        if operation_expenses_gas_gte is not None:
            filter_params.append(
                f'operationExpensesGas >= "{operation_expenses_gas_gte}"'
            )
        if operation_expenses_gas_lt is not None:
            filter_params.append(
                f'operationExpensesGas < "{operation_expenses_gas_lt}"'
            )
        if operation_expenses_gas_lte is not None:
            filter_params.append(
                f'operationExpensesGas <= "{operation_expenses_gas_lte}"'
            )
        filter_params.append(
            list_to_filter("operatingExpensesTotal", operating_expenses_total)
        )
        if operating_expenses_total_gt is not None:
            filter_params.append(
                f'operatingExpensesTotal > "{operating_expenses_total_gt}"'
            )
        if operating_expenses_total_gte is not None:
            filter_params.append(
                f'operatingExpensesTotal >= "{operating_expenses_total_gte}"'
            )
        if operating_expenses_total_lt is not None:
            filter_params.append(
                f'operatingExpensesTotal < "{operating_expenses_total_lt}"'
            )
        if operating_expenses_total_lte is not None:
            filter_params.append(
                f'operatingExpensesTotal <= "{operating_expenses_total_lte}"'
            )
        filter_params.append(
            list_to_filter("maintenanceExpensesGas", maintenance_expenses_gas)
        )
        if maintenance_expenses_gas_gt is not None:
            filter_params.append(
                f'maintenanceExpensesGas > "{maintenance_expenses_gas_gt}"'
            )
        if maintenance_expenses_gas_gte is not None:
            filter_params.append(
                f'maintenanceExpensesGas >= "{maintenance_expenses_gas_gte}"'
            )
        if maintenance_expenses_gas_lt is not None:
            filter_params.append(
                f'maintenanceExpensesGas < "{maintenance_expenses_gas_lt}"'
            )
        if maintenance_expenses_gas_lte is not None:
            filter_params.append(
                f'maintenanceExpensesGas <= "{maintenance_expenses_gas_lte}"'
            )
        filter_params.append(
            list_to_filter("maintenanceExpensesTotal", maintenance_expenses_total)
        )
        if maintenance_expenses_total_gt is not None:
            filter_params.append(
                f'maintenanceExpensesTotal > "{maintenance_expenses_total_gt}"'
            )
        if maintenance_expenses_total_gte is not None:
            filter_params.append(
                f'maintenanceExpensesTotal >= "{maintenance_expenses_total_gte}"'
            )
        if maintenance_expenses_total_lt is not None:
            filter_params.append(
                f'maintenanceExpensesTotal < "{maintenance_expenses_total_lt}"'
            )
        if maintenance_expenses_total_lte is not None:
            filter_params.append(
                f'maintenanceExpensesTotal <= "{maintenance_expenses_total_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "taxesOtherThanIncomeTaxesTotal", taxes_other_than_income_taxes_total
            )
        )
        if taxes_other_than_income_taxes_total_gt is not None:
            filter_params.append(
                f'taxesOtherThanIncomeTaxesTotal > "{taxes_other_than_income_taxes_total_gt}"'
            )
        if taxes_other_than_income_taxes_total_gte is not None:
            filter_params.append(
                f'taxesOtherThanIncomeTaxesTotal >= "{taxes_other_than_income_taxes_total_gte}"'
            )
        if taxes_other_than_income_taxes_total_lt is not None:
            filter_params.append(
                f'taxesOtherThanIncomeTaxesTotal < "{taxes_other_than_income_taxes_total_lt}"'
            )
        if taxes_other_than_income_taxes_total_lte is not None:
            filter_params.append(
                f'taxesOtherThanIncomeTaxesTotal <= "{taxes_other_than_income_taxes_total_lte}"'
            )
        filter_params.append(list_to_filter("utilityEbitda", utility_ebitda))
        if utility_ebitda_gt is not None:
            filter_params.append(f'utilityEbitda > "{utility_ebitda_gt}"')
        if utility_ebitda_gte is not None:
            filter_params.append(f'utilityEbitda >= "{utility_ebitda_gte}"')
        if utility_ebitda_lt is not None:
            filter_params.append(f'utilityEbitda < "{utility_ebitda_lt}"')
        if utility_ebitda_lte is not None:
            filter_params.append(f'utilityEbitda <= "{utility_ebitda_lte}"')
        filter_params.append(
            list_to_filter("transmissionPipelineLength", transmission_pipeline_length)
        )
        if transmission_pipeline_length_gt is not None:
            filter_params.append(
                f'transmissionPipelineLength > "{transmission_pipeline_length_gt}"'
            )
        if transmission_pipeline_length_gte is not None:
            filter_params.append(
                f'transmissionPipelineLength >= "{transmission_pipeline_length_gte}"'
            )
        if transmission_pipeline_length_lt is not None:
            filter_params.append(
                f'transmissionPipelineLength < "{transmission_pipeline_length_lt}"'
            )
        if transmission_pipeline_length_lte is not None:
            filter_params.append(
                f'transmissionPipelineLength <= "{transmission_pipeline_length_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "trptGasForOthersTransmsnVolMmcf", trpt_gas_for_others_transmsn_vol_mmcf
            )
        )
        if trpt_gas_for_others_transmsn_vol_mmcf_gt is not None:
            filter_params.append(
                f'trptGasForOthersTransmsnVolMmcf > "{trpt_gas_for_others_transmsn_vol_mmcf_gt}"'
            )
        if trpt_gas_for_others_transmsn_vol_mmcf_gte is not None:
            filter_params.append(
                f'trptGasForOthersTransmsnVolMmcf >= "{trpt_gas_for_others_transmsn_vol_mmcf_gte}"'
            )
        if trpt_gas_for_others_transmsn_vol_mmcf_lt is not None:
            filter_params.append(
                f'trptGasForOthersTransmsnVolMmcf < "{trpt_gas_for_others_transmsn_vol_mmcf_lt}"'
            )
        if trpt_gas_for_others_transmsn_vol_mmcf_lte is not None:
            filter_params.append(
                f'trptGasForOthersTransmsnVolMmcf <= "{trpt_gas_for_others_transmsn_vol_mmcf_lte}"'
            )
        filter_params.append(
            list_to_filter("landAndRightsTransEoy000", land_and_rights_trans_eoy000)
        )
        if land_and_rights_trans_eoy000_gt is not None:
            filter_params.append(
                f'landAndRightsTransEoy000 > "{land_and_rights_trans_eoy000_gt}"'
            )
        if land_and_rights_trans_eoy000_gte is not None:
            filter_params.append(
                f'landAndRightsTransEoy000 >= "{land_and_rights_trans_eoy000_gte}"'
            )
        if land_and_rights_trans_eoy000_lt is not None:
            filter_params.append(
                f'landAndRightsTransEoy000 < "{land_and_rights_trans_eoy000_lt}"'
            )
        if land_and_rights_trans_eoy000_lte is not None:
            filter_params.append(
                f'landAndRightsTransEoy000 <= "{land_and_rights_trans_eoy000_lte}"'
            )
        filter_params.append(
            list_to_filter("rightsOfWayTransEoy000", rights_of_way_trans_eoy000)
        )
        if rights_of_way_trans_eoy000_gt is not None:
            filter_params.append(
                f'rightsOfWayTransEoy000 > "{rights_of_way_trans_eoy000_gt}"'
            )
        if rights_of_way_trans_eoy000_gte is not None:
            filter_params.append(
                f'rightsOfWayTransEoy000 >= "{rights_of_way_trans_eoy000_gte}"'
            )
        if rights_of_way_trans_eoy000_lt is not None:
            filter_params.append(
                f'rightsOfWayTransEoy000 < "{rights_of_way_trans_eoy000_lt}"'
            )
        if rights_of_way_trans_eoy000_lte is not None:
            filter_params.append(
                f'rightsOfWayTransEoy000 <= "{rights_of_way_trans_eoy000_lte}"'
            )
        filter_params.append(
            list_to_filter("strucAndImprovTranEoy000", struc_and_improv_tran_eoy000)
        )
        if struc_and_improv_tran_eoy000_gt is not None:
            filter_params.append(
                f'strucAndImprovTranEoy000 > "{struc_and_improv_tran_eoy000_gt}"'
            )
        if struc_and_improv_tran_eoy000_gte is not None:
            filter_params.append(
                f'strucAndImprovTranEoy000 >= "{struc_and_improv_tran_eoy000_gte}"'
            )
        if struc_and_improv_tran_eoy000_lt is not None:
            filter_params.append(
                f'strucAndImprovTranEoy000 < "{struc_and_improv_tran_eoy000_lt}"'
            )
        if struc_and_improv_tran_eoy000_lte is not None:
            filter_params.append(
                f'strucAndImprovTranEoy000 <= "{struc_and_improv_tran_eoy000_lte}"'
            )
        filter_params.append(
            list_to_filter("mainsTransmissionEoy000", mains_transmission_eoy000)
        )
        if mains_transmission_eoy000_gt is not None:
            filter_params.append(
                f'mainsTransmissionEoy000 > "{mains_transmission_eoy000_gt}"'
            )
        if mains_transmission_eoy000_gte is not None:
            filter_params.append(
                f'mainsTransmissionEoy000 >= "{mains_transmission_eoy000_gte}"'
            )
        if mains_transmission_eoy000_lt is not None:
            filter_params.append(
                f'mainsTransmissionEoy000 < "{mains_transmission_eoy000_lt}"'
            )
        if mains_transmission_eoy000_lte is not None:
            filter_params.append(
                f'mainsTransmissionEoy000 <= "{mains_transmission_eoy000_lte}"'
            )
        filter_params.append(
            list_to_filter("comprstaequipTransEoy000", comprstaequip_trans_eoy000)
        )
        if comprstaequip_trans_eoy000_gt is not None:
            filter_params.append(
                f'comprstaequipTransEoy000 > "{comprstaequip_trans_eoy000_gt}"'
            )
        if comprstaequip_trans_eoy000_gte is not None:
            filter_params.append(
                f'comprstaequipTransEoy000 >= "{comprstaequip_trans_eoy000_gte}"'
            )
        if comprstaequip_trans_eoy000_lt is not None:
            filter_params.append(
                f'comprstaequipTransEoy000 < "{comprstaequip_trans_eoy000_lt}"'
            )
        if comprstaequip_trans_eoy000_lte is not None:
            filter_params.append(
                f'comprstaequipTransEoy000 <= "{comprstaequip_trans_eoy000_lte}"'
            )
        filter_params.append(
            list_to_filter("measRegStaEqTransEoy000", meas_reg_sta_eq_trans_eoy000)
        )
        if meas_reg_sta_eq_trans_eoy000_gt is not None:
            filter_params.append(
                f'measRegStaEqTransEoy000 > "{meas_reg_sta_eq_trans_eoy000_gt}"'
            )
        if meas_reg_sta_eq_trans_eoy000_gte is not None:
            filter_params.append(
                f'measRegStaEqTransEoy000 >= "{meas_reg_sta_eq_trans_eoy000_gte}"'
            )
        if meas_reg_sta_eq_trans_eoy000_lt is not None:
            filter_params.append(
                f'measRegStaEqTransEoy000 < "{meas_reg_sta_eq_trans_eoy000_lt}"'
            )
        if meas_reg_sta_eq_trans_eoy000_lte is not None:
            filter_params.append(
                f'measRegStaEqTransEoy000 <= "{meas_reg_sta_eq_trans_eoy000_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "communicationEquipTransEoy000", communication_equip_trans_eoy000
            )
        )
        if communication_equip_trans_eoy000_gt is not None:
            filter_params.append(
                f'communicationEquipTransEoy000 > "{communication_equip_trans_eoy000_gt}"'
            )
        if communication_equip_trans_eoy000_gte is not None:
            filter_params.append(
                f'communicationEquipTransEoy000 >= "{communication_equip_trans_eoy000_gte}"'
            )
        if communication_equip_trans_eoy000_lt is not None:
            filter_params.append(
                f'communicationEquipTransEoy000 < "{communication_equip_trans_eoy000_lt}"'
            )
        if communication_equip_trans_eoy000_lte is not None:
            filter_params.append(
                f'communicationEquipTransEoy000 <= "{communication_equip_trans_eoy000_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "totalTransmissionPlantAddns000", total_transmission_plant_addns000
            )
        )
        if total_transmission_plant_addns000_gt is not None:
            filter_params.append(
                f'totalTransmissionPlantAddns000 > "{total_transmission_plant_addns000_gt}"'
            )
        if total_transmission_plant_addns000_gte is not None:
            filter_params.append(
                f'totalTransmissionPlantAddns000 >= "{total_transmission_plant_addns000_gte}"'
            )
        if total_transmission_plant_addns000_lt is not None:
            filter_params.append(
                f'totalTransmissionPlantAddns000 < "{total_transmission_plant_addns000_lt}"'
            )
        if total_transmission_plant_addns000_lte is not None:
            filter_params.append(
                f'totalTransmissionPlantAddns000 <= "{total_transmission_plant_addns000_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "totalTransmissionPlantRet000", total_transmission_plant_ret000
            )
        )
        if total_transmission_plant_ret000_gt is not None:
            filter_params.append(
                f'totalTransmissionPlantRet000 > "{total_transmission_plant_ret000_gt}"'
            )
        if total_transmission_plant_ret000_gte is not None:
            filter_params.append(
                f'totalTransmissionPlantRet000 >= "{total_transmission_plant_ret000_gte}"'
            )
        if total_transmission_plant_ret000_lt is not None:
            filter_params.append(
                f'totalTransmissionPlantRet000 < "{total_transmission_plant_ret000_lt}"'
            )
        if total_transmission_plant_ret000_lte is not None:
            filter_params.append(
                f'totalTransmissionPlantRet000 <= "{total_transmission_plant_ret000_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "totalTransmissionPlantAdjust000", total_transmission_plant_adjust000
            )
        )
        if total_transmission_plant_adjust000_gt is not None:
            filter_params.append(
                f'totalTransmissionPlantAdjust000 > "{total_transmission_plant_adjust000_gt}"'
            )
        if total_transmission_plant_adjust000_gte is not None:
            filter_params.append(
                f'totalTransmissionPlantAdjust000 >= "{total_transmission_plant_adjust000_gte}"'
            )
        if total_transmission_plant_adjust000_lt is not None:
            filter_params.append(
                f'totalTransmissionPlantAdjust000 < "{total_transmission_plant_adjust000_lt}"'
            )
        if total_transmission_plant_adjust000_lte is not None:
            filter_params.append(
                f'totalTransmissionPlantAdjust000 <= "{total_transmission_plant_adjust000_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "totalTransmissionPlantTransf000", total_transmission_plant_transf000
            )
        )
        if total_transmission_plant_transf000_gt is not None:
            filter_params.append(
                f'totalTransmissionPlantTransf000 > "{total_transmission_plant_transf000_gt}"'
            )
        if total_transmission_plant_transf000_gte is not None:
            filter_params.append(
                f'totalTransmissionPlantTransf000 >= "{total_transmission_plant_transf000_gte}"'
            )
        if total_transmission_plant_transf000_lt is not None:
            filter_params.append(
                f'totalTransmissionPlantTransf000 < "{total_transmission_plant_transf000_lt}"'
            )
        if total_transmission_plant_transf000_lte is not None:
            filter_params.append(
                f'totalTransmissionPlantTransf000 <= "{total_transmission_plant_transf000_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "totalTransmissionPlantEoy000", total_transmission_plant_eoy000
            )
        )
        if total_transmission_plant_eoy000_gt is not None:
            filter_params.append(
                f'totalTransmissionPlantEoy000 > "{total_transmission_plant_eoy000_gt}"'
            )
        if total_transmission_plant_eoy000_gte is not None:
            filter_params.append(
                f'totalTransmissionPlantEoy000 >= "{total_transmission_plant_eoy000_gte}"'
            )
        if total_transmission_plant_eoy000_lt is not None:
            filter_params.append(
                f'totalTransmissionPlantEoy000 < "{total_transmission_plant_eoy000_lt}"'
            )
        if total_transmission_plant_eoy000_lte is not None:
            filter_params.append(
                f'totalTransmissionPlantEoy000 <= "{total_transmission_plant_eoy000_lte}"'
            )
        filter_params.append(
            list_to_filter("totalGasPlantInServiceEoy", total_gas_plant_in_service_eoy)
        )
        if total_gas_plant_in_service_eoy_gt is not None:
            filter_params.append(
                f'totalGasPlantInServiceEoy > "{total_gas_plant_in_service_eoy_gt}"'
            )
        if total_gas_plant_in_service_eoy_gte is not None:
            filter_params.append(
                f'totalGasPlantInServiceEoy >= "{total_gas_plant_in_service_eoy_gte}"'
            )
        if total_gas_plant_in_service_eoy_lt is not None:
            filter_params.append(
                f'totalGasPlantInServiceEoy < "{total_gas_plant_in_service_eoy_lt}"'
            )
        if total_gas_plant_in_service_eoy_lte is not None:
            filter_params.append(
                f'totalGasPlantInServiceEoy <= "{total_gas_plant_in_service_eoy_lte}"'
            )
        filter_params.append(list_to_filter("constrWipTotal", constr_wip_total))
        if constr_wip_total_gt is not None:
            filter_params.append(f'constrWipTotal > "{constr_wip_total_gt}"')
        if constr_wip_total_gte is not None:
            filter_params.append(f'constrWipTotal >= "{constr_wip_total_gte}"')
        if constr_wip_total_lt is not None:
            filter_params.append(f'constrWipTotal < "{constr_wip_total_lt}"')
        if constr_wip_total_lte is not None:
            filter_params.append(f'constrWipTotal <= "{constr_wip_total_lte}"')
        filter_params.append(
            list_to_filter("totalUtilityPlantTotal", total_utility_plant_total)
        )
        if total_utility_plant_total_gt is not None:
            filter_params.append(
                f'totalUtilityPlantTotal > "{total_utility_plant_total_gt}"'
            )
        if total_utility_plant_total_gte is not None:
            filter_params.append(
                f'totalUtilityPlantTotal >= "{total_utility_plant_total_gte}"'
            )
        if total_utility_plant_total_lt is not None:
            filter_params.append(
                f'totalUtilityPlantTotal < "{total_utility_plant_total_lt}"'
            )
        if total_utility_plant_total_lte is not None:
            filter_params.append(
                f'totalUtilityPlantTotal <= "{total_utility_plant_total_lte}"'
            )
        filter_params.append(
            list_to_filter("tranOpSupAndEngineering", tran_op_sup_and_engineering)
        )
        if tran_op_sup_and_engineering_gt is not None:
            filter_params.append(
                f'tranOpSupAndEngineering > "{tran_op_sup_and_engineering_gt}"'
            )
        if tran_op_sup_and_engineering_gte is not None:
            filter_params.append(
                f'tranOpSupAndEngineering >= "{tran_op_sup_and_engineering_gte}"'
            )
        if tran_op_sup_and_engineering_lt is not None:
            filter_params.append(
                f'tranOpSupAndEngineering < "{tran_op_sup_and_engineering_lt}"'
            )
        if tran_op_sup_and_engineering_lte is not None:
            filter_params.append(
                f'tranOpSupAndEngineering <= "{tran_op_sup_and_engineering_lte}"'
            )
        filter_params.append(
            list_to_filter("transmissOperLoadDispatch", transmiss_oper_load_dispatch)
        )
        if transmiss_oper_load_dispatch_gt is not None:
            filter_params.append(
                f'transmissOperLoadDispatch > "{transmiss_oper_load_dispatch_gt}"'
            )
        if transmiss_oper_load_dispatch_gte is not None:
            filter_params.append(
                f'transmissOperLoadDispatch >= "{transmiss_oper_load_dispatch_gte}"'
            )
        if transmiss_oper_load_dispatch_lt is not None:
            filter_params.append(
                f'transmissOperLoadDispatch < "{transmiss_oper_load_dispatch_lt}"'
            )
        if transmiss_oper_load_dispatch_lte is not None:
            filter_params.append(
                f'transmissOperLoadDispatch <= "{transmiss_oper_load_dispatch_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "operTransCommunicationSysExp", oper_trans_communication_sys_exp
            )
        )
        if oper_trans_communication_sys_exp_gt is not None:
            filter_params.append(
                f'operTransCommunicationSysExp > "{oper_trans_communication_sys_exp_gt}"'
            )
        if oper_trans_communication_sys_exp_gte is not None:
            filter_params.append(
                f'operTransCommunicationSysExp >= "{oper_trans_communication_sys_exp_gte}"'
            )
        if oper_trans_communication_sys_exp_lt is not None:
            filter_params.append(
                f'operTransCommunicationSysExp < "{oper_trans_communication_sys_exp_lt}"'
            )
        if oper_trans_communication_sys_exp_lte is not None:
            filter_params.append(
                f'operTransCommunicationSysExp <= "{oper_trans_communication_sys_exp_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "operTransComprStaLaborAndExp", oper_trans_compr_sta_labor_and_exp
            )
        )
        if oper_trans_compr_sta_labor_and_exp_gt is not None:
            filter_params.append(
                f'operTransComprStaLaborAndExp > "{oper_trans_compr_sta_labor_and_exp_gt}"'
            )
        if oper_trans_compr_sta_labor_and_exp_gte is not None:
            filter_params.append(
                f'operTransComprStaLaborAndExp >= "{oper_trans_compr_sta_labor_and_exp_gte}"'
            )
        if oper_trans_compr_sta_labor_and_exp_lt is not None:
            filter_params.append(
                f'operTransComprStaLaborAndExp < "{oper_trans_compr_sta_labor_and_exp_lt}"'
            )
        if oper_trans_compr_sta_labor_and_exp_lte is not None:
            filter_params.append(
                f'operTransComprStaLaborAndExp <= "{oper_trans_compr_sta_labor_and_exp_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "operTransGasForComprStFuel", oper_trans_gas_for_compr_st_fuel
            )
        )
        if oper_trans_gas_for_compr_st_fuel_gt is not None:
            filter_params.append(
                f'operTransGasForComprStFuel > "{oper_trans_gas_for_compr_st_fuel_gt}"'
            )
        if oper_trans_gas_for_compr_st_fuel_gte is not None:
            filter_params.append(
                f'operTransGasForComprStFuel >= "{oper_trans_gas_for_compr_st_fuel_gte}"'
            )
        if oper_trans_gas_for_compr_st_fuel_lt is not None:
            filter_params.append(
                f'operTransGasForComprStFuel < "{oper_trans_gas_for_compr_st_fuel_lt}"'
            )
        if oper_trans_gas_for_compr_st_fuel_lte is not None:
            filter_params.append(
                f'operTransGasForComprStFuel <= "{oper_trans_gas_for_compr_st_fuel_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "operTransOthFuelAndPwrForComprSt",
                oper_trans_oth_fuel_and_pwr_for_compr_st,
            )
        )
        if oper_trans_oth_fuel_and_pwr_for_compr_st_gt is not None:
            filter_params.append(
                f'operTransOthFuelAndPwrForComprSt > "{oper_trans_oth_fuel_and_pwr_for_compr_st_gt}"'
            )
        if oper_trans_oth_fuel_and_pwr_for_compr_st_gte is not None:
            filter_params.append(
                f'operTransOthFuelAndPwrForComprSt >= "{oper_trans_oth_fuel_and_pwr_for_compr_st_gte}"'
            )
        if oper_trans_oth_fuel_and_pwr_for_compr_st_lt is not None:
            filter_params.append(
                f'operTransOthFuelAndPwrForComprSt < "{oper_trans_oth_fuel_and_pwr_for_compr_st_lt}"'
            )
        if oper_trans_oth_fuel_and_pwr_for_compr_st_lte is not None:
            filter_params.append(
                f'operTransOthFuelAndPwrForComprSt <= "{oper_trans_oth_fuel_and_pwr_for_compr_st_lte}"'
            )
        filter_params.append(list_to_filter("operTransMainsExp", oper_trans_mains_exp))
        if oper_trans_mains_exp_gt is not None:
            filter_params.append(f'operTransMainsExp > "{oper_trans_mains_exp_gt}"')
        if oper_trans_mains_exp_gte is not None:
            filter_params.append(f'operTransMainsExp >= "{oper_trans_mains_exp_gte}"')
        if oper_trans_mains_exp_lt is not None:
            filter_params.append(f'operTransMainsExp < "{oper_trans_mains_exp_lt}"')
        if oper_trans_mains_exp_lte is not None:
            filter_params.append(f'operTransMainsExp <= "{oper_trans_mains_exp_lte}"')
        filter_params.append(
            list_to_filter("operTransMeasAndRegStaExp", oper_trans_meas_and_reg_sta_exp)
        )
        if oper_trans_meas_and_reg_sta_exp_gt is not None:
            filter_params.append(
                f'operTransMeasAndRegStaExp > "{oper_trans_meas_and_reg_sta_exp_gt}"'
            )
        if oper_trans_meas_and_reg_sta_exp_gte is not None:
            filter_params.append(
                f'operTransMeasAndRegStaExp >= "{oper_trans_meas_and_reg_sta_exp_gte}"'
            )
        if oper_trans_meas_and_reg_sta_exp_lt is not None:
            filter_params.append(
                f'operTransMeasAndRegStaExp < "{oper_trans_meas_and_reg_sta_exp_lt}"'
            )
        if oper_trans_meas_and_reg_sta_exp_lte is not None:
            filter_params.append(
                f'operTransMeasAndRegStaExp <= "{oper_trans_meas_and_reg_sta_exp_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "operTransTransmAndComprByOth", oper_trans_transm_and_compr_by_oth
            )
        )
        if oper_trans_transm_and_compr_by_oth_gt is not None:
            filter_params.append(
                f'operTransTransmAndComprByOth > "{oper_trans_transm_and_compr_by_oth_gt}"'
            )
        if oper_trans_transm_and_compr_by_oth_gte is not None:
            filter_params.append(
                f'operTransTransmAndComprByOth >= "{oper_trans_transm_and_compr_by_oth_gte}"'
            )
        if oper_trans_transm_and_compr_by_oth_lt is not None:
            filter_params.append(
                f'operTransTransmAndComprByOth < "{oper_trans_transm_and_compr_by_oth_lt}"'
            )
        if oper_trans_transm_and_compr_by_oth_lte is not None:
            filter_params.append(
                f'operTransTransmAndComprByOth <= "{oper_trans_transm_and_compr_by_oth_lte}"'
            )
        filter_params.append(
            list_to_filter("tranOpMiscTransmissionExp", tran_op_misc_transmission_exp)
        )
        if tran_op_misc_transmission_exp_gt is not None:
            filter_params.append(
                f'tranOpMiscTransmissionExp > "{tran_op_misc_transmission_exp_gt}"'
            )
        if tran_op_misc_transmission_exp_gte is not None:
            filter_params.append(
                f'tranOpMiscTransmissionExp >= "{tran_op_misc_transmission_exp_gte}"'
            )
        if tran_op_misc_transmission_exp_lt is not None:
            filter_params.append(
                f'tranOpMiscTransmissionExp < "{tran_op_misc_transmission_exp_lt}"'
            )
        if tran_op_misc_transmission_exp_lte is not None:
            filter_params.append(
                f'tranOpMiscTransmissionExp <= "{tran_op_misc_transmission_exp_lte}"'
            )
        filter_params.append(list_to_filter("transmissOperRents", transmiss_oper_rents))
        if transmiss_oper_rents_gt is not None:
            filter_params.append(f'transmissOperRents > "{transmiss_oper_rents_gt}"')
        if transmiss_oper_rents_gte is not None:
            filter_params.append(f'transmissOperRents >= "{transmiss_oper_rents_gte}"')
        if transmiss_oper_rents_lt is not None:
            filter_params.append(f'transmissOperRents < "{transmiss_oper_rents_lt}"')
        if transmiss_oper_rents_lte is not None:
            filter_params.append(f'transmissOperRents <= "{transmiss_oper_rents_lte}"')
        filter_params.append(
            list_to_filter("transmissTranOperationExp", transmiss_tran_operation_exp)
        )
        if transmiss_tran_operation_exp_gt is not None:
            filter_params.append(
                f'transmissTranOperationExp > "{transmiss_tran_operation_exp_gt}"'
            )
        if transmiss_tran_operation_exp_gte is not None:
            filter_params.append(
                f'transmissTranOperationExp >= "{transmiss_tran_operation_exp_gte}"'
            )
        if transmiss_tran_operation_exp_lt is not None:
            filter_params.append(
                f'transmissTranOperationExp < "{transmiss_tran_operation_exp_lt}"'
            )
        if transmiss_tran_operation_exp_lte is not None:
            filter_params.append(
                f'transmissTranOperationExp <= "{transmiss_tran_operation_exp_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "transmissMaintSupvsnAndEngin", transmiss_maint_supvsn_and_engin
            )
        )
        if transmiss_maint_supvsn_and_engin_gt is not None:
            filter_params.append(
                f'transmissMaintSupvsnAndEngin > "{transmiss_maint_supvsn_and_engin_gt}"'
            )
        if transmiss_maint_supvsn_and_engin_gte is not None:
            filter_params.append(
                f'transmissMaintSupvsnAndEngin >= "{transmiss_maint_supvsn_and_engin_gte}"'
            )
        if transmiss_maint_supvsn_and_engin_lt is not None:
            filter_params.append(
                f'transmissMaintSupvsnAndEngin < "{transmiss_maint_supvsn_and_engin_lt}"'
            )
        if transmiss_maint_supvsn_and_engin_lte is not None:
            filter_params.append(
                f'transmissMaintSupvsnAndEngin <= "{transmiss_maint_supvsn_and_engin_lte}"'
            )
        filter_params.append(
            list_to_filter("transmissMaintOfStructures", transmiss_maint_of_structures)
        )
        if transmiss_maint_of_structures_gt is not None:
            filter_params.append(
                f'transmissMaintOfStructures > "{transmiss_maint_of_structures_gt}"'
            )
        if transmiss_maint_of_structures_gte is not None:
            filter_params.append(
                f'transmissMaintOfStructures >= "{transmiss_maint_of_structures_gte}"'
            )
        if transmiss_maint_of_structures_lt is not None:
            filter_params.append(
                f'transmissMaintOfStructures < "{transmiss_maint_of_structures_lt}"'
            )
        if transmiss_maint_of_structures_lte is not None:
            filter_params.append(
                f'transmissMaintOfStructures <= "{transmiss_maint_of_structures_lte}"'
            )
        filter_params.append(list_to_filter("maintTransMains", maint_trans_mains))
        if maint_trans_mains_gt is not None:
            filter_params.append(f'maintTransMains > "{maint_trans_mains_gt}"')
        if maint_trans_mains_gte is not None:
            filter_params.append(f'maintTransMains >= "{maint_trans_mains_gte}"')
        if maint_trans_mains_lt is not None:
            filter_params.append(f'maintTransMains < "{maint_trans_mains_lt}"')
        if maint_trans_mains_lte is not None:
            filter_params.append(f'maintTransMains <= "{maint_trans_mains_lte}"')
        filter_params.append(
            list_to_filter(
                "maintTransCompressorStaEquip", maint_trans_compressor_sta_equip
            )
        )
        if maint_trans_compressor_sta_equip_gt is not None:
            filter_params.append(
                f'maintTransCompressorStaEquip > "{maint_trans_compressor_sta_equip_gt}"'
            )
        if maint_trans_compressor_sta_equip_gte is not None:
            filter_params.append(
                f'maintTransCompressorStaEquip >= "{maint_trans_compressor_sta_equip_gte}"'
            )
        if maint_trans_compressor_sta_equip_lt is not None:
            filter_params.append(
                f'maintTransCompressorStaEquip < "{maint_trans_compressor_sta_equip_lt}"'
            )
        if maint_trans_compressor_sta_equip_lte is not None:
            filter_params.append(
                f'maintTransCompressorStaEquip <= "{maint_trans_compressor_sta_equip_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "maintTransMeasAndRegStaEquip", maint_trans_meas_and_reg_sta_equip
            )
        )
        if maint_trans_meas_and_reg_sta_equip_gt is not None:
            filter_params.append(
                f'maintTransMeasAndRegStaEquip > "{maint_trans_meas_and_reg_sta_equip_gt}"'
            )
        if maint_trans_meas_and_reg_sta_equip_gte is not None:
            filter_params.append(
                f'maintTransMeasAndRegStaEquip >= "{maint_trans_meas_and_reg_sta_equip_gte}"'
            )
        if maint_trans_meas_and_reg_sta_equip_lt is not None:
            filter_params.append(
                f'maintTransMeasAndRegStaEquip < "{maint_trans_meas_and_reg_sta_equip_lt}"'
            )
        if maint_trans_meas_and_reg_sta_equip_lte is not None:
            filter_params.append(
                f'maintTransMeasAndRegStaEquip <= "{maint_trans_meas_and_reg_sta_equip_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "maintTransCommunicationEquip", maint_trans_communication_equip
            )
        )
        if maint_trans_communication_equip_gt is not None:
            filter_params.append(
                f'maintTransCommunicationEquip > "{maint_trans_communication_equip_gt}"'
            )
        if maint_trans_communication_equip_gte is not None:
            filter_params.append(
                f'maintTransCommunicationEquip >= "{maint_trans_communication_equip_gte}"'
            )
        if maint_trans_communication_equip_lt is not None:
            filter_params.append(
                f'maintTransCommunicationEquip < "{maint_trans_communication_equip_lt}"'
            )
        if maint_trans_communication_equip_lte is not None:
            filter_params.append(
                f'maintTransCommunicationEquip <= "{maint_trans_communication_equip_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "transmissMaintOfMiscTranPlt", transmiss_maint_of_misc_tran_plt
            )
        )
        if transmiss_maint_of_misc_tran_plt_gt is not None:
            filter_params.append(
                f'transmissMaintOfMiscTranPlt > "{transmiss_maint_of_misc_tran_plt_gt}"'
            )
        if transmiss_maint_of_misc_tran_plt_gte is not None:
            filter_params.append(
                f'transmissMaintOfMiscTranPlt >= "{transmiss_maint_of_misc_tran_plt_gte}"'
            )
        if transmiss_maint_of_misc_tran_plt_lt is not None:
            filter_params.append(
                f'transmissMaintOfMiscTranPlt < "{transmiss_maint_of_misc_tran_plt_lt}"'
            )
        if transmiss_maint_of_misc_tran_plt_lte is not None:
            filter_params.append(
                f'transmissMaintOfMiscTranPlt <= "{transmiss_maint_of_misc_tran_plt_lte}"'
            )
        filter_params.append(list_to_filter("transmissMaintExp", transmiss_maint_exp))
        if transmiss_maint_exp_gt is not None:
            filter_params.append(f'transmissMaintExp > "{transmiss_maint_exp_gt}"')
        if transmiss_maint_exp_gte is not None:
            filter_params.append(f'transmissMaintExp >= "{transmiss_maint_exp_gte}"')
        if transmiss_maint_exp_lt is not None:
            filter_params.append(f'transmissMaintExp < "{transmiss_maint_exp_lt}"')
        if transmiss_maint_exp_lte is not None:
            filter_params.append(f'transmissMaintExp <= "{transmiss_maint_exp_lte}"')
        filter_params.append(list_to_filter("transmissOandMexp", transmiss_oand_mexp))
        if transmiss_oand_mexp_gt is not None:
            filter_params.append(f'transmissOandMexp > "{transmiss_oand_mexp_gt}"')
        if transmiss_oand_mexp_gte is not None:
            filter_params.append(f'transmissOandMexp >= "{transmiss_oand_mexp_gte}"')
        if transmiss_oand_mexp_lt is not None:
            filter_params.append(f'transmissOandMexp < "{transmiss_oand_mexp_lt}"')
        if transmiss_oand_mexp_lte is not None:
            filter_params.append(f'transmissOandMexp <= "{transmiss_oand_mexp_lte}"')
        filter_params.append(
            list_to_filter(
                "peak1IntPipeNoNoticeTransp", peak1_int_pipe_no_notice_transp
            )
        )
        if peak1_int_pipe_no_notice_transp_gt is not None:
            filter_params.append(
                f'peak1IntPipeNoNoticeTransp > "{peak1_int_pipe_no_notice_transp_gt}"'
            )
        if peak1_int_pipe_no_notice_transp_gte is not None:
            filter_params.append(
                f'peak1IntPipeNoNoticeTransp >= "{peak1_int_pipe_no_notice_transp_gte}"'
            )
        if peak1_int_pipe_no_notice_transp_lt is not None:
            filter_params.append(
                f'peak1IntPipeNoNoticeTransp < "{peak1_int_pipe_no_notice_transp_lt}"'
            )
        if peak1_int_pipe_no_notice_transp_lte is not None:
            filter_params.append(
                f'peak1IntPipeNoNoticeTransp <= "{peak1_int_pipe_no_notice_transp_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak1OthDthNoNoticeTransport", peak1_oth_dth_no_notice_transport
            )
        )
        if peak1_oth_dth_no_notice_transport_gt is not None:
            filter_params.append(
                f'peak1OthDthNoNoticeTransport > "{peak1_oth_dth_no_notice_transport_gt}"'
            )
        if peak1_oth_dth_no_notice_transport_gte is not None:
            filter_params.append(
                f'peak1OthDthNoNoticeTransport >= "{peak1_oth_dth_no_notice_transport_gte}"'
            )
        if peak1_oth_dth_no_notice_transport_lt is not None:
            filter_params.append(
                f'peak1OthDthNoNoticeTransport < "{peak1_oth_dth_no_notice_transport_lt}"'
            )
        if peak1_oth_dth_no_notice_transport_lte is not None:
            filter_params.append(
                f'peak1OthDthNoNoticeTransport <= "{peak1_oth_dth_no_notice_transport_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak1TotalDthNoNoticeTransp", peak1_total_dth_no_notice_transp
            )
        )
        if peak1_total_dth_no_notice_transp_gt is not None:
            filter_params.append(
                f'peak1TotalDthNoNoticeTransp > "{peak1_total_dth_no_notice_transp_gt}"'
            )
        if peak1_total_dth_no_notice_transp_gte is not None:
            filter_params.append(
                f'peak1TotalDthNoNoticeTransp >= "{peak1_total_dth_no_notice_transp_gte}"'
            )
        if peak1_total_dth_no_notice_transp_lt is not None:
            filter_params.append(
                f'peak1TotalDthNoNoticeTransp < "{peak1_total_dth_no_notice_transp_lt}"'
            )
        if peak1_total_dth_no_notice_transp_lte is not None:
            filter_params.append(
                f'peak1TotalDthNoNoticeTransp <= "{peak1_total_dth_no_notice_transp_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak1IntPipeDthOthFirmTransp", peak1_int_pipe_dth_oth_firm_transp
            )
        )
        if peak1_int_pipe_dth_oth_firm_transp_gt is not None:
            filter_params.append(
                f'peak1IntPipeDthOthFirmTransp > "{peak1_int_pipe_dth_oth_firm_transp_gt}"'
            )
        if peak1_int_pipe_dth_oth_firm_transp_gte is not None:
            filter_params.append(
                f'peak1IntPipeDthOthFirmTransp >= "{peak1_int_pipe_dth_oth_firm_transp_gte}"'
            )
        if peak1_int_pipe_dth_oth_firm_transp_lt is not None:
            filter_params.append(
                f'peak1IntPipeDthOthFirmTransp < "{peak1_int_pipe_dth_oth_firm_transp_lt}"'
            )
        if peak1_int_pipe_dth_oth_firm_transp_lte is not None:
            filter_params.append(
                f'peak1IntPipeDthOthFirmTransp <= "{peak1_int_pipe_dth_oth_firm_transp_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak1OthDthOtherFirmTransport", peak1_oth_dth_other_firm_transport
            )
        )
        if peak1_oth_dth_other_firm_transport_gt is not None:
            filter_params.append(
                f'peak1OthDthOtherFirmTransport > "{peak1_oth_dth_other_firm_transport_gt}"'
            )
        if peak1_oth_dth_other_firm_transport_gte is not None:
            filter_params.append(
                f'peak1OthDthOtherFirmTransport >= "{peak1_oth_dth_other_firm_transport_gte}"'
            )
        if peak1_oth_dth_other_firm_transport_lt is not None:
            filter_params.append(
                f'peak1OthDthOtherFirmTransport < "{peak1_oth_dth_other_firm_transport_lt}"'
            )
        if peak1_oth_dth_other_firm_transport_lte is not None:
            filter_params.append(
                f'peak1OthDthOtherFirmTransport <= "{peak1_oth_dth_other_firm_transport_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak1TotalDthOthFirmTransport", peak1_total_dth_oth_firm_transport
            )
        )
        if peak1_total_dth_oth_firm_transport_gt is not None:
            filter_params.append(
                f'peak1TotalDthOthFirmTransport > "{peak1_total_dth_oth_firm_transport_gt}"'
            )
        if peak1_total_dth_oth_firm_transport_gte is not None:
            filter_params.append(
                f'peak1TotalDthOthFirmTransport >= "{peak1_total_dth_oth_firm_transport_gte}"'
            )
        if peak1_total_dth_oth_firm_transport_lt is not None:
            filter_params.append(
                f'peak1TotalDthOthFirmTransport < "{peak1_total_dth_oth_firm_transport_lt}"'
            )
        if peak1_total_dth_oth_firm_transport_lte is not None:
            filter_params.append(
                f'peak1TotalDthOthFirmTransport <= "{peak1_total_dth_oth_firm_transport_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak1IntPipeDthInterrTransp", peak1_int_pipe_dth_interr_transp
            )
        )
        if peak1_int_pipe_dth_interr_transp_gt is not None:
            filter_params.append(
                f'peak1IntPipeDthInterrTransp > "{peak1_int_pipe_dth_interr_transp_gt}"'
            )
        if peak1_int_pipe_dth_interr_transp_gte is not None:
            filter_params.append(
                f'peak1IntPipeDthInterrTransp >= "{peak1_int_pipe_dth_interr_transp_gte}"'
            )
        if peak1_int_pipe_dth_interr_transp_lt is not None:
            filter_params.append(
                f'peak1IntPipeDthInterrTransp < "{peak1_int_pipe_dth_interr_transp_lt}"'
            )
        if peak1_int_pipe_dth_interr_transp_lte is not None:
            filter_params.append(
                f'peak1IntPipeDthInterrTransp <= "{peak1_int_pipe_dth_interr_transp_lte}"'
            )
        filter_params.append(
            list_to_filter("peak1OthDthInterrTransport", peak1_oth_dth_interr_transport)
        )
        if peak1_oth_dth_interr_transport_gt is not None:
            filter_params.append(
                f'peak1OthDthInterrTransport > "{peak1_oth_dth_interr_transport_gt}"'
            )
        if peak1_oth_dth_interr_transport_gte is not None:
            filter_params.append(
                f'peak1OthDthInterrTransport >= "{peak1_oth_dth_interr_transport_gte}"'
            )
        if peak1_oth_dth_interr_transport_lt is not None:
            filter_params.append(
                f'peak1OthDthInterrTransport < "{peak1_oth_dth_interr_transport_lt}"'
            )
        if peak1_oth_dth_interr_transport_lte is not None:
            filter_params.append(
                f'peak1OthDthInterrTransport <= "{peak1_oth_dth_interr_transport_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak1TotalDthInterrTransport", peak1_total_dth_interr_transport
            )
        )
        if peak1_total_dth_interr_transport_gt is not None:
            filter_params.append(
                f'peak1TotalDthInterrTransport > "{peak1_total_dth_interr_transport_gt}"'
            )
        if peak1_total_dth_interr_transport_gte is not None:
            filter_params.append(
                f'peak1TotalDthInterrTransport >= "{peak1_total_dth_interr_transport_gte}"'
            )
        if peak1_total_dth_interr_transport_lt is not None:
            filter_params.append(
                f'peak1TotalDthInterrTransport < "{peak1_total_dth_interr_transport_lt}"'
            )
        if peak1_total_dth_interr_transport_lte is not None:
            filter_params.append(
                f'peak1TotalDthInterrTransport <= "{peak1_total_dth_interr_transport_lte}"'
            )
        filter_params.append(
            list_to_filter("peak1IntPipeDthOthTransp", peak1_int_pipe_dth_oth_transp)
        )
        if peak1_int_pipe_dth_oth_transp_gt is not None:
            filter_params.append(
                f'peak1IntPipeDthOthTransp > "{peak1_int_pipe_dth_oth_transp_gt}"'
            )
        if peak1_int_pipe_dth_oth_transp_gte is not None:
            filter_params.append(
                f'peak1IntPipeDthOthTransp >= "{peak1_int_pipe_dth_oth_transp_gte}"'
            )
        if peak1_int_pipe_dth_oth_transp_lt is not None:
            filter_params.append(
                f'peak1IntPipeDthOthTransp < "{peak1_int_pipe_dth_oth_transp_lt}"'
            )
        if peak1_int_pipe_dth_oth_transp_lte is not None:
            filter_params.append(
                f'peak1IntPipeDthOthTransp <= "{peak1_int_pipe_dth_oth_transp_lte}"'
            )
        filter_params.append(
            list_to_filter("peak1OthDthOtherTransport", peak1_oth_dth_other_transport)
        )
        if peak1_oth_dth_other_transport_gt is not None:
            filter_params.append(
                f'peak1OthDthOtherTransport > "{peak1_oth_dth_other_transport_gt}"'
            )
        if peak1_oth_dth_other_transport_gte is not None:
            filter_params.append(
                f'peak1OthDthOtherTransport >= "{peak1_oth_dth_other_transport_gte}"'
            )
        if peak1_oth_dth_other_transport_lt is not None:
            filter_params.append(
                f'peak1OthDthOtherTransport < "{peak1_oth_dth_other_transport_lt}"'
            )
        if peak1_oth_dth_other_transport_lte is not None:
            filter_params.append(
                f'peak1OthDthOtherTransport <= "{peak1_oth_dth_other_transport_lte}"'
            )
        filter_params.append(
            list_to_filter("peak1TotalDthOthTransport", peak1_total_dth_oth_transport)
        )
        if peak1_total_dth_oth_transport_gt is not None:
            filter_params.append(
                f'peak1TotalDthOthTransport > "{peak1_total_dth_oth_transport_gt}"'
            )
        if peak1_total_dth_oth_transport_gte is not None:
            filter_params.append(
                f'peak1TotalDthOthTransport >= "{peak1_total_dth_oth_transport_gte}"'
            )
        if peak1_total_dth_oth_transport_lt is not None:
            filter_params.append(
                f'peak1TotalDthOthTransport < "{peak1_total_dth_oth_transport_lt}"'
            )
        if peak1_total_dth_oth_transport_lte is not None:
            filter_params.append(
                f'peak1TotalDthOthTransport <= "{peak1_total_dth_oth_transport_lte}"'
            )
        filter_params.append(
            list_to_filter("peak1IntPipeDthTransp", peak1_int_pipe_dth_transp)
        )
        if peak1_int_pipe_dth_transp_gt is not None:
            filter_params.append(
                f'peak1IntPipeDthTransp > "{peak1_int_pipe_dth_transp_gt}"'
            )
        if peak1_int_pipe_dth_transp_gte is not None:
            filter_params.append(
                f'peak1IntPipeDthTransp >= "{peak1_int_pipe_dth_transp_gte}"'
            )
        if peak1_int_pipe_dth_transp_lt is not None:
            filter_params.append(
                f'peak1IntPipeDthTransp < "{peak1_int_pipe_dth_transp_lt}"'
            )
        if peak1_int_pipe_dth_transp_lte is not None:
            filter_params.append(
                f'peak1IntPipeDthTransp <= "{peak1_int_pipe_dth_transp_lte}"'
            )
        filter_params.append(
            list_to_filter("peak1OthDthTransport", peak1_oth_dth_transport)
        )
        if peak1_oth_dth_transport_gt is not None:
            filter_params.append(
                f'peak1OthDthTransport > "{peak1_oth_dth_transport_gt}"'
            )
        if peak1_oth_dth_transport_gte is not None:
            filter_params.append(
                f'peak1OthDthTransport >= "{peak1_oth_dth_transport_gte}"'
            )
        if peak1_oth_dth_transport_lt is not None:
            filter_params.append(
                f'peak1OthDthTransport < "{peak1_oth_dth_transport_lt}"'
            )
        if peak1_oth_dth_transport_lte is not None:
            filter_params.append(
                f'peak1OthDthTransport <= "{peak1_oth_dth_transport_lte}"'
            )
        filter_params.append(
            list_to_filter("peak1TotalDthTransport", peak1_total_dth_transport)
        )
        if peak1_total_dth_transport_gt is not None:
            filter_params.append(
                f'peak1TotalDthTransport > "{peak1_total_dth_transport_gt}"'
            )
        if peak1_total_dth_transport_gte is not None:
            filter_params.append(
                f'peak1TotalDthTransport >= "{peak1_total_dth_transport_gte}"'
            )
        if peak1_total_dth_transport_lt is not None:
            filter_params.append(
                f'peak1TotalDthTransport < "{peak1_total_dth_transport_lt}"'
            )
        if peak1_total_dth_transport_lte is not None:
            filter_params.append(
                f'peak1TotalDthTransport <= "{peak1_total_dth_transport_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak3IntPipeNoNoticeTransp", peak3_int_pipe_no_notice_transp
            )
        )
        if peak3_int_pipe_no_notice_transp_gt is not None:
            filter_params.append(
                f'peak3IntPipeNoNoticeTransp > "{peak3_int_pipe_no_notice_transp_gt}"'
            )
        if peak3_int_pipe_no_notice_transp_gte is not None:
            filter_params.append(
                f'peak3IntPipeNoNoticeTransp >= "{peak3_int_pipe_no_notice_transp_gte}"'
            )
        if peak3_int_pipe_no_notice_transp_lt is not None:
            filter_params.append(
                f'peak3IntPipeNoNoticeTransp < "{peak3_int_pipe_no_notice_transp_lt}"'
            )
        if peak3_int_pipe_no_notice_transp_lte is not None:
            filter_params.append(
                f'peak3IntPipeNoNoticeTransp <= "{peak3_int_pipe_no_notice_transp_lte}"'
            )
        filter_params.append(
            list_to_filter("peak3OthNoNoticeTransport", peak3_oth_no_notice_transport)
        )
        if peak3_oth_no_notice_transport_gt is not None:
            filter_params.append(
                f'peak3OthNoNoticeTransport > "{peak3_oth_no_notice_transport_gt}"'
            )
        if peak3_oth_no_notice_transport_gte is not None:
            filter_params.append(
                f'peak3OthNoNoticeTransport >= "{peak3_oth_no_notice_transport_gte}"'
            )
        if peak3_oth_no_notice_transport_lt is not None:
            filter_params.append(
                f'peak3OthNoNoticeTransport < "{peak3_oth_no_notice_transport_lt}"'
            )
        if peak3_oth_no_notice_transport_lte is not None:
            filter_params.append(
                f'peak3OthNoNoticeTransport <= "{peak3_oth_no_notice_transport_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak3TotalNoNoticeTransport", peak3_total_no_notice_transport
            )
        )
        if peak3_total_no_notice_transport_gt is not None:
            filter_params.append(
                f'peak3TotalNoNoticeTransport > "{peak3_total_no_notice_transport_gt}"'
            )
        if peak3_total_no_notice_transport_gte is not None:
            filter_params.append(
                f'peak3TotalNoNoticeTransport >= "{peak3_total_no_notice_transport_gte}"'
            )
        if peak3_total_no_notice_transport_lt is not None:
            filter_params.append(
                f'peak3TotalNoNoticeTransport < "{peak3_total_no_notice_transport_lt}"'
            )
        if peak3_total_no_notice_transport_lte is not None:
            filter_params.append(
                f'peak3TotalNoNoticeTransport <= "{peak3_total_no_notice_transport_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak3IntPipeDthOthFirmTransp", peak3_int_pipe_dth_oth_firm_transp
            )
        )
        if peak3_int_pipe_dth_oth_firm_transp_gt is not None:
            filter_params.append(
                f'peak3IntPipeDthOthFirmTransp > "{peak3_int_pipe_dth_oth_firm_transp_gt}"'
            )
        if peak3_int_pipe_dth_oth_firm_transp_gte is not None:
            filter_params.append(
                f'peak3IntPipeDthOthFirmTransp >= "{peak3_int_pipe_dth_oth_firm_transp_gte}"'
            )
        if peak3_int_pipe_dth_oth_firm_transp_lt is not None:
            filter_params.append(
                f'peak3IntPipeDthOthFirmTransp < "{peak3_int_pipe_dth_oth_firm_transp_lt}"'
            )
        if peak3_int_pipe_dth_oth_firm_transp_lte is not None:
            filter_params.append(
                f'peak3IntPipeDthOthFirmTransp <= "{peak3_int_pipe_dth_oth_firm_transp_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak3OthDthOtherFirmTransport", peak3_oth_dth_other_firm_transport
            )
        )
        if peak3_oth_dth_other_firm_transport_gt is not None:
            filter_params.append(
                f'peak3OthDthOtherFirmTransport > "{peak3_oth_dth_other_firm_transport_gt}"'
            )
        if peak3_oth_dth_other_firm_transport_gte is not None:
            filter_params.append(
                f'peak3OthDthOtherFirmTransport >= "{peak3_oth_dth_other_firm_transport_gte}"'
            )
        if peak3_oth_dth_other_firm_transport_lt is not None:
            filter_params.append(
                f'peak3OthDthOtherFirmTransport < "{peak3_oth_dth_other_firm_transport_lt}"'
            )
        if peak3_oth_dth_other_firm_transport_lte is not None:
            filter_params.append(
                f'peak3OthDthOtherFirmTransport <= "{peak3_oth_dth_other_firm_transport_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak3TotalDthOthFirmTransp", peak3_total_dth_oth_firm_transp
            )
        )
        if peak3_total_dth_oth_firm_transp_gt is not None:
            filter_params.append(
                f'peak3TotalDthOthFirmTransp > "{peak3_total_dth_oth_firm_transp_gt}"'
            )
        if peak3_total_dth_oth_firm_transp_gte is not None:
            filter_params.append(
                f'peak3TotalDthOthFirmTransp >= "{peak3_total_dth_oth_firm_transp_gte}"'
            )
        if peak3_total_dth_oth_firm_transp_lt is not None:
            filter_params.append(
                f'peak3TotalDthOthFirmTransp < "{peak3_total_dth_oth_firm_transp_lt}"'
            )
        if peak3_total_dth_oth_firm_transp_lte is not None:
            filter_params.append(
                f'peak3TotalDthOthFirmTransp <= "{peak3_total_dth_oth_firm_transp_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak3IntPipeDthInterrTransp", peak3_int_pipe_dth_interr_transp
            )
        )
        if peak3_int_pipe_dth_interr_transp_gt is not None:
            filter_params.append(
                f'peak3IntPipeDthInterrTransp > "{peak3_int_pipe_dth_interr_transp_gt}"'
            )
        if peak3_int_pipe_dth_interr_transp_gte is not None:
            filter_params.append(
                f'peak3IntPipeDthInterrTransp >= "{peak3_int_pipe_dth_interr_transp_gte}"'
            )
        if peak3_int_pipe_dth_interr_transp_lt is not None:
            filter_params.append(
                f'peak3IntPipeDthInterrTransp < "{peak3_int_pipe_dth_interr_transp_lt}"'
            )
        if peak3_int_pipe_dth_interr_transp_lte is not None:
            filter_params.append(
                f'peak3IntPipeDthInterrTransp <= "{peak3_int_pipe_dth_interr_transp_lte}"'
            )
        filter_params.append(
            list_to_filter("peak3OthDthInterrTransport", peak3_oth_dth_interr_transport)
        )
        if peak3_oth_dth_interr_transport_gt is not None:
            filter_params.append(
                f'peak3OthDthInterrTransport > "{peak3_oth_dth_interr_transport_gt}"'
            )
        if peak3_oth_dth_interr_transport_gte is not None:
            filter_params.append(
                f'peak3OthDthInterrTransport >= "{peak3_oth_dth_interr_transport_gte}"'
            )
        if peak3_oth_dth_interr_transport_lt is not None:
            filter_params.append(
                f'peak3OthDthInterrTransport < "{peak3_oth_dth_interr_transport_lt}"'
            )
        if peak3_oth_dth_interr_transport_lte is not None:
            filter_params.append(
                f'peak3OthDthInterrTransport <= "{peak3_oth_dth_interr_transport_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak3TotalDthInterrTransport", peak3_total_dth_interr_transport
            )
        )
        if peak3_total_dth_interr_transport_gt is not None:
            filter_params.append(
                f'peak3TotalDthInterrTransport > "{peak3_total_dth_interr_transport_gt}"'
            )
        if peak3_total_dth_interr_transport_gte is not None:
            filter_params.append(
                f'peak3TotalDthInterrTransport >= "{peak3_total_dth_interr_transport_gte}"'
            )
        if peak3_total_dth_interr_transport_lt is not None:
            filter_params.append(
                f'peak3TotalDthInterrTransport < "{peak3_total_dth_interr_transport_lt}"'
            )
        if peak3_total_dth_interr_transport_lte is not None:
            filter_params.append(
                f'peak3TotalDthInterrTransport <= "{peak3_total_dth_interr_transport_lte}"'
            )
        filter_params.append(
            list_to_filter("peak3IntPipeDthOthTransp", peak3_int_pipe_dth_oth_transp)
        )
        if peak3_int_pipe_dth_oth_transp_gt is not None:
            filter_params.append(
                f'peak3IntPipeDthOthTransp > "{peak3_int_pipe_dth_oth_transp_gt}"'
            )
        if peak3_int_pipe_dth_oth_transp_gte is not None:
            filter_params.append(
                f'peak3IntPipeDthOthTransp >= "{peak3_int_pipe_dth_oth_transp_gte}"'
            )
        if peak3_int_pipe_dth_oth_transp_lt is not None:
            filter_params.append(
                f'peak3IntPipeDthOthTransp < "{peak3_int_pipe_dth_oth_transp_lt}"'
            )
        if peak3_int_pipe_dth_oth_transp_lte is not None:
            filter_params.append(
                f'peak3IntPipeDthOthTransp <= "{peak3_int_pipe_dth_oth_transp_lte}"'
            )
        filter_params.append(
            list_to_filter("peak3OthDthOtherTransport", peak3_oth_dth_other_transport)
        )
        if peak3_oth_dth_other_transport_gt is not None:
            filter_params.append(
                f'peak3OthDthOtherTransport > "{peak3_oth_dth_other_transport_gt}"'
            )
        if peak3_oth_dth_other_transport_gte is not None:
            filter_params.append(
                f'peak3OthDthOtherTransport >= "{peak3_oth_dth_other_transport_gte}"'
            )
        if peak3_oth_dth_other_transport_lt is not None:
            filter_params.append(
                f'peak3OthDthOtherTransport < "{peak3_oth_dth_other_transport_lt}"'
            )
        if peak3_oth_dth_other_transport_lte is not None:
            filter_params.append(
                f'peak3OthDthOtherTransport <= "{peak3_oth_dth_other_transport_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "peak3TotalDthOtherTransport", peak3_total_dth_other_transport
            )
        )
        if peak3_total_dth_other_transport_gt is not None:
            filter_params.append(
                f'peak3TotalDthOtherTransport > "{peak3_total_dth_other_transport_gt}"'
            )
        if peak3_total_dth_other_transport_gte is not None:
            filter_params.append(
                f'peak3TotalDthOtherTransport >= "{peak3_total_dth_other_transport_gte}"'
            )
        if peak3_total_dth_other_transport_lt is not None:
            filter_params.append(
                f'peak3TotalDthOtherTransport < "{peak3_total_dth_other_transport_lt}"'
            )
        if peak3_total_dth_other_transport_lte is not None:
            filter_params.append(
                f'peak3TotalDthOtherTransport <= "{peak3_total_dth_other_transport_lte}"'
            )
        filter_params.append(
            list_to_filter("peak3IntPipeDthTransp", peak3_int_pipe_dth_transp)
        )
        if peak3_int_pipe_dth_transp_gt is not None:
            filter_params.append(
                f'peak3IntPipeDthTransp > "{peak3_int_pipe_dth_transp_gt}"'
            )
        if peak3_int_pipe_dth_transp_gte is not None:
            filter_params.append(
                f'peak3IntPipeDthTransp >= "{peak3_int_pipe_dth_transp_gte}"'
            )
        if peak3_int_pipe_dth_transp_lt is not None:
            filter_params.append(
                f'peak3IntPipeDthTransp < "{peak3_int_pipe_dth_transp_lt}"'
            )
        if peak3_int_pipe_dth_transp_lte is not None:
            filter_params.append(
                f'peak3IntPipeDthTransp <= "{peak3_int_pipe_dth_transp_lte}"'
            )
        filter_params.append(
            list_to_filter("peak3OthDthTransport", peak3_oth_dth_transport)
        )
        if peak3_oth_dth_transport_gt is not None:
            filter_params.append(
                f'peak3OthDthTransport > "{peak3_oth_dth_transport_gt}"'
            )
        if peak3_oth_dth_transport_gte is not None:
            filter_params.append(
                f'peak3OthDthTransport >= "{peak3_oth_dth_transport_gte}"'
            )
        if peak3_oth_dth_transport_lt is not None:
            filter_params.append(
                f'peak3OthDthTransport < "{peak3_oth_dth_transport_lt}"'
            )
        if peak3_oth_dth_transport_lte is not None:
            filter_params.append(
                f'peak3OthDthTransport <= "{peak3_oth_dth_transport_lte}"'
            )
        filter_params.append(
            list_to_filter("peak3TotalDthTransport", peak3_total_dth_transport)
        )
        if peak3_total_dth_transport_gt is not None:
            filter_params.append(
                f'peak3TotalDthTransport > "{peak3_total_dth_transport_gt}"'
            )
        if peak3_total_dth_transport_gte is not None:
            filter_params.append(
                f'peak3TotalDthTransport >= "{peak3_total_dth_transport_gte}"'
            )
        if peak3_total_dth_transport_lt is not None:
            filter_params.append(
                f'peak3TotalDthTransport < "{peak3_total_dth_transport_lt}"'
            )
        if peak3_total_dth_transport_lte is not None:
            filter_params.append(
                f'peak3TotalDthTransport <= "{peak3_total_dth_transport_lte}"'
            )
        filter_params.append(
            list_to_filter("gasOfOthRecdForGathering", gas_of_oth_recd_for_gathering)
        )
        if gas_of_oth_recd_for_gathering_gt is not None:
            filter_params.append(
                f'gasOfOthRecdForGathering > "{gas_of_oth_recd_for_gathering_gt}"'
            )
        if gas_of_oth_recd_for_gathering_gte is not None:
            filter_params.append(
                f'gasOfOthRecdForGathering >= "{gas_of_oth_recd_for_gathering_gte}"'
            )
        if gas_of_oth_recd_for_gathering_lt is not None:
            filter_params.append(
                f'gasOfOthRecdForGathering < "{gas_of_oth_recd_for_gathering_lt}"'
            )
        if gas_of_oth_recd_for_gathering_lte is not None:
            filter_params.append(
                f'gasOfOthRecdForGathering <= "{gas_of_oth_recd_for_gathering_lte}"'
            )
        filter_params.append(list_to_filter("reciepts", reciepts))
        if reciepts_gt is not None:
            filter_params.append(f'reciepts > "{reciepts_gt}"')
        if reciepts_gte is not None:
            filter_params.append(f'reciepts >= "{reciepts_gte}"')
        if reciepts_lt is not None:
            filter_params.append(f'reciepts < "{reciepts_lt}"')
        if reciepts_lte is not None:
            filter_params.append(f'reciepts <= "{reciepts_lte}"')
        filter_params.append(
            list_to_filter("delivOfGasTransOrComprOth", deliv_of_gas_trans_or_compr_oth)
        )
        if deliv_of_gas_trans_or_compr_oth_gt is not None:
            filter_params.append(
                f'delivOfGasTransOrComprOth > "{deliv_of_gas_trans_or_compr_oth_gt}"'
            )
        if deliv_of_gas_trans_or_compr_oth_gte is not None:
            filter_params.append(
                f'delivOfGasTransOrComprOth >= "{deliv_of_gas_trans_or_compr_oth_gte}"'
            )
        if deliv_of_gas_trans_or_compr_oth_lt is not None:
            filter_params.append(
                f'delivOfGasTransOrComprOth < "{deliv_of_gas_trans_or_compr_oth_lt}"'
            )
        if deliv_of_gas_trans_or_compr_oth_lte is not None:
            filter_params.append(
                f'delivOfGasTransOrComprOth <= "{deliv_of_gas_trans_or_compr_oth_lte}"'
            )
        filter_params.append(
            list_to_filter("gasDeliveredAsImbalances", gas_delivered_as_imbalances)
        )
        if gas_delivered_as_imbalances_gt is not None:
            filter_params.append(
                f'gasDeliveredAsImbalances > "{gas_delivered_as_imbalances_gt}"'
            )
        if gas_delivered_as_imbalances_gte is not None:
            filter_params.append(
                f'gasDeliveredAsImbalances >= "{gas_delivered_as_imbalances_gte}"'
            )
        if gas_delivered_as_imbalances_lt is not None:
            filter_params.append(
                f'gasDeliveredAsImbalances < "{gas_delivered_as_imbalances_lt}"'
            )
        if gas_delivered_as_imbalances_lte is not None:
            filter_params.append(
                f'gasDeliveredAsImbalances <= "{gas_delivered_as_imbalances_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "gasUsedForCompressorStaFuel", gas_used_for_compressor_sta_fuel
            )
        )
        if gas_used_for_compressor_sta_fuel_gt is not None:
            filter_params.append(
                f'gasUsedForCompressorStaFuel > "{gas_used_for_compressor_sta_fuel_gt}"'
            )
        if gas_used_for_compressor_sta_fuel_gte is not None:
            filter_params.append(
                f'gasUsedForCompressorStaFuel >= "{gas_used_for_compressor_sta_fuel_gte}"'
            )
        if gas_used_for_compressor_sta_fuel_lt is not None:
            filter_params.append(
                f'gasUsedForCompressorStaFuel < "{gas_used_for_compressor_sta_fuel_lt}"'
            )
        if gas_used_for_compressor_sta_fuel_lte is not None:
            filter_params.append(
                f'gasUsedForCompressorStaFuel <= "{gas_used_for_compressor_sta_fuel_lte}"'
            )
        filter_params.append(list_to_filter("natGasOtherDeliv", nat_gas_other_deliv))
        if nat_gas_other_deliv_gt is not None:
            filter_params.append(f'natGasOtherDeliv > "{nat_gas_other_deliv_gt}"')
        if nat_gas_other_deliv_gte is not None:
            filter_params.append(f'natGasOtherDeliv >= "{nat_gas_other_deliv_gte}"')
        if nat_gas_other_deliv_lt is not None:
            filter_params.append(f'natGasOtherDeliv < "{nat_gas_other_deliv_lt}"')
        if nat_gas_other_deliv_lte is not None:
            filter_params.append(f'natGasOtherDeliv <= "{nat_gas_other_deliv_lte}"')
        filter_params.append(list_to_filter("totalDeliveries", total_deliveries))
        if total_deliveries_gt is not None:
            filter_params.append(f'totalDeliveries > "{total_deliveries_gt}"')
        if total_deliveries_gte is not None:
            filter_params.append(f'totalDeliveries >= "{total_deliveries_gte}"')
        if total_deliveries_lt is not None:
            filter_params.append(f'totalDeliveries < "{total_deliveries_lt}"')
        if total_deliveries_lte is not None:
            filter_params.append(f'totalDeliveries <= "{total_deliveries_lte}"')
        filter_params.append(list_to_filter("gasStoredBoy", gas_stored_boy))
        if gas_stored_boy_gt is not None:
            filter_params.append(f'gasStoredBoy > "{gas_stored_boy_gt}"')
        if gas_stored_boy_gte is not None:
            filter_params.append(f'gasStoredBoy >= "{gas_stored_boy_gte}"')
        if gas_stored_boy_lt is not None:
            filter_params.append(f'gasStoredBoy < "{gas_stored_boy_lt}"')
        if gas_stored_boy_lte is not None:
            filter_params.append(f'gasStoredBoy <= "{gas_stored_boy_lte}"')
        filter_params.append(
            list_to_filter(
                "gasStoredGasDelivToStorage", gas_stored_gas_deliv_to_storage
            )
        )
        if gas_stored_gas_deliv_to_storage_gt is not None:
            filter_params.append(
                f'gasStoredGasDelivToStorage > "{gas_stored_gas_deliv_to_storage_gt}"'
            )
        if gas_stored_gas_deliv_to_storage_gte is not None:
            filter_params.append(
                f'gasStoredGasDelivToStorage >= "{gas_stored_gas_deliv_to_storage_gte}"'
            )
        if gas_stored_gas_deliv_to_storage_lt is not None:
            filter_params.append(
                f'gasStoredGasDelivToStorage < "{gas_stored_gas_deliv_to_storage_lt}"'
            )
        if gas_stored_gas_deliv_to_storage_lte is not None:
            filter_params.append(
                f'gasStoredGasDelivToStorage <= "{gas_stored_gas_deliv_to_storage_lte}"'
            )
        filter_params.append(
            list_to_filter(
                "gasStoredGasWithdrFromStor", gas_stored_gas_withdr_from_stor
            )
        )
        if gas_stored_gas_withdr_from_stor_gt is not None:
            filter_params.append(
                f'gasStoredGasWithdrFromStor > "{gas_stored_gas_withdr_from_stor_gt}"'
            )
        if gas_stored_gas_withdr_from_stor_gte is not None:
            filter_params.append(
                f'gasStoredGasWithdrFromStor >= "{gas_stored_gas_withdr_from_stor_gte}"'
            )
        if gas_stored_gas_withdr_from_stor_lt is not None:
            filter_params.append(
                f'gasStoredGasWithdrFromStor < "{gas_stored_gas_withdr_from_stor_lt}"'
            )
        if gas_stored_gas_withdr_from_stor_lte is not None:
            filter_params.append(
                f'gasStoredGasWithdrFromStor <= "{gas_stored_gas_withdr_from_stor_lte}"'
            )
        filter_params.append(
            list_to_filter("gasStoredOthDebOrCredNet", gas_stored_oth_deb_or_cred_net)
        )
        if gas_stored_oth_deb_or_cred_net_gt is not None:
            filter_params.append(
                f'gasStoredOthDebOrCredNet > "{gas_stored_oth_deb_or_cred_net_gt}"'
            )
        if gas_stored_oth_deb_or_cred_net_gte is not None:
            filter_params.append(
                f'gasStoredOthDebOrCredNet >= "{gas_stored_oth_deb_or_cred_net_gte}"'
            )
        if gas_stored_oth_deb_or_cred_net_lt is not None:
            filter_params.append(
                f'gasStoredOthDebOrCredNet < "{gas_stored_oth_deb_or_cred_net_lt}"'
            )
        if gas_stored_oth_deb_or_cred_net_lte is not None:
            filter_params.append(
                f'gasStoredOthDebOrCredNet <= "{gas_stored_oth_deb_or_cred_net_lte}"'
            )
        filter_params.append(list_to_filter("gasStoredEoy", gas_stored_eoy))
        if gas_stored_eoy_gt is not None:
            filter_params.append(f'gasStoredEoy > "{gas_stored_eoy_gt}"')
        if gas_stored_eoy_gte is not None:
            filter_params.append(f'gasStoredEoy >= "{gas_stored_eoy_gte}"')
        if gas_stored_eoy_lt is not None:
            filter_params.append(f'gasStoredEoy < "{gas_stored_eoy_lt}"')
        if gas_stored_eoy_lte is not None:
            filter_params.append(f'gasStoredEoy <= "{gas_stored_eoy_lte}"')
        filter_params.append(
            list_to_filter("gasStoredGasVolumeDth", gas_stored_gas_volume_dth)
        )
        if gas_stored_gas_volume_dth_gt is not None:
            filter_params.append(
                f'gasStoredGasVolumeDth > "{gas_stored_gas_volume_dth_gt}"'
            )
        if gas_stored_gas_volume_dth_gte is not None:
            filter_params.append(
                f'gasStoredGasVolumeDth >= "{gas_stored_gas_volume_dth_gte}"'
            )
        if gas_stored_gas_volume_dth_lt is not None:
            filter_params.append(
                f'gasStoredGasVolumeDth < "{gas_stored_gas_volume_dth_lt}"'
            )
        if gas_stored_gas_volume_dth_lte is not None:
            filter_params.append(
                f'gasStoredGasVolumeDth <= "{gas_stored_gas_volume_dth_lte}"'
            )
        filter_params.append(
            list_to_filter("gasStoredAmountPerDth", gas_stored_amount_per_dth)
        )
        if gas_stored_amount_per_dth_gt is not None:
            filter_params.append(
                f'gasStoredAmountPerDth > "{gas_stored_amount_per_dth_gt}"'
            )
        if gas_stored_amount_per_dth_gte is not None:
            filter_params.append(
                f'gasStoredAmountPerDth >= "{gas_stored_amount_per_dth_gte}"'
            )
        if gas_stored_amount_per_dth_lt is not None:
            filter_params.append(
                f'gasStoredAmountPerDth < "{gas_stored_amount_per_dth_lt}"'
            )
        if gas_stored_amount_per_dth_lte is not None:
            filter_params.append(
                f'gasStoredAmountPerDth <= "{gas_stored_amount_per_dth_lte}"'
            )

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/pipeline-profiles-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    
    def get_modeled_demand_forecast(
        self,
        *,
        flow_date: Optional[date] = None,
        flow_date_lt: Optional[date] = None,
        flow_date_lte: Optional[date] = None,
        flow_date_gt: Optional[date] = None,
        flow_date_gte: Optional[date] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        forecast_date: Optional[datetime] = None,
        forecast_date_lt: Optional[datetime] = None,
        forecast_date_lte: Optional[datetime] = None,
        forecast_date_gt: Optional[datetime] = None,
        forecast_date_gte: Optional[datetime] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        model_id: Optional[Union[list[str], Series[str], str]] = None,
        model_type: Optional[Union[list[str], Series[str], str]] = None,
        model_type_id: Optional[Union[list[str], Series[str], str]] = None,
        market_type: Optional[Union[list[str], Series[str], str]] = None,
        market_type_id: Optional[Union[list[str], Series[str], str]] = None,
        function_type: Optional[Union[list[str], Series[str], str]] = None,
        function_type_id: Optional[Union[list[str], Series[str], str]] = None,
        point_of_view: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        volume: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        A 14-day ahead forecast for daily natural gas demand at a sector level for power, industrial, residential & commercial, fuel losses and total demand in the US lower 48 across all geographies. Canada and Mexico is included, but sector-level details may vary.

        Parameters
        ----------

         flow_date: Optional[date], optional
             The calendar date or gas day the activity occurred., by default None
         flow_date_gt: Optional[date], optional
             filter by `flow_date > x`, by default None
         flow_date_gte: Optional[date], optional
             filter by `flow_date >= x`, by default None
         flow_date_lt: Optional[date], optional
             filter by `flow_date < x`, by default None
         flow_date_lte: Optional[date], optional
             filter by `flow_date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         forecast_date: Optional[datetime], optional
             Standard Forecast Date., by default None
         forecast_date_gt: Optional[datetime], optional
             filter by `forecast_date > x`, by default None
         forecast_date_gte: Optional[datetime], optional
             filter by `forecast_date >= x`, by default None
         forecast_date_lt: Optional[datetime], optional
             filter by `forecast_date < x`, by default None
         forecast_date_lte: Optional[datetime], optional
             filter by `forecast_date <= x`, by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Daily, Weekly, Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Daily, Weekly, Monthly, Seasonal, Annual. Weekly date frequencies are based on the defined EIA storage week of Friday-Thursday. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         model_id: Optional[Union[list[str], Series[str], str]]
             Internal use, Model ID value., by default None
         model_type: Optional[Union[list[str], Series[str], str]]
             Model types can vary among supply, demand and other market fundamentals. The type describes the fundamentals the model output represents., by default None
         model_type_id: Optional[Union[list[str], Series[str], str]]
             ID associated with Model type., by default None
         market_type: Optional[Union[list[str], Series[str], str]]
             Market Type name, actual or forecast, by default None
         market_type_id: Optional[Union[list[str], Series[str], str]]
             ID associated with Market type., by default None
         function_type: Optional[Union[list[str], Series[str], str]]
             The name of the Function Type such as prediction, aggregation, allocation, ten year average., by default None
         function_type_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a Function Type such as 1 is prediction, 2 is aggregation, 3 is allocation, 4 is ten year average., by default None
         point_of_view: Optional[Union[list[str], Series[str], str]]
             Point of View for the values. Point of view based on a geographic hierarchy of country, region, subregion, or producing area., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         volume: Optional[Union[list[str], Series[str], str]]
             Volume., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("flowDate", flow_date))
        if flow_date_gt is not None:
            filter_params.append(f'flowDate > "{flow_date_gt}"')
        if flow_date_gte is not None:
            filter_params.append(f'flowDate >= "{flow_date_gte}"')
        if flow_date_lt is not None:
            filter_params.append(f'flowDate < "{flow_date_lt}"')
        if flow_date_lte is not None:
            filter_params.append(f'flowDate <= "{flow_date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("forecastDate", forecast_date))
        if forecast_date_gt is not None:
            filter_params.append(f'forecastDate > "{forecast_date_gt}"')
        if forecast_date_gte is not None:
            filter_params.append(f'forecastDate >= "{forecast_date_gte}"')
        if forecast_date_lt is not None:
            filter_params.append(f'forecastDate < "{forecast_date_lt}"')
        if forecast_date_lte is not None:
            filter_params.append(f'forecastDate <= "{forecast_date_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(list_to_filter("modelId", model_id))
        filter_params.append(list_to_filter("modelType", model_type))
        filter_params.append(list_to_filter("modelTypeId", model_type_id))
        filter_params.append(list_to_filter("marketType", market_type))
        filter_params.append(list_to_filter("marketTypeId", market_type_id))
        filter_params.append(list_to_filter("functionType", function_type))
        filter_params.append(list_to_filter("functionTypeId", function_type_id))
        filter_params.append(list_to_filter("pointOfView", point_of_view))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("volume", volume))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/modeled-demand-forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_no_notice_flow_data(
        self,
        *,
        flow_date: Optional[date] = None,
        flow_date_lt: Optional[date] = None,
        flow_date_lte: Optional[date] = None,
        flow_date_gt: Optional[date] = None,
        flow_date_gte: Optional[date] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        legacy_bentek_point_id: Optional[Union[list[str], Series[str], str]] = None,
        component_point_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_operator_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_operator_name: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_name: Optional[Union[list[str], Series[str], str]] = None,
        point_name: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_id_primary: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_primary: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_id_secondary: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_secondary: Optional[Union[list[str], Series[str], str]] = None,
        location_type_code: Optional[Union[list[str], Series[str], str]] = None,
        location_description: Optional[Union[list[str], Series[str], str]] = None,
        location_type_id: Optional[Union[list[str], Series[str], str]] = None,
        flow_direction: Optional[Union[list[str], Series[str], str]] = None,
        common_code: Optional[Union[list[str], Series[str], str]] = None,
        zone: Optional[Union[list[str], Series[str], str]] = None,
        company_id: Optional[Union[list[str], Series[str], str]] = None,
        connecting_party: Optional[Union[list[str], Series[str], str]] = None,
        loc_prop: Optional[Union[list[str], Series[str], str]] = None,
        point_is_active: Optional[Union[list[bool], Series[bool], bool]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        state_id: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        county_id: Optional[Union[list[str], Series[str], str]] = None,
        county: Optional[Union[list[str], Series[str], str]] = None,
        producing_area_id: Optional[Union[list[str], Series[str], str]] = None,
        producing_area: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        scheduled_volume: Optional[Union[list[str], Series[str], str]] = None,
        actual_volume: Optional[Union[list[str], Series[str], str]] = None,
        utilization: Optional[Union[list[str], Series[str], str]] = None,
        design_capacity: Optional[Union[list[str], Series[str], str]] = None,
        operational_capacity: Optional[Union[list[str], Series[str], str]] = None,
        actual_capacity: Optional[Union[list[str], Series[str], str]] = None,
        operationally_available: Optional[Union[list[str], Series[str], str]] = None,
        interruptible_flow: Optional[Union[list[str], Series[str], str]] = None,
        data_source: Optional[Union[list[str], Series[str], str]] = None,
        posting_date_time: Optional[datetime] = None,
        posting_date_time_lt: Optional[datetime] = None,
        posting_date_time_lte: Optional[datetime] = None,
        posting_date_time_gt: Optional[datetime] = None,
        posting_date_time_gte: Optional[datetime] = None,
        create_date: Optional[datetime] = None,
        create_date_lt: Optional[datetime] = None,
        create_date_lte: Optional[datetime] = None,
        create_date_gt: Optional[datetime] = None,
        create_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        No-notice data is limited to US interstate pipelines that offer no-notice transportation service contracts in the tariff. The details and provided information these pipelines post can vary. The overall dataset is similar to pipeline flows in its structure, but reported separately given the focused nature of no-notice services.

        Parameters
        ----------

         flow_date: Optional[date],
             The calendar date or gas day the activity occurred.
         flow_date_gt: Optional[date], optional
             filter by `flow_date > x`, by default None
         flow_date_gte: Optional[date], optional
             filter by `flow_date >= x`, by default None
         flow_date_lt: Optional[date], optional
             filter by `flow_date < x`, by default None
         flow_date_lte: Optional[date], optional
             filter by `flow_date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Daily, Weekly, Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Daily, Weekly, Monthly, Seasonal, Annual. Weekly date frequencies are based on the defined EIA storage week of Friday-Thursday. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         legacy_bentek_point_id: Optional[Union[list[str], Series[str], str]]
             Point ID for a meter used by the legacy Bentek service., by default None
         component_point_id: Optional[Union[list[str], Series[str], str]]
             Point ID for a meter used by the Americas Gas service., by default None
         pipeline_operator_id: Optional[Union[list[str], Series[str], str]]
             ID associated with the common parent owner or operator of a pipeline system., by default None
         pipeline_operator_name: Optional[Union[list[str], Series[str], str]]
             The name of the common parent owner or operator of a pipeline system., by default None
         pipeline_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a pipeline system, utilizes legacy Bentek pipeline ids when applicable., by default None
         pipeline_name: Optional[Union[list[str], Series[str], str]]
             The display name of a pipeline system, utilizes legacy Bentek names when applicable., by default None
         point_name: Optional[Union[list[str], Series[str], str]]
             The display name of a meter or point, utilizes legacy Bentek point name when applicable., by default None
         meter_type_id_primary: Optional[Union[list[str], Series[str], str]]
             An ID for the primary type of classification and purpose of a meter or point, utilizes legacy PointLogic definitions and ID., by default None
         meter_type_primary: Optional[Union[list[str], Series[str], str]]
             The primary type of classification and purpose of a meter or point, utilizes legacy PointLogic definitions., by default None
         meter_type_id_secondary: Optional[Union[list[str], Series[str], str]]
             A secondary type classification and purpose of a meter or point, meant to provide an extra level of detail and utilizes legacy Bentek ids., by default None
         meter_type_secondary: Optional[Union[list[str], Series[str], str]]
             A secondary type classification and purpose of a meter or point, meant to provide an extra level of detail, utilizes legacy Bentek definitions., by default None
         location_type_code: Optional[Union[list[str], Series[str], str]]
             Location type code is a one letter abbreviation of the location description. Location types are sourced from legacy Bentek. These are similar to Flow Direction but serve as a secondary attribute., by default None
         location_description: Optional[Union[list[str], Series[str], str]]
             Location types are sourced from legacy Bentek. These are similar to Flow Direction but serve as a secondary attribute., by default None
         location_type_id: Optional[Union[list[str], Series[str], str]]
             An ID for the location type sourced from legacy Bentek. These are similar to Flow Direction but serve as a secondary attribute., by default None
         flow_direction: Optional[Union[list[str], Series[str], str]]
             Flow direction indicates the orientation of a point such as receipt, delivery, bi-directional or the reported flow direction of segment or compressor. Attribute is sourced from legacy PointLogic. Flow direction is primary to the similar and secondary attribute of Location Type., by default None
         common_code: Optional[Union[list[str], Series[str], str]]
             Common Code, when used, may provide additional information about the point as reported by the pipeline., by default None
         zone: Optional[Union[list[str], Series[str], str]]
             A designation for where on the pipeline system the point is located, corresponding to a pipeline’s operational and market design. Zonal information is sourced from the legacy Bentek service., by default None
         company_id: Optional[Union[list[str], Series[str], str]]
             An ID sourced from the legacy Bentek service used to identify the connecting business or company name of a meter., by default None
         connecting_party: Optional[Union[list[str], Series[str], str]]
             The downstream connecting business name of a meter as reported by the pipeline, utilizes legacy Bentek service., by default None
         loc_prop: Optional[Union[list[str], Series[str], str]]
             The location propriety code reported by the pipeline for a specific meter., by default None
         point_is_active: Optional[Union[list[bool], Series[bool], bool]]
             A true or false return if a point or meter is active and in use within the Americas Gas service., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion in which a meter or point resides., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         state_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the state or province utilizes legacy Bentek IDs., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The political boundaries that define a state or province within country., by default None
         county_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the county within a US state in which a meter or point resides, utilizes legacy Bentek IDs., by default None
         county: Optional[Union[list[str], Series[str], str]]
             The political boundaries of a defined county within a US state in which a meter or point resides., by default None
         producing_area_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for a defined Producing Area, utilizes legacy PointLogic IDs., by default None
         producing_area: Optional[Union[list[str], Series[str], str]]
             Defined aggregation of counties within a state that is a best fit representation of prominent oil and gas plays and basins., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         scheduled_volume: Optional[Union[list[str], Series[str], str]]
             Scheduled volume as reported by the pipeline for a specific meter, segment or compressor., by default None
         actual_volume: Optional[Union[list[str], Series[str], str]]
             Actual volume as reported by the pipeline for a specific meter, segment or compressor., by default None
         utilization: Optional[Union[list[str], Series[str], str]]
             Utilization rate in decimal form or in percentage terms. Utilization for a given meter is calculated by dividing the scheduled volume by its operational capacity., by default None
         design_capacity: Optional[Union[list[str], Series[str], str]]
             The volumetric max that a given meter, segment or compressor can receive or deliver as reported by the pipeline., by default None
         operational_capacity: Optional[Union[list[str], Series[str], str]]
             The pipeline’s reported daily operational capacity for a specific meter, segment or compressor can receive or deliver., by default None
         actual_capacity: Optional[Union[list[str], Series[str], str]]
             The actual capacity for a specific meter, segment or compressor can receive or deliver., by default None
         operationally_available: Optional[Union[list[str], Series[str], str]]
             The remaining volume available for a given meter as reported by the pipeline. The difference between operational capacity and scheduled volume., by default None
         interruptible_flow: Optional[Union[list[str], Series[str], str]]
             If available, the volume scheduled on an interruptible contract., by default None
         data_source: Optional[Union[list[str], Series[str], str]]
             Source of the data., by default None
         posting_date_time: Optional[datetime], optional
             Day and time the record was last posted., by default None
         posting_date_time_gt: Optional[datetime], optional
             filter by `posting_date_time > x`, by default None
         posting_date_time_gte: Optional[datetime], optional
             filter by `posting_date_time >= x`, by default None
         posting_date_time_lt: Optional[datetime], optional
             filter by `posting_date_time < x`, by default None
         posting_date_time_lte: Optional[datetime], optional
             filter by `posting_date_time <= x`, by default None
         create_date: Optional[datetime], optional
             The date and time stamp of when the record was created., by default None
         create_date_gt: Optional[datetime], optional
             filter by `create_date > x`, by default None
         create_date_gte: Optional[datetime], optional
             filter by `create_date >= x`, by default None
         create_date_lt: Optional[datetime], optional
             filter by `create_date < x`, by default None
         create_date_lte: Optional[datetime], optional
             filter by `create_date <= x`, by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("flowDate", flow_date))
        if flow_date_gt is not None:
            filter_params.append(f'flowDate > "{flow_date_gt}"')
        if flow_date_gte is not None:
            filter_params.append(f'flowDate >= "{flow_date_gte}"')
        if flow_date_lt is not None:
            filter_params.append(f'flowDate < "{flow_date_lt}"')
        if flow_date_lte is not None:
            filter_params.append(f'flowDate <= "{flow_date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(
            list_to_filter("legacyBentekPointId", legacy_bentek_point_id)
        )
        filter_params.append(list_to_filter("componentPointId", component_point_id))
        filter_params.append(list_to_filter("pipelineOperatorId", pipeline_operator_id))
        filter_params.append(
            list_to_filter("pipelineOperatorName", pipeline_operator_name)
        )
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("pipelineName", pipeline_name))
        filter_params.append(list_to_filter("pointName", point_name))
        filter_params.append(
            list_to_filter("meterTypeIdPrimary", meter_type_id_primary)
        )
        filter_params.append(list_to_filter("meterTypePrimary", meter_type_primary))
        filter_params.append(
            list_to_filter("meterTypeIdSecondary", meter_type_id_secondary)
        )
        filter_params.append(list_to_filter("meterTypeSecondary", meter_type_secondary))
        filter_params.append(list_to_filter("locationTypeCode", location_type_code))
        filter_params.append(
            list_to_filter("locationDescription", location_description)
        )
        filter_params.append(list_to_filter("locationTypeId", location_type_id))
        filter_params.append(list_to_filter("flowDirection", flow_direction))
        filter_params.append(list_to_filter("commonCode", common_code))
        filter_params.append(list_to_filter("zone", zone))
        filter_params.append(list_to_filter("companyId", company_id))
        filter_params.append(list_to_filter("connectingParty", connecting_party))
        filter_params.append(list_to_filter("locProp", loc_prop))
        filter_params.append(list_to_filter("pointIsActive", point_is_active))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("stateId", state_id))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("countyId", county_id))
        filter_params.append(list_to_filter("county", county))
        filter_params.append(list_to_filter("producingAreaId", producing_area_id))
        filter_params.append(list_to_filter("producingArea", producing_area))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("scheduledVolume", scheduled_volume))
        filter_params.append(list_to_filter("actualVolume", actual_volume))
        filter_params.append(list_to_filter("utilization", utilization))
        filter_params.append(list_to_filter("designCapacity", design_capacity))
        filter_params.append(
            list_to_filter("operationalCapacity", operational_capacity)
        )
        filter_params.append(list_to_filter("actualCapacity", actual_capacity))
        filter_params.append(
            list_to_filter("operationallyAvailable", operationally_available)
        )
        filter_params.append(list_to_filter("interruptibleFlow", interruptible_flow))
        filter_params.append(list_to_filter("dataSource", data_source))
        filter_params.append(list_to_filter("postingDateTime", posting_date_time))
        if posting_date_time_gt is not None:
            filter_params.append(f'postingDateTime > "{posting_date_time_gt}"')
        if posting_date_time_gte is not None:
            filter_params.append(f'postingDateTime >= "{posting_date_time_gte}"')
        if posting_date_time_lt is not None:
            filter_params.append(f'postingDateTime < "{posting_date_time_lt}"')
        if posting_date_time_lte is not None:
            filter_params.append(f'postingDateTime <= "{posting_date_time_lte}"')
        filter_params.append(list_to_filter("createDate", create_date))
        if create_date_gt is not None:
            filter_params.append(f'createDate > "{create_date_gt}"')
        if create_date_gte is not None:
            filter_params.append(f'createDate >= "{create_date_gte}"')
        if create_date_lt is not None:
            filter_params.append(f'createDate < "{create_date_lt}"')
        if create_date_lte is not None:
            filter_params.append(f'createDate <= "{create_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/no-notice-flow-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_gas_quality_data(
        self,
        *,
        measurement_date: datetime,
        measurement_date_lt: Optional[datetime] = None,
        measurement_date_lte: Optional[datetime] = None,
        measurement_date_gt: Optional[datetime] = None,
        measurement_date_gte: Optional[datetime] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        legacy_bentek_point_id: Optional[Union[list[str], Series[str], str]] = None,
        component_point_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_operator_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_operator_name: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_name: Optional[Union[list[str], Series[str], str]] = None,
        gas_composition_type_id: Optional[Union[list[str], Series[str], str]] = None,
        gas_composition_name: Optional[Union[list[str], Series[str], str]] = None,
        point_name: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_id: Optional[Union[list[str], Series[str], str]] = None,
        meter_type: Optional[Union[list[str], Series[str], str]] = None,
        common_code: Optional[Union[list[str], Series[str], str]] = None,
        zone: Optional[Union[list[str], Series[str], str]] = None,
        company_id: Optional[Union[list[str], Series[str], str]] = None,
        connecting_party: Optional[Union[list[str], Series[str], str]] = None,
        loc_prop: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        state_id: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        county_id: Optional[Union[list[str], Series[str], str]] = None,
        county: Optional[Union[list[str], Series[str], str]] = None,
        producing_area_id: Optional[Union[list[str], Series[str], str]] = None,
        producing_area: Optional[Union[list[str], Series[str], str]] = None,
        latitude: Optional[Union[list[float], Series[float], float]] = None,
        longitude: Optional[Union[list[float], Series[float], float]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        heating_value: Optional[Union[list[str], Series[str], str]] = None,
        specific_gravity: Optional[Union[list[str], Series[str], str]] = None,
        wobbe: Optional[Union[list[str], Series[str], str]] = None,
        create_date: Optional[datetime] = None,
        create_date_lt: Optional[datetime] = None,
        create_date_lte: Optional[datetime] = None,
        create_date_gt: Optional[datetime] = None,
        create_date_gte: Optional[datetime] = None,
        data_active: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Pipeline reported gas quality data consists of selected measurement values related to the composition of the natural gas stream at a given point and time.

        Parameters
        ----------

         measurement_date: datetime
             Measurement date is when the analysis occurred and is equal to the gas day or flow date.
         measurement_date_gt: Optional[datetime], optional
             filter by `measurement_date > x`, by default None
         measurement_date_gte: Optional[datetime], optional
             filter by `measurement_date >= x`, by default None
         measurement_date_lt: Optional[datetime], optional
             filter by `measurement_date < x`, by default None
         measurement_date_lte: Optional[datetime], optional
             filter by `measurement_date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Daily, Weekly, Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Daily, Weekly, Monthly, Seasonal, Annual. Weekly date frequencies are based on the defined EIA storage week of Friday-Thursday. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         legacy_bentek_point_id: Optional[Union[list[str], Series[str], str]]
             Point ID for a meter used by the legacy Bentek service., by default None
         component_point_id: Optional[Union[list[str], Series[str], str]]
             Point ID for a meter used by the Americas Gas service., by default None
         pipeline_operator_id: Optional[Union[list[str], Series[str], str]]
             ID associated with the common parent owner or operator of a pipeline system., by default None
         pipeline_operator_name: Optional[Union[list[str], Series[str], str]]
             The name of the common parent owner or operator of a pipeline system., by default None
         pipeline_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a pipeline system, utilizes legacy Bentek pipeline ids when applicable., by default None
         pipeline_name: Optional[Union[list[str], Series[str], str]]
             The display name of a pipeline system, utilizes legacy Bentek names when applicable., by default None
         gas_composition_type_id: Optional[Union[list[str], Series[str], str]]
             Unique ID for the reported gas composition name., by default None
         gas_composition_name: Optional[Union[list[str], Series[str], str]]
             Reported gases within the gas stream such as methane, ethane, propane, butane, pentanes and others., by default None
         point_name: Optional[Union[list[str], Series[str], str]]
             The display name of a meter or point, utilizes legacy Bentek point name when applicable., by default None
         meter_type_id: Optional[Union[list[str], Series[str], str]]
             The classification and purpose of a meter or point, meant to provide an extra level of detail and utilizes legacy Bentek ids., by default None
         meter_type: Optional[Union[list[str], Series[str], str]]
             The classification and purpose of a meter or point, meant to provide an extra level of detail, utilizes legacy Bentek definitions., by default None
         common_code: Optional[Union[list[str], Series[str], str]]
             Common code, when used, may provide additional information about the point as reported by the pipeline., by default None
         zone: Optional[Union[list[str], Series[str], str]]
             A designation for where on the pipeline system the point is located, corresponding to a pipeline’s operational and market design. Zonal information is sourced from the legacy Bentek service., by default None
         company_id: Optional[Union[list[str], Series[str], str]]
             An ID sourced from the legacy Bentek service used to identify the connecting business or company name of a meter., by default None
         connecting_party: Optional[Union[list[str], Series[str], str]]
             The downstream connecting business name of a meter as reported by the pipeline, utilizes legacy Bentek service., by default None
         loc_prop: Optional[Union[list[str], Series[str], str]]
             The location propriety code reported by the pipeline for a specific meter., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         state_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the state or province utilizes legacy Bentek IDs., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The political boundaries that define a state or province within country., by default None
         county_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the county within a US state in which a meter or point resides, utilizes legacy Bentek IDs., by default None
         county: Optional[Union[list[str], Series[str], str]]
             The political boundaries of a defined county within a US state in which a meter or point resides., by default None
         producing_area_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for a defined Producing Area, utilizes legacy PointLogic IDs., by default None
         producing_area: Optional[Union[list[str], Series[str], str]]
             Defined aggregation of counties within a state that is a best fit representation of prominent oil and gas plays and basins., by default None
         latitude: Optional[Union[list[float], Series[float], float]]
             A geography based coordinate that specifies the north–south position of a point on the surface of the Earth. Latitude and longitude are used to identify the precise location of a pipeline meter or point., by default None
         longitude: Optional[Union[list[float], Series[float], float]]
             A geography based coordinate that specifies the east–west position of a point on the surface of the Earth. Latitude and longitude are used to identify the precise location of a pipeline meter or point., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         value: Optional[Union[list[str], Series[str], str]]
             Reported pipeline value., by default None
         heating_value: Optional[Union[list[str], Series[str], str]]
             A measure of the heat content of the gas stream, expressed in Btu per cubic foot., by default None
         specific_gravity: Optional[Union[list[str], Series[str], str]]
             A measure of the density of a substance in comparison to the density of water., by default None
         wobbe: Optional[Union[list[str], Series[str], str]]
             A measure of the interchangeability of gases when they are used as a fuel., by default None
         create_date: Optional[datetime], optional
             The date and time stamp of when the record was created., by default None
         create_date_gt: Optional[datetime], optional
             filter by `create_date > x`, by default None
         create_date_gte: Optional[datetime], optional
             filter by `create_date >= x`, by default None
         create_date_lt: Optional[datetime], optional
             filter by `create_date < x`, by default None
         create_date_lte: Optional[datetime], optional
             filter by `create_date <= x`, by default None
         data_active: Optional[Union[list[str], Series[str], str]]
             A true or false return if the record is active., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("measurementDate", measurement_date))
        if measurement_date_gt is not None:
            filter_params.append(f'measurementDate > "{measurement_date_gt}"')
        if measurement_date_gte is not None:
            filter_params.append(f'measurementDate >= "{measurement_date_gte}"')
        if measurement_date_lt is not None:
            filter_params.append(f'measurementDate < "{measurement_date_lt}"')
        if measurement_date_lte is not None:
            filter_params.append(f'measurementDate <= "{measurement_date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(
            list_to_filter("legacyBentekPointId", legacy_bentek_point_id)
        )
        filter_params.append(list_to_filter("componentPointId", component_point_id))
        filter_params.append(list_to_filter("pipelineOperatorId", pipeline_operator_id))
        filter_params.append(
            list_to_filter("pipelineOperatorName", pipeline_operator_name)
        )
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("pipelineName", pipeline_name))
        filter_params.append(
            list_to_filter("gasCompositionTypeId", gas_composition_type_id)
        )
        filter_params.append(list_to_filter("gasCompositionName", gas_composition_name))
        filter_params.append(list_to_filter("pointName", point_name))
        filter_params.append(list_to_filter("meterTypeId", meter_type_id))
        filter_params.append(list_to_filter("meterType", meter_type))
        filter_params.append(list_to_filter("commonCode", common_code))
        filter_params.append(list_to_filter("zone", zone))
        filter_params.append(list_to_filter("companyId", company_id))
        filter_params.append(list_to_filter("connectingParty", connecting_party))
        filter_params.append(list_to_filter("locProp", loc_prop))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("stateId", state_id))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("countyId", county_id))
        filter_params.append(list_to_filter("county", county))
        filter_params.append(list_to_filter("producingAreaId", producing_area_id))
        filter_params.append(list_to_filter("producingArea", producing_area))
        filter_params.append(list_to_filter("latitude", latitude))
        filter_params.append(list_to_filter("longitude", longitude))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("value", value))
        filter_params.append(list_to_filter("heatingValue", heating_value))
        filter_params.append(list_to_filter("specificGravity", specific_gravity))
        filter_params.append(list_to_filter("wobbe", wobbe))
        filter_params.append(list_to_filter("createDate", create_date))
        if create_date_gt is not None:
            filter_params.append(f'createDate > "{create_date_gt}"')
        if create_date_gte is not None:
            filter_params.append(f'createDate >= "{create_date_gte}"')
        if create_date_lt is not None:
            filter_params.append(f'createDate < "{create_date_lt}"')
        if create_date_lte is not None:
            filter_params.append(f'createDate <= "{create_date_lte}"')
        filter_params.append(list_to_filter("dataActive", data_active))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/gas-quality-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_notices_data(
        self,
        *,
        posting_date_time: Optional[datetime] = None,
        posting_date_time_lt: Optional[datetime] = None,
        posting_date_time_lte: Optional[datetime] = None,
        posting_date_time_gt: Optional[datetime] = None,
        posting_date_time_gte: Optional[datetime] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        effective_date: Optional[datetime] = None,
        effective_date_lt: Optional[datetime] = None,
        effective_date_lte: Optional[datetime] = None,
        effective_date_gt: Optional[datetime] = None,
        effective_date_gte: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        end_date_lt: Optional[datetime] = None,
        end_date_lte: Optional[datetime] = None,
        end_date_gt: Optional[datetime] = None,
        end_date_gte: Optional[datetime] = None,
        pipeline_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_short_name: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_name: Optional[Union[list[str], Series[str], str]] = None,
        notice_category_id: Optional[Union[list[str], Series[str], str]] = None,
        category_code: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
        notice_type_id: Optional[Union[list[str], Series[str], str]] = None,
        notice_type: Optional[Union[list[str], Series[str], str]] = None,
        notice_status_id: Optional[Union[list[str], Series[str], str]] = None,
        notice_status: Optional[Union[list[str], Series[str], str]] = None,
        response_code_id: Optional[Union[list[str], Series[str], str]] = None,
        response: Optional[Union[list[str], Series[str], str]] = None,
        notice_id: Optional[Union[list[str], Series[str], str]] = None,
        prior_notice_identifier: Optional[Union[list[str], Series[str], str]] = None,
        subject: Optional[Union[list[str], Series[str], str]] = None,
        comments: Optional[Union[list[str], Series[str], str]] = None,
        content: Optional[Union[list[str], Series[str], str]] = None,
        create_date: Optional[datetime] = None,
        create_date_lt: Optional[datetime] = None,
        create_date_lte: Optional[datetime] = None,
        create_date_gt: Optional[datetime] = None,
        create_date_gte: Optional[datetime] = None,
        valid_from: Optional[datetime] = None,
        valid_from_lt: Optional[datetime] = None,
        valid_from_lte: Optional[datetime] = None,
        valid_from_gt: Optional[datetime] = None,
        valid_from_gte: Optional[datetime] = None,
        valid_to: Optional[datetime] = None,
        valid_to_lt: Optional[datetime] = None,
        valid_to_lte: Optional[datetime] = None,
        valid_to_gt: Optional[datetime] = None,
        valid_to_gte: Optional[datetime] = None,
        data_active: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Natural gas pipeline reported notices pertaining to the daily operating conditions and other communications of their pipeline system.

        Parameters
        ----------

         posting_date_time: datetime
             Day and time the record was last posted.
         posting_date_time_gt: Optional[datetime], optional
             filter by `posting_date_time > x`, by default None
         posting_date_time_gte: Optional[datetime], optional
             filter by `posting_date_time >= x`, by default None
         posting_date_time_lt: Optional[datetime], optional
             filter by `posting_date_time < x`, by default None
         posting_date_time_lte: Optional[datetime], optional
             filter by `posting_date_time <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         effective_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         effective_date_gt: Optional[datetime], optional
             filter by `effective_date > x`, by default None
         effective_date_gte: Optional[datetime], optional
             filter by `effective_date >= x`, by default None
         effective_date_lt: Optional[datetime], optional
             filter by `effective_date < x`, by default None
         effective_date_lte: Optional[datetime], optional
             filter by `effective_date <= x`, by default None
         end_date: Optional[datetime], optional
             Date and time the record was ended., by default None
         end_date_gt: Optional[datetime], optional
             filter by `end_date > x`, by default None
         end_date_gte: Optional[datetime], optional
             filter by `end_date >= x`, by default None
         end_date_lt: Optional[datetime], optional
             filter by `end_date < x`, by default None
         end_date_lte: Optional[datetime], optional
             filter by `end_date <= x`, by default None
         pipeline_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a pipeline system, utilizes legacy Bentek pipeline ids when applicable., by default None
         pipeline_short_name: Optional[Union[list[str], Series[str], str]]
             The short name of a pipeline system, utilizes legacy Bentek names when applicable., by default None
         pipeline_name: Optional[Union[list[str], Series[str], str]]
             The display name of a pipeline system, utilizes legacy Bentek names when applicable., by default None
         notice_category_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier number assigned to different categories of the notice., by default None
         category_code: Optional[Union[list[str], Series[str], str]]
             A short code or abbreviation for each category., by default None
         category: Optional[Union[list[str], Series[str], str]]
             The nature or significance of the pipeline category., by default None
         notice_type_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier number assigned to different types of the notice., by default None
         notice_type: Optional[Union[list[str], Series[str], str]]
             The type of the notice., by default None
         notice_status_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier number assigned to different statuses of the notice., by default None
         notice_status: Optional[Union[list[str], Series[str], str]]
             Current status of the notice., by default None
         response_code_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier number assigned to different types of responses required for the notice., by default None
         response: Optional[Union[list[str], Series[str], str]]
             The urgency and timeline associated with each response code., by default None
         notice_id: Optional[Union[list[str], Series[str], str]]
             Notice identifier of the notice., by default None
         prior_notice_identifier: Optional[Union[list[str], Series[str], str]]
             Prior notice identifier of the notice., by default None
         subject: Optional[Union[list[str], Series[str], str]]
             The subject of the pipeline notice., by default None
         comments: Optional[Union[list[str], Series[str], str]]
             Comments of the pipeline notice., by default None
         content: Optional[Union[list[str], Series[str], str]]
             The content of the pipeline notice., by default None
         create_date: Optional[datetime], optional
             The date and time stamp of when the record was created., by default None
         create_date_gt: Optional[datetime], optional
             filter by `create_date > x`, by default None
         create_date_gte: Optional[datetime], optional
             filter by `create_date >= x`, by default None
         create_date_lt: Optional[datetime], optional
             filter by `create_date < x`, by default None
         create_date_lte: Optional[datetime], optional
             filter by `create_date <= x`, by default None
         valid_from: Optional[datetime], optional
             Valid from Date, by default None
         valid_from_gt: Optional[datetime], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[datetime], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[datetime], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[datetime], optional
             filter by `valid_from <= x`, by default None
         valid_to: Optional[datetime], optional
             The date and time of when the record was valid to., by default None
         valid_to_gt: Optional[datetime], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[datetime], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[datetime], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[datetime], optional
             filter by `valid_to <= x`, by default None
         data_active: Optional[Union[list[str], Series[str], str]]
             A true or false return if the record is active., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("postingDateTime", posting_date_time))
        if posting_date_time_gt is not None:
            filter_params.append(f'postingDateTime > "{posting_date_time_gt}"')
        if posting_date_time_gte is not None:
            filter_params.append(f'postingDateTime >= "{posting_date_time_gte}"')
        if posting_date_time_lt is not None:
            filter_params.append(f'postingDateTime < "{posting_date_time_lt}"')
        if posting_date_time_lte is not None:
            filter_params.append(f'postingDateTime <= "{posting_date_time_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("effectiveDate", effective_date))
        if effective_date_gt is not None:
            filter_params.append(f'effectiveDate > "{effective_date_gt}"')
        if effective_date_gte is not None:
            filter_params.append(f'effectiveDate >= "{effective_date_gte}"')
        if effective_date_lt is not None:
            filter_params.append(f'effectiveDate < "{effective_date_lt}"')
        if effective_date_lte is not None:
            filter_params.append(f'effectiveDate <= "{effective_date_lte}"')
        filter_params.append(list_to_filter("endDate", end_date))
        if end_date_gt is not None:
            filter_params.append(f'endDate > "{end_date_gt}"')
        if end_date_gte is not None:
            filter_params.append(f'endDate >= "{end_date_gte}"')
        if end_date_lt is not None:
            filter_params.append(f'endDate < "{end_date_lt}"')
        if end_date_lte is not None:
            filter_params.append(f'endDate <= "{end_date_lte}"')
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("pipelineShortName", pipeline_short_name))
        filter_params.append(list_to_filter("pipelineName", pipeline_name))
        filter_params.append(list_to_filter("noticeCategoryId", notice_category_id))
        filter_params.append(list_to_filter("categoryCode", category_code))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("noticeTypeId", notice_type_id))
        filter_params.append(list_to_filter("noticeType", notice_type))
        filter_params.append(list_to_filter("noticeStatusId", notice_status_id))
        filter_params.append(list_to_filter("noticeStatus", notice_status))
        filter_params.append(list_to_filter("responseCodeId", response_code_id))
        filter_params.append(list_to_filter("response", response))
        filter_params.append(list_to_filter("noticeId", notice_id))
        filter_params.append(
            list_to_filter("priorNoticeIdentifier", prior_notice_identifier)
        )
        filter_params.append(list_to_filter("subject", subject))
        filter_params.append(list_to_filter("comments", comments))
        filter_params.append(list_to_filter("content", content))
        filter_params.append(list_to_filter("createDate", create_date))
        if create_date_gt is not None:
            filter_params.append(f'createDate > "{create_date_gt}"')
        if create_date_gte is not None:
            filter_params.append(f'createDate >= "{create_date_gte}"')
        if create_date_lt is not None:
            filter_params.append(f'createDate < "{create_date_lt}"')
        if create_date_lte is not None:
            filter_params.append(f'createDate <= "{create_date_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("dataActive", data_active))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/notices-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_tariff_rate_data(
        self,
        *,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        pipeline_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_name: Optional[Union[list[str], Series[str], str]] = None,
        service_type_id: Optional[Union[list[str], Series[str], str]] = None,
        rate_name: Optional[Union[list[str], Series[str], str]] = None,
        rate_item_name: Optional[Union[list[str], Series[str], str]] = None,
        rate_description: Optional[Union[list[str], Series[str], str]] = None,
        rate_type_id: Optional[Union[list[str], Series[str], str]] = None,
        rate_amount: Optional[Union[list[str], Series[str], str]] = None,
        rate_unit_id: Optional[Union[list[str], Series[str], str]] = None,
        rate_unit: Optional[Union[list[str], Series[str], str]] = None,
        rate_frequency_id: Optional[Union[list[str], Series[str], str]] = None,
        rate_frequency: Optional[Union[list[str], Series[str], str]] = None,
        season_id: Optional[Union[list[str], Series[str], str]] = None,
        season: Optional[Union[list[str], Series[str], str]] = None,
        gas_unit_id: Optional[Union[list[str], Series[str], str]] = None,
        gas_unit_name: Optional[Union[list[str], Series[str], str]] = None,
        zone_id_receipt: Optional[Union[list[str], Series[str], str]] = None,
        receipt_zone: Optional[Union[list[str], Series[str], str]] = None,
        zone_id_delivery: Optional[Union[list[str], Series[str], str]] = None,
        delivery_zone: Optional[Union[list[str], Series[str], str]] = None,
        date_effective: Optional[date] = None,
        date_effective_lt: Optional[date] = None,
        date_effective_lte: Optional[date] = None,
        date_effective_gt: Optional[date] = None,
        date_effective_gte: Optional[date] = None,
        date_superceded: Optional[datetime] = None,
        date_superceded_lt: Optional[datetime] = None,
        date_superceded_lte: Optional[datetime] = None,
        date_superceded_gt: Optional[datetime] = None,
        date_superceded_gte: Optional[datetime] = None,
        date_issued: Optional[datetime] = None,
        date_issued_lt: Optional[datetime] = None,
        date_issued_lte: Optional[datetime] = None,
        date_issued_gt: Optional[datetime] = None,
        date_issued_gte: Optional[datetime] = None,
        valid_from: Optional[datetime] = None,
        valid_from_lt: Optional[datetime] = None,
        valid_from_lte: Optional[datetime] = None,
        valid_from_gt: Optional[datetime] = None,
        valid_from_gte: Optional[datetime] = None,
        valid_to: Optional[datetime] = None,
        valid_to_lt: Optional[datetime] = None,
        valid_to_lte: Optional[datetime] = None,
        valid_to_gt: Optional[datetime] = None,
        valid_to_gte: Optional[datetime] = None,
        data_active: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Natural gas pipeline tariff rates contains the posted pipeline's applicable charges for transportation service on their system from source to destination.

        Parameters
        ----------

         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         pipeline_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a pipeline system, utilizes legacy Bentek pipeline ids when applicable., by default None
         pipeline_name: Optional[Union[list[str], Series[str], str]]
             The display name of a pipeline system, utilizes legacy Bentek names when applicable., by default None
         service_type_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the service type., by default None
         rate_name: Optional[Union[list[str], Series[str], str]]
             The specific name or identifier assigned to a particular rate or charge in the pipeline tariff., by default None
         rate_item_name: Optional[Union[list[str], Series[str], str]]
             The specific name or description of the rate or charge being applied., by default None
         rate_description: Optional[Union[list[str], Series[str], str]]
             The category or classification of the rate being applied., by default None
         rate_type_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the rate type., by default None
         rate_amount: Optional[Union[list[str], Series[str], str]]
             The numerical value or percentage associated with the rate or charge., by default None
         rate_unit_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the rate unit., by default None
         rate_unit: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used to quantify the monetary value or percentage associated with the rate or charge., by default None
         rate_frequency_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the rate frequency., by default None
         rate_frequency: Optional[Union[list[str], Series[str], str]]
             The frequency at which the rate or charge is applied or calculated., by default None
         season_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the season., by default None
         season: Optional[Union[list[str], Series[str], str]]
             Season per gas industry standards (April - October in the same year = summer and November - March bridge the calendar years = winter)., by default None
         gas_unit_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the gas unit., by default None
         gas_unit_name: Optional[Union[list[str], Series[str], str]]
             The unit of measurement used to quantify the volume or quantity of gas being transported or supplied., by default None
         zone_id_receipt: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the receipt zone., by default None
         receipt_zone: Optional[Union[list[str], Series[str], str]]
             A designated area where the gas is received or collected from., by default None
         zone_id_delivery: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the delivery zone., by default None
         delivery_zone: Optional[Union[list[str], Series[str], str]]
             A designated area where the gas is delivered or transported to., by default None
         date_effective: Optional[date], optional
             The date when the rate becomes effective., by default None
         date_effective_gt: Optional[date], optional
             filter by `date_effective > x`, by default None
         date_effective_gte: Optional[date], optional
             filter by `date_effective >= x`, by default None
         date_effective_lt: Optional[date], optional
             filter by `date_effective < x`, by default None
         date_effective_lte: Optional[date], optional
             filter by `date_effective <= x`, by default None
         date_superceded: Optional[datetime], optional
             The date when the rate is superceded by a new effective rate., by default None
         date_superceded_gt: Optional[datetime], optional
             filter by `date_superceded > x`, by default None
         date_superceded_gte: Optional[datetime], optional
             filter by `date_superceded >= x`, by default None
         date_superceded_lt: Optional[datetime], optional
             filter by `date_superceded < x`, by default None
         date_superceded_lte: Optional[datetime], optional
             filter by `date_superceded <= x`, by default None
         date_issued: Optional[datetime], optional
             The date when the rate was issued., by default None
         date_issued_gt: Optional[datetime], optional
             filter by `date_issued > x`, by default None
         date_issued_gte: Optional[datetime], optional
             filter by `date_issued >= x`, by default None
         date_issued_lt: Optional[datetime], optional
             filter by `date_issued < x`, by default None
         date_issued_lte: Optional[datetime], optional
             filter by `date_issued <= x`, by default None
         valid_from: Optional[datetime], optional
             The date and time stamp of when the record was valid from., by default None
         valid_from_gt: Optional[datetime], optional
             filter by `valid_from > x`, by default None
         valid_from_gte: Optional[datetime], optional
             filter by `valid_from >= x`, by default None
         valid_from_lt: Optional[datetime], optional
             filter by `valid_from < x`, by default None
         valid_from_lte: Optional[datetime], optional
             filter by `valid_from <= x`, by default None
         valid_to: Optional[datetime], optional
             The date and time stamp of when the record was valid to., by default None
         valid_to_gt: Optional[datetime], optional
             filter by `valid_to > x`, by default None
         valid_to_gte: Optional[datetime], optional
             filter by `valid_to >= x`, by default None
         valid_to_lt: Optional[datetime], optional
             filter by `valid_to < x`, by default None
         valid_to_lte: Optional[datetime], optional
             filter by `valid_to <= x`, by default None
         data_active: Optional[Union[list[str], Series[str], str]]
             A true or false return if the record is active., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("pipelineName", pipeline_name))
        filter_params.append(list_to_filter("serviceTypeId", service_type_id))
        filter_params.append(list_to_filter("rateName", rate_name))
        filter_params.append(list_to_filter("rateItemName", rate_item_name))
        filter_params.append(list_to_filter("rateDescription", rate_description))
        filter_params.append(list_to_filter("rateTypeId", rate_type_id))
        filter_params.append(list_to_filter("rateAmount", rate_amount))
        filter_params.append(list_to_filter("rateUnitId", rate_unit_id))
        filter_params.append(list_to_filter("rateUnit", rate_unit))
        filter_params.append(list_to_filter("rateFrequencyId", rate_frequency_id))
        filter_params.append(list_to_filter("rateFrequency", rate_frequency))
        filter_params.append(list_to_filter("seasonId", season_id))
        filter_params.append(list_to_filter("season", season))
        filter_params.append(list_to_filter("gasUnitId", gas_unit_id))
        filter_params.append(list_to_filter("gasUnitName", gas_unit_name))
        filter_params.append(list_to_filter("zoneIdReceipt", zone_id_receipt))
        filter_params.append(list_to_filter("receiptZone", receipt_zone))
        filter_params.append(list_to_filter("zoneIdDelivery", zone_id_delivery))
        filter_params.append(list_to_filter("deliveryZone", delivery_zone))
        filter_params.append(list_to_filter("dateEffective", date_effective))
        if date_effective_gt is not None:
            filter_params.append(f'dateEffective > "{date_effective_gt}"')
        if date_effective_gte is not None:
            filter_params.append(f'dateEffective >= "{date_effective_gte}"')
        if date_effective_lt is not None:
            filter_params.append(f'dateEffective < "{date_effective_lt}"')
        if date_effective_lte is not None:
            filter_params.append(f'dateEffective <= "{date_effective_lte}"')
        filter_params.append(list_to_filter("dateSuperceded", date_superceded))
        if date_superceded_gt is not None:
            filter_params.append(f'dateSuperceded > "{date_superceded_gt}"')
        if date_superceded_gte is not None:
            filter_params.append(f'dateSuperceded >= "{date_superceded_gte}"')
        if date_superceded_lt is not None:
            filter_params.append(f'dateSuperceded < "{date_superceded_lt}"')
        if date_superceded_lte is not None:
            filter_params.append(f'dateSuperceded <= "{date_superceded_lte}"')
        filter_params.append(list_to_filter("dateIssued", date_issued))
        if date_issued_gt is not None:
            filter_params.append(f'dateIssued > "{date_issued_gt}"')
        if date_issued_gte is not None:
            filter_params.append(f'dateIssued >= "{date_issued_gte}"')
        if date_issued_lt is not None:
            filter_params.append(f'dateIssued < "{date_issued_lt}"')
        if date_issued_lte is not None:
            filter_params.append(f'dateIssued <= "{date_issued_lte}"')
        filter_params.append(list_to_filter("validFrom", valid_from))
        if valid_from_gt is not None:
            filter_params.append(f'validFrom > "{valid_from_gt}"')
        if valid_from_gte is not None:
            filter_params.append(f'validFrom >= "{valid_from_gte}"')
        if valid_from_lt is not None:
            filter_params.append(f'validFrom < "{valid_from_lt}"')
        if valid_from_lte is not None:
            filter_params.append(f'validFrom <= "{valid_from_lte}"')
        filter_params.append(list_to_filter("validTo", valid_to))
        if valid_to_gt is not None:
            filter_params.append(f'validTo > "{valid_to_gt}"')
        if valid_to_gte is not None:
            filter_params.append(f'validTo >= "{valid_to_gte}"')
        if valid_to_lt is not None:
            filter_params.append(f'validTo < "{valid_to_lt}"')
        if valid_to_lte is not None:
            filter_params.append(f'validTo <= "{valid_to_lte}"')
        filter_params.append(list_to_filter("dataActive", data_active))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/tariff-rate-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_index_of_customer_data(
        self,
        *,
        year: Optional[Union[list[str], Series[str], str]] = None,
        quarter: Optional[Union[list[str], Series[str], str]] = None,
        ioc_key: Optional[Union[list[str], Series[str], str]] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        pipeline_filer_name: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_name: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_id: Optional[Union[list[str], Series[str], str]] = None,
        shipper_name: Optional[Union[list[str], Series[str], str]] = None,
        point_name: Optional[Union[list[str], Series[str], str]] = None,
        legacy_point_logic_point_id: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        legacy_point_logic_lci_id: Optional[Union[list[str], Series[str], str]] = None,
        component_point_id: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_primary: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_i_d_primary: Optional[Union[list[str], Series[str], str]] = None,
        dir: Optional[Union[list[str], Series[str], str]] = None,
        loc_prop: Optional[Union[list[str], Series[str], str]] = None,
        zone: Optional[Union[list[str], Series[str], str]] = None,
        county: Optional[Union[list[str], Series[str], str]] = None,
        county_id: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        state_id: Optional[Union[list[str], Series[str], str]] = None,
        sub_region: Optional[Union[list[str], Series[str], str]] = None,
        sub_region_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        max_trans: Optional[Union[list[float], Series[float], float]] = None,
        max_storage: Optional[Union[list[float], Series[float], float]] = None,
        point_max_trans: Optional[Union[list[float], Series[float], float]] = None,
        point_max_stor: Optional[Union[list[float], Series[float], float]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        agent: Optional[Union[list[str], Series[str], str]] = None,
        rate_schedule: Optional[Union[list[str], Series[str], str]] = None,
        contract_number: Optional[Union[list[str], Series[str], str]] = None,
        contract_start_date: Optional[datetime] = None,
        contract_start_date_lt: Optional[datetime] = None,
        contract_start_date_lte: Optional[datetime] = None,
        contract_start_date_gt: Optional[datetime] = None,
        contract_start_date_gte: Optional[datetime] = None,
        contract_end_date: Optional[datetime] = None,
        contract_end_date_lt: Optional[datetime] = None,
        contract_end_date_lte: Optional[datetime] = None,
        contract_end_date_gt: Optional[datetime] = None,
        contract_end_date_gte: Optional[datetime] = None,
        contract_months: Optional[Union[list[float], Series[float], float]] = None,
        roll_over_period: Optional[Union[list[float], Series[float], float]] = None,
        negotiated_rate: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Quarterly interstate pipeline data that represents an index of all of its firm transportation and storage customers under contract as of the first day of the calendar quarter. Contractual data is sourced from FERC form 549B.

        Parameters
        ----------

         year: Optional[Union[list[str], Series[str], str]]
             The year of when the record was reported., by default None
         quarter: Optional[Union[list[str], Series[str], str]]
             The quarter of when the record was reported., by default None
         ioc_key: Optional[Union[list[str], Series[str], str]]
             IOCKEY represents a unique Contract, Shipper, or Point combination., by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         pipeline_filer_name: Optional[Union[list[str], Series[str], str]]
             The filer name of a pipeline system., by default None
         pipeline_name: Optional[Union[list[str], Series[str], str]]
             The display name of a pipeline system, utilizes legacy Bentek names when applicable., by default None
         pipeline_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a pipeline system, utilizes legacy Bentek pipeline ids when applicable., by default None
         shipper_name: Optional[Union[list[str], Series[str], str]]
             The name of the company responsible for transporting natural gas from the production or supply source to the end consumer., by default None
         point_name: Optional[Union[list[str], Series[str], str]]
             The display name of a meter or point, utilizes legacy Bentek point name when applicable., by default None
         legacy_point_logic_point_id: Optional[Union[list[str], Series[str], str]]
             Point ID for a meter used by the legacy PointLogic service., by default None
         legacy_point_logic_lci_id: Optional[Union[list[str], Series[str], str]]
             Alternative point ID for a meter used by the legacy PointLogic service., by default None
         component_point_id: Optional[Union[list[str], Series[str], str]]
             Point ID for a meter used by the Americas Gas service., by default None
         meter_type_primary: Optional[Union[list[str], Series[str], str]]
             The primary type of classification and purpose of a meter or point, utilizes legacy PointLogic definitions., by default None
         meter_type_i_d_primary: Optional[Union[list[str], Series[str], str]]
             An ID for the primary type of classification and purpose of a meter or point, utilizes legacy PointLogic definitions and ID., by default None
         dir: Optional[Union[list[str], Series[str], str]]
             Direction indicates the orientation of a point such as receipt, delivery, bi-directional or the reported flow direction of segment or compressor. These codes are from the NAESB Business Practice Standards Manual relating to the Capacity Release - Firm Transportation and Storage - Award Notice (Award Download), N1 Record. M2 - Receipt Point MQ - Delivery Point MV - Mainline S8 - Pipeline Segment defined by 2 Point records (second of 2 Point records) S9 - Pipeline Segment defined by 1 Point record (or first of 2 Point records) SB - Storage Area IJ - Injection Point WR - Withdrawal Point., by default None
         loc_prop: Optional[Union[list[str], Series[str], str]]
             The location propriety code reported by the pipeline for a specific meter., by default None
         zone: Optional[Union[list[str], Series[str], str]]
             A designation for where on the pipeline system the point is located, corresponding to a pipeline’s operational and market design. Zonal information is sourced from the legacy Bentek service., by default None
         county: Optional[Union[list[str], Series[str], str]]
             The political boundaries of a defined county within a US state in which a meter or point resides., by default None
         county_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the county within a US state in which a meter or point resides, utilizes legacy Bentek IDs., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The political boundaries that define a state or province within country., by default None
         state_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the state or province utilizes legacy Bentek IDs., by default None
         sub_region: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         sub_region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         max_trans: Optional[Union[list[float], Series[float], float]]
             Maximum quantity of natural gas that can be transported per day., by default None
         max_storage: Optional[Union[list[float], Series[float], float]]
             Maximum quantity or the largest quantity of natural gas the pipeline is obligated to store for the Shipper under the contract., by default None
         point_max_trans: Optional[Union[list[float], Series[float], float]]
             The maximum amount of natural gas that can be transported to a specific point per day., by default None
         point_max_stor: Optional[Union[list[float], Series[float], float]]
             The maximum amount of natural gas that can be stored at a specific point., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         agent: Optional[Union[list[str], Series[str], str]]
             The company or entity responsible for the natural gas trading.  Agent or Asset Manager., by default None
         rate_schedule: Optional[Union[list[str], Series[str], str]]
             The specific rate schedule associated with the contract.  The rate is the as reported rate on the FERC Form 2 or 2A., by default None
         contract_number: Optional[Union[list[str], Series[str], str]]
             The unique identifier for the contract., by default None
         contract_start_date: Optional[datetime], optional
             Start date of the contract., by default None
         contract_start_date_gt: Optional[datetime], optional
             filter by `contract_start_date > x`, by default None
         contract_start_date_gte: Optional[datetime], optional
             filter by `contract_start_date >= x`, by default None
         contract_start_date_lt: Optional[datetime], optional
             filter by `contract_start_date < x`, by default None
         contract_start_date_lte: Optional[datetime], optional
             filter by `contract_start_date <= x`, by default None
         contract_end_date: Optional[datetime], optional
             End date of the contract., by default None
         contract_end_date_gt: Optional[datetime], optional
             filter by `contract_end_date > x`, by default None
         contract_end_date_gte: Optional[datetime], optional
             filter by `contract_end_date >= x`, by default None
         contract_end_date_lt: Optional[datetime], optional
             filter by `contract_end_date < x`, by default None
         contract_end_date_lte: Optional[datetime], optional
             filter by `contract_end_date <= x`, by default None
         contract_months: Optional[Union[list[float], Series[float], float]]
             Total number of months for the contract., by default None
         roll_over_period: Optional[Union[list[float], Series[float], float]]
             The number of days in the roll-over period after which the contract can be renewed or extended. Also known as evergreen period. If the contract continues on a monthly basis then 31 should be entered; if annual enter 365; if unknown leave blank., by default None
         negotiated_rate: Optional[Union[list[str], Series[str], str]]
             Indicates whether the rate was negotiated (Yes/No)., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("quarter", quarter))
        filter_params.append(list_to_filter("iocKey", ioc_key))
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("pipelineFilerName", pipeline_filer_name))
        filter_params.append(list_to_filter("pipelineName", pipeline_name))
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("shipperName", shipper_name))
        filter_params.append(list_to_filter("pointName", point_name))
        filter_params.append(
            list_to_filter("legacyPointLogicPointId", legacy_point_logic_point_id)
        )
        filter_params.append(
            list_to_filter("legacyPointLogicLciId", legacy_point_logic_lci_id)
        )
        filter_params.append(list_to_filter("componentPointId", component_point_id))
        filter_params.append(list_to_filter("meterTypePrimary", meter_type_primary))
        filter_params.append(
            list_to_filter("meterTypeIDPrimary", meter_type_i_d_primary)
        )
        filter_params.append(list_to_filter("dir", dir))
        filter_params.append(list_to_filter("locProp", loc_prop))
        filter_params.append(list_to_filter("zone", zone))
        filter_params.append(list_to_filter("county", county))
        filter_params.append(list_to_filter("countyId", county_id))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("stateId", state_id))
        filter_params.append(list_to_filter("subRegion", sub_region))
        filter_params.append(list_to_filter("subRegionId", sub_region_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("maxTrans", max_trans))
        filter_params.append(list_to_filter("maxStorage", max_storage))
        filter_params.append(list_to_filter("pointMaxTrans", point_max_trans))
        filter_params.append(list_to_filter("pointMaxStor", point_max_stor))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("agent", agent))
        filter_params.append(list_to_filter("rateSchedule", rate_schedule))
        filter_params.append(list_to_filter("contractNumber", contract_number))
        filter_params.append(list_to_filter("contractStartDate", contract_start_date))
        if contract_start_date_gt is not None:
            filter_params.append(f'contractStartDate > "{contract_start_date_gt}"')
        if contract_start_date_gte is not None:
            filter_params.append(f'contractStartDate >= "{contract_start_date_gte}"')
        if contract_start_date_lt is not None:
            filter_params.append(f'contractStartDate < "{contract_start_date_lt}"')
        if contract_start_date_lte is not None:
            filter_params.append(f'contractStartDate <= "{contract_start_date_lte}"')
        filter_params.append(list_to_filter("contractEndDate", contract_end_date))
        if contract_end_date_gt is not None:
            filter_params.append(f'contractEndDate > "{contract_end_date_gt}"')
        if contract_end_date_gte is not None:
            filter_params.append(f'contractEndDate >= "{contract_end_date_gte}"')
        if contract_end_date_lt is not None:
            filter_params.append(f'contractEndDate < "{contract_end_date_lt}"')
        if contract_end_date_lte is not None:
            filter_params.append(f'contractEndDate <= "{contract_end_date_lte}"')
        filter_params.append(list_to_filter("contractMonths", contract_months))
        filter_params.append(list_to_filter("rollOverPeriod", roll_over_period))
        filter_params.append(list_to_filter("negotiatedRate", negotiated_rate))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/index-of-customer-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_production_oil_data(
        self,
        *,
        flow_date: Optional[date] = None,
        flow_date_lt: Optional[date] = None,
        flow_date_lte: Optional[date] = None,
        flow_date_gt: Optional[date] = None,
        flow_date_gte: Optional[date] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        model_id: Optional[Union[list[str], Series[str], str]] = None,
        model_type: Optional[Union[list[str], Series[str], str]] = None,
        model_type_id: Optional[Union[list[str], Series[str], str]] = None,
        market_type: Optional[Union[list[str], Series[str], str]] = None,
        market_type_id: Optional[Union[list[str], Series[str], str]] = None,
        function_type: Optional[Union[list[str], Series[str], str]] = None,
        function_type_id: Optional[Union[list[str], Series[str], str]] = None,
        point_of_view: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        state_id: Optional[Union[list[str], Series[str], str]] = None,
        producing_area: Optional[Union[list[str], Series[str], str]] = None,
        producing_area_id: Optional[Union[list[str], Series[str], str]] = None,
        padd: Optional[Union[list[str], Series[str], str]] = None,
        padd_name: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        volume: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Crude oil production by both PADD and by the geographical hierarchies utilized for natural gas production dataset expressed at a monthly average granularity.

        Parameters
        ----------

         flow_date: Optional[date], optional
             The calendar date or gas day the activity occurred.
         flow_date_gt: Optional[date], optional
             filter by `flow_date > x`, by default None
         flow_date_gte: Optional[date], optional
             filter by `flow_date >= x`, by default None
         flow_date_lt: Optional[date], optional
             filter by `flow_date < x`, by default None
         flow_date_lte: Optional[date], optional
             filter by `flow_date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Oil production can only be Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Monthly, Seasonal, Annual. Weekly date frequencies are based on the defined EIA storage week of Friday-Thursday. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         model_id: Optional[Union[list[str], Series[str], str]]
             Internal use, Model ID value., by default None
         model_type: Optional[Union[list[str], Series[str], str]]
             Model types can vary among supply, demand and other market fundamentals. The type describes the fundamentals the model output represents., by default None
         model_type_id: Optional[Union[list[str], Series[str], str]]
             ID associated with Model type., by default None
         market_type: Optional[Union[list[str], Series[str], str]]
             Oil market type relates to the point of view data, such as country, region, subregion, special area or producing area., by default None
         market_type_id: Optional[Union[list[str], Series[str], str]]
             ID associated with Market type., by default None
         function_type: Optional[Union[list[str], Series[str], str]]
             The name of the Function Type such as prediction, aggregation, allocation, ten year average., by default None
         function_type_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a Function Type such as 1 is prediction, 2 is aggregation, 3 is allocation, 4 is ten year average., by default None
         point_of_view: Optional[Union[list[str], Series[str], str]]
             Point of View for the values. Point of view based on a geographic hierarchy of country, region, subregion, or producing area., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The political boundaries that define a state or province within country., by default None
         state_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the state or province utilizes legacy Bentek IDs., by default None
         producing_area: Optional[Union[list[str], Series[str], str]]
             Defined aggregation of counties within a state that is a best fit representation of prominent oil and gas plays and basins., by default None
         producing_area_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for a defined Producing Area utilizes legacy PointLogic IDs., by default None
         padd: Optional[Union[list[str], Series[str], str]]
             Petroleum Administration for Defense Districts (PADDs) are geographic aggregations of the 50 States and the District of Columbia into five districts: PADD 1 is the East Coast, PADD 2 the Midwest, PADD 3 the Gulf Coast, PADD 4 the Rocky Mountain Region, and PADD 5 the West Coast., by default None
         padd_name: Optional[Union[list[str], Series[str], str]]
             Petroleum Administration for Defense Districts (PADDs) are geographic aggregations of the 50 States and the District of Columbia into five districts., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         volume: Optional[Union[list[str], Series[str], str]]
             Volume., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("flowDate", flow_date))
        if flow_date_gt is not None:
            filter_params.append(f'flowDate > "{flow_date_gt}"')
        if flow_date_gte is not None:
            filter_params.append(f'flowDate >= "{flow_date_gte}"')
        if flow_date_lt is not None:
            filter_params.append(f'flowDate < "{flow_date_lt}"')
        if flow_date_lte is not None:
            filter_params.append(f'flowDate <= "{flow_date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(list_to_filter("modelId", model_id))
        filter_params.append(list_to_filter("modelType", model_type))
        filter_params.append(list_to_filter("modelTypeId", model_type_id))
        filter_params.append(list_to_filter("marketType", market_type))
        filter_params.append(list_to_filter("marketTypeId", market_type_id))
        filter_params.append(list_to_filter("functionType", function_type))
        filter_params.append(list_to_filter("functionTypeId", function_type_id))
        filter_params.append(list_to_filter("pointOfView", point_of_view))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("stateId", state_id))
        filter_params.append(list_to_filter("producingArea", producing_area))
        filter_params.append(list_to_filter("producingAreaId", producing_area_id))
        filter_params.append(list_to_filter("padd", padd))
        filter_params.append(list_to_filter("paddName", padd_name))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("volume", volume))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/production-oil-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_facility_flow_data(
        self,
        *,
        flow_date: Optional[date] = None,
        flow_date_lt: Optional[date] = None,
        flow_date_lte: Optional[date] = None,
        flow_date_gt: Optional[date] = None,
        flow_date_gte: Optional[date] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        facility_type: Optional[Union[list[str], Series[str], str]] = None,
        facility_type_id: Optional[Union[list[str], Series[str], str]] = None,
        view_type: Optional[Union[list[str], Series[str], str]] = None,
        view_type_id: Optional[Union[list[str], Series[str], str]] = None,
        name: Optional[Union[list[str], Series[str], str]] = None,
        model_id: Optional[Union[list[str], Series[str], str]] = None,
        component_point_id: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_name: Optional[Union[list[str], Series[str], str]] = None,
        pipeline_id: Optional[Union[list[str], Series[str], str]] = None,
        point_name: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_primary: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_id_primary: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_secondary: Optional[Union[list[str], Series[str], str]] = None,
        meter_type_id_secondary: Optional[Union[list[str], Series[str], str]] = None,
        flow_direction: Optional[Union[list[str], Series[str], str]] = None,
        flow_direction_code: Optional[Union[list[str], Series[str], str]] = None,
        flow_direction_id: Optional[Union[list[str], Series[str], str]] = None,
        zone: Optional[Union[list[str], Series[str], str]] = None,
        connecting_party: Optional[Union[list[str], Series[str], str]] = None,
        loc_prop: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        state: Optional[Union[list[str], Series[str], str]] = None,
        state_id: Optional[Union[list[str], Series[str], str]] = None,
        county: Optional[Union[list[str], Series[str], str]] = None,
        county_id: Optional[Union[list[str], Series[str], str]] = None,
        producing_area: Optional[Union[list[str], Series[str], str]] = None,
        producing_area_id: Optional[Union[list[str], Series[str], str]] = None,
        padd_name: Optional[Union[list[str], Series[str], str]] = None,
        plant_field_name: Optional[Union[list[str], Series[str], str]] = None,
        plant_field_code: Optional[Union[list[str], Series[str], str]] = None,
        utility_company_name: Optional[Union[list[str], Series[str], str]] = None,
        utility_company_id: Optional[str] = None,
        utility_company_id_lt: Optional[str] = None,
        utility_company_id_lte: Optional[str] = None,
        utility_company_id_gt: Optional[str] = None,
        utility_company_id_gte: Optional[str] = None,
        nerc_region: Optional[Union[list[str], Series[str], str]] = None,
        balancing_authority_name: Optional[Union[list[str], Series[str], str]] = None,
        balancing_authority_code: Optional[Union[list[str], Series[str], str]] = None,
        sector_name: Optional[Union[list[str], Series[str], str]] = None,
        storage_reservoir_name: Optional[Union[list[str], Series[str], str]] = None,
        storage_field_type: Optional[Union[list[str], Series[str], str]] = None,
        eia_storage_region: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        volume: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Facility level data and flows refer to curated datasets or aggregated information from specific meters in interstate gas pipeline flows. This data includes details about natural gas power plants, processing plants, storage facilities, and the activity from and to LNG plants.

        Parameters
        ----------

         flow_date: Optional[date], optional
             The calendar date or gas day the activity occurred.
         flow_date_gt: Optional[date], optional
             filter by `flow_date > x`, by default None
         flow_date_gte: Optional[date], optional
             filter by `flow_date >= x`, by default None
         flow_date_lt: Optional[date], optional
             filter by `flow_date < x`, by default None
         flow_date_lte: Optional[date], optional
             filter by `flow_date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Daily, Weekly, Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Daily, Weekly, Monthly, Seasonal, Annual. Weekly date frequencies are based on the defined EIA storage week of Friday-Thursday. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         facility_type: Optional[Union[list[str], Series[str], str]]
             The type of facility that is being served by the pipeline such as Gas Processing Plant, Power Plant, Storage, LNG Feedgas or LNG Sendout., by default None
         facility_type_id: Optional[Union[list[str], Series[str], str]]
             The facility type ID is a numeric value (1 to 5) that corresponds to the sequential order of the facility types described., by default None
         view_type: Optional[Union[list[str], Series[str], str]]
             The nature of the line item data and its time series. Facility view type contains the named facility and applicable metadata. Sample view type details the underlining meter-level data in the aggregated Facility view type., by default None
         view_type_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a view type where Facility is equal to 1 and Sample 2., by default None
         name: Optional[Union[list[str], Series[str], str]]
             The name of the facility., by default None
         model_id: Optional[Union[list[str], Series[str], str]]
             Internal use, Model ID value., by default None
         component_point_id: Optional[Union[list[str], Series[str], str]]
             Point ID for a meter used by the Americas Gas service., by default None
         pipeline_name: Optional[Union[list[str], Series[str], str]]
             The display name of a pipeline system, utilizes legacy Bentek names when applicable., by default None
         pipeline_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a pipeline system, utilizes legacy Bentek pipeline ids when applicable., by default None
         point_name: Optional[Union[list[str], Series[str], str]]
             The display name of a meter or point, utilizes legacy Bentek point name when applicable., by default None
         meter_type_primary: Optional[Union[list[str], Series[str], str]]
             The primary type of classification and purpose of a meter or point, utilizes legacy PointLogic definitions., by default None
         meter_type_id_primary: Optional[Union[list[str], Series[str], str]]
             An ID for the primary type of classification and purpose of a meter or point, utilizes legacy PointLogic definitions and ID., by default None
         meter_type_secondary: Optional[Union[list[str], Series[str], str]]
             A secondary type classification and purpose of a meter or point, meant to provide an extra level of detail, utilizes legacy Bentek definitions., by default None
         meter_type_id_secondary: Optional[Union[list[str], Series[str], str]]
             A secondary type classification and purpose of a meter or point, meant to provide an extra level of detail and utilizes legacy Bentek ids., by default None
         flow_direction: Optional[Union[list[str], Series[str], str]]
             Flow direction indicates the orientation of a point such as receipt, delivery, bi-directional or the reported flow direction of segment or compressor. Attribute is sourced from legacy PointLogic. Flow direction is primary to the similar and secondary attribute of Location Type., by default None
         flow_direction_code: Optional[Union[list[str], Series[str], str]]
             A one letter code for Flow Direction such as ‘R’ for receipt or ‘D’ for delivery. Attribute is sourced from legacy PointLogic. Flow direction is primary to the similar and secondary attribute of Location Type., by default None
         flow_direction_id: Optional[Union[list[str], Series[str], str]]
             Flow direction identification number for the orientation of a point such as receipt, delivery, bi-directional or the reported flow direction of segment or compressor. Attribute is sourced from legacy PointLogic. Flow direction is primary to the similar and secondary attribute of Location Type., by default None
         zone: Optional[Union[list[str], Series[str], str]]
             A designation for where on the pipeline system the point is located, corresponding to a pipeline’s operational and market design. Zonal information is sourced from the legacy Bentek service., by default None
         connecting_party: Optional[Union[list[str], Series[str], str]]
             The downstream connecting business name of a meter as reported by the pipeline, utilizes legacy Bentek service., by default None
         loc_prop: Optional[Union[list[str], Series[str], str]]
             The location propriety code reported by the pipeline for a specific meter., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         state: Optional[Union[list[str], Series[str], str]]
             The political boundaries that define a state or province within country., by default None
         state_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the state or province utilizes legacy Bentek IDs., by default None
         county: Optional[Union[list[str], Series[str], str]]
             The political boundaries of a defined county within a US state in which a meter or point resides., by default None
         county_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the county within a US state in which a meter or point resides, utilizes legacy Bentek IDs., by default None
         producing_area: Optional[Union[list[str], Series[str], str]]
             Defined aggregation of counties within a state that is a best fit representation of prominent oil and gas plays and basins., by default None
         producing_area_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for a defined Producing Area, utilizes legacy PointLogic IDs., by default None
         padd_name: Optional[Union[list[str], Series[str], str]]
             Petroleum Administration for Defense Districts (PADDs) are geographic aggregations of the 50 States and the District of Columbia into five districts., by default None
         plant_field_name: Optional[Union[list[str], Series[str], str]]
             The plant or field name where the facility resides. When applicable, the name will align to EIA-860 or EIA-191 data., by default None
         plant_field_code: Optional[Union[list[str], Series[str], str]]
             The plant or field code where the facility resides. When applicable, the code will align to EIA-860 or EIA-191 data., by default None
         utility_company_name: Optional[Union[list[str], Series[str], str]]
             The name of the utility company that owns or operates the facility. When applicable, the name will align to EIA-860 or EIA-191 data., by default None
         utility_company_id: Optional[str], optional
             The assigned ID of the utility company that owns or operates the facility. When applicable, the name will align to EIA-860 or EIA-191 data., by default None
         utility_company_id_gt: Optional[str], optional
             filter by `utility_company_id > x`, by default None
         utility_company_id_gte: Optional[str], optional
             filter by `utility_company_id >= x`, by default None
         utility_company_id_lt: Optional[str], optional
             filter by `utility_company_id < x`, by default None
         utility_company_id_lte: Optional[str], optional
             filter by `utility_company_id <= x`, by default None
         nerc_region: Optional[Union[list[str], Series[str], str]]
             North American Reliability Council region., by default None
         balancing_authority_name: Optional[Union[list[str], Series[str], str]]
             The name of a balancing authority. When applicable, the name will align to EIA-860., by default None
         balancing_authority_code: Optional[Union[list[str], Series[str], str]]
             The code assigned to a balancing authority by the FERC. When applicable, the name will align to EIA-860., by default None
         sector_name: Optional[Union[list[str], Series[str], str]]
             The sector which the facility resides. Power for example can be an IPP Non-CHP = Independent Power Producer and Not a commercial combined heat and power plant aka: cogen facility. When applicable, the name will align to EIA-860., by default None
         storage_reservoir_name: Optional[Union[list[str], Series[str], str]]
             The name of the storage reservoir. When applicable, the name will align to EIA-191., by default None
         storage_field_type: Optional[Union[list[str], Series[str], str]]
             The type of storage field. When applicable, the name will align to EIA-191., by default None
         eia_storage_region: Optional[Union[list[str], Series[str], str]]
             The storage region names as assigned by the EIA- Energy Information Administration., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         volume: Optional[Union[list[str], Series[str], str]]
             Volume., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 5000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("flowDate", flow_date))
        if flow_date_gt is not None:
            filter_params.append(f'flowDate > "{flow_date_gt}"')
        if flow_date_gte is not None:
            filter_params.append(f'flowDate >= "{flow_date_gte}"')
        if flow_date_lt is not None:
            filter_params.append(f'flowDate < "{flow_date_lt}"')
        if flow_date_lte is not None:
            filter_params.append(f'flowDate <= "{flow_date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(list_to_filter("facilityType", facility_type))
        filter_params.append(list_to_filter("facilityTypeId", facility_type_id))
        filter_params.append(list_to_filter("viewType", view_type))
        filter_params.append(list_to_filter("viewTypeId", view_type_id))
        filter_params.append(list_to_filter("name", name))
        filter_params.append(list_to_filter("modelId", model_id))
        filter_params.append(list_to_filter("componentPointId", component_point_id))
        filter_params.append(list_to_filter("pipelineName", pipeline_name))
        filter_params.append(list_to_filter("pipelineId", pipeline_id))
        filter_params.append(list_to_filter("pointName", point_name))
        filter_params.append(list_to_filter("meterTypePrimary", meter_type_primary))
        filter_params.append(
            list_to_filter("meterTypeIdPrimary", meter_type_id_primary)
        )
        filter_params.append(list_to_filter("meterTypeSecondary", meter_type_secondary))
        filter_params.append(
            list_to_filter("meterTypeIdSecondary", meter_type_id_secondary)
        )
        filter_params.append(list_to_filter("flowDirection", flow_direction))
        filter_params.append(list_to_filter("flowDirectionCode", flow_direction_code))
        filter_params.append(list_to_filter("flowDirectionId", flow_direction_id))
        filter_params.append(list_to_filter("zone", zone))
        filter_params.append(list_to_filter("connectingParty", connecting_party))
        filter_params.append(list_to_filter("locProp", loc_prop))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("state", state))
        filter_params.append(list_to_filter("stateId", state_id))
        filter_params.append(list_to_filter("county", county))
        filter_params.append(list_to_filter("countyId", county_id))
        filter_params.append(list_to_filter("producingArea", producing_area))
        filter_params.append(list_to_filter("producingAreaId", producing_area_id))
        filter_params.append(list_to_filter("paddName", padd_name))
        filter_params.append(list_to_filter("plantFieldName", plant_field_name))
        filter_params.append(list_to_filter("plantFieldCode", plant_field_code))
        filter_params.append(list_to_filter("utilityCompanyName", utility_company_name))
        filter_params.append(list_to_filter("utilityCompanyId", utility_company_id))
        if utility_company_id_gt is not None:
            filter_params.append(f'utilityCompanyId > "{utility_company_id_gt}"')
        if utility_company_id_gte is not None:
            filter_params.append(f'utilityCompanyId >= "{utility_company_id_gte}"')
        if utility_company_id_lt is not None:
            filter_params.append(f'utilityCompanyId < "{utility_company_id_lt}"')
        if utility_company_id_lte is not None:
            filter_params.append(f'utilityCompanyId <= "{utility_company_id_lte}"')
        filter_params.append(list_to_filter("nercRegion", nerc_region))
        filter_params.append(
            list_to_filter("balancingAuthorityName", balancing_authority_name)
        )
        filter_params.append(
            list_to_filter("balancingAuthorityCode", balancing_authority_code)
        )
        filter_params.append(list_to_filter("sectorName", sector_name))
        filter_params.append(
            list_to_filter("storageReservoirName", storage_reservoir_name)
        )
        filter_params.append(list_to_filter("storageFieldType", storage_field_type))
        filter_params.append(list_to_filter("eiaStorageRegion", eia_storage_region))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("volume", volume))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/facility-flow-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    
    def get_market_balances_data(
        self,
        *,
        flow_date: Optional[date] = None,
        flow_date_lt: Optional[date] = None,
        flow_date_lte: Optional[date] = None,
        flow_date_gt: Optional[date] = None,
        flow_date_gte: Optional[date] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        geography_type: Optional[Union[list[str], Series[str], str]] = None,
        geography_pov: Optional[Union[list[str], Series[str], str]] = None,
        geography_pov_id: Optional[Union[list[str], Series[str], str]] = None,
        model_id: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        category: Optional[Union[list[str], Series[str], str]] = None,
        row_order_ranking: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        A detailed accounting of all supply and demand elements pertaining to a domain, region, or subregion. Volumes are expressed in million cubic feet per day (MMcf/d).

        Parameters
        ----------

         flow_date: Optional[date], optional
             The calendar date or gas day the activity occurred.
         flow_date_gt: Optional[date], optional
             filter by `flow_date > x`, by default None
         flow_date_gte: Optional[date], optional
             filter by `flow_date >= x`, by default None
         flow_date_lt: Optional[date], optional
             filter by `flow_date < x`, by default None
         flow_date_lte: Optional[date], optional
             filter by `flow_date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Daily, Weekly, Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Daily, Weekly, Monthly, Seasonal, Annual. Weekly date frequencies are based on the defined EIA storage week of Friday-Thursday. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         geography_type: Optional[Union[list[str], Series[str], str]]
             Geography type for the point of view based on geographic hierarchy of country, region, subregion, substate, or special area., by default None
         geography_pov: Optional[Union[list[str], Series[str], str]]
             Geography point of view based on geographic hierarchy of country, region, subregion, substate, or special area., by default None
         geography_pov_id: Optional[Union[list[str], Series[str], str]]
             An ID for the geography point of view., by default None
         model_id: Optional[Union[list[str], Series[str], str]]
             Internal use, Model ID value., by default None
         concept: Optional[Union[list[str], Series[str], str]]
             Data concept such as Supply, Demand, LNG, Storage, Flows, etc., by default None
         category: Optional[Union[list[str], Series[str], str]]
             Category that is unique within a Concept., by default None
         row_order_ranking: Optional[Union[list[str], Series[str], str]]
             A number used to define a consistent row order of the data within a geography point of view., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         value: Optional[Union[list[str], Series[str], str]]
             Value of the record., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("flowDate", flow_date))
        if flow_date_gt is not None:
            filter_params.append(f'flowDate > "{flow_date_gt}"')
        if flow_date_gte is not None:
            filter_params.append(f'flowDate >= "{flow_date_gte}"')
        if flow_date_lt is not None:
            filter_params.append(f'flowDate < "{flow_date_lt}"')
        if flow_date_lte is not None:
            filter_params.append(f'flowDate <= "{flow_date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("geographyType", geography_type))
        filter_params.append(list_to_filter("geographyPov", geography_pov))
        filter_params.append(list_to_filter("geographyPovId", geography_pov_id))
        filter_params.append(list_to_filter("modelId", model_id))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("category", category))
        filter_params.append(list_to_filter("rowOrderRanking", row_order_ranking))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("value", value))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/market-balances-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_regional_summaries_flowdata(
        self,
        *,
        flow_date: Optional[date] = None,
        flow_date_lt: Optional[date] = None,
        flow_date_lte: Optional[date] = None,
        flow_date_gt: Optional[date] = None,
        flow_date_gte: Optional[date] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        model_id: Optional[Union[list[str], Series[str], str]] = None,
        view_type: Optional[Union[list[str], Series[str], str]] = None,
        view_type_id: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        geography_pov: Optional[Union[list[str], Series[str], str]] = None,
        geography_pov_id: Optional[Union[list[str], Series[str], str]] = None,
        name: Optional[Union[list[str], Series[str], str]] = None,
        row_order_ranking: Optional[Union[list[str], Series[str], str]] = None,
        neighboring_geography: Optional[Union[list[str], Series[str], str]] = None,
        neighboring_geography_id: Optional[Union[list[str], Series[str], str]] = None,
        volume: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        data_active: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Regional flows are aggregated summaries of natural gas pipeline flows between defined geographies. Pipeline Summaries provide a pipeline by pipeline accounting of the regional flows between defined geographies.

        Parameters
        ----------

         flow_date: Optional[date]
             The calendar date or gas day the activity occurred.
         flow_date_gt: Optional[date], optional
             filter by `flow_date > x`, by default None
         flow_date_gte: Optional[date], optional
             filter by `flow_date >= x`, by default None
         flow_date_lt: Optional[date], optional
             filter by `flow_date < x`, by default None
         flow_date_lte: Optional[date], optional
             filter by `flow_date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Daily, Weekly, Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Daily, Weekly, Monthly, Seasonal, Annual. Weekly date frequencies are based on the defined EIA storage week of Friday-Thursday. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         model_id: Optional[Union[list[str], Series[str], str]]
             Internal use, Model ID value., by default None
         view_type: Optional[Union[list[str], Series[str], str]]
             The name of the View Type such as Country Summary, Region Summary, Region Detail, Subregion Detail. Additional and more granular view types exist for the Gulf Coast region such as Gulf Coast: Subregion Summary, Gulf Coast: Substate Detail and Gulf Coast: Special Area Detail., by default None
         view_type_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a View Type. Country Summary =1, Region Summary =2, Region Detail =3, Subregion Detail =4, Gulf Coast: Subregion Summary =5, Gulf Coast: Substate Detail =6, Gulf Coast: Special Area Detail =7, by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada or Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         geography_pov: Optional[Union[list[str], Series[str], str]]
             Geography point of view based on geographic hierarchy of country, region, subregion, substate, or special area., by default None
         geography_pov_id: Optional[Union[list[str], Series[str], str]]
             An ID for the geography point of view., by default None
         name: Optional[Union[list[str], Series[str], str]]
             Name of the pipeline summary or regional flows., by default None
         row_order_ranking: Optional[Union[list[str], Series[str], str]]
             A number used to define a consistent row order of the data within a geography point of view., by default None
         neighboring_geography: Optional[Union[list[str], Series[str], str]]
             Neighboring geography point of view based on geographic hierarchy of country, region, subregion, substate, or special area., by default None
         neighboring_geography_id: Optional[Union[list[str], Series[str], str]]
             An ID for the neighboring geography point of view., by default None
         volume: Optional[Union[list[str], Series[str], str]]
             Volume., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         data_active: Optional[Union[list[str], Series[str], str]]
             A true or false return if the record is active., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("flowDate", flow_date))
        if flow_date_gt is not None:
            filter_params.append(f'flowDate > "{flow_date_gt}"')
        if flow_date_gte is not None:
            filter_params.append(f'flowDate >= "{flow_date_gte}"')
        if flow_date_lt is not None:
            filter_params.append(f'flowDate < "{flow_date_lt}"')
        if flow_date_lte is not None:
            filter_params.append(f'flowDate <= "{flow_date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(list_to_filter("modelId", model_id))
        filter_params.append(list_to_filter("viewType", view_type))
        filter_params.append(list_to_filter("viewTypeId", view_type_id))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("geographyPov", geography_pov))
        filter_params.append(list_to_filter("geographyPovId", geography_pov_id))
        filter_params.append(list_to_filter("name", name))
        filter_params.append(list_to_filter("rowOrderRanking", row_order_ranking))
        filter_params.append(
            list_to_filter("neighboringGeography", neighboring_geography)
        )
        filter_params.append(
            list_to_filter("neighboringGeographyId", neighboring_geography_id)
        )
        filter_params.append(list_to_filter("volume", volume))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("dataActive", data_active))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/regional-summaries-flowdata",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response
    

    def get_storage_data(
        self,
        *,
        flow_date: Optional[date] = None,
        flow_date_lt: Optional[date] = None,
        flow_date_lte: Optional[date] = None,
        flow_date_gt: Optional[date] = None,
        flow_date_gte: Optional[date] = None,
        last_modified_date: Optional[datetime] = None,
        last_modified_date_lt: Optional[datetime] = None,
        last_modified_date_lte: Optional[datetime] = None,
        last_modified_date_gt: Optional[datetime] = None,
        last_modified_date_gte: Optional[datetime] = None,
        date_frequency: Optional[Union[list[str], Series[str], str]] = None,
        date_frequency_desc: Optional[Union[list[str], Series[str], str]] = None,
        source: Optional[Union[list[str], Series[str], str]] = None,
        view_type: Optional[Union[list[str], Series[str], str]] = None,
        view_type_id: Optional[Union[list[str], Series[str], str]] = None,
        value_type: Optional[Union[list[str], Series[str], str]] = None,
        value_type_id: Optional[Union[list[str], Series[str], str]] = None,
        domain: Optional[Union[list[str], Series[str], str]] = None,
        domain_id: Optional[Union[list[str], Series[str], str]] = None,
        region: Optional[Union[list[str], Series[str], str]] = None,
        region_id: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        subregion_id: Optional[Union[list[str], Series[str], str]] = None,
        geography_pov: Optional[Union[list[str], Series[str], str]] = None,
        geography_pov_id: Optional[Union[list[str], Series[str], str]] = None,
        geography_type: Optional[Union[list[str], Series[str], str]] = None,
        model_id: Optional[Union[list[str], Series[str], str]] = None,
        working_capacity: Optional[Union[list[str], Series[str], str]] = None,
        utilization: Optional[Union[list[str], Series[str], str]] = None,
        uom: Optional[Union[list[str], Series[str], str]] = None,
        volume: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        Underground natural gas storage data and metrics detailing inventory and activity for both Americas Gas geographies and EIA regions.

        Parameters
        ----------

         flow_date: Optional[date]
             The calendar date or gas day the activity occurred.
         flow_date_gt: Optional[date], optional
             filter by `flow_date > x`, by default None
         flow_date_gte: Optional[date], optional
             filter by `flow_date >= x`, by default None
         flow_date_lt: Optional[date], optional
             filter by `flow_date < x`, by default None
         flow_date_lte: Optional[date], optional
             filter by `flow_date <= x`, by default None
         last_modified_date: Optional[datetime], optional
             Date and time the record was last updated., by default None
         last_modified_date_gt: Optional[datetime], optional
             filter by `last_modified_date > x`, by default None
         last_modified_date_gte: Optional[datetime], optional
             filter by `last_modified_date >= x`, by default None
         last_modified_date_lt: Optional[datetime], optional
             filter by `last_modified_date < x`, by default None
         last_modified_date_lte: Optional[datetime], optional
             filter by `last_modified_date <= x`, by default None
         date_frequency: Optional[Union[list[str], Series[str], str]]
             Daily, Weekly, Monthly, Seasonal, Annual., by default None
         date_frequency_desc: Optional[Union[list[str], Series[str], str]]
             The time period averages of the dataset such as Daily, Weekly, Monthly, Seasonal, Annual. Weekly date frequencies are based on the defined EIA storage week of Friday-Thursday. Seasonal date frequencies define Summer as April to October and Winter as November to March., by default None
         source: Optional[Union[list[str], Series[str], str]]
             Source of the data, EIA or Americas Gas., by default None
         view_type: Optional[Union[list[str], Series[str], str]]
             The type of view, which can be either Inventory, Activity, or Sample., by default None
         view_type_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a view type where Inventory is equal to 1, Activity 2 and Sample 3., by default None
         value_type: Optional[Union[list[str], Series[str], str]]
             The types of values presented in the view, such as Actual/Estimate, 5-Year Average, Delta to 5-Year Average, 5-Year Maximum, 5-Year Minimum, Last Year, Delta to Last Year, Delta to 5-Year Maximum, Delta to 5-Year Minimum., by default None
         value_type_id: Optional[Union[list[str], Series[str], str]]
             The ID given to a value type numbered 1 through 9 listed in order as expressed in Value Type description., by default None
         domain: Optional[Union[list[str], Series[str], str]]
             US Lower-48, Canada and Mexico., by default None
         domain_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the domain., by default None
         region: Optional[Union[list[str], Series[str], str]]
             A defined geographic region within the Americas Gas service. Regions are an aggregation of states or provinces within a country., by default None
         region_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic region., by default None
         subregion: Optional[Union[list[str], Series[str], str]]
             A defined geographic subregion within the Americas Gas service. A substate geography is sometimes referred to as a subregion. Subregions are an aggregation of specific counties within a region and a country., by default None
         subregion_id: Optional[Union[list[str], Series[str], str]]
             A unique identification number for the geographic subregion., by default None
         geography_pov: Optional[Union[list[str], Series[str], str]]
             Geography point of view based on geographic hierarchy of country, region, subregion, substate, or special area., by default None
         geography_pov_id: Optional[Union[list[str], Series[str], str]]
             An ID for the geography point of view., by default None
         geography_type: Optional[Union[list[str], Series[str], str]]
             Geography type for the point of view based on geographic hierarchy of country, region, subregion, substate, or special area., by default None
         model_id: Optional[Union[list[str], Series[str], str]]
             Internal use, Model ID value., by default None
         working_capacity: Optional[Union[list[str], Series[str], str]]
             Estimates for working storage capacity of a given geography. These estimates are based historical analysis of actual inventory and may vary from theoretical or published figures., by default None
         utilization: Optional[Union[list[str], Series[str], str]]
             Utilization rate in decimal form or in percentage terms. Utilization is calculated by dividing the volume by its working capacity., by default None
         uom: Optional[Union[list[str], Series[str], str]]
             Unit of measure., by default None
         volume: Optional[Union[list[str], Series[str], str]]
             Volume., by default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("flowDate", flow_date))
        if flow_date_gt is not None:
            filter_params.append(f'flowDate > "{flow_date_gt}"')
        if flow_date_gte is not None:
            filter_params.append(f'flowDate >= "{flow_date_gte}"')
        if flow_date_lt is not None:
            filter_params.append(f'flowDate < "{flow_date_lt}"')
        if flow_date_lte is not None:
            filter_params.append(f'flowDate <= "{flow_date_lte}"')
        filter_params.append(list_to_filter("lastModifiedDate", last_modified_date))
        if last_modified_date_gt is not None:
            filter_params.append(f'lastModifiedDate > "{last_modified_date_gt}"')
        if last_modified_date_gte is not None:
            filter_params.append(f'lastModifiedDate >= "{last_modified_date_gte}"')
        if last_modified_date_lt is not None:
            filter_params.append(f'lastModifiedDate < "{last_modified_date_lt}"')
        if last_modified_date_lte is not None:
            filter_params.append(f'lastModifiedDate <= "{last_modified_date_lte}"')
        filter_params.append(list_to_filter("dateFrequency", date_frequency))
        filter_params.append(list_to_filter("dateFrequencyDesc", date_frequency_desc))
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("viewType", view_type))
        filter_params.append(list_to_filter("viewTypeId", view_type_id))
        filter_params.append(list_to_filter("valueType", value_type))
        filter_params.append(list_to_filter("valueTypeId", value_type_id))
        filter_params.append(list_to_filter("domain", domain))
        filter_params.append(list_to_filter("domainId", domain_id))
        filter_params.append(list_to_filter("region", region))
        filter_params.append(list_to_filter("regionId", region_id))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("subregionId", subregion_id))
        filter_params.append(list_to_filter("geographyPov", geography_pov))
        filter_params.append(list_to_filter("geographyPovId", geography_pov_id))
        filter_params.append(list_to_filter("geographyType", geography_type))
        filter_params.append(list_to_filter("modelId", model_id))
        filter_params.append(list_to_filter("workingCapacity", working_capacity))
        filter_params.append(list_to_filter("utilization", utilization))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("volume", volume))

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/analytics/gas/na-gas/v1/storage-data",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
        """
        Converts the API response to a pandas DataFrame and ensures date columns are in ISO8601 format.

        Parameters
        ----------
        resp : Response
            The API response object.

        Returns
        -------
        pd.DataFrame
            The processed DataFrame with date columns in ISO8601 format.
        """
        j = resp.json()
        df = pd.json_normalize(j["results"])

        date_columns = [
            "lastModifiedDate", "flowDate", "forecastDate", "postingDatetime",
            "createDate", "measurementDate", "effectiveDate", "endDate",
            "validFrom", "validTo", "dateEffective", "dateRetire", "dateIssued",
            "date", "contractStartDate", "contractEndDate", "inServiceDate",
            "projectCreatedDate", "projectUpdatedDate", "preFileDate",
            "projectFileDate", "projectApprovalDate", "componentCreateDate"
        ]

        # ISO8601 format
        for column in date_columns:
            if column in df.columns:
                if parse(pd.__version__) >= parse("2"):
                    df[column] = pd.to_datetime(
                        df[column], utc=True, format="ISO8601", errors="coerce"
                    )
                else:
                    df[column] = pd.to_datetime(df[column], errors="coerce", utc=True)

        return df