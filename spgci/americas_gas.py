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
        "pipeline-flows", 
        "modeled-demand-actual",
        "natural-gas-production",
        "population-weighted-weather",
        "outlook-production-play",
        "outlook-marketbalances-prices",
        "pipeline-storage-projects",
    ]
    def get_unique_values(
        self,
        dataset: _datasets,
        columns: Optional[Union[list[str], str]],
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
        """
        dataset_to_path = {
            "reference-data-pipeline-flows": "/analytics/gas/na-gas/v1/reference-data/pipeline-flows",
            "pipeline-flows": "/analytics/gas/na-gas/v1/pipeline-flows",
            "modeled-demand-actual": "/analytics/gas/na-gas/v1/modeled-demand-actual",
            "natural-gas-production": "/analytics/gas/na-gas/v1/natural-gas-production",
            "population-weighted-weather": "/analytics/gas/na-gas/v1/population-weighted-weather",
            "outlook-production-play": "/analytics/gas/na-gas/v1/outlook-production-play",
            "outlook-marketbalances-prices": "/analytics/gas/na-gas/v1/outlook-marketbalances-prices",
            "pipeline-storage-projects": "/analytics/gas/na-gas/v1/pipeline-storage-projects",
        }

        if dataset not in dataset_to_path:
            valid = "\n".join(dataset_to_path.keys())
            raise ValueError(
                f"Dataset '{dataset}' not found. Valid datasets:\n{valid}"
            )

        path = dataset_to_path[dataset]
        col_value = ", ".join(columns) if isinstance(columns, list) else columns or ""
        params = {"GroupBy": col_value, "pageSize": 5000}

        def to_df(resp: Response):
            j = resp.json()
            return DataFrame(j["aggResultValue"])

        return get_data(path, params, to_df, paginate=True)

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
    

    def get_modeled_demand_actual(
        self,
        *,
        flow_date: date,
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
        flow_date: date,
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
        flow_date: date,
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