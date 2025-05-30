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
    "pipeline-flows",
    ]

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
            "projectFileDate", "projectApprovalDate"
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