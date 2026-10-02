from __future__ import annotations
from datetime import date
from typing import Literal, Optional, Union
import pandas as pd
from pandas import DataFrame, Series
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter

PpaDataset = Literal[
    "low-carbon-electricity",
]


class CetPpa:
    """Client for CET power purchase agreement (PPA) datasets."""

    _dataset_to_path = {
        "low-carbon-electricity": "analytics/cet/ppa/v1/low-carbon-electricity",
    }

    def get_unique_values(
        self,
        dataset: PpaDataset,
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

    def get_low_carbon_electricity(
        self,
        *,
        record_id: Optional[Union[list[str], Series[str], str]] = None,
        project_name: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        state_province: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        technologies_present: Optional[Union[list[str], Series[str], str]] = None,
        project_capacity_megawatt: Optional[Union[list[str], Series[str], str]] = None,
        project_estimated_capacity_factor: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        contracted_capacity_megawatt: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        contracted_generation_gigawatt: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        battery_storage_capacity_megawatt: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        battery_discharge_capacity_megawatt: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        es_project_storage_duration_hours: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        electrolyser_capacity_megawatt: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        project_commissioning_date: Optional[date] = None,
        project_commissioning_date_lt: Optional[date] = None,
        project_commissioning_date_lte: Optional[date] = None,
        project_commissioning_date_gt: Optional[date] = None,
        project_commissioning_date_gte: Optional[date] = None,
        date_announced: Optional[date] = None,
        date_announced_lt: Optional[date] = None,
        date_announced_lte: Optional[date] = None,
        date_announced_gt: Optional[date] = None,
        date_announced_gte: Optional[date] = None,
        date_announced_year: Optional[Union[list[str], Series[str], str]] = None,
        date_announced_quarter: Optional[Union[list[str], Series[str], str]] = None,
        date_announced_month: Optional[Union[list[str], Series[str], str]] = None,
        offtake_contract_start_date: Optional[date] = None,
        offtake_contract_start_date_lt: Optional[date] = None,
        offtake_contract_start_date_lte: Optional[date] = None,
        offtake_contract_start_date_gt: Optional[date] = None,
        offtake_contract_start_date_gte: Optional[date] = None,
        offtake_contract_duration_year: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        offtake_contract_end_year: Optional[Union[list[str], Series[str], str]] = None,
        offtake_contract_start_year: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        offtake_contract_duration_category: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        developer: Optional[Union[list[str], Series[str], str]] = None,
        offtaker: Optional[Union[list[str], Series[str], str]] = None,
        offtaker_type: Optional[Union[list[str], Series[str], str]] = None,
        offtake_contract_type: Optional[Union[list[str], Series[str], str]] = None,
        offtake_contract_sub_type: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        offtaker_activity_type: Optional[Union[list[str], Series[str], str]] = None,
        offtaker_activity_sub_type: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the low-carbon-electricity dataset.

        Parameters
        ----------
        record_id: Optional[Union[list[str], Series[str], str]]
            Unique alphanumeric identifier assigned to each record.
        project_name: Optional[Union[list[str], Series[str], str]]
            Official or publicly known name of the project.
        region_major: Optional[Union[list[str], Series[str], str]]
            High-level macro-region associated with the geography.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or sovereign territory where the asset is located.
        state_province: Optional[Union[list[str], Series[str], str]]
            State, province, or equivalent first-level administrative division.
        technology: Optional[Union[list[str], Series[str], str]]
            Primary energy generation, storage, transmission, or conversion technology.
        technologies_present: Optional[Union[list[str], Series[str], str]]
            All technologies deployed within a hybrid project.
        project_capacity_megawatt: Optional[Union[list[str], Series[str], str]]
            Total installed or planned project capacity in megawatts.
        project_estimated_capacity_factor: Optional[Union[list[str], Series[str], str]]
            Estimated capacity factor at commissioning.
        contracted_capacity_megawatt: Optional[Union[list[str], Series[str], str]]
            Contracted power capacity under the referenced offtake agreement.
        contracted_generation_gigawatt: Optional[Union[list[str], Series[str], str]]
            Contracted annual generation/energy volume for the offtake arrangement.
        battery_storage_capacity_megawatt: Optional[Union[list[str], Series[str], str]]
            Battery storage system rated power capacity.
        battery_discharge_capacity_megawatt: Optional[Union[list[str], Series[str], str]]
            Battery discharge quantity associated with the project record.
        es_project_storage_duration_hours: Optional[Union[list[str], Series[str], str]]
            Duration of storage capability measured in hours for energy storage projects.
        electrolyser_capacity_megawatt: Optional[Union[list[str], Series[str], str]]
            Electrolyser nameplate input power capacity.
        project_commissioning_date: Optional[date]
            Date the project is commissioned / enters operation.
        project_commissioning_date_lt: Optional[date]
            Filter records where project commissioning date is less than the supplied date.
        project_commissioning_date_lte: Optional[date]
            Filter records where project commissioning date is less than or equal to the supplied date.
        project_commissioning_date_gt: Optional[date]
            Filter records where project commissioning date is greater than the supplied date.
        project_commissioning_date_gte: Optional[date]
            Filter records where project commissioning date is greater than or equal to the supplied date.
        date_announced: Optional[date]
            Full date when the entity was publicly announced or disclosed.
        date_announced_lt: Optional[date]
            Filter records where date announced is less than the supplied date.
        date_announced_lte: Optional[date]
            Filter records where date announced is less than or equal to the supplied date.
        date_announced_gt: Optional[date]
            Filter records where date announced is greater than the supplied date.
        date_announced_gte: Optional[date]
            Filter records where date announced is greater than or equal to the supplied date.
        date_announced_year: Optional[Union[list[str], Series[str], str]]
            Calendar year when the project/contract was announced.
        date_announced_quarter: Optional[Union[list[str], Series[str], str]]
            Calendar quarter of announcement (Q1, Q2, Q3, Q4).
        date_announced_month: Optional[Union[list[str], Series[str], str]]
            Calendar month number of announcement.
        offtake_contract_start_date: Optional[date]
            Start date for the offtake contract obligations.
        offtake_contract_start_date_lt: Optional[date]
            Filter records where offtake contract start date is less than the supplied date.
        offtake_contract_start_date_lte: Optional[date]
            Filter records where offtake contract start date is less than or equal to the supplied date.
        offtake_contract_start_date_gt: Optional[date]
            Filter records where offtake contract start date is greater than the supplied date.
        offtake_contract_start_date_gte: Optional[date]
            Filter records where offtake contract start date is greater than or equal to the supplied date.
        offtake_contract_duration_year: Optional[Union[list[str], Series[str], str]]
            Duration of the offtake agreement in years.
        offtake_contract_end_year: Optional[Union[list[str], Series[str], str]]
            Calendar year for when the contract is due to finish.
        offtake_contract_start_year: Optional[Union[list[str], Series[str], str]]
            Calendar year for when the contract is due to start.
        offtake_contract_duration_category: Optional[Union[list[str], Series[str], str]]
            Standardized contract duration bucket/range.
        developer: Optional[Union[list[str], Series[str], str]]
            Company or group of companies responsible for developing the entity.
        offtaker: Optional[Union[list[str], Series[str], str]]
            Company or entity purchasing electricity or clean energy products under the offtake agreement.
        offtaker_type: Optional[Union[list[str], Series[str], str]]
            Type of company or entity that is the offtaker of the agreement.
        offtake_contract_type: Optional[Union[list[str], Series[str], str]]
            Type of agreement (e.g., PPA, BESS, Other).
        offtake_contract_sub_type: Optional[Union[list[str], Series[str], str]]
            Specific offtake structure/mode (e.g., VPPA, Green Tariff).
        offtaker_activity_type: Optional[Union[list[str], Series[str], str]]
            Aggregated offtaker activity grouping.
        offtaker_activity_sub_type: Optional[Union[list[str], Series[str], str]]
            More granular offtaker activity descriptor.
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
        filter_params.append(list_to_filter("recordId", record_id))
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(
            list_to_filter("technologiesPresent", technologies_present)
        )
        filter_params.append(
            list_to_filter("projectCapacityMegawatt", project_capacity_megawatt)
        )
        filter_params.append(
            list_to_filter(
                "projectEstimatedCapacityFactor", project_estimated_capacity_factor
            )
        )
        filter_params.append(
            list_to_filter(
                "contractedCapacityMegawatt", contracted_capacity_megawatt
            )
        )
        filter_params.append(
            list_to_filter(
                "contractedGenerationGigawatt", contracted_generation_gigawatt
            )
        )
        filter_params.append(
            list_to_filter(
                "batteryStorageCapacityMegawatt", battery_storage_capacity_megawatt
            )
        )
        filter_params.append(
            list_to_filter(
                "batteryDischargeCapacityMegawatt",
                battery_discharge_capacity_megawatt,
            )
        )
        filter_params.append(
            list_to_filter(
                "esProjectStorageDurationHours", es_project_storage_duration_hours
            )
        )
        filter_params.append(
            list_to_filter(
                "electrolyserCapacityMegawatt", electrolyser_capacity_megawatt
            )
        )
        filter_params.append(
            list_to_filter("projectCommissioningDate", project_commissioning_date)
        )
        if project_commissioning_date_lt is not None:
            filter_params.append(
                f'projectCommissioningDate < "{project_commissioning_date_lt}"'
            )
        if project_commissioning_date_lte is not None:
            filter_params.append(
                f'projectCommissioningDate <= "{project_commissioning_date_lte}"'
            )
        if project_commissioning_date_gt is not None:
            filter_params.append(
                f'projectCommissioningDate > "{project_commissioning_date_gt}"'
            )
        if project_commissioning_date_gte is not None:
            filter_params.append(
                f'projectCommissioningDate >= "{project_commissioning_date_gte}"'
            )
        filter_params.append(list_to_filter("dateAnnounced", date_announced))
        if date_announced_lt is not None:
            filter_params.append(f'dateAnnounced < "{date_announced_lt}"')
        if date_announced_lte is not None:
            filter_params.append(f'dateAnnounced <= "{date_announced_lte}"')
        if date_announced_gt is not None:
            filter_params.append(f'dateAnnounced > "{date_announced_gt}"')
        if date_announced_gte is not None:
            filter_params.append(f'dateAnnounced >= "{date_announced_gte}"')
        filter_params.append(list_to_filter("dateAnnouncedYear", date_announced_year))
        filter_params.append(
            list_to_filter("dateAnnouncedQuarter", date_announced_quarter)
        )
        filter_params.append(
            list_to_filter("dateAnnouncedMonth", date_announced_month)
        )
        filter_params.append(
            list_to_filter("offtakeContractStartDate", offtake_contract_start_date)
        )
        if offtake_contract_start_date_lt is not None:
            filter_params.append(
                f'offtakeContractStartDate < "{offtake_contract_start_date_lt}"'
            )
        if offtake_contract_start_date_lte is not None:
            filter_params.append(
                f'offtakeContractStartDate <= "{offtake_contract_start_date_lte}"'
            )
        if offtake_contract_start_date_gt is not None:
            filter_params.append(
                f'offtakeContractStartDate > "{offtake_contract_start_date_gt}"'
            )
        if offtake_contract_start_date_gte is not None:
            filter_params.append(
                f'offtakeContractStartDate >= "{offtake_contract_start_date_gte}"'
            )
        filter_params.append(
            list_to_filter(
                "offtakeContractDurationYear", offtake_contract_duration_year
            )
        )
        filter_params.append(
            list_to_filter("offtakeContractEndYear", offtake_contract_end_year)
        )
        filter_params.append(
            list_to_filter("offtakeContractStartYear", offtake_contract_start_year)
        )
        filter_params.append(
            list_to_filter(
                "offtakeContractDurationCategory",
                offtake_contract_duration_category,
            )
        )
        filter_params.append(list_to_filter("developer", developer))
        filter_params.append(list_to_filter("offtaker", offtaker))
        filter_params.append(list_to_filter("offtakerType", offtaker_type))
        filter_params.append(
            list_to_filter("offtakeContractType", offtake_contract_type)
        )
        filter_params.append(
            list_to_filter("offtakeContractSubType", offtake_contract_sub_type)
        )
        filter_params.append(
            list_to_filter("offtakerActivityType", offtaker_activity_type)
        )
        filter_params.append(
            list_to_filter("offtakerActivitySubType", offtaker_activity_sub_type)
        )
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/ppa/v1/low-carbon-electricity",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    @staticmethod
    def _convert_unique_values_to_df(resp: Response) -> DataFrame:
        return CetPpa._normalize(resp, "aggResultValue")

    @staticmethod
    def _convert_to_df(resp: Response) -> DataFrame:
        return CetPpa._normalize(resp, "results")

    @staticmethod
    def _normalize(resp: Response, key: str) -> DataFrame:
        df = pd.json_normalize(resp.json()[key])
        for column in [
            "projectCommissioningDate",
            "dateAnnounced",
            "offtakeContractStartDate",
        ]:
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column], utc=True, format="ISO8601", errors="coerce"
                )
        return df
