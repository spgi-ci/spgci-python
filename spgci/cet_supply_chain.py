from __future__ import annotations
from datetime import date
from typing import Literal, Optional, Union
import pandas as pd
from pandas import DataFrame, Series
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter

SupplyChainDataset = Literal[
    "pv-module-market",
    "pv-module-company",
    "pv-inverter-market",
    "electrolyzer-orders",
    "wind-turbine-orders",
    "battery-orders",
]


class CetSupplyChain:
    """Client for CET supply chain datasets (PV modules and inverters, electrolyzer, wind turbine and battery orders)."""

    _dataset_to_path = {
        "pv-module-market": "analytics/cet/supplychain/v1/pv-module-market",
        "pv-module-company": "analytics/cet/supplychain/v1/pv-module-company",
        "pv-inverter-market": "analytics/cet/supplychain/v1/pv-inverter-market",
        "electrolyzer-orders": "analytics/cet/supplychain/v1/electrolyzer-orders",
        "wind-turbine-orders": "analytics/cet/supplychain/v1/wind-turbine-orders",
        "battery-orders": "analytics/cet/supplychain/v1/battery-orders",
    }

    def get_unique_values(
        self,
        dataset: SupplyChainDataset,
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

    def get_pv_module_market(
        self,
        *,
        vintage_rank: Optional[Union[list[str], Series[str], str]] = None,
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
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        outlook_segment: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        component_supplier_tier: Optional[Union[list[str], Series[str], str]] = None,
        wafer_technology: Optional[Union[list[str], Series[str], str]] = None,
        wafer_size: Optional[Union[list[str], Series[str], str]] = None,
        cell_technology: Optional[Union[list[str], Series[str], str]] = None,
        module_technology: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        currency_detailed: Optional[Union[list[str], Series[str], str]] = None,
        outlook_period: Optional[Union[list[str], Series[str], str]] = None,
        quarter: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the pv-module-market dataset (pv module market).

        Parameters
        ----------
        vintage_rank: Optional[Union[list[str], Series[str], str]]
            Rank of the data vintage, where 1 represents the latest available release.
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
        component_major: Optional[Union[list[str], Series[str], str]]
            Top-level solar PV supply chain category for the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Specific solar PV supply chain component covered by the record.
        outlook_segment: Optional[Union[list[str], Series[str], str]]
            Analytical segmentation used to classify the reported market outlook.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported market geography.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported market geography.
        component_supplier_tier: Optional[Union[list[str], Series[str], str]]
            Supplier tier classification associated with the reported market segment.
        wafer_technology: Optional[Union[list[str], Series[str], str]]
            Wafer technology type and crystalline structure associated with the reported data.
        wafer_size: Optional[Union[list[str], Series[str], str]]
            Wafer size format associated with the reported data.
        cell_technology: Optional[Union[list[str], Series[str], str]]
            Solar cell technology or architecture associated with the reported data.
        module_technology: Optional[Union[list[str], Series[str], str]]
            Solar module technology type associated with the reported data.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported numeric value.
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported numeric value is measured.
        currency: Optional[Union[list[str], Series[str], str]]
            Currency denomination for monetary values, where applicable.
        currency_detailed: Optional[Union[list[str], Series[str], str]]
            Detailed currency specification, including real or nominal basis and base year where applicable.
        outlook_period: Optional[Union[list[str], Series[str], str]]
            Reporting frequency of the data.
        quarter: Optional[Union[list[str], Series[str], str]]
            Calendar quarter associated with the reported value, where applicable.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            The reported numeric value.
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
        filter_params.append(list_to_filter("vintageRank", vintage_rank))
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
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params.append(list_to_filter("outlookSegment", outlook_segment))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(
            list_to_filter("componentSupplierTier", component_supplier_tier)
        )
        filter_params.append(list_to_filter("waferTechnology", wafer_technology))
        filter_params.append(list_to_filter("waferSize", wafer_size))
        filter_params.append(list_to_filter("cellTechnology", cell_technology))
        filter_params.append(list_to_filter("moduleTechnology", module_technology))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("currencyDetailed", currency_detailed))
        filter_params.append(list_to_filter("outlookPeriod", outlook_period))
        filter_params.append(list_to_filter("quarter", quarter))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/supplychain/v1/pv-module-market",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_pv_module_company(
        self,
        *,
        vintage_rank: Optional[Union[list[str], Series[str], str]] = None,
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
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        component_supplier_company: Optional[Union[list[str], Series[str], str]] = None,
        component_supplier_company_id: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        component_supplier_hq_geography: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        component_supplier_hq_region_minor: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        component_supplier_hq_region_major: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        component_supplier_tier: Optional[Union[list[str], Series[str], str]] = None,
        is_top_20_company: Optional[Union[list[str], Series[str], str]] = None,
        polysilicon_production_process: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        wafer_technology: Optional[Union[list[str], Series[str], str]] = None,
        wafer_size: Optional[Union[list[str], Series[str], str]] = None,
        cell_technology: Optional[Union[list[str], Series[str], str]] = None,
        module_technology: Optional[Union[list[str], Series[str], str]] = None,
        shipment_type: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        currency_detailed: Optional[Union[list[str], Series[str], str]] = None,
        outlook_period: Optional[Union[list[str], Series[str], str]] = None,
        quarter: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the pv-module-company dataset (pv module company).

        Parameters
        ----------
        vintage_rank: Optional[Union[list[str], Series[str], str]]
            Rank of the data vintage, where 1 represents the latest available release.
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
        component_major: Optional[Union[list[str], Series[str], str]]
            Top-level solar PV supply chain category for the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Specific solar PV supply chain component covered by the record.
        component_supplier_company: Optional[Union[list[str], Series[str], str]]
            Name of the company supplying the relevant solar PV component.
        component_supplier_company_id: Optional[Union[list[str], Series[str], str]]
            Unique internal identifier assigned to the component supplier company.
        component_supplier_hq_geography: Optional[Union[list[str], Series[str], str]]
            Country where the component supplier company is headquartered.
        component_supplier_hq_region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the supplier company's headquarters location.
        component_supplier_hq_region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the supplier company's headquarters location.
        component_supplier_tier: Optional[Union[list[str], Series[str], str]]
            Supplier tier classification assigned to the component supplier company.
        is_top_20_company: Optional[Union[list[str], Series[str], str]]
            Indicates whether the supplier company is ranked among the top 20 companies in the relevant category.
        polysilicon_production_process: Optional[Union[list[str], Series[str], str]]
            Polysilicon manufacturing process associated with the reported data.
        wafer_technology: Optional[Union[list[str], Series[str], str]]
            Wafer technology type and crystalline structure associated with the reported data.
        wafer_size: Optional[Union[list[str], Series[str], str]]
            Wafer size format associated with the reported data.
        cell_technology: Optional[Union[list[str], Series[str], str]]
            Solar cell technology or architecture associated with the reported data.
        module_technology: Optional[Union[list[str], Series[str], str]]
            Solar module technology type associated with the reported data.
        shipment_type: Optional[Union[list[str], Series[str], str]]
            Shipment or sales channel classification associated with the reported data.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported market geography.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported market geography.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported numeric value.
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported numeric value is measured.
        currency: Optional[Union[list[str], Series[str], str]]
            Currency denomination for monetary values, where applicable.
        currency_detailed: Optional[Union[list[str], Series[str], str]]
            Detailed currency specification, including real or nominal basis and base year where applicable.
        outlook_period: Optional[Union[list[str], Series[str], str]]
            Reporting frequency of the data.
        quarter: Optional[Union[list[str], Series[str], str]]
            Calendar quarter associated with the reported value, where applicable.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            The reported numeric value.
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
        filter_params.append(list_to_filter("vintageRank", vintage_rank))
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
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params.append(
            list_to_filter("componentSupplierCompany", component_supplier_company)
        )
        filter_params.append(
            list_to_filter("componentSupplierCompanyId", component_supplier_company_id)
        )
        filter_params.append(
            list_to_filter(
                "componentSupplierHqGeography", component_supplier_hq_geography
            )
        )
        filter_params.append(
            list_to_filter(
                "componentSupplierHqRegionMinor", component_supplier_hq_region_minor
            )
        )
        filter_params.append(
            list_to_filter(
                "componentSupplierHqRegionMajor", component_supplier_hq_region_major
            )
        )
        filter_params.append(
            list_to_filter("componentSupplierTier", component_supplier_tier)
        )
        filter_params.append(list_to_filter("isTop20Company", is_top_20_company))
        filter_params.append(
            list_to_filter(
                "polysiliconProductionProcess", polysilicon_production_process
            )
        )
        filter_params.append(list_to_filter("waferTechnology", wafer_technology))
        filter_params.append(list_to_filter("waferSize", wafer_size))
        filter_params.append(list_to_filter("cellTechnology", cell_technology))
        filter_params.append(list_to_filter("moduleTechnology", module_technology))
        filter_params.append(list_to_filter("shipmentType", shipment_type))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("currencyDetailed", currency_detailed))
        filter_params.append(list_to_filter("outlookPeriod", outlook_period))
        filter_params.append(list_to_filter("quarter", quarter))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/supplychain/v1/pv-module-company",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_pv_inverter_market(
        self,
        *,
        vintage_rank: Optional[Union[list[str], Series[str], str]] = None,
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
        component_major: Optional[Union[list[str], Series[str], str]] = None,
        component_minor: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        region_minor: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        inverter_power_class: Optional[Union[list[str], Series[str], str]] = None,
        inverter_power_rating: Optional[Union[list[str], Series[str], str]] = None,
        inverter_type: Optional[Union[list[str], Series[str], str]] = None,
        system_type_major: Optional[Union[list[str], Series[str], str]] = None,
        system_type_minor: Optional[Union[list[str], Series[str], str]] = None,
        size_minor: Optional[Union[list[str], Series[str], str]] = None,
        size_major: Optional[Union[list[str], Series[str], str]] = None,
        siting_minor: Optional[Union[list[str], Series[str], str]] = None,
        siting_major: Optional[Union[list[str], Series[str], str]] = None,
        concept: Optional[Union[list[str], Series[str], str]] = None,
        unit_of_measurement: Optional[Union[list[str], Series[str], str]] = None,
        currency: Optional[Union[list[str], Series[str], str]] = None,
        currency_detailed: Optional[Union[list[str], Series[str], str]] = None,
        outlook_segment: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[Union[list[str], Series[str], str]] = None,
        value: Optional[Union[list[str], Series[str], str]] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the pv-inverter-market dataset (pv inverter market).

        Parameters
        ----------
        vintage_rank: Optional[Union[list[str], Series[str], str]]
            Rank of the data vintage, where 1 represents the latest available release.
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
        component_major: Optional[Union[list[str], Series[str], str]]
            Top-level supply chain category for the reported data.
        component_minor: Optional[Union[list[str], Series[str], str]]
            Specific inverter supply chain component or product category.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or market to which the reported data applies.
        region_minor: Optional[Union[list[str], Series[str], str]]
            Sub-regional grouping of the reported market geography.
        region_major: Optional[Union[list[str], Series[str], str]]
            Major regional grouping of the reported market geography.
        inverter_power_class: Optional[Union[list[str], Series[str], str]]
            Inverter classification by electrical configuration and power segment.
        inverter_power_rating: Optional[Union[list[str], Series[str], str]]
            Rated inverter power range associated with the reported data.
        inverter_type: Optional[Union[list[str], Series[str], str]]
            Inverter product configuration or system design type.
        system_type_major: Optional[Union[list[str], Series[str], str]]
            High-level end-use system category for the inverter market.
        system_type_minor: Optional[Union[list[str], Series[str], str]]
            Detailed end-use system category for the inverter market.
        size_minor: Optional[Union[list[str], Series[str], str]]
            Detailed system size range associated with the reported data.
        size_major: Optional[Union[list[str], Series[str], str]]
            High-level system size grouping associated with the reported data.
        siting_minor: Optional[Union[list[str], Series[str], str]]
            Grid connection classification for the system or installation.
        siting_major: Optional[Union[list[str], Series[str], str]]
            High-level siting classification for the system or installation.
        concept: Optional[Union[list[str], Series[str], str]]
            Metric or analytical concept represented by the reported numeric value.
        unit_of_measurement: Optional[Union[list[str], Series[str], str]]
            Unit in which the reported numeric value is measured.
        currency: Optional[Union[list[str], Series[str], str]]
            Currency denomination for monetary values, where applicable.
        currency_detailed: Optional[Union[list[str], Series[str], str]]
            Detailed currency specification, including real or nominal basis and base year where applicable.
        outlook_segment: Optional[Union[list[str], Series[str], str]]
            Analytical segmentation used to classify the reported market outlook.
        year: Optional[Union[list[str], Series[str], str]]
            Calendar year associated with the reported value.
        value: Optional[Union[list[str], Series[str], str]]
            The reported numeric value.
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
        filter_params.append(list_to_filter("vintageRank", vintage_rank))
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
        filter_params.append(list_to_filter("componentMajor", component_major))
        filter_params.append(list_to_filter("componentMinor", component_minor))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("regionMinor", region_minor))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("inverterPowerClass", inverter_power_class))
        filter_params.append(
            list_to_filter("inverterPowerRating", inverter_power_rating)
        )
        filter_params.append(list_to_filter("inverterType", inverter_type))
        filter_params.append(list_to_filter("systemTypeMajor", system_type_major))
        filter_params.append(list_to_filter("systemTypeMinor", system_type_minor))
        filter_params.append(list_to_filter("sizeMinor", size_minor))
        filter_params.append(list_to_filter("sizeMajor", size_major))
        filter_params.append(list_to_filter("sitingMinor", siting_minor))
        filter_params.append(list_to_filter("sitingMajor", siting_major))
        filter_params.append(list_to_filter("concept", concept))
        filter_params.append(list_to_filter("unitOfMeasurement", unit_of_measurement))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("currencyDetailed", currency_detailed))
        filter_params.append(list_to_filter("outlookSegment", outlook_segment))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("value", value))
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/supplychain/v1/pv-inverter-market",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_electrolyzer_orders(
        self,
        *,
        electrolyzer_order_name: Optional[Union[list[str], Series[str], str]] = None,
        date_announced: Optional[date] = None,
        date_announced_lt: Optional[date] = None,
        date_announced_lte: Optional[date] = None,
        date_announced_gt: Optional[date] = None,
        date_announced_gte: Optional[date] = None,
        year_announced: Optional[Union[list[str], Series[str], str]] = None,
        half_year_announced: Optional[Union[list[str], Series[str], str]] = None,
        quarter_announced: Optional[Union[list[str], Series[str], str]] = None,
        record_id: Optional[Union[list[str], Series[str], str]] = None,
        order_type: Optional[Union[list[str], Series[str], str]] = None,
        order_major_type: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        state_province: Optional[Union[list[str], Series[str], str]] = None,
        project_name: Optional[Union[list[str], Series[str], str]] = None,
        project_name_native: Optional[Union[list[str], Series[str], str]] = None,
        includes_power_supply: Optional[Union[list[str], Series[str], str]] = None,
        includes_separation: Optional[Union[list[str], Series[str], str]] = None,
        includes_purification: Optional[Union[list[str], Series[str], str]] = None,
        hydrogen_project_name: Optional[Union[list[str], Series[str], str]] = None,
        hydrogen_project_record_id: Optional[Union[list[str], Series[str], str]] = None,
        order_customer: Optional[Union[list[str], Series[str], str]] = None,
        order_customer_head_quarters: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        electrolyzer_supplier: Optional[Union[list[str], Series[str], str]] = None,
        electrolyzer_supplier_head_quarters: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        electrolyzer_technology_supplied: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        order_capacity_uom: Optional[Union[list[str], Series[str], str]] = None,
        total_order_capacity: Optional[Union[list[str], Series[str], str]] = None,
        electrolyzer_quantity_unit: Optional[Union[list[str], Series[str], str]] = None,
        electrolyzer_output_capacity_nm3_per_hour: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        project_output_capacity_nm3_per_hour: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        electrolyzer_manufacturing_location: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        electrolyzer_manufacturing_factory: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        electrolyzer_manufacturing_factory_id: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        electrolyzer_shipment_start_date: Optional[date] = None,
        electrolyzer_shipment_start_date_lt: Optional[date] = None,
        electrolyzer_shipment_start_date_lte: Optional[date] = None,
        electrolyzer_shipment_start_date_gt: Optional[date] = None,
        electrolyzer_shipment_start_date_gte: Optional[date] = None,
        electrolyzer_shipment_start_year: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        electrolyzer_shipment_end_date: Optional[date] = None,
        electrolyzer_shipment_end_date_lt: Optional[date] = None,
        electrolyzer_shipment_end_date_lte: Optional[date] = None,
        electrolyzer_shipment_end_date_gt: Optional[date] = None,
        electrolyzer_shipment_end_date_gte: Optional[date] = None,
        electrolyzer_shipment_end_year: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        expected_project_commissioning_date: Optional[date] = None,
        expected_project_commissioning_date_lt: Optional[date] = None,
        expected_project_commissioning_date_lte: Optional[date] = None,
        expected_project_commissioning_date_gt: Optional[date] = None,
        expected_project_commissioning_date_gte: Optional[date] = None,
        expected_project_commissioning_year: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        investment_currency: Optional[Union[list[str], Series[str], str]] = None,
        primary_investment_concept: Optional[Union[list[str], Series[str], str]] = None,
        order_investment_value_local: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        unit_price_usd_per_kilowatt: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        created_datetime: Optional[date] = None,
        created_datetime_lt: Optional[date] = None,
        created_datetime_lte: Optional[date] = None,
        created_datetime_gt: Optional[date] = None,
        created_datetime_gte: Optional[date] = None,
        modified_datetime: Optional[date] = None,
        modified_datetime_lt: Optional[date] = None,
        modified_datetime_lte: Optional[date] = None,
        modified_datetime_gt: Optional[date] = None,
        modified_datetime_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the electrolyzer-orders dataset (electrolyzer orders).

        Parameters
        ----------
        electrolyzer_order_name: Optional[Union[list[str], Series[str], str]]
            Unique name assigned to each electrolyzer order record.
        date_announced: Optional[date]
            Full date when the order was publicly announced or disclosed by a stakeholder.
        date_announced_lt: Optional[date]
            Filter records where date announced is less than the supplied date.
        date_announced_lte: Optional[date]
            Filter records where date announced is less than or equal to the supplied date.
        date_announced_gt: Optional[date]
            Filter records where date announced is greater than the supplied date.
        date_announced_gte: Optional[date]
            Filter records where date announced is greater than or equal to the supplied date.
        year_announced: Optional[Union[list[str], Series[str], str]]
            Calendar year in which the order was publicly announced (YYYY).
        half_year_announced: Optional[Union[list[str], Series[str], str]]
            Half of the calendar year in which the order was announced (H1 or H2).
        quarter_announced: Optional[Union[list[str], Series[str], str]]
            Calendar quarter in which the order was announced (Q1, Q2, Q3, Q4).
        record_id: Optional[Union[list[str], Series[str], str]]
            Unique alphanumeric identifier assigned to each record, including a team/source prefix, technology tag, concept label and sequential code (e.g., CETPROJSMR8).
        order_type: Optional[Union[list[str], Series[str], str]]
            Type of order (e.g., Project order, Framework agreement, Turbine supply only), indicating contract structure and scope.
        order_major_type: Optional[Union[list[str], Series[str], str]]
            High-level grouping of order type into broader categories such as Firm order or Non-firm order.
        technology: Optional[Union[list[str], Series[str], str]]
            Primary energy generation, storage, transmission, or conversion technology associated with the asset or facility.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or territory classification (S&P Global Ontology) for the order.
        region_major: Optional[Union[list[str], Series[str], str]]
            High-level macro-region associated with the geography (e.g., Asia Pacific, Europe, Middle East & Africa, Americas).
        state_province: Optional[Union[list[str], Series[str], str]]
            State, province, or equivalent first-level administrative division associated with the geography.
        project_name: Optional[Union[list[str], Series[str], str]]
            Official or publicly known name of the project.
        project_name_native: Optional[Union[list[str], Series[str], str]]
            Native or local-language version of the project name, if applicable.
        includes_power_supply: Optional[Union[list[str], Series[str], str]]
            Indicates whether the order includes associated power supply equipment or infrastructure.
        includes_separation: Optional[Union[list[str], Series[str], str]]
            Indicates whether the order includes gas separation systems beyond the core electrolyzer stack.
        includes_purification: Optional[Union[list[str], Series[str], str]]
            Indicates whether the order includes downstream hydrogen purification equipment.
        hydrogen_project_name: Optional[Union[list[str], Series[str], str]]
            Name of the hydrogen project associated with the order.
        hydrogen_project_record_id: Optional[Union[list[str], Series[str], str]]
            Unique alphanumeric identifier assigned to the hydrogen project.
        order_customer: Optional[Union[list[str], Series[str], str]]
            Name of the company placing the order (e.g., project developer, utility, IPP).
        order_customer_head_quarters: Optional[Union[list[str], Series[str], str]]
            Country or territory where the customer placing the order is headquartered.
        electrolyzer_supplier: Optional[Union[list[str], Series[str], str]]
            Company that received the order to supply the hydrogen electrolyzer.
        electrolyzer_supplier_head_quarters: Optional[Union[list[str], Series[str], str]]
            Headquarters location of the electrolyzer supplier.
        electrolyzer_technology_supplied: Optional[Union[list[str], Series[str], str]]
            Electrolyzer technology supplied in the order, such as Alkaline, PEM, Solid Oxide, or AEM.
        order_capacity_uom: Optional[Union[list[str], Series[str], str]]
            Unit of measurement for the capacity specified in the order.
        total_order_capacity: Optional[Union[list[str], Series[str], str]]
            Total capacity of the order, expressed in the unit given by the order capacity unit of measurement.
        electrolyzer_quantity_unit: Optional[Union[list[str], Series[str], str]]
            Number of electrolyzer units ordered.
        electrolyzer_output_capacity_nm3_per_hour: Optional[Union[list[str], Series[str], str]]
            Output capacity of a single ordered electrolyzer unit in normal cubic metres per hour.
        project_output_capacity_nm3_per_hour: Optional[Union[list[str], Series[str], str]]
            Total order size in normal cubic metres per hour (unit capacity multiplied by number of units).
        electrolyzer_manufacturing_location: Optional[Union[list[str], Series[str], str]]
            Country or territory where the electrolyzer will be manufactured.
        electrolyzer_manufacturing_factory: Optional[Union[list[str], Series[str], str]]
            Name of the factory that manufactured the electrolyzer.
        electrolyzer_manufacturing_factory_id: Optional[Union[list[str], Series[str], str]]
            Unique identifier of the electrolyzer factory, linking orders to factory records.
        electrolyzer_shipment_start_date: Optional[date]
            Date when shipment of the electrolyzer began.
        electrolyzer_shipment_start_date_lt: Optional[date]
            Filter records where electrolyzer shipment start date is less than the supplied date.
        electrolyzer_shipment_start_date_lte: Optional[date]
            Filter records where electrolyzer shipment start date is less than or equal to the supplied date.
        electrolyzer_shipment_start_date_gt: Optional[date]
            Filter records where electrolyzer shipment start date is greater than the supplied date.
        electrolyzer_shipment_start_date_gte: Optional[date]
            Filter records where electrolyzer shipment start date is greater than or equal to the supplied date.
        electrolyzer_shipment_start_year: Optional[Union[list[str], Series[str], str]]
            Calendar year when shipment of the electrolyzer began.
        electrolyzer_shipment_end_date: Optional[date]
            Date when shipment of the electrolyzer was completed.
        electrolyzer_shipment_end_date_lt: Optional[date]
            Filter records where electrolyzer shipment end date is less than the supplied date.
        electrolyzer_shipment_end_date_lte: Optional[date]
            Filter records where electrolyzer shipment end date is less than or equal to the supplied date.
        electrolyzer_shipment_end_date_gt: Optional[date]
            Filter records where electrolyzer shipment end date is greater than the supplied date.
        electrolyzer_shipment_end_date_gte: Optional[date]
            Filter records where electrolyzer shipment end date is greater than or equal to the supplied date.
        electrolyzer_shipment_end_year: Optional[Union[list[str], Series[str], str]]
            Calendar year when shipment of the electrolyzer was completed.
        expected_project_commissioning_date: Optional[date]
            Date by which the project using the supplied equipment is expected to be commissioned.
        expected_project_commissioning_date_lt: Optional[date]
            Filter records where expected project commissioning date is less than the supplied date.
        expected_project_commissioning_date_lte: Optional[date]
            Filter records where expected project commissioning date is less than or equal to the supplied date.
        expected_project_commissioning_date_gt: Optional[date]
            Filter records where expected project commissioning date is greater than the supplied date.
        expected_project_commissioning_date_gte: Optional[date]
            Filter records where expected project commissioning date is greater than or equal to the supplied date.
        expected_project_commissioning_year: Optional[Union[list[str], Series[str], str]]
            Calendar year by which the project using the supplied equipment is expected to be commissioned.
        investment_currency: Optional[Union[list[str], Series[str], str]]
            Official currency associated with the total investment value.
        primary_investment_concept: Optional[Union[list[str], Series[str], str]]
            Scope of the reported investment value (e.g., equipment only or total project cost).
        order_investment_value_local: Optional[Union[list[str], Series[str], str]]
            Total investment value of the order in local currency.
        unit_price_usd_per_kilowatt: Optional[Union[list[str], Series[str], str]]
            Price of the electrolyzer in US dollars per kilowatt of rated power input.
        created_datetime: Optional[date]
            Timestamp when the record was first created.
        created_datetime_lt: Optional[date]
            Filter records where created datetime is less than the supplied date.
        created_datetime_lte: Optional[date]
            Filter records where created datetime is less than or equal to the supplied date.
        created_datetime_gt: Optional[date]
            Filter records where created datetime is greater than the supplied date.
        created_datetime_gte: Optional[date]
            Filter records where created datetime is greater than or equal to the supplied date.
        modified_datetime: Optional[date]
            Timestamp of the most recent update to the record.
        modified_datetime_lt: Optional[date]
            Filter records where modified datetime is less than the supplied date.
        modified_datetime_lte: Optional[date]
            Filter records where modified datetime is less than or equal to the supplied date.
        modified_datetime_gt: Optional[date]
            Filter records where modified datetime is greater than the supplied date.
        modified_datetime_gte: Optional[date]
            Filter records where modified datetime is greater than or equal to the supplied date.
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
        filter_params.append(
            list_to_filter("electrolyzerOrderName", electrolyzer_order_name)
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
        filter_params.append(list_to_filter("yearAnnounced", year_announced))
        filter_params.append(list_to_filter("halfYearAnnounced", half_year_announced))
        filter_params.append(list_to_filter("quarterAnnounced", quarter_announced))
        filter_params.append(list_to_filter("recordId", record_id))
        filter_params.append(list_to_filter("orderType", order_type))
        filter_params.append(list_to_filter("orderMajorType", order_major_type))
        filter_params.append(list_to_filter("technology", technology))
        filter_params.append(list_to_filter("geography", geography))
        filter_params.append(list_to_filter("regionMajor", region_major))
        filter_params.append(list_to_filter("stateProvince", state_province))
        filter_params.append(list_to_filter("projectName", project_name))
        filter_params.append(list_to_filter("projectNameNative", project_name_native))
        filter_params.append(
            list_to_filter("includesPowerSupply", includes_power_supply)
        )
        filter_params.append(list_to_filter("includesSeparation", includes_separation))
        filter_params.append(
            list_to_filter("includesPurification", includes_purification)
        )
        filter_params.append(
            list_to_filter("hydrogenProjectName", hydrogen_project_name)
        )
        filter_params.append(
            list_to_filter("hydrogenProjectRecordId", hydrogen_project_record_id)
        )
        filter_params.append(list_to_filter("orderCustomer", order_customer))
        filter_params.append(
            list_to_filter("orderCustomerHeadQuarters", order_customer_head_quarters)
        )
        filter_params.append(
            list_to_filter("electrolyzerSupplier", electrolyzer_supplier)
        )
        filter_params.append(
            list_to_filter(
                "electrolyzerSupplierHeadQuarters", electrolyzer_supplier_head_quarters
            )
        )
        filter_params.append(
            list_to_filter(
                "electrolyzerTechnologySupplied", electrolyzer_technology_supplied
            )
        )
        filter_params.append(list_to_filter("orderCapacityUom", order_capacity_uom))
        filter_params.append(list_to_filter("totalOrderCapacity", total_order_capacity))
        filter_params.append(
            list_to_filter("electrolyzerQuantityUnit", electrolyzer_quantity_unit)
        )
        filter_params.append(
            list_to_filter(
                "electrolyzerOutputCapacityNm3PerHour",
                electrolyzer_output_capacity_nm3_per_hour,
            )
        )
        filter_params.append(
            list_to_filter(
                "projectOutputCapacityNm3PerHour", project_output_capacity_nm3_per_hour
            )
        )
        filter_params.append(
            list_to_filter(
                "electrolyzerManufacturingLocation", electrolyzer_manufacturing_location
            )
        )
        filter_params.append(
            list_to_filter(
                "electrolyzerManufacturingFactory", electrolyzer_manufacturing_factory
            )
        )
        filter_params.append(
            list_to_filter(
                "electrolyzerManufacturingFactoryId",
                electrolyzer_manufacturing_factory_id,
            )
        )
        filter_params.append(
            list_to_filter(
                "electrolyzerShipmentStartDate", electrolyzer_shipment_start_date
            )
        )
        if electrolyzer_shipment_start_date_lt is not None:
            filter_params.append(
                f'electrolyzerShipmentStartDate < "{electrolyzer_shipment_start_date_lt}"'
            )
        if electrolyzer_shipment_start_date_lte is not None:
            filter_params.append(
                f'electrolyzerShipmentStartDate <= "{electrolyzer_shipment_start_date_lte}"'
            )
        if electrolyzer_shipment_start_date_gt is not None:
            filter_params.append(
                f'electrolyzerShipmentStartDate > "{electrolyzer_shipment_start_date_gt}"'
            )
        if electrolyzer_shipment_start_date_gte is not None:
            filter_params.append(
                f'electrolyzerShipmentStartDate >= "{electrolyzer_shipment_start_date_gte}"'
            )
        filter_params.append(
            list_to_filter(
                "electrolyzerShipmentStartYear", electrolyzer_shipment_start_year
            )
        )
        filter_params.append(
            list_to_filter(
                "electrolyzerShipmentEndDate", electrolyzer_shipment_end_date
            )
        )
        if electrolyzer_shipment_end_date_lt is not None:
            filter_params.append(
                f'electrolyzerShipmentEndDate < "{electrolyzer_shipment_end_date_lt}"'
            )
        if electrolyzer_shipment_end_date_lte is not None:
            filter_params.append(
                f'electrolyzerShipmentEndDate <= "{electrolyzer_shipment_end_date_lte}"'
            )
        if electrolyzer_shipment_end_date_gt is not None:
            filter_params.append(
                f'electrolyzerShipmentEndDate > "{electrolyzer_shipment_end_date_gt}"'
            )
        if electrolyzer_shipment_end_date_gte is not None:
            filter_params.append(
                f'electrolyzerShipmentEndDate >= "{electrolyzer_shipment_end_date_gte}"'
            )
        filter_params.append(
            list_to_filter(
                "electrolyzerShipmentEndYear", electrolyzer_shipment_end_year
            )
        )
        filter_params.append(
            list_to_filter(
                "expectedProjectCommissioningDate", expected_project_commissioning_date
            )
        )
        if expected_project_commissioning_date_lt is not None:
            filter_params.append(
                f'expectedProjectCommissioningDate < "{expected_project_commissioning_date_lt}"'
            )
        if expected_project_commissioning_date_lte is not None:
            filter_params.append(
                f'expectedProjectCommissioningDate <= "{expected_project_commissioning_date_lte}"'
            )
        if expected_project_commissioning_date_gt is not None:
            filter_params.append(
                f'expectedProjectCommissioningDate > "{expected_project_commissioning_date_gt}"'
            )
        if expected_project_commissioning_date_gte is not None:
            filter_params.append(
                f'expectedProjectCommissioningDate >= "{expected_project_commissioning_date_gte}"'
            )
        filter_params.append(
            list_to_filter(
                "expectedProjectCommissioningYear", expected_project_commissioning_year
            )
        )
        filter_params.append(list_to_filter("investmentCurrency", investment_currency))
        filter_params.append(
            list_to_filter("primaryInvestmentConcept", primary_investment_concept)
        )
        filter_params.append(
            list_to_filter("orderInvestmentValueLocal", order_investment_value_local)
        )
        filter_params.append(
            list_to_filter("unitPriceUsdPerKilowatt", unit_price_usd_per_kilowatt)
        )
        filter_params.append(list_to_filter("createdDatetime", created_datetime))
        if created_datetime_lt is not None:
            filter_params.append(f'createdDatetime < "{created_datetime_lt}"')
        if created_datetime_lte is not None:
            filter_params.append(f'createdDatetime <= "{created_datetime_lte}"')
        if created_datetime_gt is not None:
            filter_params.append(f'createdDatetime > "{created_datetime_gt}"')
        if created_datetime_gte is not None:
            filter_params.append(f'createdDatetime >= "{created_datetime_gte}"')
        filter_params.append(list_to_filter("modifiedDatetime", modified_datetime))
        if modified_datetime_lt is not None:
            filter_params.append(f'modifiedDatetime < "{modified_datetime_lt}"')
        if modified_datetime_lte is not None:
            filter_params.append(f'modifiedDatetime <= "{modified_datetime_lte}"')
        if modified_datetime_gt is not None:
            filter_params.append(f'modifiedDatetime > "{modified_datetime_gt}"')
        if modified_datetime_gte is not None:
            filter_params.append(f'modifiedDatetime >= "{modified_datetime_gte}"')
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/supplychain/v1/electrolyzer-orders",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_wind_turbine_orders(
        self,
        *,
        wind_turbine_order_name: Optional[Union[list[str], Series[str], str]] = None,
        date_announced: Optional[date] = None,
        date_announced_lt: Optional[date] = None,
        date_announced_lte: Optional[date] = None,
        date_announced_gt: Optional[date] = None,
        date_announced_gte: Optional[date] = None,
        year_announced: Optional[Union[list[str], Series[str], str]] = None,
        half_year_announced: Optional[Union[list[str], Series[str], str]] = None,
        quarter_announced: Optional[Union[list[str], Series[str], str]] = None,
        record_id: Optional[Union[list[str], Series[str], str]] = None,
        order_type: Optional[Union[list[str], Series[str], str]] = None,
        order_major_type: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        state_province: Optional[Union[list[str], Series[str], str]] = None,
        project_name: Optional[Union[list[str], Series[str], str]] = None,
        project_name_native: Optional[Union[list[str], Series[str], str]] = None,
        order_customer: Optional[Union[list[str], Series[str], str]] = None,
        wind_turbine_supplier_head_quarters: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        wind_turbine_supplier: Optional[Union[list[str], Series[str], str]] = None,
        order_capacity_uom: Optional[Union[list[str], Series[str], str]] = None,
        wind_turbine_configuration: Optional[Union[list[str], Series[str], str]] = None,
        wind_turbine_model: Optional[Union[list[str], Series[str], str]] = None,
        wind_turbine_platform: Optional[Union[list[str], Series[str], str]] = None,
        wind_turbine_quantity_unit: Optional[Union[list[str], Series[str], str]] = None,
        wind_turbine_nominal_rating_uom_megawatt: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        wind_turbine_rotor_diameter_meter: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        wind_turbine_hub_height_meter: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        total_order_capacity: Optional[Union[list[str], Series[str], str]] = None,
        wind_turbine_swept_area_square_meter: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        wind_turbine_specific_power_watts_per_square_meter: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        wind_turbine_nominal_rating_segment: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        wind_turbine_rotor_diameter_segment: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        wind_turbine_specific_power_segment: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        wind_turbine_iec_class: Optional[Union[list[str], Series[str], str]] = None,
        wind_turbine_drivetrain_type: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        wind_turbine_generator_type: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        date_expected_wind_turbine_shipment: Optional[date] = None,
        date_expected_wind_turbine_shipment_lt: Optional[date] = None,
        date_expected_wind_turbine_shipment_lte: Optional[date] = None,
        date_expected_wind_turbine_shipment_gt: Optional[date] = None,
        date_expected_wind_turbine_shipment_gte: Optional[date] = None,
        year_expected_wind_turbine_shipment: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        date_expected_wind_turbine_commissioning: Optional[date] = None,
        date_expected_wind_turbine_commissioning_lt: Optional[date] = None,
        date_expected_wind_turbine_commissioning_lte: Optional[date] = None,
        date_expected_wind_turbine_commissioning_gt: Optional[date] = None,
        date_expected_wind_turbine_commissioning_gte: Optional[date] = None,
        year_expected_wind_turbine_commissioning: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        oandm_package: Optional[Union[list[str], Series[str], str]] = None,
        oandm_duration_year: Optional[Union[list[str], Series[str], str]] = None,
        investment_currency: Optional[Union[list[str], Series[str], str]] = None,
        created_datetime: Optional[date] = None,
        created_datetime_lt: Optional[date] = None,
        created_datetime_lte: Optional[date] = None,
        created_datetime_gt: Optional[date] = None,
        created_datetime_gte: Optional[date] = None,
        modified_datetime: Optional[date] = None,
        modified_datetime_lt: Optional[date] = None,
        modified_datetime_lte: Optional[date] = None,
        modified_datetime_gt: Optional[date] = None,
        modified_datetime_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the wind-turbine-orders dataset (wind turbine orders).

        Parameters
        ----------
        wind_turbine_order_name: Optional[Union[list[str], Series[str], str]]
            Unique identifier assigned to each wind turbine order record.
        date_announced: Optional[date]
            Full date when the order was publicly announced or disclosed by a stakeholder.
        date_announced_lt: Optional[date]
            Filter records where date announced is less than the supplied date.
        date_announced_lte: Optional[date]
            Filter records where date announced is less than or equal to the supplied date.
        date_announced_gt: Optional[date]
            Filter records where date announced is greater than the supplied date.
        date_announced_gte: Optional[date]
            Filter records where date announced is greater than or equal to the supplied date.
        year_announced: Optional[Union[list[str], Series[str], str]]
            Calendar year in which the order was publicly announced (YYYY).
        half_year_announced: Optional[Union[list[str], Series[str], str]]
            Half of the calendar year in which the order was announced (H1 or H2).
        quarter_announced: Optional[Union[list[str], Series[str], str]]
            Calendar quarter in which the order was announced (Q1, Q2, Q3, Q4).
        record_id: Optional[Union[list[str], Series[str], str]]
            Unique alphanumeric identifier assigned to each record, including a team/source prefix, technology tag, concept label and sequential code (e.g., CETPROJSMR8).
        order_type: Optional[Union[list[str], Series[str], str]]
            Type of order (e.g., Project order, Framework agreement, Turbine supply only), indicating contract structure and scope.
        order_major_type: Optional[Union[list[str], Series[str], str]]
            High-level grouping of order type into broader categories such as Firm order or Non-firm order.
        technology: Optional[Union[list[str], Series[str], str]]
            Primary energy generation, storage, transmission, or conversion technology associated with the asset or facility.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or territory classification (S&P Global Ontology) for the order.
        region_major: Optional[Union[list[str], Series[str], str]]
            High-level macro-region associated with the geography (e.g., Asia Pacific, Europe, Middle East & Africa, Americas).
        state_province: Optional[Union[list[str], Series[str], str]]
            State, province, or equivalent first-level administrative division associated with the geography.
        project_name: Optional[Union[list[str], Series[str], str]]
            Official or publicly known name of the project.
        project_name_native: Optional[Union[list[str], Series[str], str]]
            Native or local-language version of the project name, if applicable.
        order_customer: Optional[Union[list[str], Series[str], str]]
            Name of the company placing the turbine order (e.g., project developer, utility, IPP).
        wind_turbine_supplier_head_quarters: Optional[Union[list[str], Series[str], str]]
            Country where the wind turbine supplier is headquartered.
        wind_turbine_supplier: Optional[Union[list[str], Series[str], str]]
            Name of the OEM supplying the wind turbines under the order.
        order_capacity_uom: Optional[Union[list[str], Series[str], str]]
            Unit of measurement for the capacity specified in the order.
        wind_turbine_configuration: Optional[Union[list[str], Series[str], str]]
            Unique combination of rotor diameter and nominal rating of a wind turbine.
        wind_turbine_model: Optional[Union[list[str], Series[str], str]]
            Official model designation of the wind turbine (e.g., Vestas V150-4.2 MW).
        wind_turbine_platform: Optional[Union[list[str], Series[str], str]]
            Standardized turbine platform family to which the model belongs (e.g., GE Cypress, Nordex Delta4000).
        wind_turbine_quantity_unit: Optional[Union[list[str], Series[str], str]]
            Number of wind turbine units in the order.
        wind_turbine_nominal_rating_uom_megawatt: Optional[Union[list[str], Series[str], str]]
            Nominal power rating of the wind turbine configuration in megawatts.
        wind_turbine_rotor_diameter_meter: Optional[Union[list[str], Series[str], str]]
            Rotor diameter of the turbine in meters.
        wind_turbine_hub_height_meter: Optional[Union[list[str], Series[str], str]]
            Hub (tower) height of the turbine in meters.
        total_order_capacity: Optional[Union[list[str], Series[str], str]]
            Total order size in megawatts.
        wind_turbine_swept_area_square_meter: Optional[Union[list[str], Series[str], str]]
            Rotor swept area in square meters, calculated from rotor diameter.
        wind_turbine_specific_power_watts_per_square_meter: Optional[Union[list[str], Series[str], str]]
            Specific power of the ordered turbine in watts per square meter of swept area.
        wind_turbine_nominal_rating_segment: Optional[Union[list[str], Series[str], str]]
            Nominal power rating bucket (e.g., 3-3.99 MW, 4-4.99 MW).
        wind_turbine_rotor_diameter_segment: Optional[Union[list[str], Series[str], str]]
            Rotor diameter bucket (e.g., 120-139 m, 140-159 m).
        wind_turbine_specific_power_segment: Optional[Union[list[str], Series[str], str]]
            Specific power bucket (e.g., <200, 200-229, 230-259 W/m2).
        wind_turbine_iec_class: Optional[Union[list[str], Series[str], str]]
            IEC wind class of the ordered turbine (e.g., IEC I, IEC II, IEC III, IEC S).
        wind_turbine_drivetrain_type: Optional[Union[list[str], Series[str], str]]
            Drivetrain architecture of the ordered turbine (e.g., geared, hybrid, direct drive).
        wind_turbine_generator_type: Optional[Union[list[str], Series[str], str]]
            Generator technology used in the ordered turbine (e.g., DFIG, synchronous, permanent magnet).
        date_expected_wind_turbine_shipment: Optional[date]
            Date when turbine shipments to the project site are expected to start.
        date_expected_wind_turbine_shipment_lt: Optional[date]
            Filter records where date expected wind turbine shipment is less than the supplied date.
        date_expected_wind_turbine_shipment_lte: Optional[date]
            Filter records where date expected wind turbine shipment is less than or equal to the supplied date.
        date_expected_wind_turbine_shipment_gt: Optional[date]
            Filter records where date expected wind turbine shipment is greater than the supplied date.
        date_expected_wind_turbine_shipment_gte: Optional[date]
            Filter records where date expected wind turbine shipment is greater than or equal to the supplied date.
        year_expected_wind_turbine_shipment: Optional[Union[list[str], Series[str], str]]
            Calendar year when turbine shipments to the project site are expected to start.
        date_expected_wind_turbine_commissioning: Optional[date]
            Date when the turbines are expected to be commissioned; proxies the project's expected COD.
        date_expected_wind_turbine_commissioning_lt: Optional[date]
            Filter records where date expected wind turbine commissioning is less than the supplied date.
        date_expected_wind_turbine_commissioning_lte: Optional[date]
            Filter records where date expected wind turbine commissioning is less than or equal to the supplied date.
        date_expected_wind_turbine_commissioning_gt: Optional[date]
            Filter records where date expected wind turbine commissioning is greater than the supplied date.
        date_expected_wind_turbine_commissioning_gte: Optional[date]
            Filter records where date expected wind turbine commissioning is greater than or equal to the supplied date.
        year_expected_wind_turbine_commissioning: Optional[Union[list[str], Series[str], str]]
            Calendar year when the turbines are expected to be commissioned; proxies the project's expected COD.
        oandm_package: Optional[Union[list[str], Series[str], str]]
            Type of operations and maintenance (O&M) package included in the supply agreement (e.g., AOM 4000, AOM 5000).
        oandm_duration_year: Optional[Union[list[str], Series[str], str]]
            Duration of the O&M contract in years.
        investment_currency: Optional[Union[list[str], Series[str], str]]
            Official currency associated with the total investment value.
        created_datetime: Optional[date]
            Timestamp when the record was first created.
        created_datetime_lt: Optional[date]
            Filter records where created datetime is less than the supplied date.
        created_datetime_lte: Optional[date]
            Filter records where created datetime is less than or equal to the supplied date.
        created_datetime_gt: Optional[date]
            Filter records where created datetime is greater than the supplied date.
        created_datetime_gte: Optional[date]
            Filter records where created datetime is greater than or equal to the supplied date.
        modified_datetime: Optional[date]
            Timestamp of the most recent update to the record.
        modified_datetime_lt: Optional[date]
            Filter records where modified datetime is less than the supplied date.
        modified_datetime_lte: Optional[date]
            Filter records where modified datetime is less than or equal to the supplied date.
        modified_datetime_gt: Optional[date]
            Filter records where modified datetime is greater than the supplied date.
        modified_datetime_gte: Optional[date]
            Filter records where modified datetime is greater than or equal to the supplied date.
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
        filter_params.append(
            list_to_filter("windTurbineOrderName", wind_turbine_order_name)
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
        filter_params.append(list_to_filter("yearAnnounced", year_announced))
        filter_params.append(list_to_filter("halfYearAnnounced", half_year_announced))
        filter_params.append(list_to_filter("quarterAnnounced", quarter_announced))
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
        filter_params.append(
            list_to_filter("windTurbineSupplierHQ", wind_turbine_supplier_head_quarters)
        )
        filter_params.append(
            list_to_filter("windTurbineSupplier", wind_turbine_supplier)
        )
        filter_params.append(list_to_filter("orderCapacityUOM", order_capacity_uom))
        filter_params.append(
            list_to_filter("windTurbineConfiguration", wind_turbine_configuration)
        )
        filter_params.append(list_to_filter("windTurbineModel", wind_turbine_model))
        filter_params.append(
            list_to_filter("windTurbinePlatform", wind_turbine_platform)
        )
        filter_params.append(
            list_to_filter("windTurbineQuantityUnit", wind_turbine_quantity_unit)
        )
        filter_params.append(
            list_to_filter(
                "windTurbineNominalRatingUOMMegawatt",
                wind_turbine_nominal_rating_uom_megawatt,
            )
        )
        filter_params.append(
            list_to_filter(
                "windTurbineRotorDiameterMeter", wind_turbine_rotor_diameter_meter
            )
        )
        filter_params.append(
            list_to_filter("windTurbineHubHeightMeter", wind_turbine_hub_height_meter)
        )
        filter_params.append(list_to_filter("totalOrderCapacity", total_order_capacity))
        filter_params.append(
            list_to_filter(
                "windTurbineSweptAreaSquareMeter", wind_turbine_swept_area_square_meter
            )
        )
        filter_params.append(
            list_to_filter(
                "windTurbineSpecificPowerWattsPerSquareMeter",
                wind_turbine_specific_power_watts_per_square_meter,
            )
        )
        filter_params.append(
            list_to_filter(
                "windTurbineNominalRatingSegment", wind_turbine_nominal_rating_segment
            )
        )
        filter_params.append(
            list_to_filter(
                "windTurbineRotorDiameterSegment", wind_turbine_rotor_diameter_segment
            )
        )
        filter_params.append(
            list_to_filter(
                "windTurbineSpecificPowerSegment", wind_turbine_specific_power_segment
            )
        )
        filter_params.append(
            list_to_filter("windTurbineIecClass", wind_turbine_iec_class)
        )
        filter_params.append(
            list_to_filter("windTurbineDrivetrainType", wind_turbine_drivetrain_type)
        )
        filter_params.append(
            list_to_filter("windTurbineGeneratorType", wind_turbine_generator_type)
        )
        filter_params.append(
            list_to_filter(
                "dateExpectedWindTurbineShipment", date_expected_wind_turbine_shipment
            )
        )
        if date_expected_wind_turbine_shipment_lt is not None:
            filter_params.append(
                f'dateExpectedWindTurbineShipment < "{date_expected_wind_turbine_shipment_lt}"'
            )
        if date_expected_wind_turbine_shipment_lte is not None:
            filter_params.append(
                f'dateExpectedWindTurbineShipment <= "{date_expected_wind_turbine_shipment_lte}"'
            )
        if date_expected_wind_turbine_shipment_gt is not None:
            filter_params.append(
                f'dateExpectedWindTurbineShipment > "{date_expected_wind_turbine_shipment_gt}"'
            )
        if date_expected_wind_turbine_shipment_gte is not None:
            filter_params.append(
                f'dateExpectedWindTurbineShipment >= "{date_expected_wind_turbine_shipment_gte}"'
            )
        filter_params.append(
            list_to_filter(
                "yearExpectedWindTurbineShipment", year_expected_wind_turbine_shipment
            )
        )
        filter_params.append(
            list_to_filter(
                "dateExpectedWindTurbineCommissioning",
                date_expected_wind_turbine_commissioning,
            )
        )
        if date_expected_wind_turbine_commissioning_lt is not None:
            filter_params.append(
                f'dateExpectedWindTurbineCommissioning < "{date_expected_wind_turbine_commissioning_lt}"'
            )
        if date_expected_wind_turbine_commissioning_lte is not None:
            filter_params.append(
                f'dateExpectedWindTurbineCommissioning <= "{date_expected_wind_turbine_commissioning_lte}"'
            )
        if date_expected_wind_turbine_commissioning_gt is not None:
            filter_params.append(
                f'dateExpectedWindTurbineCommissioning > "{date_expected_wind_turbine_commissioning_gt}"'
            )
        if date_expected_wind_turbine_commissioning_gte is not None:
            filter_params.append(
                f'dateExpectedWindTurbineCommissioning >= "{date_expected_wind_turbine_commissioning_gte}"'
            )
        filter_params.append(
            list_to_filter(
                "yearExpectedWindTurbineCommissioning",
                year_expected_wind_turbine_commissioning,
            )
        )
        filter_params.append(list_to_filter("oandMPackage", oandm_package))
        filter_params.append(list_to_filter("oandMDurationYear", oandm_duration_year))
        filter_params.append(list_to_filter("investmentCurrency", investment_currency))
        filter_params.append(list_to_filter("createdTime", created_datetime))
        if created_datetime_lt is not None:
            filter_params.append(f'createdTime < "{created_datetime_lt}"')
        if created_datetime_lte is not None:
            filter_params.append(f'createdTime <= "{created_datetime_lte}"')
        if created_datetime_gt is not None:
            filter_params.append(f'createdTime > "{created_datetime_gt}"')
        if created_datetime_gte is not None:
            filter_params.append(f'createdTime >= "{created_datetime_gte}"')
        filter_params.append(list_to_filter("modifiedTime", modified_datetime))
        if modified_datetime_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_datetime_lt}"')
        if modified_datetime_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_datetime_lte}"')
        if modified_datetime_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_datetime_gt}"')
        if modified_datetime_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_datetime_gte}"')
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/supplychain/v1/wind-turbine-orders",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    def get_battery_orders(
        self,
        *,
        battery_order_name: Optional[Union[list[str], Series[str], str]] = None,
        date_announced: Optional[date] = None,
        date_announced_lt: Optional[date] = None,
        date_announced_lte: Optional[date] = None,
        date_announced_gt: Optional[date] = None,
        date_announced_gte: Optional[date] = None,
        year_announced: Optional[Union[list[str], Series[str], str]] = None,
        half_year_announced: Optional[Union[list[str], Series[str], str]] = None,
        quarter_announced: Optional[Union[list[str], Series[str], str]] = None,
        record_id: Optional[Union[list[str], Series[str], str]] = None,
        order_type: Optional[Union[list[str], Series[str], str]] = None,
        order_major_type: Optional[Union[list[str], Series[str], str]] = None,
        technology: Optional[Union[list[str], Series[str], str]] = None,
        geography: Optional[Union[list[str], Series[str], str]] = None,
        region_major: Optional[Union[list[str], Series[str], str]] = None,
        state_province: Optional[Union[list[str], Series[str], str]] = None,
        project_name: Optional[Union[list[str], Series[str], str]] = None,
        project_name_native: Optional[Union[list[str], Series[str], str]] = None,
        order_customer: Optional[Union[list[str], Series[str], str]] = None,
        order_customer_head_quarters: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        order_customer_type: Optional[Union[list[str], Series[str], str]] = None,
        battery_supplier: Optional[Union[list[str], Series[str], str]] = None,
        battery_supplier_head_quarters: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        battery_supplier_type: Optional[Union[list[str], Series[str], str]] = None,
        battery_product_type_supplied: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        battery_product_supplied: Optional[Union[list[str], Series[str], str]] = None,
        battery_cell_technology: Optional[Union[list[str], Series[str], str]] = None,
        battery_cell_technology_major: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        battery_manufacturing_location: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        battery_manufacturing_factory: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        order_capacity_uom: Optional[Union[list[str], Series[str], str]] = None,
        total_order_capacity: Optional[Union[list[str], Series[str], str]] = None,
        battery_shipment_start_date: Optional[date] = None,
        battery_shipment_start_date_lt: Optional[date] = None,
        battery_shipment_start_date_lte: Optional[date] = None,
        battery_shipment_start_date_gt: Optional[date] = None,
        battery_shipment_start_date_gte: Optional[date] = None,
        battery_shipment_start_year: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        battery_shipment_end_date: Optional[date] = None,
        battery_shipment_end_date_lt: Optional[date] = None,
        battery_shipment_end_date_lte: Optional[date] = None,
        battery_shipment_end_date_gt: Optional[date] = None,
        battery_shipment_end_date_gte: Optional[date] = None,
        battery_shipment_end_year: Optional[Union[list[str], Series[str], str]] = None,
        expected_project_commissioning_date: Optional[date] = None,
        expected_project_commissioning_date_lt: Optional[date] = None,
        expected_project_commissioning_date_lte: Optional[date] = None,
        expected_project_commissioning_date_gt: Optional[date] = None,
        expected_project_commissioning_date_gte: Optional[date] = None,
        expected_project_commissioning_year: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        investment_currency: Optional[Union[list[str], Series[str], str]] = None,
        created_datetime: Optional[date] = None,
        created_datetime_lt: Optional[date] = None,
        created_datetime_lte: Optional[date] = None,
        created_datetime_gt: Optional[date] = None,
        created_datetime_gte: Optional[date] = None,
        modified_datetime: Optional[date] = None,
        modified_datetime_lt: Optional[date] = None,
        modified_datetime_lte: Optional[date] = None,
        modified_datetime_gt: Optional[date] = None,
        modified_datetime_gte: Optional[date] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 5000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """Get data from the battery-orders dataset (battery orders).

        Parameters
        ----------
        battery_order_name: Optional[Union[list[str], Series[str], str]]
            Name assigned to the battery equipment supply order.
        date_announced: Optional[date]
            Full date when the order was publicly announced or disclosed by a stakeholder.
        date_announced_lt: Optional[date]
            Filter records where date announced is less than the supplied date.
        date_announced_lte: Optional[date]
            Filter records where date announced is less than or equal to the supplied date.
        date_announced_gt: Optional[date]
            Filter records where date announced is greater than the supplied date.
        date_announced_gte: Optional[date]
            Filter records where date announced is greater than or equal to the supplied date.
        year_announced: Optional[Union[list[str], Series[str], str]]
            Calendar year in which the order was publicly announced (YYYY).
        half_year_announced: Optional[Union[list[str], Series[str], str]]
            Half of the calendar year in which the order was announced (H1 or H2).
        quarter_announced: Optional[Union[list[str], Series[str], str]]
            Calendar quarter in which the order was announced (Q1, Q2, Q3, Q4).
        record_id: Optional[Union[list[str], Series[str], str]]
            Unique alphanumeric identifier assigned to each record, including a team/source prefix, technology tag, concept label and sequential code (e.g., CETPROJSMR8).
        order_type: Optional[Union[list[str], Series[str], str]]
            Type of order (e.g., Project order, Framework agreement, Turbine supply only), indicating contract structure and scope.
        order_major_type: Optional[Union[list[str], Series[str], str]]
            High-level grouping of order type into broader categories such as Firm order or Non-firm order.
        technology: Optional[Union[list[str], Series[str], str]]
            Primary energy generation, storage, transmission, or conversion technology associated with the asset or facility.
        geography: Optional[Union[list[str], Series[str], str]]
            Country or territory classification (S&P Global Ontology) for the order.
        region_major: Optional[Union[list[str], Series[str], str]]
            High-level macro-region associated with the geography (e.g., Asia Pacific, Europe, Middle East & Africa, Americas).
        state_province: Optional[Union[list[str], Series[str], str]]
            State, province, or equivalent first-level administrative division associated with the geography.
        project_name: Optional[Union[list[str], Series[str], str]]
            Official or publicly known name of the project.
        project_name_native: Optional[Union[list[str], Series[str], str]]
            Native or local-language version of the project name, if applicable.
        order_customer: Optional[Union[list[str], Series[str], str]]
            Name of the company placing the order (e.g., project developer, utility, IPP).
        order_customer_head_quarters: Optional[Union[list[str], Series[str], str]]
            Country or territory where the customer placing the order is headquartered.
        order_customer_type: Optional[Union[list[str], Series[str], str]]
            Type of customer purchasing the equipment.
        battery_supplier: Optional[Union[list[str], Series[str], str]]
            Name of the company supplying battery equipment under the order.
        battery_supplier_head_quarters: Optional[Union[list[str], Series[str], str]]
            Country or territory where the battery supplier is headquartered.
        battery_supplier_type: Optional[Union[list[str], Series[str], str]]
            Role of the supplier, such as cell manufacturer, pack assembler, or system integrator.
        battery_product_type_supplied: Optional[Union[list[str], Series[str], str]]
            Category of battery product supplied, such as cells, modules, packs, containers, or turnkey systems.
        battery_product_supplied: Optional[Union[list[str], Series[str], str]]
            Name of the specific battery product supplied under the order.
        battery_cell_technology: Optional[Union[list[str], Series[str], str]]
            Battery cell technology supplied under the order.
        battery_cell_technology_major: Optional[Union[list[str], Series[str], str]]
            High-level classification of the battery cell technology.
        battery_manufacturing_location: Optional[Union[list[str], Series[str], str]]
            Country or territory where the battery product was manufactured.
        battery_manufacturing_factory: Optional[Union[list[str], Series[str], str]]
            Name of the factory that manufactured the battery product.
        order_capacity_uom: Optional[Union[list[str], Series[str], str]]
            Unit of measurement for the capacity specified in the order.
        total_order_capacity: Optional[Union[list[str], Series[str], str]]
            Total order size in megawatt hours (MWh), per the order capacity unit of measurement.
        battery_shipment_start_date: Optional[date]
            Date when shipment of the battery equipment began.
        battery_shipment_start_date_lt: Optional[date]
            Filter records where battery shipment start date is less than the supplied date.
        battery_shipment_start_date_lte: Optional[date]
            Filter records where battery shipment start date is less than or equal to the supplied date.
        battery_shipment_start_date_gt: Optional[date]
            Filter records where battery shipment start date is greater than the supplied date.
        battery_shipment_start_date_gte: Optional[date]
            Filter records where battery shipment start date is greater than or equal to the supplied date.
        battery_shipment_start_year: Optional[Union[list[str], Series[str], str]]
            Calendar year when shipment of the battery equipment began.
        battery_shipment_end_date: Optional[date]
            Date when shipment of the battery equipment was completed.
        battery_shipment_end_date_lt: Optional[date]
            Filter records where battery shipment end date is less than the supplied date.
        battery_shipment_end_date_lte: Optional[date]
            Filter records where battery shipment end date is less than or equal to the supplied date.
        battery_shipment_end_date_gt: Optional[date]
            Filter records where battery shipment end date is greater than the supplied date.
        battery_shipment_end_date_gte: Optional[date]
            Filter records where battery shipment end date is greater than or equal to the supplied date.
        battery_shipment_end_year: Optional[Union[list[str], Series[str], str]]
            Calendar year when shipment of the battery equipment was completed.
        expected_project_commissioning_date: Optional[date]
            Date by which the project using the supplied equipment is expected to be commissioned.
        expected_project_commissioning_date_lt: Optional[date]
            Filter records where expected project commissioning date is less than the supplied date.
        expected_project_commissioning_date_lte: Optional[date]
            Filter records where expected project commissioning date is less than or equal to the supplied date.
        expected_project_commissioning_date_gt: Optional[date]
            Filter records where expected project commissioning date is greater than the supplied date.
        expected_project_commissioning_date_gte: Optional[date]
            Filter records where expected project commissioning date is greater than or equal to the supplied date.
        expected_project_commissioning_year: Optional[Union[list[str], Series[str], str]]
            Calendar year by which the project using the supplied equipment is expected to be commissioned.
        investment_currency: Optional[Union[list[str], Series[str], str]]
            Official currency associated with the total investment value.
        created_datetime: Optional[date]
            Timestamp when the record was first created.
        created_datetime_lt: Optional[date]
            Filter records where created datetime is less than the supplied date.
        created_datetime_lte: Optional[date]
            Filter records where created datetime is less than or equal to the supplied date.
        created_datetime_gt: Optional[date]
            Filter records where created datetime is greater than the supplied date.
        created_datetime_gte: Optional[date]
            Filter records where created datetime is greater than or equal to the supplied date.
        modified_datetime: Optional[date]
            Timestamp of the most recent update to the record.
        modified_datetime_lt: Optional[date]
            Filter records where modified datetime is less than the supplied date.
        modified_datetime_lte: Optional[date]
            Filter records where modified datetime is less than or equal to the supplied date.
        modified_datetime_gt: Optional[date]
            Filter records where modified datetime is greater than the supplied date.
        modified_datetime_gte: Optional[date]
            Filter records where modified datetime is greater than or equal to the supplied date.
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
        filter_params.append(list_to_filter("batteryOrderName", battery_order_name))
        filter_params.append(list_to_filter("dateAnnounced", date_announced))
        if date_announced_lt is not None:
            filter_params.append(f'dateAnnounced < "{date_announced_lt}"')
        if date_announced_lte is not None:
            filter_params.append(f'dateAnnounced <= "{date_announced_lte}"')
        if date_announced_gt is not None:
            filter_params.append(f'dateAnnounced > "{date_announced_gt}"')
        if date_announced_gte is not None:
            filter_params.append(f'dateAnnounced >= "{date_announced_gte}"')
        filter_params.append(list_to_filter("yearAnnounced", year_announced))
        filter_params.append(list_to_filter("halfYearAnnounced", half_year_announced))
        filter_params.append(list_to_filter("quarterAnnounced", quarter_announced))
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
        filter_params.append(
            list_to_filter("orderCustomerHQ", order_customer_head_quarters)
        )
        filter_params.append(
            list_to_filter("batteryOrderCustomerType", order_customer_type)
        )
        filter_params.append(list_to_filter("batterySupplier", battery_supplier))
        filter_params.append(
            list_to_filter("batterySupplierHQ", battery_supplier_head_quarters)
        )
        filter_params.append(
            list_to_filter("batterySupplierType", battery_supplier_type)
        )
        filter_params.append(
            list_to_filter("batteryProductTypeSupplied", battery_product_type_supplied)
        )
        filter_params.append(
            list_to_filter("batteryProductSupplied", battery_product_supplied)
        )
        filter_params.append(
            list_to_filter("batteryCellTechnology", battery_cell_technology)
        )
        filter_params.append(
            list_to_filter("batteryCellTechnologyMajor", battery_cell_technology_major)
        )
        filter_params.append(
            list_to_filter(
                "batteryManufacturingLocation", battery_manufacturing_location
            )
        )
        filter_params.append(
            list_to_filter("batteryManufacturingFactory", battery_manufacturing_factory)
        )
        filter_params.append(list_to_filter("orderCapacityUOM", order_capacity_uom))
        filter_params.append(list_to_filter("totalOrderCapacity", total_order_capacity))
        filter_params.append(
            list_to_filter("batteryShipmentStartDate", battery_shipment_start_date)
        )
        if battery_shipment_start_date_lt is not None:
            filter_params.append(
                f'batteryShipmentStartDate < "{battery_shipment_start_date_lt}"'
            )
        if battery_shipment_start_date_lte is not None:
            filter_params.append(
                f'batteryShipmentStartDate <= "{battery_shipment_start_date_lte}"'
            )
        if battery_shipment_start_date_gt is not None:
            filter_params.append(
                f'batteryShipmentStartDate > "{battery_shipment_start_date_gt}"'
            )
        if battery_shipment_start_date_gte is not None:
            filter_params.append(
                f'batteryShipmentStartDate >= "{battery_shipment_start_date_gte}"'
            )
        filter_params.append(
            list_to_filter("batteryShipmentStartYear", battery_shipment_start_year)
        )
        filter_params.append(
            list_to_filter("batteryShipmentEndDate", battery_shipment_end_date)
        )
        if battery_shipment_end_date_lt is not None:
            filter_params.append(
                f'batteryShipmentEndDate < "{battery_shipment_end_date_lt}"'
            )
        if battery_shipment_end_date_lte is not None:
            filter_params.append(
                f'batteryShipmentEndDate <= "{battery_shipment_end_date_lte}"'
            )
        if battery_shipment_end_date_gt is not None:
            filter_params.append(
                f'batteryShipmentEndDate > "{battery_shipment_end_date_gt}"'
            )
        if battery_shipment_end_date_gte is not None:
            filter_params.append(
                f'batteryShipmentEndDate >= "{battery_shipment_end_date_gte}"'
            )
        filter_params.append(
            list_to_filter("batteryShipmentEndYear", battery_shipment_end_year)
        )
        filter_params.append(
            list_to_filter(
                "expectedProjectCommissioningDate", expected_project_commissioning_date
            )
        )
        if expected_project_commissioning_date_lt is not None:
            filter_params.append(
                f'expectedProjectCommissioningDate < "{expected_project_commissioning_date_lt}"'
            )
        if expected_project_commissioning_date_lte is not None:
            filter_params.append(
                f'expectedProjectCommissioningDate <= "{expected_project_commissioning_date_lte}"'
            )
        if expected_project_commissioning_date_gt is not None:
            filter_params.append(
                f'expectedProjectCommissioningDate > "{expected_project_commissioning_date_gt}"'
            )
        if expected_project_commissioning_date_gte is not None:
            filter_params.append(
                f'expectedProjectCommissioningDate >= "{expected_project_commissioning_date_gte}"'
            )
        filter_params.append(
            list_to_filter(
                "expectedProjectCommissioningYear", expected_project_commissioning_year
            )
        )
        filter_params.append(list_to_filter("investmentCurrency", investment_currency))
        filter_params.append(list_to_filter("createdTime", created_datetime))
        if created_datetime_lt is not None:
            filter_params.append(f'createdTime < "{created_datetime_lt}"')
        if created_datetime_lte is not None:
            filter_params.append(f'createdTime <= "{created_datetime_lte}"')
        if created_datetime_gt is not None:
            filter_params.append(f'createdTime > "{created_datetime_gt}"')
        if created_datetime_gte is not None:
            filter_params.append(f'createdTime >= "{created_datetime_gte}"')
        filter_params.append(list_to_filter("modifiedTime", modified_datetime))
        if modified_datetime_lt is not None:
            filter_params.append(f'modifiedTime < "{modified_datetime_lt}"')
        if modified_datetime_lte is not None:
            filter_params.append(f'modifiedTime <= "{modified_datetime_lte}"')
        if modified_datetime_gt is not None:
            filter_params.append(f'modifiedTime > "{modified_datetime_gt}"')
        if modified_datetime_gte is not None:
            filter_params.append(f'modifiedTime >= "{modified_datetime_gte}"')
        filter_params = [fp for fp in filter_params if fp != ""]
        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif filter_params:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"
        params = {"page": page, "pageSize": page_size, "filter": filter_exp}
        return get_data(
            path="analytics/cet/supplychain/v1/battery-orders",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )

    @staticmethod
    def _convert_unique_values_to_df(resp: Response) -> DataFrame:
        return CetSupplyChain._normalize(resp, "aggResultValue")

    @staticmethod
    def _convert_to_df(resp: Response) -> DataFrame:
        return CetSupplyChain._normalize(resp, "results")

    @staticmethod
    def _normalize(resp: Response, key: str) -> DataFrame:
        df = pd.json_normalize(resp.json()[key])
        for column in [
            "batteryShipmentEndDate",
            "batteryShipmentStartDate",
            "createdDatetime",
            "createdTime",
            "dateAnnounced",
            "dateExpectedWindTurbineCommissioning",
            "dateExpectedWindTurbineShipment",
            "electrolyzerShipmentEndDate",
            "electrolyzerShipmentStartDate",
            "expectedProjectCommissioningDate",
            "lastUpdated",
            "modifiedDatetime",
            "modifiedTime",
            "vintage",
        ]:
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column], utc=True, format="ISO8601", errors="coerce"
                )
        return df
