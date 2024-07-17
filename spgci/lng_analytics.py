
from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date
import pandas as pd

class Lng_analytics:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _forecast_annual_prices_endpoint = "/price/annual-forecast"
    _forecast_monthly_prices_endpoint = "/price/monthly-forecast"
    _historical_bilateral_customs_prices_endpoint = "/price/historical/bilateral-custom"
    _historical_monthly_prices_endpoint = "/price/historical/monthly"
    _liquefaction_economics_endpoint = "/assets-contracts/liquefaction-economics"
    _country_coasts_endpoint = "/assets-contracts/country-coasts"
    _current_liquefaction_endpoint = "/assets-contracts/current-liquefaction"
    _current_regasification_endpoint = "/assets-contracts/current-regasification"
    _liquefaction_projects_endpoint = "/assets-contracts/liquefaction-projects"
    _liquefaction_train_ownership_endpoint = "/assets-contracts/liquefaction-train-ownership"
    _liquefaction_trains_endpoint = "/assets-contracts/liquefaction-trains"
    _offtake_contracts_endpoint = "/assets-contracts/offtake-contracts"
    _regasification_contracts_endpoint = "/assets-contracts/regasification-contracts"
    _regasification_phase_ownership_endpoint = "/assets-contracts/regasification-phase-ownership"
    _regasification_phases_endpoint = "/assets-contracts/regasification-phases"
    _regasification_projects_endpoint = "/assets-contracts/regasification-projects"
    _vessel_endpoint = "/assets-contracts/vessel"
    _offtake_contracts_monthly_estimated_buildout_endpoint = "/assets-contracts/monthly-estimated-buildout/offtake-contracts"
    _liquefaction_capacity_monthly_estimated_buildout_endpoint = "/assets-contracts/monthly-estimated-buildout/liquefaction-capacity"
    _regasification_contract_monthly_estimated_buildout_endpoint = "/assets-contracts/monthly-estimated-buildout/regasification-contracts"
    _regasification_capacity_monthly_estimated_buildout_endpoint = "/assets-contracts/monthly-estimated-buildout/regasification-capacity"
    _feedstock_profiles_endpoint = "/assets-contracts/feedstock"


    def get_price_annual_forecast(
        self, year: Optional[Union[list[str], Series[str], str]] = None, priceMarkerName: Optional[Union[list[str], Series[str], str]] = None, priceMarkerUom: Optional[Union[list[str], Series[str], str]] = None, priceMarkerCurrency: Optional[Union[list[str], Series[str], str]] = None, priceMarker: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         year: Optional[Union[list[str], Series[str], str]]
             The date for which the price forecast is provided, be default None
         priceMarkerName: Optional[Union[list[str], Series[str], str]]
             The name of the price marker, be default None
         priceMarkerUom: Optional[Union[list[str], Series[str], str]]
             The unit of measure for a given price for the indicated time period, be default None
         priceMarkerCurrency: Optional[Union[list[str], Series[str], str]]
             The currency for a given price for the indicated time period, be default None
         priceMarker: Optional[Union[list[str], Series[str], str]]
             The price value of a given price marker for the indicated time period, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Forecast annual prices record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("priceMarkerName", priceMarkerName))
        filter_params.append(list_to_filter("priceMarkerUom", priceMarkerUom))
        filter_params.append(list_to_filter("priceMarkerCurrency", priceMarkerCurrency))
        filter_params.append(list_to_filter("priceMarker", priceMarker))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/price/annual-forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_price_monthly_forecast(
        self, date: Optional[Union[list[date], Series[date], date]] = None, priceMarkerName: Optional[Union[list[str], Series[str], str]] = None, priceMarkerUom: Optional[Union[list[str], Series[str], str]] = None, priceMarkerCurrency: Optional[Union[list[str], Series[str], str]] = None, priceMarker: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         date: Optional[Union[list[date], Series[date], date]]
             The date for which the price forecast is provided, be default None
         priceMarkerName: Optional[Union[list[str], Series[str], str]]
             The name of the price marker, be default None
         priceMarkerUom: Optional[Union[list[str], Series[str], str]]
             The unit of measure for a given price for the indicated time period, be default None
         priceMarkerCurrency: Optional[Union[list[str], Series[str], str]]
             The currency for a given price for the indicated time period, be default None
         priceMarker: Optional[Union[list[str], Series[str], str]]
             The price value of a given price marker for the indicated time period, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Forecast monthly prices record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("date", date))
        filter_params.append(list_to_filter("priceMarkerName", priceMarkerName))
        filter_params.append(list_to_filter("priceMarkerUom", priceMarkerUom))
        filter_params.append(list_to_filter("priceMarkerCurrency", priceMarkerCurrency))
        filter_params.append(list_to_filter("priceMarker", priceMarker))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/price/monthly-forecast",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_price_historical_bilateral_custom(
        self, month: Optional[Union[list[date], Series[date], date]] = None, importMarket: Optional[Union[list[str], Series[str], str]] = None, supplySource: Optional[Union[list[str], Series[str], str]] = None, uom: Optional[Union[list[str], Series[str], str]] = None, currency: Optional[Union[list[str], Series[str], str]] = None, price: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         month: Optional[Union[list[date], Series[date], date]]
             The month for which the price data is recorded, be default None
         importMarket: Optional[Union[list[str], Series[str], str]]
             The market or country where the LNG is being imported, be default None
         supplySource: Optional[Union[list[str], Series[str], str]]
             The source or country from which the LNG is being supplied, be default None
         uom: Optional[Union[list[str], Series[str], str]]
             The unit of measure for a given price for the indicated time period, be default None
         currency: Optional[Union[list[str], Series[str], str]]
             The currency for a given price for the indicated time period, be default None
         price: Optional[Union[list[str], Series[str], str]]
             The price value of a given bilateral combination of import market and supply source for the indicated time period, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Historical bilateral customs prices record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("month", month))
        filter_params.append(list_to_filter("importMarket", importMarket))
        filter_params.append(list_to_filter("supplySource", supplySource))
        filter_params.append(list_to_filter("uom", uom))
        filter_params.append(list_to_filter("currency", currency))
        filter_params.append(list_to_filter("price", price))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/price/historical/bilateral-custom",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_price_historical_monthly(
        self, date: Optional[Union[list[date], Series[date], date]] = None, priceMarkerName: Optional[Union[list[str], Series[str], str]] = None, priceMarkerUom: Optional[Union[list[str], Series[str], str]] = None, priceMarkerCurrency: Optional[Union[list[str], Series[str], str]] = None, priceMarker: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         date: Optional[Union[list[date], Series[date], date]]
             The date for which the price data is recorded, be default None
         priceMarkerName: Optional[Union[list[str], Series[str], str]]
             The name of the price marker, be default None
         priceMarkerUom: Optional[Union[list[str], Series[str], str]]
             The unit of measure for a given price for the indicated time period, be default None
         priceMarkerCurrency: Optional[Union[list[str], Series[str], str]]
             The currency for a given price for the indicated time period, be default None
         priceMarker: Optional[Union[list[str], Series[str], str]]
             The price value of a given price marker for the indicated time period, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Historical monthly prices record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("date", date))
        filter_params.append(list_to_filter("priceMarkerName", priceMarkerName))
        filter_params.append(list_to_filter("priceMarkerUom", priceMarkerUom))
        filter_params.append(list_to_filter("priceMarkerCurrency", priceMarkerCurrency))
        filter_params.append(list_to_filter("priceMarker", priceMarker))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/price/historical/monthly",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_liquefaction_economics(
        self, economicGroup: Optional[Union[list[str], Series[str], str]] = None, liquefactionProject: Optional[Union[list[str], Series[str], str]] = None, supplyMarket: Optional[Union[list[str], Series[str], str]] = None, startYear: Optional[Union[list[str], Series[str], str]] = None, capitalRecovered: Optional[Union[list[str], Series[str], str]] = None, midstreamDiscountRate: Optional[Union[list[str], Series[str], str]] = None, upstreamDiscountRate: Optional[Union[list[str], Series[str], str]] = None, upstreamPricingScenario: Optional[Union[list[str], Series[str], str]] = None, upstreamPrice: Optional[Union[list[str], Series[str], str]] = None, upstreamPricingUom: Optional[Union[list[str], Series[str], str]] = None, upstreamPricingCurrency: Optional[Union[list[str], Series[str], str]] = None, liquefactionCapacity: Optional[Union[list[str], Series[str], str]] = None, liquefactionCapacityUom: Optional[Union[list[str], Series[str], str]] = None, economicsCategory: Optional[Union[list[str], Series[str], str]] = None, economicsCategoryUom: Optional[Union[list[str], Series[str], str]] = None, economicsCategoryCurrency: Optional[Union[list[str], Series[str], str]] = None, economicsCategoryCost: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         economicGroup: Optional[Union[list[str], Series[str], str]]
             Classification of the project based on economic characteristics, be default None
         liquefactionProject: Optional[Union[list[str], Series[str], str]]
             Name of the specific liquefaction project, be default None
         supplyMarket: Optional[Union[list[str], Series[str], str]]
             The market from which the feedstock is sourced, be default None
         startYear: Optional[Union[list[str], Series[str], str]]
             Year of project start-up, be default None
         capitalRecovered: Optional[Union[list[str], Series[str], str]]
             Yes or no whether all capital is recovered, be default None
         midstreamDiscountRate: Optional[Union[list[str], Series[str], str]]
             Discount rate applied to midstream cash flows, be default None
         upstreamDiscountRate: Optional[Union[list[str], Series[str], str]]
             Discount rate applied to upstream cash flows, be default None
         upstreamPricingScenario: Optional[Union[list[str], Series[str], str]]
             The name of the category defining the upstream modeling scenario, be default None
         upstreamPrice: Optional[Union[list[str], Series[str], str]]
             Numeric value of the upstream modeling scenario, be default None
         upstreamPricingUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the upstream modeling scenario, be default None
         upstreamPricingCurrency: Optional[Union[list[str], Series[str], str]]
             Currency of the upstream modeling scenario, be default None
         liquefactionCapacity: Optional[Union[list[str], Series[str], str]]
             Annual liquefaction capacity, be default None
         liquefactionCapacityUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure for Annual liquefaction capacity, be default None
         economicsCategory: Optional[Union[list[str], Series[str], str]]
             The name of the cost component in liquefaction project economic analysis, be default None
         economicsCategoryUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the cost for the specific component of liquefaction project economic analysis, be default None
         economicsCategoryCurrency: Optional[Union[list[str], Series[str], str]]
             Currency of the cost for the specific component of liquefaction project economic analysis, be default None
         economicsCategoryCost: Optional[Union[list[str], Series[str], str]]
             Numeric value of the cost for the specific component of liquefaction project economic analysis, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Liquefaction Economics record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("economicGroup", economicGroup))
        filter_params.append(list_to_filter("liquefactionProject", liquefactionProject))
        filter_params.append(list_to_filter("supplyMarket", supplyMarket))
        filter_params.append(list_to_filter("startYear", startYear))
        filter_params.append(list_to_filter("capitalRecovered", capitalRecovered))
        filter_params.append(list_to_filter("midstreamDiscountRate", midstreamDiscountRate))
        filter_params.append(list_to_filter("upstreamDiscountRate", upstreamDiscountRate))
        filter_params.append(list_to_filter("upstreamPricingScenario", upstreamPricingScenario))
        filter_params.append(list_to_filter("upstreamPrice", upstreamPrice))
        filter_params.append(list_to_filter("upstreamPricingUom", upstreamPricingUom))
        filter_params.append(list_to_filter("upstreamPricingCurrency", upstreamPricingCurrency))
        filter_params.append(list_to_filter("liquefactionCapacity", liquefactionCapacity))
        filter_params.append(list_to_filter("liquefactionCapacityUom", liquefactionCapacityUom))
        filter_params.append(list_to_filter("economicsCategory", economicsCategory))
        filter_params.append(list_to_filter("economicsCategoryUom", economicsCategoryUom))
        filter_params.append(list_to_filter("economicsCategoryCurrency", economicsCategoryCurrency))
        filter_params.append(list_to_filter("economicsCategoryCost", economicsCategoryCost))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/liquefaction-economics",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_country_coasts(
        self, countryCoastId: Optional[Union[list[str], Series[str], str]] = None, countryCoast: Optional[Union[list[str], Series[str], str]] = None, country: Optional[Union[list[str], Series[str], str]] = None, basin: Optional[Union[list[str], Series[str], str]] = None, regionExport: Optional[Union[list[str], Series[str], str]] = None, regionImport: Optional[Union[list[str], Series[str], str]] = None, regionCrossBasinImport: Optional[Union[list[str], Series[str], str]] = None, regionGeneral: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         countryCoastId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each country's coastline, be default None
         countryCoast: Optional[Union[list[str], Series[str], str]]
             Specific coast and country identity, be default None
         country: Optional[Union[list[str], Series[str], str]]
             Name of the country associated with the coast, be default None
         basin: Optional[Union[list[str], Series[str], str]]
             The geographic basin where the country coast is located, be default None
         regionExport: Optional[Union[list[str], Series[str], str]]
             Regional classification for export country coasts, be default None
         regionImport: Optional[Union[list[str], Series[str], str]]
             Regional classification for import country coasts, be default None
         regionCrossBasinImport: Optional[Union[list[str], Series[str], str]]
             Cross-basin trade regional classification for different country coasts, be default None
         regionGeneral: Optional[Union[list[str], Series[str], str]]
             General regional classification for country coasts, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Country coasts record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("countryCoastId", countryCoastId))
        filter_params.append(list_to_filter("countryCoast", countryCoast))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("basin", basin))
        filter_params.append(list_to_filter("regionExport", regionExport))
        filter_params.append(list_to_filter("regionImport", regionImport))
        filter_params.append(list_to_filter("regionCrossBasinImport", regionCrossBasinImport))
        filter_params.append(list_to_filter("regionGeneral", regionGeneral))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/country-coasts",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_current_liquefaction(
        self, supplyProject: Optional[Union[list[str], Series[str], str]] = None, exportCountry: Optional[Union[list[str], Series[str], str]] = None, cbExportRegion: Optional[Union[list[str], Series[str], str]] = None, exportBasin: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         supplyProject: Optional[Union[list[str], Series[str], str]]
             Name of the LNG supply project, be default None
         exportCountry: Optional[Union[list[str], Series[str], str]]
             Country where the supply project is located, be default None
         cbExportRegion: Optional[Union[list[str], Series[str], str]]
             Cross-border region from which LNG is exported, be default None
         exportBasin: Optional[Union[list[str], Series[str], str]]
             The geographic basin where the supply project is located, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Current liuquefaction record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("supplyProject", supplyProject))
        filter_params.append(list_to_filter("exportCountry", exportCountry))
        filter_params.append(list_to_filter("cbExportRegion", cbExportRegion))
        filter_params.append(list_to_filter("exportBasin", exportBasin))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/current-liquefaction",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_current_regasification(
        self, importPort: Optional[Union[list[str], Series[str], str]] = None, importCountry: Optional[Union[list[str], Series[str], str]] = None, ifReexportCbExportRegion: Optional[Union[list[str], Series[str], str]] = None, importBasin: Optional[Union[list[str], Series[str], str]] = None, cbImportRegion: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         importPort: Optional[Union[list[str], Series[str], str]]
             Name of existing (or soon-to-be-existing) regasification terminal, be default None
         importCountry: Optional[Union[list[str], Series[str], str]]
             Country where the regasification port is located, be default None
         ifReexportCbExportRegion: Optional[Union[list[str], Series[str], str]]
             If this is terminal is/becomes a re-export facility, the corresponding cross-basin export basin, be default None
         importBasin: Optional[Union[list[str], Series[str], str]]
             Geological basin associated with the import location, be default None
         cbImportRegion: Optional[Union[list[str], Series[str], str]]
             Cross-border region for LNG imports, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Current regaisification record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("importPort", importPort))
        filter_params.append(list_to_filter("importCountry", importCountry))
        filter_params.append(list_to_filter("ifReexportCbExportRegion", ifReexportCbExportRegion))
        filter_params.append(list_to_filter("importBasin", importBasin))
        filter_params.append(list_to_filter("cbImportRegion", cbImportRegion))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/current-regasification",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_liquefaction_projects(
        self, liquefactionProjectId: Optional[Union[list[str], Series[str], str]] = None, liquefactionProject: Optional[Union[list[str], Series[str], str]] = None, countryCoast: Optional[Union[list[str], Series[str], str]] = None, supplyMarket: Optional[Union[list[str], Series[str], str]] = None, supplyBasin: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         liquefactionProjectId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each liquefaction project, be default None
         liquefactionProject: Optional[Union[list[str], Series[str], str]]
             Name of the liquefaction project, be default None
         countryCoast: Optional[Union[list[str], Series[str], str]]
             Country and coast identity where the project is located, be default None
         supplyMarket: Optional[Union[list[str], Series[str], str]]
             Market where the liquefaction project is located, be default None
         supplyBasin: Optional[Union[list[str], Series[str], str]]
             Geographic basin where the liquefaction project is located, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Liquefication projects record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("liquefactionProjectId", liquefactionProjectId))
        filter_params.append(list_to_filter("liquefactionProject", liquefactionProject))
        filter_params.append(list_to_filter("countryCoast", countryCoast))
        filter_params.append(list_to_filter("supplyMarket", supplyMarket))
        filter_params.append(list_to_filter("supplyBasin", supplyBasin))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/liquefaction-projects",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_liquefaction_train_ownership(
        self, ownershipId: Optional[Union[list[str], Series[str], str]] = None, liquefactionTrainId: Optional[Union[list[str], Series[str], str]] = None, liquefactionTrain: Optional[Union[list[str], Series[str], str]] = None, shareholder: Optional[Union[list[str], Series[str], str]] = None, share: Optional[Union[list[str], Series[str], str]] = None, ownershipComment: Optional[Union[list[str], Series[str], str]] = None, createdDate: Optional[Union[list[str], Series[str], str]] = None, shareholderModifiedDate: Optional[Union[list[str], Series[str], str]] = None, shareModifiedDate: Optional[Union[list[str], Series[str], str]] = None, ownershipStartDate: Optional[Union[list[str], Series[str], str]] = None, ownershipEndDate: Optional[Union[list[str], Series[str], str]] = None, currentOwner: Optional[Union[list[str], Series[str], str]] = None, countryCoast: Optional[Union[list[str], Series[str], str]] = None, supplyMarket: Optional[Union[list[str], Series[str], str]] = None, liquefactionProject: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         ownershipId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for the ownership record, be default None
         liquefactionTrainId: Optional[Union[list[str], Series[str], str]]
             Identifier for the specific liquefaction train, be default None
         liquefactionTrain: Optional[Union[list[str], Series[str], str]]
             Name of the liquefaction train, be default None
         shareholder: Optional[Union[list[str], Series[str], str]]
             Company holding ownership in the liquefaction train, be default None
         share: Optional[Union[list[str], Series[str], str]]
             Percentage of ownership held by the shareholder, be default None
         ownershipComment: Optional[Union[list[str], Series[str], str]]
             Additional notes on the ownership, be default None
         createdDate: Optional[Union[list[str], Series[str], str]]
             Date when the ownership record was created, be default None
         shareholderModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date of last modification to shareholder information, be default None
         shareModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date of last change to the ownership share, be default None
         ownershipStartDate: Optional[Union[list[str], Series[str], str]]
             Date when the ownership began, be default None
         ownershipEndDate: Optional[Union[list[str], Series[str], str]]
             Date when the ownership ended, if applicable, be default None
         currentOwner: Optional[Union[list[str], Series[str], str]]
             Indicates if the shareholder is a current owner, be default None
         countryCoast: Optional[Union[list[str], Series[str], str]]
             Country and coast identity associated with the liquefaction train, be default None
         supplyMarket: Optional[Union[list[str], Series[str], str]]
             Market where the liquefaction train is located, be default None
         liquefactionProject: Optional[Union[list[str], Series[str], str]]
             The project to which the train belongs, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Liquefaction train ownership record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("ownershipId", ownershipId))
        filter_params.append(list_to_filter("liquefactionTrainId", liquefactionTrainId))
        filter_params.append(list_to_filter("liquefactionTrain", liquefactionTrain))
        filter_params.append(list_to_filter("shareholder", shareholder))
        filter_params.append(list_to_filter("share", share))
        filter_params.append(list_to_filter("ownershipComment", ownershipComment))
        filter_params.append(list_to_filter("createdDate", createdDate))
        filter_params.append(list_to_filter("shareholderModifiedDate", shareholderModifiedDate))
        filter_params.append(list_to_filter("shareModifiedDate", shareModifiedDate))
        filter_params.append(list_to_filter("ownershipStartDate", ownershipStartDate))
        filter_params.append(list_to_filter("ownershipEndDate", ownershipEndDate))
        filter_params.append(list_to_filter("currentOwner", currentOwner))
        filter_params.append(list_to_filter("countryCoast", countryCoast))
        filter_params.append(list_to_filter("supplyMarket", supplyMarket))
        filter_params.append(list_to_filter("liquefactionProject", liquefactionProject))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/liquefaction-train-ownership",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_liquefaction_trains(
        self, liquefactionTrain: Optional[Union[list[str], Series[str], str]] = None, liquefactionProject: Optional[Union[list[str], Series[str], str]] = None, liquefactionTrainId: Optional[Union[list[str], Series[str], str]] = None, alternateLiquefactionTrainName: Optional[Union[list[str], Series[str], str]] = None, contractGroup: Optional[Union[list[str], Series[str], str]] = None, trainStatus: Optional[Union[list[str], Series[str], str]] = None, announcedStartDate: Optional[Union[list[str], Series[str], str]] = None, estimatedStartDate: Optional[Union[list[str], Series[str], str]] = None, capexDate: Optional[Union[list[str], Series[str], str]] = None, offlineDate: Optional[Union[list[str], Series[str], str]] = None, greenBrownfield: Optional[Union[list[str], Series[str], str]] = None, liquefactionTechnology: Optional[Union[list[str], Series[str], str]] = None, longitude: Optional[Union[list[str], Series[str], str]] = None, latitude: Optional[Union[list[str], Series[str], str]] = None, createdDate: Optional[Union[list[str], Series[str], str]] = None, statusModifiedDate: Optional[Union[list[str], Series[str], str]] = None, capacityModifiedDate: Optional[Union[list[str], Series[str], str]] = None, announcedStartDateModifiedDate: Optional[Union[list[str], Series[str], str]] = None, estimatedStartDateModifiedDate: Optional[Union[list[str], Series[str], str]] = None, liquefactionProjectId: Optional[Union[list[str], Series[str], str]] = None, announcedStartDateAtFinalInvestmentDecision: Optional[Union[list[str], Series[str], str]] = None, latestAnnouncedFinalInvestmentDecisionDate: Optional[Union[list[str], Series[str], str]] = None, estimatedFirstCargoDate: Optional[Union[list[str], Series[str], str]] = None, riskFactorFeedstockAvailability: Optional[Union[list[str], Series[str], str]] = None, riskFactorPoliticsAndGeopolitics: Optional[Union[list[str], Series[str], str]] = None, riskFactorEnvironmentalRegulation: Optional[Union[list[str], Series[str], str]] = None, riskFactorDomesticGasNeeds: Optional[Union[list[str], Series[str], str]] = None, riskFactorPartnerPriorities: Optional[Union[list[str], Series[str], str]] = None, riskFactorProjectEconomics: Optional[Union[list[str], Series[str], str]] = None, riskFactorAbilityToExecute: Optional[Union[list[str], Series[str], str]] = None, riskFactorContracts: Optional[Union[list[str], Series[str], str]] = None, riskFactorOverall: Optional[Union[list[str], Series[str], str]] = None, numberOfStorageTanks: Optional[Union[list[str], Series[str], str]] = None, numberOfBerths: Optional[Union[list[str], Series[str], str]] = None, estimatedFinalInvestmentDecisionDate: Optional[Union[list[str], Series[str], str]] = None, trainComments: Optional[Union[list[str], Series[str], str]] = None, numberOfTrains: Optional[Union[list[str], Series[str], str]] = None, flngCharterer: Optional[Union[list[str], Series[str], str]] = None, projectType: Optional[Union[list[str], Series[str], str]] = None, liquefactionTrainFeature: Optional[Union[list[str], Series[str], str]] = None, liquefactionTrainFeatureUom: Optional[Union[list[str], Series[str], str]] = None, liquefactionTrainFeatureCurrency: Optional[Union[list[str], Series[str], str]] = None, liquefactionTrainFeatureValue: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         liquefactionTrain: Optional[Union[list[str], Series[str], str]]
             Name of the individual liquefaction unit, be default None
         liquefactionProject: Optional[Union[list[str], Series[str], str]]
             Associated liquefaction project, be default None
         liquefactionTrainId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for the train, be default None
         alternateLiquefactionTrainName: Optional[Union[list[str], Series[str], str]]
             Other names for the train, be default None
         contractGroup: Optional[Union[list[str], Series[str], str]]
             Group convention based on how offtake contracts are associated, be default None
         trainStatus: Optional[Union[list[str], Series[str], str]]
             Current operational status of the train, be default None
         announcedStartDate: Optional[Union[list[str], Series[str], str]]
             Publicly declared start date, be default None
         estimatedStartDate: Optional[Union[list[str], Series[str], str]]
             Our projected date for commercial operations to begin, be default None
         capexDate: Optional[Union[list[str], Series[str], str]]
             Date of the capital expenditure figure, be default None
         offlineDate: Optional[Union[list[str], Series[str], str]]
             Date when the train went offline, if applicable, be default None
         greenBrownfield: Optional[Union[list[str], Series[str], str]]
             Classification as a new (greenfield) or upgraded (brownfield) project, be default None
         liquefactionTechnology: Optional[Union[list[str], Series[str], str]]
             Technology used for liquefaction, be default None
         longitude: Optional[Union[list[str], Series[str], str]]
             Longitude, be default None
         latitude: Optional[Union[list[str], Series[str], str]]
             Latitude, be default None
         createdDate: Optional[Union[list[str], Series[str], str]]
             Date when the train record was created, be default None
         statusModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the train's status was last updated, be default None
         capacityModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the train's capacity information was last updated, be default None
         announcedStartDateModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the announced start date was last updated, be default None
         estimatedStartDateModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the estimated start date was last updated, be default None
         liquefactionProjectId: Optional[Union[list[str], Series[str], str]]
             Identifier for the associated liquefaction project, be default None
         announcedStartDateAtFinalInvestmentDecision: Optional[Union[list[str], Series[str], str]]
             Start date announced at the time of the final investment decision, be default None
         latestAnnouncedFinalInvestmentDecisionDate: Optional[Union[list[str], Series[str], str]]
             Most recent final investment decision date, be default None
         estimatedFirstCargoDate: Optional[Union[list[str], Series[str], str]]
             Projected date for the first shipment of LNG, be default None
         riskFactorFeedstockAvailability: Optional[Union[list[str], Series[str], str]]
             Various risks associated with feedstock availability, be default None
         riskFactorPoliticsAndGeopolitics: Optional[Union[list[str], Series[str], str]]
             Various risks associated with politics, be default None
         riskFactorEnvironmentalRegulation: Optional[Union[list[str], Series[str], str]]
             Various risks associated with environmental regulation, be default None
         riskFactorDomesticGasNeeds: Optional[Union[list[str], Series[str], str]]
             Various risks associated with domestic gas needs, be default None
         riskFactorPartnerPriorities: Optional[Union[list[str], Series[str], str]]
             Various risks associated with partner priorities, be default None
         riskFactorProjectEconomics: Optional[Union[list[str], Series[str], str]]
             Various risks associated with project economics, be default None
         riskFactorAbilityToExecute: Optional[Union[list[str], Series[str], str]]
             Various risks associated with execution ability, be default None
         riskFactorContracts: Optional[Union[list[str], Series[str], str]]
             Various risks associated with contracts, be default None
         riskFactorOverall: Optional[Union[list[str], Series[str], str]]
             Various risks associated with overall project risk, be default None
         numberOfStorageTanks: Optional[Union[list[str], Series[str], str]]
             Number of LNG storage tanks, be default None
         numberOfBerths: Optional[Union[list[str], Series[str], str]]
             Number of docking berths for LNG carriers, be default None
         estimatedFinalInvestmentDecisionDate: Optional[Union[list[str], Series[str], str]]
             Projected date for the final investment decision, be default None
         trainComments: Optional[Union[list[str], Series[str], str]]
             Additional remarks about the liquefaction train, be default None
         numberOfTrains: Optional[Union[list[str], Series[str], str]]
             Total number of liquefaction trains for each record, be default None
         flngCharterer: Optional[Union[list[str], Series[str], str]]
             Entity chartering any floating LNG facilities, be default None
         projectType: Optional[Union[list[str], Series[str], str]]
             Classification of the project type (e.g., onshore, offshore, floating), be default None
         liquefactionTrainFeature: Optional[Union[list[str], Series[str], str]]
             Types of features of liquefaction trains ranging from capacity to storage to capital expenditure (capex) to other facility-specific characteristics, be default None
         liquefactionTrainFeatureUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the corresponding liquefaction train feature, be default None
         liquefactionTrainFeatureCurrency: Optional[Union[list[str], Series[str], str]]
             Currency of the corresponding liquefaction train feature, be default None
         liquefactionTrainFeatureValue: Optional[Union[list[str], Series[str], str]]
             Numeric values of the corresponding liquefaction train feature, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Liquefaction Trains record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("liquefactionTrain", liquefactionTrain))
        filter_params.append(list_to_filter("liquefactionProject", liquefactionProject))
        filter_params.append(list_to_filter("liquefactionTrainId", liquefactionTrainId))
        filter_params.append(list_to_filter("alternateLiquefactionTrainName", alternateLiquefactionTrainName))
        filter_params.append(list_to_filter("contractGroup", contractGroup))
        filter_params.append(list_to_filter("trainStatus", trainStatus))
        filter_params.append(list_to_filter("announcedStartDate", announcedStartDate))
        filter_params.append(list_to_filter("estimatedStartDate", estimatedStartDate))
        filter_params.append(list_to_filter("capexDate", capexDate))
        filter_params.append(list_to_filter("offlineDate", offlineDate))
        filter_params.append(list_to_filter("greenBrownfield", greenBrownfield))
        filter_params.append(list_to_filter("liquefactionTechnology", liquefactionTechnology))
        filter_params.append(list_to_filter("longitude", longitude))
        filter_params.append(list_to_filter("latitude", latitude))
        filter_params.append(list_to_filter("createdDate", createdDate))
        filter_params.append(list_to_filter("statusModifiedDate", statusModifiedDate))
        filter_params.append(list_to_filter("capacityModifiedDate", capacityModifiedDate))
        filter_params.append(list_to_filter("announcedStartDateModifiedDate", announcedStartDateModifiedDate))
        filter_params.append(list_to_filter("estimatedStartDateModifiedDate", estimatedStartDateModifiedDate))
        filter_params.append(list_to_filter("liquefactionProjectId", liquefactionProjectId))
        filter_params.append(list_to_filter("announcedStartDateAtFinalInvestmentDecision", announcedStartDateAtFinalInvestmentDecision))
        filter_params.append(list_to_filter("latestAnnouncedFinalInvestmentDecisionDate", latestAnnouncedFinalInvestmentDecisionDate))
        filter_params.append(list_to_filter("estimatedFirstCargoDate", estimatedFirstCargoDate))
        filter_params.append(list_to_filter("riskFactorFeedstockAvailability", riskFactorFeedstockAvailability))
        filter_params.append(list_to_filter("riskFactorPoliticsAndGeopolitics", riskFactorPoliticsAndGeopolitics))
        filter_params.append(list_to_filter("riskFactorEnvironmentalRegulation", riskFactorEnvironmentalRegulation))
        filter_params.append(list_to_filter("riskFactorDomesticGasNeeds", riskFactorDomesticGasNeeds))
        filter_params.append(list_to_filter("riskFactorPartnerPriorities", riskFactorPartnerPriorities))
        filter_params.append(list_to_filter("riskFactorProjectEconomics", riskFactorProjectEconomics))
        filter_params.append(list_to_filter("riskFactorAbilityToExecute", riskFactorAbilityToExecute))
        filter_params.append(list_to_filter("riskFactorContracts", riskFactorContracts))
        filter_params.append(list_to_filter("riskFactorOverall", riskFactorOverall))
        filter_params.append(list_to_filter("numberOfStorageTanks", numberOfStorageTanks))
        filter_params.append(list_to_filter("numberOfBerths", numberOfBerths))
        filter_params.append(list_to_filter("estimatedFinalInvestmentDecisionDate", estimatedFinalInvestmentDecisionDate))
        filter_params.append(list_to_filter("trainComments", trainComments))
        filter_params.append(list_to_filter("numberOfTrains", numberOfTrains))
        filter_params.append(list_to_filter("flngCharterer", flngCharterer))
        filter_params.append(list_to_filter("projectType", projectType))
        filter_params.append(list_to_filter("liquefactionTrainFeature", liquefactionTrainFeature))
        filter_params.append(list_to_filter("liquefactionTrainFeatureUom", liquefactionTrainFeatureUom))
        filter_params.append(list_to_filter("liquefactionTrainFeatureCurrency", liquefactionTrainFeatureCurrency))
        filter_params.append(list_to_filter("liquefactionTrainFeatureValue", liquefactionTrainFeatureValue))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/liquefaction-trains",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_offtake_contracts(
        self, offtakeContractId: Optional[Union[list[str], Series[str], str]] = None, liquefactionProject: Optional[Union[list[str], Series[str], str]] = None, liquefactionProjectId: Optional[Union[list[str], Series[str], str]] = None, contractGroup: Optional[Union[list[str], Series[str], str]] = None, exporter: Optional[Union[list[str], Series[str], str]] = None, buyer: Optional[Union[list[str], Series[str], str]] = None, assumedDestination: Optional[Union[list[str], Series[str], str]] = None, originalSigningDate: Optional[Union[list[str], Series[str], str]] = None, latestContractRevisionDate: Optional[Union[list[str], Series[str], str]] = None, announcedStartDate: Optional[Union[list[str], Series[str], str]] = None, preliminarySigningDate: Optional[Union[list[str], Series[str], str]] = None, lengthYears: Optional[Union[list[str], Series[str], str]] = None, contractModelType: Optional[Union[list[str], Series[str], str]] = None, percentageOfTrain: Optional[Union[list[str], Series[str], str]] = None, destinationFlexibility: Optional[Union[list[str], Series[str], str]] = None, contractStatus: Optional[Union[list[str], Series[str], str]] = None, shippingTerms: Optional[Union[list[str], Series[str], str]] = None, recontractedTo: Optional[Union[list[str], Series[str], str]] = None, recontractedFrom: Optional[Union[list[str], Series[str], str]] = None, contractPriceLinkageType: Optional[Union[list[str], Series[str], str]] = None, contractPriceSlope: Optional[Union[list[str], Series[str], str]] = None, contractPriceConstant: Optional[Union[list[str], Series[str], str]] = None, contractPriceFloor: Optional[Union[list[str], Series[str], str]] = None, contractPriceCeiling: Optional[Union[list[str], Series[str], str]] = None, contractPriceLinkage: Optional[Union[list[str], Series[str], str]] = None, contractPriceAsOfDate: Optional[Union[list[str], Series[str], str]] = None, contractPriceMonthLagForLinkage: Optional[Union[list[str], Series[str], str]] = None, contractPriceAverageOfMonthsForLinkage: Optional[Union[list[str], Series[str], str]] = None, contractGeneralComments: Optional[Union[list[str], Series[str], str]] = None, contractPriceAdditionalVariable: Optional[Union[list[str], Series[str], str]] = None, contractPriceComments: Optional[Union[list[str], Series[str], str]] = None, fidEnabling: Optional[Union[list[str], Series[str], str]] = None, greenOrBrownfield: Optional[Union[list[str], Series[str], str]] = None, createdDate: Optional[Union[list[str], Series[str], str]] = None, buyerModifiedDate: Optional[Union[list[str], Series[str], str]] = None, announcedStartModifiedDate: Optional[Union[list[str], Series[str], str]] = None, lengthModifiedDate: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, publishedVolumeModifiedDate: Optional[Union[list[str], Series[str], str]] = None, estimatedBuildoutModifiedDate: Optional[Union[list[str], Series[str], str]] = None, contractVolumeType: Optional[Union[list[str], Series[str], str]] = None, contractVolumeUom: Optional[Union[list[str], Series[str], str]] = None, contractVolume: Optional[Union[list[str], Series[str], str]] = None, supplyMarket: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         offtakeContractId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for the contract, be default None
         liquefactionProject: Optional[Union[list[str], Series[str], str]]
             Name of the associated liquefaction project, be default None
         liquefactionProjectId: Optional[Union[list[str], Series[str], str]]
             Identifier for the liquefaction project, be default None
         contractGroup: Optional[Union[list[str], Series[str], str]]
             Group or consortium involved in the contract, be default None
         exporter: Optional[Union[list[str], Series[str], str]]
             Entity responsible for exporting the LNG, be default None
         buyer: Optional[Union[list[str], Series[str], str]]
             Entity purchasing the LNG, be default None
         assumedDestination: Optional[Union[list[str], Series[str], str]]
             Expected delivery location for the LNG, be default None
         originalSigningDate: Optional[Union[list[str], Series[str], str]]
             Date when the contract was first signed, be default None
         latestContractRevisionDate: Optional[Union[list[str], Series[str], str]]
             Date of the most recent contract amendment, be default None
         announcedStartDate: Optional[Union[list[str], Series[str], str]]
             Publicly declared start date of the contract, be default None
         preliminarySigningDate: Optional[Union[list[str], Series[str], str]]
             Date when the contract was preliminarily signed, be default None
         lengthYears: Optional[Union[list[str], Series[str], str]]
             Duration of the contract in years, be default None
         contractModelType: Optional[Union[list[str], Series[str], str]]
             Type of contract model used, be default None
         percentageOfTrain: Optional[Union[list[str], Series[str], str]]
             Share of the liquefaction train's capacity allocated to the contract, be default None
         destinationFlexibility: Optional[Union[list[str], Series[str], str]]
             Designation if the offtake contract is destination-fixed or is flexible, be default None
         contractStatus: Optional[Union[list[str], Series[str], str]]
             Current status of the contract, be default None
         shippingTerms: Optional[Union[list[str], Series[str], str]]
             Terms related to the transportation of LNG, be default None
         recontractedTo: Optional[Union[list[str], Series[str], str]]
             With which company or JV the contract was re-contract to, if it was, be default None
         recontractedFrom: Optional[Union[list[str], Series[str], str]]
             With which company or JV the contract was re-contract from, if it was, be default None
         contractPriceLinkageType: Optional[Union[list[str], Series[str], str]]
             Identifies the general commodity that the pricing formula is applied to, be default None
         contractPriceSlope: Optional[Union[list[str], Series[str], str]]
             Slope of the price formula in the contract, be default None
         contractPriceConstant: Optional[Union[list[str], Series[str], str]]
             Fixed component of the price formula, be default None
         contractPriceFloor: Optional[Union[list[str], Series[str], str]]
             Minimum contract price, be default None
         contractPriceCeiling: Optional[Union[list[str], Series[str], str]]
             Maximum contract price, be default None
         contractPriceLinkage: Optional[Union[list[str], Series[str], str]]
             Identifies the specific price marker that the pricing formula is applied to, be default None
         contractPriceAsOfDate: Optional[Union[list[str], Series[str], str]]
             Date of the reported contract price, be default None
         contractPriceMonthLagForLinkage: Optional[Union[list[str], Series[str], str]]
             Number of months prior to the targeted result that the price used as formula's reference should be taken from; e.g., 2-month lag should use the January reference price to get the March result, be default None
         contractPriceAverageOfMonthsForLinkage: Optional[Union[list[str], Series[str], str]]
             Number of months over which the reference price should be averaged, starting at the result month and working backwards, be default None
         contractGeneralComments: Optional[Union[list[str], Series[str], str]]
             General remarks about the contract, be default None
         contractPriceAdditionalVariable: Optional[Union[list[str], Series[str], str]]
             Additional variables affecting the contract price, be default None
         contractPriceComments: Optional[Union[list[str], Series[str], str]]
             Additional comments on the contract pricing, be default None
         fidEnabling: Optional[Union[list[str], Series[str], str]]
             Indicates if the contract enables the final investment decision, be default None
         greenOrBrownfield: Optional[Union[list[str], Series[str], str]]
             Indicates if the project is a new development (greenfield) or an upgrade (brownfield), be default None
         createdDate: Optional[Union[list[str], Series[str], str]]
             Date when the contract record was created, be default None
         buyerModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the buyer information was last updated, be default None
         announcedStartModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the announced start date was last updated, be default None
         lengthModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the contract length was last updated, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the contract was last modified, be default None
         publishedVolumeModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the published volume was last updated, be default None
         estimatedBuildoutModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the estimated buildout was last updated, be default None
         contractVolumeType: Optional[Union[list[str], Series[str], str]]
             Type of contract volume information, be default None
         contractVolumeUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the contract volume, be default None
         contractVolume: Optional[Union[list[str], Series[str], str]]
             Numeric values of the contract volume, be default None
         supplyMarket: Optional[Union[list[str], Series[str], str]]
             Market supplying the LNG for the contract, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("offtakeContractId", offtakeContractId))
        filter_params.append(list_to_filter("liquefactionProject", liquefactionProject))
        filter_params.append(list_to_filter("liquefactionProjectId", liquefactionProjectId))
        filter_params.append(list_to_filter("contractGroup", contractGroup))
        filter_params.append(list_to_filter("exporter", exporter))
        filter_params.append(list_to_filter("buyer", buyer))
        filter_params.append(list_to_filter("assumedDestination", assumedDestination))
        filter_params.append(list_to_filter("originalSigningDate", originalSigningDate))
        filter_params.append(list_to_filter("latestContractRevisionDate", latestContractRevisionDate))
        filter_params.append(list_to_filter("announcedStartDate", announcedStartDate))
        filter_params.append(list_to_filter("preliminarySigningDate", preliminarySigningDate))
        filter_params.append(list_to_filter("lengthYears", lengthYears))
        filter_params.append(list_to_filter("contractModelType", contractModelType))
        filter_params.append(list_to_filter("percentageOfTrain", percentageOfTrain))
        filter_params.append(list_to_filter("destinationFlexibility", destinationFlexibility))
        filter_params.append(list_to_filter("contractStatus", contractStatus))
        filter_params.append(list_to_filter("shippingTerms", shippingTerms))
        filter_params.append(list_to_filter("recontractedTo", recontractedTo))
        filter_params.append(list_to_filter("recontractedFrom", recontractedFrom))
        filter_params.append(list_to_filter("contractPriceLinkageType", contractPriceLinkageType))
        filter_params.append(list_to_filter("contractPriceSlope", contractPriceSlope))
        filter_params.append(list_to_filter("contractPriceConstant", contractPriceConstant))
        filter_params.append(list_to_filter("contractPriceFloor", contractPriceFloor))
        filter_params.append(list_to_filter("contractPriceCeiling", contractPriceCeiling))
        filter_params.append(list_to_filter("contractPriceLinkage", contractPriceLinkage))
        filter_params.append(list_to_filter("contractPriceAsOfDate", contractPriceAsOfDate))
        filter_params.append(list_to_filter("contractPriceMonthLagForLinkage", contractPriceMonthLagForLinkage))
        filter_params.append(list_to_filter("contractPriceAverageOfMonthsForLinkage", contractPriceAverageOfMonthsForLinkage))
        filter_params.append(list_to_filter("contractGeneralComments", contractGeneralComments))
        filter_params.append(list_to_filter("contractPriceAdditionalVariable", contractPriceAdditionalVariable))
        filter_params.append(list_to_filter("contractPriceComments", contractPriceComments))
        filter_params.append(list_to_filter("fidEnabling", fidEnabling))
        filter_params.append(list_to_filter("greenOrBrownfield", greenOrBrownfield))
        filter_params.append(list_to_filter("createdDate", createdDate))
        filter_params.append(list_to_filter("buyerModifiedDate", buyerModifiedDate))
        filter_params.append(list_to_filter("announcedStartModifiedDate", announcedStartModifiedDate))
        filter_params.append(list_to_filter("lengthModifiedDate", lengthModifiedDate))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        filter_params.append(list_to_filter("publishedVolumeModifiedDate", publishedVolumeModifiedDate))
        filter_params.append(list_to_filter("estimatedBuildoutModifiedDate", estimatedBuildoutModifiedDate))
        filter_params.append(list_to_filter("contractVolumeType", contractVolumeType))
        filter_params.append(list_to_filter("contractVolumeUom", contractVolumeUom))
        filter_params.append(list_to_filter("contractVolume", contractVolume))
        filter_params.append(list_to_filter("supplyMarket", supplyMarket))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/offtake-contracts",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_regasification_contracts(
        self, regasificationContractId: Optional[Union[list[str], Series[str], str]] = None, regasificationPhase: Optional[Union[list[str], Series[str], str]] = None, regasificationPhaseId: Optional[Union[list[str], Series[str], str]] = None, regasificationProject: Optional[Union[list[str], Series[str], str]] = None, capacityOwner: Optional[Union[list[str], Series[str], str]] = None, modelAs: Optional[Union[list[str], Series[str], str]] = None, capacityRights: Optional[Union[list[str], Series[str], str]] = None, capacityRightsUom: Optional[Union[list[str], Series[str], str]] = None, percentage: Optional[Union[list[str], Series[str], str]] = None, contractType: Optional[Union[list[str], Series[str], str]] = None, contractLengthYears: Optional[Union[list[str], Series[str], str]] = None, contractStartDate: Optional[Union[list[str], Series[str], str]] = None, contractComment: Optional[Union[list[str], Series[str], str]] = None, createdDate: Optional[Union[list[str], Series[str], str]] = None, capacityModifiedDate: Optional[Union[list[str], Series[str], str]] = None, startModifiedDate: Optional[Union[list[str], Series[str], str]] = None, capacityOwnerModifiedDate: Optional[Union[list[str], Series[str], str]] = None, typeModifiedDate: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         regasificationContractId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each regasification contract, be default None
         regasificationPhase: Optional[Union[list[str], Series[str], str]]
             Regasification phase associated with the contract, be default None
         regasificationPhaseId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each regasification phase, be default None
         regasificationProject: Optional[Union[list[str], Series[str], str]]
             Name of the regasification project, be default None
         capacityOwner: Optional[Union[list[str], Series[str], str]]
             Entity or company that owns the regasification capacity, be default None
         modelAs: Optional[Union[list[str], Series[str], str]]
             Modeling type used to calculate the regasification capacity, be default None
         capacityRights: Optional[Union[list[str], Series[str], str]]
             Numeric value of the capcity rights, be default None
         capacityRightsUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the capcity rights, be default None
         percentage: Optional[Union[list[str], Series[str], str]]
             Percentage of capacity rights, be default None
         contractType: Optional[Union[list[str], Series[str], str]]
             Type of regasification contract, be default None
         contractLengthYears: Optional[Union[list[str], Series[str], str]]
             Length of the contract in years, be default None
         contractStartDate: Optional[Union[list[str], Series[str], str]]
             Start date of the contract, be default None
         contractComment: Optional[Union[list[str], Series[str], str]]
             Additional comments or notes related to the contract, be default None
         createdDate: Optional[Union[list[str], Series[str], str]]
             Date when the contract was created, be default None
         capacityModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the capacity rights were modified, be default None
         startModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the contract start date was modified, be default None
         capacityOwnerModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the capacity owner was modified, be default None
         typeModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the contract type was modified, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Regasification contracts record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("regasificationContractId", regasificationContractId))
        filter_params.append(list_to_filter("regasificationPhase", regasificationPhase))
        filter_params.append(list_to_filter("regasificationPhaseId", regasificationPhaseId))
        filter_params.append(list_to_filter("regasificationProject", regasificationProject))
        filter_params.append(list_to_filter("capacityOwner", capacityOwner))
        filter_params.append(list_to_filter("modelAs", modelAs))
        filter_params.append(list_to_filter("capacityRights", capacityRights))
        filter_params.append(list_to_filter("capacityRightsUom", capacityRightsUom))
        filter_params.append(list_to_filter("percentage", percentage))
        filter_params.append(list_to_filter("contractType", contractType))
        filter_params.append(list_to_filter("contractLengthYears", contractLengthYears))
        filter_params.append(list_to_filter("contractStartDate", contractStartDate))
        filter_params.append(list_to_filter("contractComment", contractComment))
        filter_params.append(list_to_filter("createdDate", createdDate))
        filter_params.append(list_to_filter("capacityModifiedDate", capacityModifiedDate))
        filter_params.append(list_to_filter("startModifiedDate", startModifiedDate))
        filter_params.append(list_to_filter("capacityOwnerModifiedDate", capacityOwnerModifiedDate))
        filter_params.append(list_to_filter("typeModifiedDate", typeModifiedDate))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/regasification-contracts",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_regasification_phase_ownership(
        self, ownershipId: Optional[Union[list[str], Series[str], str]] = None, regasificationPhaseId: Optional[Union[list[str], Series[str], str]] = None, regasificationPhase: Optional[Union[list[str], Series[str], str]] = None, shareholder: Optional[Union[list[str], Series[str], str]] = None, share: Optional[Union[list[str], Series[str], str]] = None, ownershipComment: Optional[Union[list[str], Series[str], str]] = None, createdDate: Optional[Union[list[str], Series[str], str]] = None, shareholderModifiedDate: Optional[Union[list[str], Series[str], str]] = None, shareModifiedDate: Optional[Union[list[str], Series[str], str]] = None, ownershipStartDate: Optional[Union[list[str], Series[str], str]] = None, ownershipEndDate: Optional[Union[list[str], Series[str], str]] = None, currentOwner: Optional[Union[list[str], Series[str], str]] = None, countryCoast: Optional[Union[list[str], Series[str], str]] = None, importMarket: Optional[Union[list[str], Series[str], str]] = None, regasificationProject: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         ownershipId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each ownership record in the regasification phase, be default None
         regasificationPhaseId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each regasification phase, be default None
         regasificationPhase: Optional[Union[list[str], Series[str], str]]
             Phase of the regasification project, be default None
         shareholder: Optional[Union[list[str], Series[str], str]]
             Entity or company that holds ownership in the regasification phase, be default None
         share: Optional[Union[list[str], Series[str], str]]
             Percentage of ownership held by the shareholder, be default None
         ownershipComment: Optional[Union[list[str], Series[str], str]]
             Additional comments or notes related to the ownership, be default None
         createdDate: Optional[Union[list[str], Series[str], str]]
             Date when the ownership record was created, be default None
         shareholderModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the shareholder information was modified, be default None
         shareModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the ownership share was modified, be default None
         ownershipStartDate: Optional[Union[list[str], Series[str], str]]
             Start date of the ownership, be default None
         ownershipEndDate: Optional[Union[list[str], Series[str], str]]
             End date of the ownership, be default None
         currentOwner: Optional[Union[list[str], Series[str], str]]
             Current owner of the regasification phase, be default None
         countryCoast: Optional[Union[list[str], Series[str], str]]
             Country coast associated with the regasification phase, be default None
         importMarket: Optional[Union[list[str], Series[str], str]]
             Market where the regasification project is located, be default None
         regasificationProject: Optional[Union[list[str], Series[str], str]]
             Name of the regasification project associated with the phase, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Regasificaion phase ownership record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("ownershipId", ownershipId))
        filter_params.append(list_to_filter("regasificationPhaseId", regasificationPhaseId))
        filter_params.append(list_to_filter("regasificationPhase", regasificationPhase))
        filter_params.append(list_to_filter("shareholder", shareholder))
        filter_params.append(list_to_filter("share", share))
        filter_params.append(list_to_filter("ownershipComment", ownershipComment))
        filter_params.append(list_to_filter("createdDate", createdDate))
        filter_params.append(list_to_filter("shareholderModifiedDate", shareholderModifiedDate))
        filter_params.append(list_to_filter("shareModifiedDate", shareModifiedDate))
        filter_params.append(list_to_filter("ownershipStartDate", ownershipStartDate))
        filter_params.append(list_to_filter("ownershipEndDate", ownershipEndDate))
        filter_params.append(list_to_filter("currentOwner", currentOwner))
        filter_params.append(list_to_filter("countryCoast", countryCoast))
        filter_params.append(list_to_filter("importMarket", importMarket))
        filter_params.append(list_to_filter("regasificationProject", regasificationProject))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/regasification-phase-ownership",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_regasification_phases(
        self, regasificationPhase: Optional[Union[list[str], Series[str], str]] = None, regasificationProject: Optional[Union[list[str], Series[str], str]] = None, regasificationPhaseId: Optional[Union[list[str], Series[str], str]] = None, alternatePhaseName: Optional[Union[list[str], Series[str], str]] = None, phaseStatus: Optional[Union[list[str], Series[str], str]] = None, announcedStartDate: Optional[Union[list[str], Series[str], str]] = None, estimatedStartDate: Optional[Union[list[str], Series[str], str]] = None, operator: Optional[Union[list[str], Series[str], str]] = None, announcedStartDateOriginal: Optional[Union[list[str], Series[str], str]] = None, storageTanksNumber: Optional[Union[list[str], Series[str], str]] = None, regasificationTechnology: Optional[Union[list[str], Series[str], str]] = None, feedContractor: Optional[Union[list[str], Series[str], str]] = None, epcContractor: Optional[Union[list[str], Series[str], str]] = None, longitude: Optional[Union[list[str], Series[str], str]] = None, latitude: Optional[Union[list[str], Series[str], str]] = None, berthsNumber: Optional[Union[list[str], Series[str], str]] = None, terminalType: Optional[Union[list[str], Series[str], str]] = None, ableToReload: Optional[Union[list[str], Series[str], str]] = None, phaseComments: Optional[Union[list[str], Series[str], str]] = None, createdDate: Optional[Union[list[str], Series[str], str]] = None, statusModifiedDate: Optional[Union[list[str], Series[str], str]] = None, capacityModifiedDate: Optional[Union[list[str], Series[str], str]] = None, announcedStartDateModifiedDate: Optional[Union[list[str], Series[str], str]] = None, estimatedStartDateModifiedDate: Optional[Union[list[str], Series[str], str]] = None, datePhaseFirstAnnounced: Optional[Union[list[str], Series[str], str]] = None, smallScale: Optional[Union[list[str], Series[str], str]] = None, regasificationPhaseFeature: Optional[Union[list[str], Series[str], str]] = None, regasificationPhaseFeatureUom: Optional[Union[list[str], Series[str], str]] = None, regasificationPhaseFeatureValue: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         regasificationPhase: Optional[Union[list[str], Series[str], str]]
             Name of the regasification phase, be default None
         regasificationProject: Optional[Union[list[str], Series[str], str]]
             Name of the regasification project associated with the phase, be default None
         regasificationPhaseId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each regasification phase, be default None
         alternatePhaseName: Optional[Union[list[str], Series[str], str]]
             Alternate name or alias for the regasification phase, be default None
         phaseStatus: Optional[Union[list[str], Series[str], str]]
             Status of the regasification phase, be default None
         announcedStartDate: Optional[Union[list[str], Series[str], str]]
             Latest publically announced start date of the regasification phase, be default None
         estimatedStartDate: Optional[Union[list[str], Series[str], str]]
             Our estimated start date of the regasification phase, be default None
         operator: Optional[Union[list[str], Series[str], str]]
             Entity or company responsible for operating the regasification phase, be default None
         announcedStartDateOriginal: Optional[Union[list[str], Series[str], str]]
             Original announced start date of the regasification phase, be default None
         storageTanksNumber: Optional[Union[list[str], Series[str], str]]
             Number of storage tanks in the regasification phase, be default None
         regasificationTechnology: Optional[Union[list[str], Series[str], str]]
             Technology used for regasification in the phase, be default None
         feedContractor: Optional[Union[list[str], Series[str], str]]
             Contractor responsible for the front-end engineering and design of the regasification phase, be default None
         epcContractor: Optional[Union[list[str], Series[str], str]]
             Contractor responsible for the engineering, procurement, and construction of the regasification phase, be default None
         longitude: Optional[Union[list[str], Series[str], str]]
             Longitude, be default None
         latitude: Optional[Union[list[str], Series[str], str]]
             Latitude, be default None
         berthsNumber: Optional[Union[list[str], Series[str], str]]
             Number of berths available for ships in the regasification phase, be default None
         terminalType: Optional[Union[list[str], Series[str], str]]
             Type of regasification terminal, be default None
         ableToReload: Optional[Union[list[str], Series[str], str]]
             Indicates whether the regasification phase is capable of reloading LNG onto ships, be default None
         phaseComments: Optional[Union[list[str], Series[str], str]]
             Additional comments or notes related to the regasification phase, be default None
         createdDate: Optional[Union[list[str], Series[str], str]]
             Date when the regasification phase record was created, be default None
         statusModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the phase status was last modified, be default None
         capacityModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the capacity of the regasification phase was last modified, be default None
         announcedStartDateModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the announced start date of the regasification phase was last modified, be default None
         estimatedStartDateModifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the estimated start date of the regasification phase was last modified, be default None
         datePhaseFirstAnnounced: Optional[Union[list[str], Series[str], str]]
             Date when the regasification phase was first announced, be default None
         smallScale: Optional[Union[list[str], Series[str], str]]
             Indicates whether the regasification phase is a small-scale project, be default None
         regasificationPhaseFeature: Optional[Union[list[str], Series[str], str]]
             Types of features of regasification phases ranging from capacity to storage and other facility-specific characteristics, be default None
         regasificationPhaseFeatureUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the corresponding regasification phase feature, be default None
         regasificationPhaseFeatureValue: Optional[Union[list[str], Series[str], str]]
             Numeric values of the corresponding regasification phase feature, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Regasification phases record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("regasificationPhase", regasificationPhase))
        filter_params.append(list_to_filter("regasificationProject", regasificationProject))
        filter_params.append(list_to_filter("regasificationPhaseId", regasificationPhaseId))
        filter_params.append(list_to_filter("alternatePhaseName", alternatePhaseName))
        filter_params.append(list_to_filter("phaseStatus", phaseStatus))
        filter_params.append(list_to_filter("announcedStartDate", announcedStartDate))
        filter_params.append(list_to_filter("estimatedStartDate", estimatedStartDate))
        filter_params.append(list_to_filter("operator", operator))
        filter_params.append(list_to_filter("announcedStartDateOriginal", announcedStartDateOriginal))
        filter_params.append(list_to_filter("storageTanksNumber", storageTanksNumber))
        filter_params.append(list_to_filter("regasificationTechnology", regasificationTechnology))
        filter_params.append(list_to_filter("feedContractor", feedContractor))
        filter_params.append(list_to_filter("epcContractor", epcContractor))
        filter_params.append(list_to_filter("longitude", longitude))
        filter_params.append(list_to_filter("latitude", latitude))
        filter_params.append(list_to_filter("berthsNumber", berthsNumber))
        filter_params.append(list_to_filter("terminalType", terminalType))
        filter_params.append(list_to_filter("ableToReload", ableToReload))
        filter_params.append(list_to_filter("phaseComments", phaseComments))
        filter_params.append(list_to_filter("createdDate", createdDate))
        filter_params.append(list_to_filter("statusModifiedDate", statusModifiedDate))
        filter_params.append(list_to_filter("capacityModifiedDate", capacityModifiedDate))
        filter_params.append(list_to_filter("announcedStartDateModifiedDate", announcedStartDateModifiedDate))
        filter_params.append(list_to_filter("estimatedStartDateModifiedDate", estimatedStartDateModifiedDate))
        filter_params.append(list_to_filter("datePhaseFirstAnnounced", datePhaseFirstAnnounced))
        filter_params.append(list_to_filter("smallScale", smallScale))
        filter_params.append(list_to_filter("regasificationPhaseFeature", regasificationPhaseFeature))
        filter_params.append(list_to_filter("regasificationPhaseFeatureUom", regasificationPhaseFeatureUom))
        filter_params.append(list_to_filter("regasificationPhaseFeatureValue", regasificationPhaseFeatureValue))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/regasification-phases",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_regasification_projects(
        self, regasificationProjectId: Optional[Union[list[str], Series[str], str]] = None, regasificationProject: Optional[Union[list[str], Series[str], str]] = None, countryCoast: Optional[Union[list[str], Series[str], str]] = None, importMarket: Optional[Union[list[str], Series[str], str]] = None, importRegion: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         regasificationProjectId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each regasification project, be default None
         regasificationProject: Optional[Union[list[str], Series[str], str]]
             Name of regasification project, be default None
         countryCoast: Optional[Union[list[str], Series[str], str]]
             Country coast where the regasification project is located, be default None
         importMarket: Optional[Union[list[str], Series[str], str]]
             Market where the regasification project is located, be default None
         importRegion: Optional[Union[list[str], Series[str], str]]
             Region where the regasification project is located, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Regasification projects record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("regasificationProjectId", regasificationProjectId))
        filter_params.append(list_to_filter("regasificationProject", regasificationProject))
        filter_params.append(list_to_filter("countryCoast", countryCoast))
        filter_params.append(list_to_filter("importMarket", importMarket))
        filter_params.append(list_to_filter("importRegion", importRegion))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/regasification-projects",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_vessel(
        self, id: Optional[Union[list[str], Series[str], str]] = None, imoNumber: Optional[Union[list[str], Series[str], str]] = None, vesselName: Optional[Union[list[str], Series[str], str]] = None, propulsionSystem: Optional[Union[list[str], Series[str], str]] = None, vesselType: Optional[Union[list[str], Series[str], str]] = None, charterer: Optional[Union[list[str], Series[str], str]] = None, operator: Optional[Union[list[str], Series[str], str]] = None, shipowner: Optional[Union[list[str], Series[str], str]] = None, shipowner2: Optional[Union[list[str], Series[str], str]] = None, shipowner3: Optional[Union[list[str], Series[str], str]] = None, shipowner4: Optional[Union[list[str], Series[str], str]] = None, shipowner5: Optional[Union[list[str], Series[str], str]] = None, shipowner6: Optional[Union[list[str], Series[str], str]] = None, shipowner7: Optional[Union[list[str], Series[str], str]] = None, shipbuilder: Optional[Union[list[str], Series[str], str]] = None, countryOfBuild: Optional[Union[list[str], Series[str], str]] = None, flag: Optional[Union[list[str], Series[str], str]] = None, cargoContainmentSystem: Optional[Union[list[str], Series[str], str]] = None, vesselStatus: Optional[Union[list[str], Series[str], str]] = None, hullNumber: Optional[Union[list[str], Series[str], str]] = None, formerVesselNames: Optional[Union[list[str], Series[str], str]] = None, iceClass: Optional[Union[list[str], Series[str], str]] = None, nameCurrentlyInUse: Optional[Union[list[str], Series[str], str]] = None, contractDate: Optional[Union[list[date], Series[date], date]] = None, deliveryDate: Optional[Union[list[date], Series[date], date]] = None, retiredDate: Optional[Union[list[date], Series[date], date]] = None, vesselFeature: Optional[Union[list[str], Series[str], str]] = None, vesselFeatureUom: Optional[Union[list[str], Series[str], str]] = None, vesselFeatureCurrency: Optional[Union[list[str], Series[str], str]] = None, vesselFeatureValue: Optional[Union[list[str], Series[str], str]] = None, createdDate: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         id: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each LNG vessel, be default None
         imoNumber: Optional[Union[list[str], Series[str], str]]
             International Maritime Organization (IMO) number assigned to the vessel, be default None
         vesselName: Optional[Union[list[str], Series[str], str]]
             Name of the LNG vessel, be default None
         propulsionSystem: Optional[Union[list[str], Series[str], str]]
             Propulsion type of vessel, be default None
         vesselType: Optional[Union[list[str], Series[str], str]]
             Type or classification of the vessel, be default None
         charterer: Optional[Union[list[str], Series[str], str]]
             Entity or company that charters or leases the vessel, be default None
         operator: Optional[Union[list[str], Series[str], str]]
             Entity or company responsible for operating the vessel, be default None
         shipowner: Optional[Union[list[str], Series[str], str]]
             Primary owner of the vessel, be default None
         shipowner2: Optional[Union[list[str], Series[str], str]]
             Additional owners of the vessel, if applicable, be default None
         shipowner3: Optional[Union[list[str], Series[str], str]]
             Additional owners of the vessel, if applicable, be default None
         shipowner4: Optional[Union[list[str], Series[str], str]]
             Additional owners of the vessel, if applicable, be default None
         shipowner5: Optional[Union[list[str], Series[str], str]]
             Additional owners of the vessel, if applicable, be default None
         shipowner6: Optional[Union[list[str], Series[str], str]]
             Additional owners of the vessel, if applicable, be default None
         shipowner7: Optional[Union[list[str], Series[str], str]]
             Additional owners of the vessel, if applicable, be default None
         shipbuilder: Optional[Union[list[str], Series[str], str]]
             Shipyard or company that constructed the vessel, be default None
         countryOfBuild: Optional[Union[list[str], Series[str], str]]
             Country where the vessel was built, be default None
         flag: Optional[Union[list[str], Series[str], str]]
             Flag state or country under which the vessel is registered, be default None
         cargoContainmentSystem: Optional[Union[list[str], Series[str], str]]
             System used for containing the LNG cargo on the vessel, be default None
         vesselStatus: Optional[Union[list[str], Series[str], str]]
             Current status of the vessel, be default None
         hullNumber: Optional[Union[list[str], Series[str], str]]
             Identification number assigned to the vessel's hull, be default None
         formerVesselNames: Optional[Union[list[str], Series[str], str]]
             Previous names or aliases of the vessel, be default None
         iceClass: Optional[Union[list[str], Series[str], str]]
             Yes or no if this is an ice class vessel, be default None
         nameCurrentlyInUse: Optional[Union[list[str], Series[str], str]]
             Yes or no if the identified name is currently in use, be default None
         contractDate: Optional[Union[list[date], Series[date], date]]
             Date when the vessel contract was signed, be default None
         deliveryDate: Optional[Union[list[date], Series[date], date]]
             Date when the vessel was delivered, be default None
         retiredDate: Optional[Union[list[date], Series[date], date]]
             Date when the vessel was retired from service, be default None
         vesselFeature: Optional[Union[list[str], Series[str], str]]
             Types of features of vessels ranging from capacity to cost to other facility-specific characteristics, be default None
         vesselFeatureUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the corresponding vessel feature, be default None
         vesselFeatureCurrency: Optional[Union[list[str], Series[str], str]]
             Currency of the corresponding vessel feature, be default None
         vesselFeatureValue: Optional[Union[list[str], Series[str], str]]
             Numeric values of the corresponding vessel feature, be default None
         createdDate: Optional[Union[list[str], Series[str], str]]
             Date when the vessel record was created, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Vessel record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        filter_params.append(list_to_filter("imoNumber", imoNumber))
        filter_params.append(list_to_filter("vesselName", vesselName))
        filter_params.append(list_to_filter("propulsionSystem", propulsionSystem))
        filter_params.append(list_to_filter("vesselType", vesselType))
        filter_params.append(list_to_filter("charterer", charterer))
        filter_params.append(list_to_filter("operator", operator))
        filter_params.append(list_to_filter("shipowner", shipowner))
        filter_params.append(list_to_filter("shipowner2", shipowner2))
        filter_params.append(list_to_filter("shipowner3", shipowner3))
        filter_params.append(list_to_filter("shipowner4", shipowner4))
        filter_params.append(list_to_filter("shipowner5", shipowner5))
        filter_params.append(list_to_filter("shipowner6", shipowner6))
        filter_params.append(list_to_filter("shipowner7", shipowner7))
        filter_params.append(list_to_filter("shipbuilder", shipbuilder))
        filter_params.append(list_to_filter("countryOfBuild", countryOfBuild))
        filter_params.append(list_to_filter("flag", flag))
        filter_params.append(list_to_filter("cargoContainmentSystem", cargoContainmentSystem))
        filter_params.append(list_to_filter("vesselStatus", vesselStatus))
        filter_params.append(list_to_filter("hullNumber", hullNumber))
        filter_params.append(list_to_filter("formerVesselNames", formerVesselNames))
        filter_params.append(list_to_filter("iceClass", iceClass))
        filter_params.append(list_to_filter("nameCurrentlyInUse", nameCurrentlyInUse))
        filter_params.append(list_to_filter("contractDate", contractDate))
        filter_params.append(list_to_filter("deliveryDate", deliveryDate))
        filter_params.append(list_to_filter("retiredDate", retiredDate))
        filter_params.append(list_to_filter("vesselFeature", vesselFeature))
        filter_params.append(list_to_filter("vesselFeatureUom", vesselFeatureUom))
        filter_params.append(list_to_filter("vesselFeatureCurrency", vesselFeatureCurrency))
        filter_params.append(list_to_filter("vesselFeatureValue", vesselFeatureValue))
        filter_params.append(list_to_filter("createdDate", createdDate))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/vessel",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_monthly_estimated_buildout_offtake_contracts(
        self, liquefactionProjectId: Optional[Union[list[str], Series[str], str]] = None, liquefactionSalesContractId: Optional[Union[list[str], Series[str], str]] = None, buildoutMonthEstimated: Optional[Union[list[str], Series[str], str]] = None, buildoutOfftakeEstimated: Optional[Union[list[str], Series[str], str]] = None, createdDateEstimated: Optional[Union[list[str], Series[str], str]] = None, modifiedDateEstimated: Optional[Union[list[str], Series[str], str]] = None, buyer: Optional[Union[list[str], Series[str], str]] = None, exporter: Optional[Union[list[str], Series[str], str]] = None, contractGroup: Optional[Union[list[str], Series[str], str]] = None, supplyMarket: Optional[Union[list[str], Series[str], str]] = None, assumedDestination: Optional[Union[list[str], Series[str], str]] = None, contractType: Optional[Union[list[str], Series[str], str]] = None, liquefactionProject: Optional[Union[list[str], Series[str], str]] = None, shippingTerms: Optional[Union[list[str], Series[str], str]] = None, destinationFlexibility: Optional[Union[list[str], Series[str], str]] = None, estimatedStartDate: Optional[Union[list[str], Series[str], str]] = None, estimatedEndDate: Optional[Union[list[str], Series[str], str]] = None, lengthYears: Optional[Union[list[str], Series[str], str]] = None, originalSigning: Optional[Union[list[str], Series[str], str]] = None, annualContractVolume: Optional[Union[list[str], Series[str], str]] = None, annualContractVolumeUom: Optional[Union[list[str], Series[str], str]] = None, initialContractVolume: Optional[Union[list[str], Series[str], str]] = None, initialContractVolumeUom: Optional[Union[list[str], Series[str], str]] = None, pricingLinkage: Optional[Union[list[str], Series[str], str]] = None, specificPriceLink: Optional[Union[list[str], Series[str], str]] = None, fidEnabling: Optional[Union[list[str], Series[str], str]] = None, greenOrBrownfield: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         liquefactionProjectId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each liquefaction project, be default None
         liquefactionSalesContractId: Optional[Union[list[str], Series[str], str]]
             Id for liquefaction sales contract, be default None
         buildoutMonthEstimated: Optional[Union[list[str], Series[str], str]]
             Month for which the estimated buildout information is provided, be default None
         buildoutOfftakeEstimated: Optional[Union[list[str], Series[str], str]]
             Offtake contract based on our estimated start date for the specified month, be default None
         createdDateEstimated: Optional[Union[list[str], Series[str], str]]
             Date when the estimated buildout information was created, be default None
         modifiedDateEstimated: Optional[Union[list[str], Series[str], str]]
             Date when the estimated buildout information was last modified, be default None
         buyer: Optional[Union[list[str], Series[str], str]]
             Entity or company purchasing the LNG, be default None
         exporter: Optional[Union[list[str], Series[str], str]]
             Entity or company exporting the LNG, be default None
         contractGroup: Optional[Union[list[str], Series[str], str]]
             Group or category to which the contract belongs, be default None
         supplyMarket: Optional[Union[list[str], Series[str], str]]
             Market where the offtake contract is located, be default None
         assumedDestination: Optional[Union[list[str], Series[str], str]]
             Assumed destination for the LNG, be default None
         contractType: Optional[Union[list[str], Series[str], str]]
             The status of the capacity contract, be default None
         liquefactionProject: Optional[Union[list[str], Series[str], str]]
             Name or title of the liquefaction project associated with the contract, be default None
         shippingTerms: Optional[Union[list[str], Series[str], str]]
             Terms and conditions related to the shipping of the LNG, be default None
         destinationFlexibility: Optional[Union[list[str], Series[str], str]]
             Designation if the offtake contract is destination-fixed or is flexible, be default None
         estimatedStartDate: Optional[Union[list[str], Series[str], str]]
             Estimated start date for the offtake contract, be default None
         estimatedEndDate: Optional[Union[list[str], Series[str], str]]
             Estimated end date for the offtake contract, be default None
         lengthYears: Optional[Union[list[str], Series[str], str]]
             Duration of the contract in years, be default None
         originalSigning: Optional[Union[list[str], Series[str], str]]
             Date when the contract was originally signed, be default None
         annualContractVolume: Optional[Union[list[str], Series[str], str]]
             Numeric values of the annual contract quantity for the given offtake contract, be default None
         annualContractVolumeUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the annual contract quantity for the given offtake contract, be default None
         initialContractVolume: Optional[Union[list[str], Series[str], str]]
             Numeric values of the initial contract quantity for the given offtake contract, be default None
         initialContractVolumeUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the initial contract quantity for the given offtake contract, be default None
         pricingLinkage: Optional[Union[list[str], Series[str], str]]
             Determining contract pricing, be default None
         specificPriceLink: Optional[Union[list[str], Series[str], str]]
             Prices formula or reference for contract pricing calculation, be default None
         fidEnabling: Optional[Union[list[str], Series[str], str]]
             Category denoting the timing of when the contract was signed relative to the project's FID milestone, be default None
         greenOrBrownfield: Optional[Union[list[str], Series[str], str]]
             Whether the contract is associated with a greenfield or brownfield facility or portfolio, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the offtake contract was last modified, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("liquefactionProjectId", liquefactionProjectId))
        filter_params.append(list_to_filter("liquefactionSalesContractId", liquefactionSalesContractId))
        filter_params.append(list_to_filter("buildoutMonthEstimated", buildoutMonthEstimated))
        filter_params.append(list_to_filter("buildoutOfftakeEstimated", buildoutOfftakeEstimated))
        filter_params.append(list_to_filter("createdDateEstimated", createdDateEstimated))
        filter_params.append(list_to_filter("modifiedDateEstimated", modifiedDateEstimated))
        filter_params.append(list_to_filter("buyer", buyer))
        filter_params.append(list_to_filter("exporter", exporter))
        filter_params.append(list_to_filter("contractGroup", contractGroup))
        filter_params.append(list_to_filter("supplyMarket", supplyMarket))
        filter_params.append(list_to_filter("assumedDestination", assumedDestination))
        filter_params.append(list_to_filter("contractType", contractType))
        filter_params.append(list_to_filter("liquefactionProject", liquefactionProject))
        filter_params.append(list_to_filter("shippingTerms", shippingTerms))
        filter_params.append(list_to_filter("destinationFlexibility", destinationFlexibility))
        filter_params.append(list_to_filter("estimatedStartDate", estimatedStartDate))
        filter_params.append(list_to_filter("estimatedEndDate", estimatedEndDate))
        filter_params.append(list_to_filter("lengthYears", lengthYears))
        filter_params.append(list_to_filter("originalSigning", originalSigning))
        filter_params.append(list_to_filter("annualContractVolume", annualContractVolume))
        filter_params.append(list_to_filter("annualContractVolumeUom", annualContractVolumeUom))
        filter_params.append(list_to_filter("initialContractVolume", initialContractVolume))
        filter_params.append(list_to_filter("initialContractVolumeUom", initialContractVolumeUom))
        filter_params.append(list_to_filter("pricingLinkage", pricingLinkage))
        filter_params.append(list_to_filter("specificPriceLink", specificPriceLink))
        filter_params.append(list_to_filter("fidEnabling", fidEnabling))
        filter_params.append(list_to_filter("greenOrBrownfield", greenOrBrownfield))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/monthly-estimated-buildout/offtake-contracts",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_monthly_estimated_buildout_liquefaction_capacity(
        self, liquefactionProjectId: Optional[Union[list[str], Series[str], str]] = None, liquefactionTrainId: Optional[Union[list[str], Series[str], str]] = None, buildoutMonthEstimated: Optional[Union[list[str], Series[str], str]] = None, buildoutCapacityEstimated: Optional[Union[list[str], Series[str], str]] = None, createdDateEstimated: Optional[Union[list[str], Series[str], str]] = None, modifiedDateEstimated: Optional[Union[list[str], Series[str], str]] = None, buildoutMonthAnnounced: Optional[Union[list[str], Series[str], str]] = None, buildoutCapacityAnnounced: Optional[Union[list[str], Series[str], str]] = None, createdDateAnnounced: Optional[Union[list[str], Series[str], str]] = None, modifiedDateAnnounced: Optional[Union[list[str], Series[str], str]] = None, liquefactionTrain: Optional[Union[list[str], Series[str], str]] = None, liquefactionProject: Optional[Union[list[str], Series[str], str]] = None, initialCapacity: Optional[Union[list[str], Series[str], str]] = None, initialCapacityUom: Optional[Union[list[str], Series[str], str]] = None, estimatedStartDate: Optional[Union[list[str], Series[str], str]] = None, announcedStartDate: Optional[Union[list[str], Series[str], str]] = None, trainStatus: Optional[Union[list[str], Series[str], str]] = None, greenBrownfield: Optional[Union[list[str], Series[str], str]] = None, liquefactionTechnology: Optional[Union[list[str], Series[str], str]] = None, supplyMarket: Optional[Union[list[str], Series[str], str]] = None, trainOperator: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         liquefactionProjectId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each liquefaction project, be default None
         liquefactionTrainId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each liquefaction train within a project, be default None
         buildoutMonthEstimated: Optional[Union[list[str], Series[str], str]]
             Month for which the estimated buildout information is provided, be default None
         buildoutCapacityEstimated: Optional[Union[list[str], Series[str], str]]
             Liquefaction capacity based on our estimated start date for the specified month, be default None
         createdDateEstimated: Optional[Union[list[str], Series[str], str]]
             Date when the estimated buildout information was created, be default None
         modifiedDateEstimated: Optional[Union[list[str], Series[str], str]]
             Date when the estimated buildout information was last modified, be default None
         buildoutMonthAnnounced: Optional[Union[list[str], Series[str], str]]
             Month for which the announced buildout information is provided, be default None
         buildoutCapacityAnnounced: Optional[Union[list[str], Series[str], str]]
             Liquefaction capacity based on the announced start date for the specified month, be default None
         createdDateAnnounced: Optional[Union[list[str], Series[str], str]]
             Date when the announced buildout information was created, be default None
         modifiedDateAnnounced: Optional[Union[list[str], Series[str], str]]
             Date when the announced buildout information was last modified, be default None
         liquefactionTrain: Optional[Union[list[str], Series[str], str]]
             Name or identifier of the liquefaction train associated with the buildout information, be default None
         liquefactionProject: Optional[Union[list[str], Series[str], str]]
             Name or title of the liquefaction project associated with the buildout information, be default None
         initialCapacity: Optional[Union[list[str], Series[str], str]]
             Numeric values of the initial capacity of the liquefaction train, be default None
         initialCapacityUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the initial capacity of the liquefaction train, be default None
         estimatedStartDate: Optional[Union[list[str], Series[str], str]]
             Our estimated start date for the liquefaction train, be default None
         announcedStartDate: Optional[Union[list[str], Series[str], str]]
             Announced start date for the liquefaction train, be default None
         trainStatus: Optional[Union[list[str], Series[str], str]]
             Status of the liquefaction train, be default None
         greenBrownfield: Optional[Union[list[str], Series[str], str]]
             Indicates whether the liquefaction project is greenfield or brownfield, be default None
         liquefactionTechnology: Optional[Union[list[str], Series[str], str]]
             Technology used for liquefaction, be default None
         supplyMarket: Optional[Union[list[str], Series[str], str]]
             Market where the liquefaction train is located, be default None
         trainOperator: Optional[Union[list[str], Series[str], str]]
             Entity or company operating the liquefaction train, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Liquefaction capacity monthly estimated buildout record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("liquefactionProjectId", liquefactionProjectId))
        filter_params.append(list_to_filter("liquefactionTrainId", liquefactionTrainId))
        filter_params.append(list_to_filter("buildoutMonthEstimated", buildoutMonthEstimated))
        filter_params.append(list_to_filter("buildoutCapacityEstimated", buildoutCapacityEstimated))
        filter_params.append(list_to_filter("createdDateEstimated", createdDateEstimated))
        filter_params.append(list_to_filter("modifiedDateEstimated", modifiedDateEstimated))
        filter_params.append(list_to_filter("buildoutMonthAnnounced", buildoutMonthAnnounced))
        filter_params.append(list_to_filter("buildoutCapacityAnnounced", buildoutCapacityAnnounced))
        filter_params.append(list_to_filter("createdDateAnnounced", createdDateAnnounced))
        filter_params.append(list_to_filter("modifiedDateAnnounced", modifiedDateAnnounced))
        filter_params.append(list_to_filter("liquefactionTrain", liquefactionTrain))
        filter_params.append(list_to_filter("liquefactionProject", liquefactionProject))
        filter_params.append(list_to_filter("initialCapacity", initialCapacity))
        filter_params.append(list_to_filter("initialCapacityUom", initialCapacityUom))
        filter_params.append(list_to_filter("estimatedStartDate", estimatedStartDate))
        filter_params.append(list_to_filter("announcedStartDate", announcedStartDate))
        filter_params.append(list_to_filter("trainStatus", trainStatus))
        filter_params.append(list_to_filter("greenBrownfield", greenBrownfield))
        filter_params.append(list_to_filter("liquefactionTechnology", liquefactionTechnology))
        filter_params.append(list_to_filter("supplyMarket", supplyMarket))
        filter_params.append(list_to_filter("trainOperator", trainOperator))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/monthly-estimated-buildout/liquefaction-capacity",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_monthly_estimated_buildout_regasification_contracts(
        self, regasificationContractId: Optional[Union[list[str], Series[str], str]] = None, regasificationPhaseId: Optional[Union[list[str], Series[str], str]] = None, buildoutMonthEstimated: Optional[Union[list[str], Series[str], str]] = None, buildoutContractEstimated: Optional[Union[list[str], Series[str], str]] = None, createdDateEstimated: Optional[Union[list[str], Series[str], str]] = None, modifiedDateEstimated: Optional[Union[list[str], Series[str], str]] = None, buildoutMonthAnnounced: Optional[Union[list[str], Series[str], str]] = None, buildoutContractAnnounced: Optional[Union[list[str], Series[str], str]] = None, createdDateAnnounced: Optional[Union[list[str], Series[str], str]] = None, modifiedDateAnnounced: Optional[Union[list[str], Series[str], str]] = None, regasificationPhase: Optional[Union[list[str], Series[str], str]] = None, regasificationTerminal: Optional[Union[list[str], Series[str], str]] = None, market: Optional[Union[list[str], Series[str], str]] = None, terminalType: Optional[Union[list[str], Series[str], str]] = None, contractType: Optional[Union[list[str], Series[str], str]] = None, capacityOwner: Optional[Union[list[str], Series[str], str]] = None, modelAs: Optional[Union[list[str], Series[str], str]] = None, contractVolume: Optional[Union[list[str], Series[str], str]] = None, contractVolumeUom: Optional[Union[list[str], Series[str], str]] = None, percentageOfPhase: Optional[Union[list[str], Series[str], str]] = None, phaseCapacity: Optional[Union[list[str], Series[str], str]] = None, phaseCapacityUom: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         regasificationContractId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each regasification contract, be default None
         regasificationPhaseId: Optional[Union[list[str], Series[str], str]]
             ID of the regasification phase the contract is associated with, be default None
         buildoutMonthEstimated: Optional[Union[list[str], Series[str], str]]
             Month the buildout is estimated to start, be default None
         buildoutContractEstimated: Optional[Union[list[str], Series[str], str]]
             Contract the buildout is estimated to start, be default None
         createdDateEstimated: Optional[Union[list[str], Series[str], str]]
             Date when the estimated buildout information was created, be default None
         modifiedDateEstimated: Optional[Union[list[str], Series[str], str]]
             Date when the estimated buildout information was last modified, be default None
         buildoutMonthAnnounced: Optional[Union[list[str], Series[str], str]]
             Month for which the announced buildout information is provided, be default None
         buildoutContractAnnounced: Optional[Union[list[str], Series[str], str]]
             Regasification contract based on the announced start date for the specified month, be default None
         createdDateAnnounced: Optional[Union[list[str], Series[str], str]]
             Date when the announced buildout information was created, be default None
         modifiedDateAnnounced: Optional[Union[list[str], Series[str], str]]
             Date when the announced buildout information was last modified, be default None
         regasificationPhase: Optional[Union[list[str], Series[str], str]]
             Name of regasification phase the contract is associated with, be default None
         regasificationTerminal: Optional[Union[list[str], Series[str], str]]
             Name of regasification terminal the contract is associated with, be default None
         market: Optional[Union[list[str], Series[str], str]]
             Market where the regasification contract is located, be default None
         terminalType: Optional[Union[list[str], Series[str], str]]
             Offshore or onshore terminal, be default None
         contractType: Optional[Union[list[str], Series[str], str]]
             The status of the capcity contract, be default None
         capacityOwner: Optional[Union[list[str], Series[str], str]]
             Company of joint venture name that owns the capacity contract, be default None
         modelAs: Optional[Union[list[str], Series[str], str]]
             Modelling approach take to calculate contract buildout, be default None
         contractVolume: Optional[Union[list[str], Series[str], str]]
             Numeric values of the contract quantity for the given regasification contract, be default None
         contractVolumeUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the contract quantity for the given regasification contract, be default None
         percentageOfPhase: Optional[Union[list[str], Series[str], str]]
             If 'Model As' is 'Percentage of Phase' then the share of the phase's capacity the owner has rights to, be default None
         phaseCapacity: Optional[Union[list[str], Series[str], str]]
             Numeric values of the regasification phase capacity for the corresponding regasification contract, be default None
         phaseCapacityUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the regasification phase capacity for the corresponding regasification contract, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Date when the regasification contract was last modified, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("regasificationContractId", regasificationContractId))
        filter_params.append(list_to_filter("regasificationPhaseId", regasificationPhaseId))
        filter_params.append(list_to_filter("buildoutMonthEstimated", buildoutMonthEstimated))
        filter_params.append(list_to_filter("buildoutContractEstimated", buildoutContractEstimated))
        filter_params.append(list_to_filter("createdDateEstimated", createdDateEstimated))
        filter_params.append(list_to_filter("modifiedDateEstimated", modifiedDateEstimated))
        filter_params.append(list_to_filter("buildoutMonthAnnounced", buildoutMonthAnnounced))
        filter_params.append(list_to_filter("buildoutContractAnnounced", buildoutContractAnnounced))
        filter_params.append(list_to_filter("createdDateAnnounced", createdDateAnnounced))
        filter_params.append(list_to_filter("modifiedDateAnnounced", modifiedDateAnnounced))
        filter_params.append(list_to_filter("regasificationPhase", regasificationPhase))
        filter_params.append(list_to_filter("regasificationTerminal", regasificationTerminal))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("terminalType", terminalType))
        filter_params.append(list_to_filter("contractType", contractType))
        filter_params.append(list_to_filter("capacityOwner", capacityOwner))
        filter_params.append(list_to_filter("modelAs", modelAs))
        filter_params.append(list_to_filter("contractVolume", contractVolume))
        filter_params.append(list_to_filter("contractVolumeUom", contractVolumeUom))
        filter_params.append(list_to_filter("percentageOfPhase", percentageOfPhase))
        filter_params.append(list_to_filter("phaseCapacity", phaseCapacity))
        filter_params.append(list_to_filter("phaseCapacityUom", phaseCapacityUom))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/monthly-estimated-buildout/regasification-contracts",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_monthly_estimated_buildout_regasification_capacity(
        self, regasificationTerminalId: Optional[Union[list[str], Series[str], str]] = None, regasificationPhaseId: Optional[Union[list[str], Series[str], str]] = None, buildoutMonthEstimated: Optional[Union[list[str], Series[str], str]] = None, buildoutCapacityEstimated: Optional[Union[list[str], Series[str], str]] = None, createdDateEstimated: Optional[Union[list[str], Series[str], str]] = None, modifiedDateEstimated: Optional[Union[list[str], Series[str], str]] = None, buildoutMonthAnnounced: Optional[Union[list[str], Series[str], str]] = None, buildoutCapacityAnnounced: Optional[Union[list[str], Series[str], str]] = None, createdDateAnnounced: Optional[Union[list[str], Series[str], str]] = None, modifiedDateAnnounced: Optional[Union[list[str], Series[str], str]] = None, regasificationPhase: Optional[Union[list[str], Series[str], str]] = None, regasificationProject: Optional[Union[list[str], Series[str], str]] = None, market: Optional[Union[list[str], Series[str], str]] = None, phaseStatus: Optional[Union[list[str], Series[str], str]] = None, estimatedStartDate: Optional[Union[list[str], Series[str], str]] = None, announcedStartDate: Optional[Union[list[str], Series[str], str]] = None, initialCapacity: Optional[Union[list[str], Series[str], str]] = None, initialCapacityUom: Optional[Union[list[str], Series[str], str]] = None, terminalType: Optional[Union[list[str], Series[str], str]] = None, terminalTypeName: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         regasificationTerminalId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each regasification terminal, be default None
         regasificationPhaseId: Optional[Union[list[str], Series[str], str]]
             Unique identifier for each regasification phase within a terminal, be default None
         buildoutMonthEstimated: Optional[Union[list[str], Series[str], str]]
             Month in which the estimated buildout of regasification capacity is expected to occur, be default None
         buildoutCapacityEstimated: Optional[Union[list[str], Series[str], str]]
             Regasification capacity based on our estimated start date for the specified month in billion cubic meters, be default None
         createdDateEstimated: Optional[Union[list[str], Series[str], str]]
             Date when the estimated buildout information was created, be default None
         modifiedDateEstimated: Optional[Union[list[str], Series[str], str]]
             Date when the estimated buildout information was last modified, be default None
         buildoutMonthAnnounced: Optional[Union[list[str], Series[str], str]]
             Month for which the announced buildout information is provided, be default None
         buildoutCapacityAnnounced: Optional[Union[list[str], Series[str], str]]
             Regasification capacity based on the announced start date for the specified month, be default None
         createdDateAnnounced: Optional[Union[list[str], Series[str], str]]
             Date when the announced buildout was created, be default None
         modifiedDateAnnounced: Optional[Union[list[str], Series[str], str]]
             Date when the announced buildout was last modified, be default None
         regasificationPhase: Optional[Union[list[str], Series[str], str]]
             Name of the regasification phase, be default None
         regasificationProject: Optional[Union[list[str], Series[str], str]]
             Name of the regasification project associated with the phase, be default None
         market: Optional[Union[list[str], Series[str], str]]
             Market where the regasification phase is located, be default None
         phaseStatus: Optional[Union[list[str], Series[str], str]]
             Status of the regasification phase, be default None
         estimatedStartDate: Optional[Union[list[str], Series[str], str]]
             Our estimated start date for the regasification phase, be default None
         announcedStartDate: Optional[Union[list[str], Series[str], str]]
             Announced start date for the regasification phase, be default None
         initialCapacity: Optional[Union[list[str], Series[str], str]]
             Numeric values of the initial capacity of the regasification phase, be default None
         initialCapacityUom: Optional[Union[list[str], Series[str], str]]
             Unit of measure of the initial capacity of the regasification phase, be default None
         terminalType: Optional[Union[list[str], Series[str], str]]
             Type of regasification terminal, be default None
         terminalTypeName: Optional[Union[list[str], Series[str], str]]
             Type of regasification terminal, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Regasification capacity monthly estimated buildout record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("regasificationTerminalId", regasificationTerminalId))
        filter_params.append(list_to_filter("regasificationPhaseId", regasificationPhaseId))
        filter_params.append(list_to_filter("buildoutMonthEstimated", buildoutMonthEstimated))
        filter_params.append(list_to_filter("buildoutCapacityEstimated", buildoutCapacityEstimated))
        filter_params.append(list_to_filter("createdDateEstimated", createdDateEstimated))
        filter_params.append(list_to_filter("modifiedDateEstimated", modifiedDateEstimated))
        filter_params.append(list_to_filter("buildoutMonthAnnounced", buildoutMonthAnnounced))
        filter_params.append(list_to_filter("buildoutCapacityAnnounced", buildoutCapacityAnnounced))
        filter_params.append(list_to_filter("createdDateAnnounced", createdDateAnnounced))
        filter_params.append(list_to_filter("modifiedDateAnnounced", modifiedDateAnnounced))
        filter_params.append(list_to_filter("regasificationPhase", regasificationPhase))
        filter_params.append(list_to_filter("regasificationProject", regasificationProject))
        filter_params.append(list_to_filter("market", market))
        filter_params.append(list_to_filter("phaseStatus", phaseStatus))
        filter_params.append(list_to_filter("estimatedStartDate", estimatedStartDate))
        filter_params.append(list_to_filter("announcedStartDate", announcedStartDate))
        filter_params.append(list_to_filter("initialCapacity", initialCapacity))
        filter_params.append(list_to_filter("initialCapacityUom", initialCapacityUom))
        filter_params.append(list_to_filter("terminalType", terminalType))
        filter_params.append(list_to_filter("terminalTypeName", terminalTypeName))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/monthly-estimated-buildout/regasification-capacity",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_assets_contracts_feedstock(
        self, economicGroup: Optional[Union[list[str], Series[str], str]] = None, liquefactionProject: Optional[Union[list[str], Series[str], str]] = None, feedstockAsset: Optional[Union[list[str], Series[str], str]] = None, supplyMarket: Optional[Union[list[str], Series[str], str]] = None, year: Optional[Union[list[str], Series[str], str]] = None, productionType: Optional[Union[list[str], Series[str], str]] = None, productionUom: Optional[Union[list[str], Series[str], str]] = None, production: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         economicGroup: Optional[Union[list[str], Series[str], str]]
             Classification of the project based on economic characteristics, be default None
         liquefactionProject: Optional[Union[list[str], Series[str], str]]
             Name of the specific liquefaction project, be default None
         feedstockAsset: Optional[Union[list[str], Series[str], str]]
             The specific feedstock asset used in the liquefaction process, be default None
         supplyMarket: Optional[Union[list[str], Series[str], str]]
             The market from which the feedstock is sourced, be default None
         year: Optional[Union[list[str], Series[str], str]]
             The year to which the data corresponds, be default None
         productionType: Optional[Union[list[str], Series[str], str]]
             The category of production. This includes different commodities as well as the capacity of feedstock gas into the liquefaction project, be default None
         productionUom: Optional[Union[list[str], Series[str], str]]
             The unit of measure of the production rate for a given commidity or capacity rate of the liquefaction project, be default None
         production: Optional[Union[list[str], Series[str], str]]
             The production volumes of a given commidity or the gas in plant capacity rate of the liquefaction project, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Feedstock record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("economicGroup", economicGroup))
        filter_params.append(list_to_filter("liquefactionProject", liquefactionProject))
        filter_params.append(list_to_filter("feedstockAsset", feedstockAsset))
        filter_params.append(list_to_filter("supplyMarket", supplyMarket))
        filter_params.append(list_to_filter("year", year))
        filter_params.append(list_to_filter("productionType", productionType))
        filter_params.append(list_to_filter("productionUom", productionUom))
        filter_params.append(list_to_filter("production", production))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/analytics/assets-contracts/feedstock",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    @staticmethod
    def _convert_to_df(resp: Response) -> pd.DataFrame:
        j = resp.json()
        df = pd.json_normalize(j["results"])  # type: ignore
        
        if "modifiedDate" in df.columns:
            df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])  # type: ignore

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])  # type: ignore

        if "month" in df.columns:
            df["month"] = pd.to_datetime(df["month"])  # type: ignore

        if "createdDate" in df.columns:
            df["createdDate"] = pd.to_datetime(df["createdDate"])  # type: ignore

        if "shareholderModifiedDate" in df.columns:
            df["shareholderModifiedDate"] = pd.to_datetime(df["shareholderModifiedDate"])  # type: ignore

        if "shareModifiedDate" in df.columns:
            df["shareModifiedDate"] = pd.to_datetime(df["shareModifiedDate"])  # type: ignore

        if "ownershipStartDate" in df.columns:
            df["ownershipStartDate"] = pd.to_datetime(df["ownershipStartDate"])  # type: ignore

        if "ownershipEndDate" in df.columns:
            df["ownershipEndDate"] = pd.to_datetime(df["ownershipEndDate"])  # type: ignore

        if "announcedStartDate" in df.columns:
            df["announcedStartDate"] = pd.to_datetime(df["announcedStartDate"])  # type: ignore

        if "estimatedStartDate" in df.columns:
            df["estimatedStartDate"] = pd.to_datetime(df["estimatedStartDate"])  # type: ignore

        if "capexDate" in df.columns:
            df["capexDate"] = pd.to_datetime(df["capexDate"])  # type: ignore

        if "offlineDate" in df.columns:
            df["offlineDate"] = pd.to_datetime(df["offlineDate"])  # type: ignore

        if "statusModifiedDate" in df.columns:
            df["statusModifiedDate"] = pd.to_datetime(df["statusModifiedDate"])  # type: ignore

        if "capacityModifiedDate" in df.columns:
            df["capacityModifiedDate"] = pd.to_datetime(df["capacityModifiedDate"])  # type: ignore

        if "announcedStartDateModifiedDate" in df.columns:
            df["announcedStartDateModifiedDate"] = pd.to_datetime(df["announcedStartDateModifiedDate"])  # type: ignore

        if "estimatedStartDateModifiedDate" in df.columns:
            df["estimatedStartDateModifiedDate"] = pd.to_datetime(df["estimatedStartDateModifiedDate"])  # type: ignore

        if "announcedStartDateAtFinalInvestmentDecision" in df.columns:
            df["announcedStartDateAtFinalInvestmentDecision"] = pd.to_datetime(df["announcedStartDateAtFinalInvestmentDecision"])  # type: ignore

        if "latestAnnouncedFinalInvestmentDecisionDate" in df.columns:
            df["latestAnnouncedFinalInvestmentDecisionDate"] = pd.to_datetime(df["latestAnnouncedFinalInvestmentDecisionDate"])  # type: ignore

        if "estimatedFirstCargoDate" in df.columns:
            df["estimatedFirstCargoDate"] = pd.to_datetime(df["estimatedFirstCargoDate"])  # type: ignore

        if "estimatedFinalInvestmentDecisionDate" in df.columns:
            df["estimatedFinalInvestmentDecisionDate"] = pd.to_datetime(df["estimatedFinalInvestmentDecisionDate"])  # type: ignore

        if "originalSigningDate" in df.columns:
            df["originalSigningDate"] = pd.to_datetime(df["originalSigningDate"])  # type: ignore

        if "preliminarySigningDate" in df.columns:
            df["preliminarySigningDate"] = pd.to_datetime(df["preliminarySigningDate"])  # type: ignore

        if "contractPriceAsOfDate" in df.columns:
            df["contractPriceAsOfDate"] = pd.to_datetime(df["contractPriceAsOfDate"])  # type: ignore

        if "buyerModifiedDate" in df.columns:
            df["buyerModifiedDate"] = pd.to_datetime(df["buyerModifiedDate"])  # type: ignore

        if "announcedStartModifiedDate" in df.columns:
            df["announcedStartModifiedDate"] = pd.to_datetime(df["announcedStartModifiedDate"])  # type: ignore

        if "lengthModifiedDate" in df.columns:
            df["lengthModifiedDate"] = pd.to_datetime(df["lengthModifiedDate"])  # type: ignore

        if "publishedVolumeModifiedDate" in df.columns:
            df["publishedVolumeModifiedDate"] = pd.to_datetime(df["publishedVolumeModifiedDate"])  # type: ignore

        if "estimatedBuildoutModifiedDate" in df.columns:
            df["estimatedBuildoutModifiedDate"] = pd.to_datetime(df["estimatedBuildoutModifiedDate"])  # type: ignore

        if "contractStartDate" in df.columns:
            df["contractStartDate"] = pd.to_datetime(df["contractStartDate"])  # type: ignore

        if "startModifiedDate" in df.columns:
            df["startModifiedDate"] = pd.to_datetime(df["startModifiedDate"])  # type: ignore

        if "capacityOwnerModifiedDate" in df.columns:
            df["capacityOwnerModifiedDate"] = pd.to_datetime(df["capacityOwnerModifiedDate"])  # type: ignore

        if "typeModifiedDate" in df.columns:
            df["typeModifiedDate"] = pd.to_datetime(df["typeModifiedDate"])  # type: ignore

        if "announcedStartDateOriginal" in df.columns:
            df["announcedStartDateOriginal"] = pd.to_datetime(df["announcedStartDateOriginal"])  # type: ignore

        if "datePhaseFirstAnnounced" in df.columns:
            df["datePhaseFirstAnnounced"] = pd.to_datetime(df["datePhaseFirstAnnounced"])  # type: ignore

        if "contractDate" in df.columns:
            df["contractDate"] = pd.to_datetime(df["contractDate"])  # type: ignore

        if "deliveryDate" in df.columns:
            df["deliveryDate"] = pd.to_datetime(df["deliveryDate"])  # type: ignore

        if "retiredDate" in df.columns:
            df["retiredDate"] = pd.to_datetime(df["retiredDate"])  # type: ignore

        if "buildoutMonthEstimated" in df.columns:
            df["buildoutMonthEstimated"] = pd.to_datetime(df["buildoutMonthEstimated"])  # type: ignore

        if "createdDateEstimated" in df.columns:
            df["createdDateEstimated"] = pd.to_datetime(df["createdDateEstimated"])  # type: ignore

        if "modifiedDateEstimated" in df.columns:
            df["modifiedDateEstimated"] = pd.to_datetime(df["modifiedDateEstimated"])  # type: ignore

        if "estimatedEndDate" in df.columns:
            df["estimatedEndDate"] = pd.to_datetime(df["estimatedEndDate"])  # type: ignore

        if "originalSigning" in df.columns:
            df["originalSigning"] = pd.to_datetime(df["originalSigning"])  # type: ignore

        if "buildoutMonthAnnounced" in df.columns:
            df["buildoutMonthAnnounced"] = pd.to_datetime(df["buildoutMonthAnnounced"])  # type: ignore

        if "createdDateAnnounced" in df.columns:
            df["createdDateAnnounced"] = pd.to_datetime(df["createdDateAnnounced"])  # type: ignore

        if "modifiedDateAnnounced" in df.columns:
            df["modifiedDateAnnounced"] = pd.to_datetime(df["modifiedDateAnnounced"])  # type: ignore
        return df
    
