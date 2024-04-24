
from __future__ import annotations
from typing import List, Optional, Union
from requests import Response
from spgci.api_client import get_data
from spgci.utilities import list_to_filter
from pandas import DataFrame, Series
from datetime import date
import pandas as pd

class Lng_global:
    _endpoint = "api/v1/"
    _reference_endpoint = "reference/v1/"
    _tender_details_endpoint = "/tenders"
    _v_current_fc_demand_endpoint = "/demand-forecast/current"
    _v_current_fc_supply_endpoint = "/supply-forecast/current"
    _v_platts_demand_endpoint = "/demand-forecast/history"
    _v_platts_supply_endpoint = "/supply-forecast/history"
    _bilateral_trade_endpoint = "/cargo/historical-bilateral-trade-flows"
    _trips_endpoint = "/cargo/trips"
    _event_partial_load_endpoint = "/cargo/events/partial-load"
    _event_partial_unload_endpoint = "/cargo/events/partial-unload"
    _event_partial_reexport_endpoint = "/cargo/events/partial-reexport"


    def get_tenders(
        self, tender_id: Optional[Union[list[str], Series[str], str]] = None, awardee_id: Optional[Union[list[str], Series[str], str]] = None, issued_by: Optional[Union[list[str], Series[str], str]] = None, tender_status: Optional[Union[list[str], Series[str], str]] = None, cargo_type: Optional[Union[list[str], Series[str], str]] = None, contract_type: Optional[Union[list[str], Series[str], str]] = None, contract_option: Optional[Union[list[str], Series[str], str]] = None, size_of_tender: Optional[Union[list[str], Series[str], str]] = None, number_of_cargoes: Optional[Union[list[str], Series[str], str]] = None, volume: Optional[Union[list[str], Series[str], str]] = None, price_marker: Optional[Union[list[str], Series[str], str]] = None, price: Optional[Union[list[str], Series[str], str]] = None, country_name: Optional[Union[list[str], Series[str], str]] = None, awardee_company: Optional[Union[list[str], Series[str], str]] = None, opening_date: Optional[Union[list[date], Series[date], date]] = None, closing_date: Optional[Union[list[date], Series[date], date]] = None, validity_date: Optional[Union[list[date], Series[date], date]] = None, lifting_delivery_period_from: Optional[Union[list[date], Series[date], date]] = None, lifting_delivery_period_to: Optional[Union[list[date], Series[date], date]] = None, loading_period: Optional[Union[list[str], Series[str], str]] = None, external_notes: Optional[Union[list[str], Series[str], str]] = None, result: Optional[Union[list[str], Series[str], str]] = None, last_modified_date: Optional[Union[list[date], Series[date], date]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         tender_id: Optional[Union[list[str], Series[str], str]]
             A unique identifier of tender, be default None
         awardee_id: Optional[Union[list[str], Series[str], str]]
             Distinct identifier for winning a bid in tenders process, be default None
         issued_by: Optional[Union[list[str], Series[str], str]]
             Name of company that issued the Tender, be default None
         tender_status: Optional[Union[list[str], Series[str], str]]
             Whether a tender has been bought or sold, be default None
         cargo_type: Optional[Union[list[str], Series[str], str]]
             Type of cargo offered (Mid Term/ Multiple/ Partial/ Single cargo), be default None
         contract_type: Optional[Union[list[str], Series[str], str]]
             LNG delivery type (FOB or DES), be default None
         contract_option: Optional[Union[list[str], Series[str], str]]
             Buy or sell, be default None
         size_of_tender: Optional[Union[list[str], Series[str], str]]
             Cargo or Volume, be default None
         number_of_cargoes: Optional[Union[list[str], Series[str], str]]
             Count of Cargos for each tender, be default None
         volume: Optional[Union[list[str], Series[str], str]]
             Volume of Cargos for each tender, be default None
         price_marker: Optional[Union[list[str], Series[str], str]]
             Price Marker that the Tender is connected, be default None
         price: Optional[Union[list[str], Series[str], str]]
             Tender's price, be default None
         country_name: Optional[Union[list[str], Series[str], str]]
             Country where the cargo is available, be default None
         awardee_company: Optional[Union[list[str], Series[str], str]]
             Company/ies that the Tender was awarded, be default None
         opening_date: Optional[Union[list[date], Series[date], date]]
             Tender opening date, be default None
         closing_date: Optional[Union[list[date], Series[date], date]]
             Tender closing date, be default None
         validity_date: Optional[Union[list[date], Series[date], date]]
             When the tender is awarded, be default None
         lifting_delivery_period_from: Optional[Union[list[date], Series[date], date]]
             Tender period start date, be default None
         lifting_delivery_period_to: Optional[Union[list[date], Series[date], date]]
             Tender period end date, be default None
         loading_period: Optional[Union[list[str], Series[str], str]]
             At lifting/delivery or average over period, be default None
         external_notes: Optional[Union[list[str], Series[str], str]]
             Any other notes around the tender, be default None
         result: Optional[Union[list[str], Series[str], str]]
             The results of the tender, be default None
         last_modified_date: Optional[Union[list[date], Series[date], date]]
             The latest date of modification for the tenders, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("tender_id", tender_id))
        filter_params.append(list_to_filter("awardee_id", awardee_id))
        filter_params.append(list_to_filter("issued_by", issued_by))
        filter_params.append(list_to_filter("tender_status", tender_status))
        filter_params.append(list_to_filter("cargo_type", cargo_type))
        filter_params.append(list_to_filter("contract_type", contract_type))
        filter_params.append(list_to_filter("contract_option", contract_option))
        filter_params.append(list_to_filter("size_of_tender", size_of_tender))
        filter_params.append(list_to_filter("number_of_cargoes", number_of_cargoes))
        filter_params.append(list_to_filter("volume", volume))
        filter_params.append(list_to_filter("price_marker", price_marker))
        filter_params.append(list_to_filter("price", price))
        filter_params.append(list_to_filter("country_name", country_name))
        filter_params.append(list_to_filter("awardee_company", awardee_company))
        filter_params.append(list_to_filter("opening_date", opening_date))
        filter_params.append(list_to_filter("closing_date", closing_date))
        filter_params.append(list_to_filter("validity_date", validity_date))
        filter_params.append(list_to_filter("lifting_delivery_period_from", lifting_delivery_period_from))
        filter_params.append(list_to_filter("lifting_delivery_period_to", lifting_delivery_period_to))
        filter_params.append(list_to_filter("loading_period", loading_period))
        filter_params.append(list_to_filter("external_notes", external_notes))
        filter_params.append(list_to_filter("result", result))
        filter_params.append(list_to_filter("last_modified_date", last_modified_date))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/tenders",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_demand_forecast_current(
        self, importMarket: Optional[Union[list[str], Series[str], str]] = None, month: Optional[Union[list[date], Series[date], date]] = None, demandMillionMetricTons: Optional[Union[list[str], Series[str], str]] = None, pointInTimeMonth: Optional[Union[list[date], Series[date], date]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         importMarket: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG is imported., be default None
         month: Optional[Union[list[date], Series[date], date]]
             A unit of time representing a period of approximately 30 days, be default None
         demandMillionMetricTons: Optional[Union[list[str], Series[str], str]]
             The quantity of LNG demand, typically in metric tons, required or requested for a specific period., be default None
         pointInTimeMonth: Optional[Union[list[date], Series[date], date]]
             A specific moment within a given month, often used for precise data or event references., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the current demand forecast, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("importMarket", importMarket))
        filter_params.append(list_to_filter("month", month))
        filter_params.append(list_to_filter("demandMillionMetricTons", demandMillionMetricTons))
        filter_params.append(list_to_filter("pointInTimeMonth", pointInTimeMonth))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/demand-forecast/current",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_supply_forecast_current(
        self, exportProject: Optional[Union[list[str], Series[str], str]] = None, exportMarket: Optional[Union[list[str], Series[str], str]] = None, month: Optional[Union[list[date], Series[date], date]] = None, deliveredSupplyMillionMetricTons: Optional[Union[list[str], Series[str], str]] = None, pointInTimeMonth: Optional[Union[list[date], Series[date], date]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         exportProject: Optional[Union[list[str], Series[str], str]]
             A specific venture or initiative focused on exporting LNG supply  to international markets., be default None
         exportMarket: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG supply are being sold and shipped from a particular location., be default None
         month: Optional[Union[list[date], Series[date], date]]
             A unit of time representing a period of approximately 30 days, be default None
         deliveredSupplyMillionMetricTons: Optional[Union[list[str], Series[str], str]]
             The quantity of LNG supply , typically in metric tons, that has been delivered to its destination., be default None
         pointInTimeMonth: Optional[Union[list[date], Series[date], date]]
             A specific moment within a given month, often used for precise data or event references., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the current supply forecast, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("exportProject", exportProject))
        filter_params.append(list_to_filter("exportMarket", exportMarket))
        filter_params.append(list_to_filter("month", month))
        filter_params.append(list_to_filter("deliveredSupplyMillionMetricTons", deliveredSupplyMillionMetricTons))
        filter_params.append(list_to_filter("pointInTimeMonth", pointInTimeMonth))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/supply-forecast/current",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_demand_forecast_history(
        self, importMarket: Optional[Union[list[str], Series[str], str]] = None, month: Optional[Union[list[date], Series[date], date]] = None, demandMillionMetricTons: Optional[Union[list[str], Series[str], str]] = None, pointInTimeMonth: Optional[Union[list[date], Series[date], date]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         importMarket: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG is imported., be default None
         month: Optional[Union[list[date], Series[date], date]]
             A unit of time representing a period of approximately 30 days, be default None
         demandMillionMetricTons: Optional[Union[list[str], Series[str], str]]
             The quantity of LNG demand, typically in metric tons, required or requested for a specific period., be default None
         pointInTimeMonth: Optional[Union[list[date], Series[date], date]]
             A specific moment within a given month, often used for precise data or event references., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the historical demand forecast, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("importMarket", importMarket))
        filter_params.append(list_to_filter("month", month))
        filter_params.append(list_to_filter("demandMillionMetricTons", demandMillionMetricTons))
        filter_params.append(list_to_filter("pointInTimeMonth", pointInTimeMonth))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/demand-forecast/history",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_supply_forecast_history(
        self, exportProject: Optional[Union[list[str], Series[str], str]] = None, exportMarket: Optional[Union[list[str], Series[str], str]] = None, month: Optional[Union[list[date], Series[date], date]] = None, deliveredSupplyMillionMetricTons: Optional[Union[list[str], Series[str], str]] = None, pointInTimeMonth: Optional[Union[list[date], Series[date], date]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         exportProject: Optional[Union[list[str], Series[str], str]]
             A specific venture or initiative focused on exporting LNG supply to international markets., be default None
         exportMarket: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG supply are being sold and shipped from a particular location., be default None
         month: Optional[Union[list[date], Series[date], date]]
             A unit of time representing a period of approximately 30 days., be default None
         deliveredSupplyMillionMetricTons: Optional[Union[list[str], Series[str], str]]
             The quantity of LNG supply , typically in metric tons, that has been delivered to its destination., be default None
         pointInTimeMonth: Optional[Union[list[date], Series[date], date]]
             A specific moment within a given month, often used for precise data or event references., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             The latest date of modification for the historical supply forecast, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("exportProject", exportProject))
        filter_params.append(list_to_filter("exportMarket", exportMarket))
        filter_params.append(list_to_filter("month", month))
        filter_params.append(list_to_filter("deliveredSupplyMillionMetricTons", deliveredSupplyMillionMetricTons))
        filter_params.append(list_to_filter("pointInTimeMonth", pointInTimeMonth))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/supply-forecast/history",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_cargo_historical_bilateral_trade_flows(
        self, exportMarket: Optional[Union[list[str], Series[str], str]] = None, importMarket: Optional[Union[list[str], Series[str], str]] = None, monthArrived: Optional[Union[list[date], Series[date], date]] = None, volumeInMetricTons: Optional[Union[list[str], Series[str], str]] = None, volumeInBillionCubicMeters: Optional[Union[list[str], Series[str], str]] = None, volumeInBillionCubicFeet: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         exportMarket: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG supply are being sold and shipped., be default None
         importMarket: Optional[Union[list[str], Series[str], str]]
             The specific country where LNG supply are being bought., be default None
         monthArrived: Optional[Union[list[date], Series[date], date]]
             A unit of time representing a period of approximately 30 days., be default None
         volumeInMetricTons: Optional[Union[list[str], Series[str], str]]
             The quantity of LNG supply in metric tons, that has been delivered by the export market to the particular import market., be default None
         volumeInBillionCubicMeters: Optional[Union[list[str], Series[str], str]]
             The quantity of LNG supply in billion cubic meters, that has been delivered by the export market to the particular import market., be default None
         volumeInBillionCubicFeet: Optional[Union[list[str], Series[str], str]]
             The quantity of LNG supply in billion cubic feet, that has been delivered by the export market to the particular import market., be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             The latest date that this record was modified., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("exportMarket", exportMarket))
        filter_params.append(list_to_filter("importMarket", importMarket))
        filter_params.append(list_to_filter("monthArrived", monthArrived))
        filter_params.append(list_to_filter("volumeInMetricTons", volumeInMetricTons))
        filter_params.append(list_to_filter("volumeInBillionCubicMeters", volumeInBillionCubicMeters))
        filter_params.append(list_to_filter("volumeInBillionCubicFeet", volumeInBillionCubicFeet))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/cargo/historical-bilateral-trade-flows",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_cargo_trips(
        self, id: Optional[Union[list[str], Series[str], str]] = None, parentTripId: Optional[Union[list[str], Series[str], str]] = None, continuedFromTripId: Optional[Union[list[str], Series[str], str]] = None, vesselName: Optional[Union[list[str], Series[str], str]] = None, vesselImo: Optional[Union[list[str], Series[str], str]] = None, supplyPlant: Optional[Union[list[str], Series[str], str]] = None, tradeRoute: Optional[Union[list[str], Series[str], str]] = None, reexportPort: Optional[Union[list[str], Series[str], str]] = None, receivingPort: Optional[Union[list[str], Series[str], str]] = None, terminal: Optional[Union[list[str], Series[str], str]] = None, supplyMarket: Optional[Union[list[str], Series[str], str]] = None, receivingMarket: Optional[Union[list[str], Series[str], str]] = None, participant1Charterer: Optional[Union[list[str], Series[str], str]] = None, participant2Buyer: Optional[Union[list[str], Series[str], str]] = None, participants3Other: Optional[Union[list[str], Series[str], str]] = None, participants4Other: Optional[Union[list[str], Series[str], str]] = None, participants5Other: Optional[Union[list[str], Series[str], str]] = None, participants6Other: Optional[Union[list[str], Series[str], str]] = None, participants7Other: Optional[Union[list[str], Series[str], str]] = None, commissioningCargo: Optional[Union[list[str], Series[str], str]] = None, ladenBallast: Optional[Union[list[str], Series[str], str]] = None, supplyProjectParent: Optional[Union[list[str], Series[str], str]] = None, participant1Parent: Optional[Union[list[str], Series[str], str]] = None, participant2Parent: Optional[Union[list[str], Series[str], str]] = None, participant3Parent: Optional[Union[list[str], Series[str], str]] = None, participant4Parent: Optional[Union[list[str], Series[str], str]] = None, participant5Parent: Optional[Union[list[str], Series[str], str]] = None, participant6Parent: Optional[Union[list[str], Series[str], str]] = None, participant7Parent: Optional[Union[list[str], Series[str], str]] = None, isSpotOrShortTerm: Optional[Union[list[str], Series[str], str]] = None, capacityCbm: Optional[Union[list[str], Series[str], str]] = None, loadedCbm: Optional[Union[list[str], Series[str], str]] = None, loadedMt: Optional[Union[list[str], Series[str], str]] = None, loadedBcf: Optional[Union[list[str], Series[str], str]] = None, unloadedCbm: Optional[Union[list[str], Series[str], str]] = None, unloadedMt: Optional[Union[list[str], Series[str], str]] = None, unloadedBcf: Optional[Union[list[str], Series[str], str]] = None, boiloffRate: Optional[Union[list[str], Series[str], str]] = None, partialLoading: Optional[Union[list[str], Series[str], str]] = None, dateLoaded: Optional[Union[list[str], Series[str], str]] = None, partialUnloading: Optional[Union[list[str], Series[str], str]] = None, dateArrived: Optional[Union[list[str], Series[str], str]] = None, finalLoadedCbm: Optional[Union[list[str], Series[str], str]] = None, finalLoadedMt: Optional[Union[list[str], Series[str], str]] = None, finalLoadedBcf: Optional[Union[list[str], Series[str], str]] = None, finalUnloadedCbm: Optional[Union[list[str], Series[str], str]] = None, finalUnloadedMt: Optional[Union[list[str], Series[str], str]] = None, finalUnloadedBcf: Optional[Union[list[str], Series[str], str]] = None, ballastStartDate: Optional[Union[list[str], Series[str], str]] = None, ballastStartPort: Optional[Union[list[str], Series[str], str]] = None, ballastStartTerminal: Optional[Union[list[str], Series[str], str]] = None, ballastStartMarket: Optional[Union[list[str], Series[str], str]] = None, ballastEndDate: Optional[Union[list[str], Series[str], str]] = None, ballastEndPort: Optional[Union[list[str], Series[str], str]] = None, ballastEndMarket: Optional[Union[list[str], Series[str], str]] = None, transshipedLoadCbm: Optional[Union[list[str], Series[str], str]] = None, transshipedLoadBcf: Optional[Union[list[str], Series[str], str]] = None, transshipedUnloadCbm: Optional[Union[list[str], Series[str], str]] = None, transshipedUnloadBcf: Optional[Union[list[str], Series[str], str]] = None, transshipmentPort: Optional[Union[list[str], Series[str], str]] = None, transshipmentDate: Optional[Union[list[str], Series[str], str]] = None, transshipedLoadMt: Optional[Union[list[str], Series[str], str]] = None, transshipedUnloadMt: Optional[Union[list[str], Series[str], str]] = None, createdDate: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         id: Optional[Union[list[str], Series[str], str]]
             Trip ID, be default None
         parentTripId: Optional[Union[list[str], Series[str], str]]
             Parent trip ID, be default None
         continuedFromTripId: Optional[Union[list[str], Series[str], str]]
             ID of another trip that this one is a continuation of. This ID facilitates tracking transshipped cargoes. , be default None
         vesselName: Optional[Union[list[str], Series[str], str]]
             Vessel name, be default None
         vesselImo: Optional[Union[list[str], Series[str], str]]
             Vessel IMO number, be default None
         supplyPlant: Optional[Union[list[str], Series[str], str]]
             Supply plant, be default None
         tradeRoute: Optional[Union[list[str], Series[str], str]]
             Trade route for the trip, be default None
         reexportPort: Optional[Union[list[str], Series[str], str]]
             Source port if this is a reexport trip, be default None
         receivingPort: Optional[Union[list[str], Series[str], str]]
             Receiving port, be default None
         terminal: Optional[Union[list[str], Series[str], str]]
             Receiving terminal, be default None
         supplyMarket: Optional[Union[list[str], Series[str], str]]
             Source market, be default None
         receivingMarket: Optional[Union[list[str], Series[str], str]]
             Receiving market, be default None
         participant1Charterer: Optional[Union[list[str], Series[str], str]]
             Charterer, be default None
         participant2Buyer: Optional[Union[list[str], Series[str], str]]
             Buyer, be default None
         participants3Other: Optional[Union[list[str], Series[str], str]]
             Other participant, be default None
         participants4Other: Optional[Union[list[str], Series[str], str]]
             Other participant, be default None
         participants5Other: Optional[Union[list[str], Series[str], str]]
             Other participant, be default None
         participants6Other: Optional[Union[list[str], Series[str], str]]
             Other participant, be default None
         participants7Other: Optional[Union[list[str], Series[str], str]]
             Other participant, be default None
         commissioningCargo: Optional[Union[list[str], Series[str], str]]
             Whether or not the trip is a commissioning cargo for the designated supply project, be default None
         ladenBallast: Optional[Union[list[str], Series[str], str]]
             Whether current trip is laden or ballast, be default None
         supplyProjectParent: Optional[Union[list[str], Series[str], str]]
             Supply project, be default None
         participant1Parent: Optional[Union[list[str], Series[str], str]]
             Charterer's parent company, be default None
         participant2Parent: Optional[Union[list[str], Series[str], str]]
             Buyer's parent company, be default None
         participant3Parent: Optional[Union[list[str], Series[str], str]]
             Other participant's parent company, be default None
         participant4Parent: Optional[Union[list[str], Series[str], str]]
             Other participant's parent company, be default None
         participant5Parent: Optional[Union[list[str], Series[str], str]]
             Other participant's parent company, be default None
         participant6Parent: Optional[Union[list[str], Series[str], str]]
             Other participant's parent company, be default None
         participant7Parent: Optional[Union[list[str], Series[str], str]]
             Other participant's parent company, be default None
         isSpotOrShortTerm: Optional[Union[list[str], Series[str], str]]
             Whether the trip's commercial status is designated as spot or short-term trip or a long-term trip, be default None
         capacityCbm: Optional[Union[list[str], Series[str], str]]
             Vessel's cargo capacity in cubic meters, be default None
         loadedCbm: Optional[Union[list[str], Series[str], str]]
             Cargo volume loaded in cubic meters, be default None
         loadedMt: Optional[Union[list[str], Series[str], str]]
             Cargo volume loaded in metric tons, be default None
         loadedBcf: Optional[Union[list[str], Series[str], str]]
             Cargo volume loaded in billion cubic feet, be default None
         unloadedCbm: Optional[Union[list[str], Series[str], str]]
             Cargo volume unloaded in cubic meters, be default None
         unloadedMt: Optional[Union[list[str], Series[str], str]]
             Cargo volume unloaded in metric tons, be default None
         unloadedBcf: Optional[Union[list[str], Series[str], str]]
             Cargo volume unloaded in billion cubic feet, be default None
         boiloffRate: Optional[Union[list[str], Series[str], str]]
             Boil-off rate of the vessel, be default None
         partialLoading: Optional[Union[list[str], Series[str], str]]
             Whether current trip involves multi-port cargo loading, be default None
         dateLoaded: Optional[Union[list[str], Series[str], str]]
             Cargo load date for the trip, be default None
         partialUnloading: Optional[Union[list[str], Series[str], str]]
             Whether current trip involves multi-port cargo unloading, be default None
         dateArrived: Optional[Union[list[str], Series[str], str]]
             Cargo unload date for the trip, be default None
         finalLoadedCbm: Optional[Union[list[str], Series[str], str]]
             Final loaded volume in cubic meters including multi port loadings if more than one loading occurs, be default None
         finalLoadedMt: Optional[Union[list[str], Series[str], str]]
             Final loaded volume in metric tons including multi port loadings if more than one loading occurs, be default None
         finalLoadedBcf: Optional[Union[list[str], Series[str], str]]
             Final loaded volume in billion cubic feet including multi port loadings if more than one loading occurs, be default None
         finalUnloadedCbm: Optional[Union[list[str], Series[str], str]]
             Final unloaded volume in cubic meters including multi port unloadings if more than one unloading occurs, be default None
         finalUnloadedMt: Optional[Union[list[str], Series[str], str]]
             Final unloaded volume in metric tons including multi port unloadings if more than one unloading occurs, be default None
         finalUnloadedBcf: Optional[Union[list[str], Series[str], str]]
             Final unloaded volume in billion cubic feet including multi port unloadings if more than one unloading occurs, be default None
         ballastStartDate: Optional[Union[list[str], Series[str], str]]
             Start date of ballast trip, be default None
         ballastStartPort: Optional[Union[list[str], Series[str], str]]
             Start port of ballast trip, be default None
         ballastStartTerminal: Optional[Union[list[str], Series[str], str]]
             Start terminal of ballast trip, be default None
         ballastStartMarket: Optional[Union[list[str], Series[str], str]]
             Start market of ballast trip, be default None
         ballastEndDate: Optional[Union[list[str], Series[str], str]]
             End date of ballast trip, be default None
         ballastEndPort: Optional[Union[list[str], Series[str], str]]
             End port of ballast trip, be default None
         ballastEndMarket: Optional[Union[list[str], Series[str], str]]
             End market of ballast trip, be default None
         transshipedLoadCbm: Optional[Union[list[str], Series[str], str]]
             Loaded volume of a transshiped cargo in cubic meters, be default None
         transshipedLoadBcf: Optional[Union[list[str], Series[str], str]]
             Loaded volume of a transshiped cargo in billion cubic feet, be default None
         transshipedUnloadCbm: Optional[Union[list[str], Series[str], str]]
             Unloaded volume of a transshiped cargo in cubic meters, be default None
         transshipedUnloadBcf: Optional[Union[list[str], Series[str], str]]
             Unloaded volume of a transshiped cargo in billion cubic feet, be default None
         transshipmentPort: Optional[Union[list[str], Series[str], str]]
             Port where transshipment occurred, be default None
         transshipmentDate: Optional[Union[list[str], Series[str], str]]
             Date of transshipment, be default None
         transshipedLoadMt: Optional[Union[list[str], Series[str], str]]
             Loaded volume of a transshiped cargo in metric tons, be default None
         transshipedUnloadMt: Optional[Union[list[str], Series[str], str]]
             Unloaded volume of a transshiped cargo in metric tons, be default None
         createdDate: Optional[Union[list[str], Series[str], str]]
             Trip record created date, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Trip record latest modified date, be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        filter_params.append(list_to_filter("parentTripId", parentTripId))
        filter_params.append(list_to_filter("continuedFromTripId", continuedFromTripId))
        filter_params.append(list_to_filter("vesselName", vesselName))
        filter_params.append(list_to_filter("vesselImo", vesselImo))
        filter_params.append(list_to_filter("supplyPlant", supplyPlant))
        filter_params.append(list_to_filter("tradeRoute", tradeRoute))
        filter_params.append(list_to_filter("reexportPort", reexportPort))
        filter_params.append(list_to_filter("receivingPort", receivingPort))
        filter_params.append(list_to_filter("terminal", terminal))
        filter_params.append(list_to_filter("supplyMarket", supplyMarket))
        filter_params.append(list_to_filter("receivingMarket", receivingMarket))
        filter_params.append(list_to_filter("participant1Charterer", participant1Charterer))
        filter_params.append(list_to_filter("participant2Buyer", participant2Buyer))
        filter_params.append(list_to_filter("participants3Other", participants3Other))
        filter_params.append(list_to_filter("participants4Other", participants4Other))
        filter_params.append(list_to_filter("participants5Other", participants5Other))
        filter_params.append(list_to_filter("participants6Other", participants6Other))
        filter_params.append(list_to_filter("participants7Other", participants7Other))
        filter_params.append(list_to_filter("commissioningCargo", commissioningCargo))
        filter_params.append(list_to_filter("ladenBallast", ladenBallast))
        filter_params.append(list_to_filter("supplyProjectParent", supplyProjectParent))
        filter_params.append(list_to_filter("participant1Parent", participant1Parent))
        filter_params.append(list_to_filter("participant2Parent", participant2Parent))
        filter_params.append(list_to_filter("participant3Parent", participant3Parent))
        filter_params.append(list_to_filter("participant4Parent", participant4Parent))
        filter_params.append(list_to_filter("participant5Parent", participant5Parent))
        filter_params.append(list_to_filter("participant6Parent", participant6Parent))
        filter_params.append(list_to_filter("participant7Parent", participant7Parent))
        filter_params.append(list_to_filter("isSpotOrShortTerm", isSpotOrShortTerm))
        filter_params.append(list_to_filter("capacityCbm", capacityCbm))
        filter_params.append(list_to_filter("loadedCbm", loadedCbm))
        filter_params.append(list_to_filter("loadedMt", loadedMt))
        filter_params.append(list_to_filter("loadedBcf", loadedBcf))
        filter_params.append(list_to_filter("unloadedCbm", unloadedCbm))
        filter_params.append(list_to_filter("unloadedMt", unloadedMt))
        filter_params.append(list_to_filter("unloadedBcf", unloadedBcf))
        filter_params.append(list_to_filter("boiloffRate", boiloffRate))
        filter_params.append(list_to_filter("partialLoading", partialLoading))
        filter_params.append(list_to_filter("dateLoaded", dateLoaded))
        filter_params.append(list_to_filter("partialUnloading", partialUnloading))
        filter_params.append(list_to_filter("dateArrived", dateArrived))
        filter_params.append(list_to_filter("finalLoadedCbm", finalLoadedCbm))
        filter_params.append(list_to_filter("finalLoadedMt", finalLoadedMt))
        filter_params.append(list_to_filter("finalLoadedBcf", finalLoadedBcf))
        filter_params.append(list_to_filter("finalUnloadedCbm", finalUnloadedCbm))
        filter_params.append(list_to_filter("finalUnloadedMt", finalUnloadedMt))
        filter_params.append(list_to_filter("finalUnloadedBcf", finalUnloadedBcf))
        filter_params.append(list_to_filter("ballastStartDate", ballastStartDate))
        filter_params.append(list_to_filter("ballastStartPort", ballastStartPort))
        filter_params.append(list_to_filter("ballastStartTerminal", ballastStartTerminal))
        filter_params.append(list_to_filter("ballastStartMarket", ballastStartMarket))
        filter_params.append(list_to_filter("ballastEndDate", ballastEndDate))
        filter_params.append(list_to_filter("ballastEndPort", ballastEndPort))
        filter_params.append(list_to_filter("ballastEndMarket", ballastEndMarket))
        filter_params.append(list_to_filter("transshipedLoadCbm", transshipedLoadCbm))
        filter_params.append(list_to_filter("transshipedLoadBcf", transshipedLoadBcf))
        filter_params.append(list_to_filter("transshipedUnloadCbm", transshipedUnloadCbm))
        filter_params.append(list_to_filter("transshipedUnloadBcf", transshipedUnloadBcf))
        filter_params.append(list_to_filter("transshipmentPort", transshipmentPort))
        filter_params.append(list_to_filter("transshipmentDate", transshipmentDate))
        filter_params.append(list_to_filter("transshipedLoadMt", transshipedLoadMt))
        filter_params.append(list_to_filter("transshipedUnloadMt", transshipedUnloadMt))
        filter_params.append(list_to_filter("createdDate", createdDate))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/cargo/trips",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_cargo_events_partial_load(
        self, id: Optional[Union[list[str], Series[str], str]] = None, tripId: Optional[Union[list[str], Series[str], str]] = None, eventType: Optional[Union[list[str], Series[str], str]] = None, loadDate: Optional[Union[list[str], Series[str], str]] = None, loadedCbm: Optional[Union[list[str], Series[str], str]] = None, loadedMt: Optional[Union[list[str], Series[str], str]] = None, loadedBcf: Optional[Union[list[str], Series[str], str]] = None, supplyPlant: Optional[Union[list[str], Series[str], str]] = None, supplyMarket: Optional[Union[list[str], Series[str], str]] = None, supplyProjectParent: Optional[Union[list[str], Series[str], str]] = None, createdDate: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         id: Optional[Union[list[str], Series[str], str]]
             Event ID, be default None
         tripId: Optional[Union[list[str], Series[str], str]]
             Trip ID of the corresponding trip, be default None
         eventType: Optional[Union[list[str], Series[str], str]]
             Event type, be default None
         loadDate: Optional[Union[list[str], Series[str], str]]
             Load date of the partial loading, be default None
         loadedCbm: Optional[Union[list[str], Series[str], str]]
             Cargo volume loaded in cubic meters, be default None
         loadedMt: Optional[Union[list[str], Series[str], str]]
             Cargo volume loaded in metric tons, be default None
         loadedBcf: Optional[Union[list[str], Series[str], str]]
             Cargo volume loaded in billion cubic feet, be default None
         supplyPlant: Optional[Union[list[str], Series[str], str]]
             Supply plant of the cargo, be default None
         supplyMarket: Optional[Union[list[str], Series[str], str]]
             Supply market of the cargo, be default None
         supplyProjectParent: Optional[Union[list[str], Series[str], str]]
             Supply project's parent, be default None
         createdDate: Optional[Union[list[str], Series[str], str]]
             Event record created date, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Event record latest modified date., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        filter_params.append(list_to_filter("tripId", tripId))
        filter_params.append(list_to_filter("eventType", eventType))
        filter_params.append(list_to_filter("loadDate", loadDate))
        filter_params.append(list_to_filter("loadedCbm", loadedCbm))
        filter_params.append(list_to_filter("loadedMt", loadedMt))
        filter_params.append(list_to_filter("loadedBcf", loadedBcf))
        filter_params.append(list_to_filter("supplyPlant", supplyPlant))
        filter_params.append(list_to_filter("supplyMarket", supplyMarket))
        filter_params.append(list_to_filter("supplyProjectParent", supplyProjectParent))
        filter_params.append(list_to_filter("createdDate", createdDate))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/cargo/events/partial-load",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_cargo_events_partial_unload(
        self, id: Optional[Union[list[str], Series[str], str]] = None, tripId: Optional[Union[list[str], Series[str], str]] = None, eventType: Optional[Union[list[str], Series[str], str]] = None, arrivalDate: Optional[Union[list[str], Series[str], str]] = None, buyer: Optional[Union[list[str], Series[str], str]] = None, isSpotOrShortTerm: Optional[Union[list[str], Series[str], str]] = None, unloadedCbm: Optional[Union[list[str], Series[str], str]] = None, unloadedMt: Optional[Union[list[str], Series[str], str]] = None, unloadedBcf: Optional[Union[list[str], Series[str], str]] = None, receivingPort: Optional[Union[list[str], Series[str], str]] = None, terminal: Optional[Union[list[str], Series[str], str]] = None, receivingMarket: Optional[Union[list[str], Series[str], str]] = None, createdDate: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         id: Optional[Union[list[str], Series[str], str]]
             Event ID, be default None
         tripId: Optional[Union[list[str], Series[str], str]]
             Trip ID of the corresponding trip, be default None
         eventType: Optional[Union[list[str], Series[str], str]]
             Event type, be default None
         arrivalDate: Optional[Union[list[str], Series[str], str]]
             Arrival date of the partial unloading, be default None
         buyer: Optional[Union[list[str], Series[str], str]]
             Buyer of the trip, be default None
         isSpotOrShortTerm: Optional[Union[list[str], Series[str], str]]
             Whether the delivery is a short-term trade or not, be default None
         unloadedCbm: Optional[Union[list[str], Series[str], str]]
             Cargo volume unloaded in cubic meters, be default None
         unloadedMt: Optional[Union[list[str], Series[str], str]]
             Cargo volume unloaded in metric tons, be default None
         unloadedBcf: Optional[Union[list[str], Series[str], str]]
             Cargo volume unloaded in billion cubic feet, be default None
         receivingPort: Optional[Union[list[str], Series[str], str]]
             Receiving port, be default None
         terminal: Optional[Union[list[str], Series[str], str]]
             Receiving terminal, be default None
         receivingMarket: Optional[Union[list[str], Series[str], str]]
             Receiving market, be default None
         createdDate: Optional[Union[list[str], Series[str], str]]
             Event record created date, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Event record latest modified date., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        filter_params.append(list_to_filter("tripId", tripId))
        filter_params.append(list_to_filter("eventType", eventType))
        filter_params.append(list_to_filter("arrivalDate", arrivalDate))
        filter_params.append(list_to_filter("buyer", buyer))
        filter_params.append(list_to_filter("isSpotOrShortTerm", isSpotOrShortTerm))
        filter_params.append(list_to_filter("unloadedCbm", unloadedCbm))
        filter_params.append(list_to_filter("unloadedMt", unloadedMt))
        filter_params.append(list_to_filter("unloadedBcf", unloadedBcf))
        filter_params.append(list_to_filter("receivingPort", receivingPort))
        filter_params.append(list_to_filter("terminal", terminal))
        filter_params.append(list_to_filter("receivingMarket", receivingMarket))
        filter_params.append(list_to_filter("createdDate", createdDate))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/cargo/events/partial-unload",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response


    def get_cargo_events_partial_reexport(
        self, id: Optional[Union[list[str], Series[str], str]] = None, tripId: Optional[Union[list[str], Series[str], str]] = None, eventType: Optional[Union[list[str], Series[str], str]] = None, reexportDate: Optional[Union[list[str], Series[str], str]] = None, loadedCbm: Optional[Union[list[str], Series[str], str]] = None, loadedMt: Optional[Union[list[str], Series[str], str]] = None, loadedBcf: Optional[Union[list[str], Series[str], str]] = None, supplyMarket: Optional[Union[list[str], Series[str], str]] = None, reexportPort: Optional[Union[list[str], Series[str], str]] = None, createdDate: Optional[Union[list[str], Series[str], str]] = None, modifiedDate: Optional[Union[list[str], Series[str], str]] = None, filter_exp: Optional[str] = None, page: int = 1, page_size: int = 1000, raw: bool = False, paginate: bool = False
    ) -> Union[DataFrame, Response]:
        """
        Fetch the data based on the filter expression.

        Parameters
        ----------
        
         id: Optional[Union[list[str], Series[str], str]]
             Event ID, be default None
         tripId: Optional[Union[list[str], Series[str], str]]
             Trip ID of the corresponding trip, be default None
         eventType: Optional[Union[list[str], Series[str], str]]
             Event type, be default None
         reexportDate: Optional[Union[list[str], Series[str], str]]
             Load date of the partial re-exporting, be default None
         loadedCbm: Optional[Union[list[str], Series[str], str]]
             Cargo volume re-exported in cubic meters, be default None
         loadedMt: Optional[Union[list[str], Series[str], str]]
             Cargo volume re-exported in metric tons, be default None
         loadedBcf: Optional[Union[list[str], Series[str], str]]
             Cargo volume re-exported in billion cubic feet, be default None
         supplyMarket: Optional[Union[list[str], Series[str], str]]
             Supply market of the re-export of the cargo, be default None
         reexportPort: Optional[Union[list[str], Series[str], str]]
             Source port of the re-export of the cargo, be default None
         createdDate: Optional[Union[list[str], Series[str], str]]
             Event record created date, be default None
         modifiedDate: Optional[Union[list[str], Series[str], str]]
             Event record latest modified date., be default None
         filter_exp: Optional[str] = None,
         page: int = 1,
         page_size: int = 1000,
         raw: bool = False,
         paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        filter_params.append(list_to_filter("tripId", tripId))
        filter_params.append(list_to_filter("eventType", eventType))
        filter_params.append(list_to_filter("reexportDate", reexportDate))
        filter_params.append(list_to_filter("loadedCbm", loadedCbm))
        filter_params.append(list_to_filter("loadedMt", loadedMt))
        filter_params.append(list_to_filter("loadedBcf", loadedBcf))
        filter_params.append(list_to_filter("supplyMarket", supplyMarket))
        filter_params.append(list_to_filter("reexportPort", reexportPort))
        filter_params.append(list_to_filter("createdDate", createdDate))
        filter_params.append(list_to_filter("modifiedDate", modifiedDate))
        
        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/lng/v1/cargo/events/partial-reexport",
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
        
        if "opening_date" in df.columns:
            df["opening_date"] = pd.to_datetime(df["opening_date"])  # type: ignore

        if "closing_date" in df.columns:
            df["closing_date"] = pd.to_datetime(df["closing_date"])  # type: ignore

        if "validity_date" in df.columns:
            df["validity_date"] = pd.to_datetime(df["validity_date"])  # type: ignore

        if "lifting_delivery_period_from" in df.columns:
            df["lifting_delivery_period_from"] = pd.to_datetime(df["lifting_delivery_period_from"])  # type: ignore

        if "lifting_delivery_period_to" in df.columns:
            df["lifting_delivery_period_to"] = pd.to_datetime(df["lifting_delivery_period_to"])  # type: ignore

        if "last_modified_date" in df.columns:
            df["last_modified_date"] = pd.to_datetime(df["last_modified_date"])  # type: ignore

        if "month" in df.columns:
            df["month"] = pd.to_datetime(df["month"])  # type: ignore

        if "pointInTimeMonth" in df.columns:
            df["pointInTimeMonth"] = pd.to_datetime(df["pointInTimeMonth"])  # type: ignore

        if "modifiedDate" in df.columns:
            df["modifiedDate"] = pd.to_datetime(df["modifiedDate"])  # type: ignore

        if "monthArrived" in df.columns:
            df["monthArrived"] = pd.to_datetime(df["monthArrived"])  # type: ignore

        if "dateLoaded" in df.columns:
            df["dateLoaded"] = pd.to_datetime(df["dateLoaded"])  # type: ignore

        if "dateArrived" in df.columns:
            df["dateArrived"] = pd.to_datetime(df["dateArrived"])  # type: ignore

        if "ballastStartDate" in df.columns:
            df["ballastStartDate"] = pd.to_datetime(df["ballastStartDate"])  # type: ignore

        if "ballastEndDate" in df.columns:
            df["ballastEndDate"] = pd.to_datetime(df["ballastEndDate"])  # type: ignore

        if "transshipmentDate" in df.columns:
            df["transshipmentDate"] = pd.to_datetime(df["transshipmentDate"])  # type: ignore

        if "createdDate" in df.columns:
            df["createdDate"] = pd.to_datetime(df["createdDate"])  # type: ignore

        if "dateValue" in df.columns:
            df["dateValue"] = pd.to_datetime(df["dateValue"])  # type: ignore

        if "loadDate" in df.columns:
            df["loadDate"] = pd.to_datetime(df["loadDate"])  # type: ignore

        if "arrivalDate" in df.columns:
            df["arrivalDate"] = pd.to_datetime(df["arrivalDate"])  # type: ignore

        if "reexportDate" in df.columns:
            df["reexportDate"] = pd.to_datetime(df["reexportDate"])  # type: ignore
        return df
    
