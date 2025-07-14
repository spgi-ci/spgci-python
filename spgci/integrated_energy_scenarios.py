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
from typing import Union, Optional, List, Literal
from pandas import DataFrame, Series  # type: ignore
from datetime import datetime
from packaging.version import parse
from requests import Response
from .utilities import list_to_filter
from .api_client import get_data
import pandas as pd


class IntegratedEnergyScenarios:

    _datasets = Literal[
        "coal-market",
        "employment",
        "final-energy-consumption",
        "gdp",
        "ghg-emissions",
        "natural-gas-markets",
        "oil-consumption-by-product",
        "oil-consumption-by-sector",
        "population-by-age",
        "population-urban-rural",
        "power-market-by-technology",
        "power-market-demand",
        "primary-energy-demand",
        "demand-sector-hierarchy",
        "geo-hierarchy",
        "ghg-sector-hierarchy",
        "ghg-source-hierarchy",
    ]

    def get_unique_values(
        self,
        dataset: _datasets,
        columns: Optional[Union[list[str], str]],
    ) -> DataFrame:
        dataset_to_path = {
            "coal-market": "carbon-scenarios/ies/v1/coal-market",
            "employment": "carbon-scenarios/ies/v1/employment",
            "final-energy-consumption": "carbon-scenarios/ies/v1/final-energy-consumption",
            "gdp": "carbon-scenarios/ies/v1/gdp",
            "ghg-emissions": "carbon-scenarios/ies/v1/ghg-emission",
            "natural-gas-markets": "carbon-scenarios/ies/v1/natural-gas-market",
            "oil-consumption-by-product": "carbon-scenarios/ies/v1/oil-consumption-by-product",
            "oil-consumption-by-sector": "carbon-scenarios/ies/v1/oil-consumption-by-sector",
            "population-by-age": "carbon-scenarios/ies/v1/population-by-age",
            "population-urban-rural": "carbon-scenarios/ies/v1/population-urban-rural",
            "power-market-by-technology": "carbon-scenarios/ies/v1/power-market-by-technology",
            "power-market-demand": "carbon-scenarios/ies/v1/power-market-demand",
            "primary-energy-demand": "carbon-scenarios/ies/v1/primary-energy-demand",
            "demand-sector-hierarchy": "carbon-scenarios/ies/v1/hierarchy/demand-sector",
            "geo-hierarchy": "carbon-scenarios/ies/v1/hierarchy/geo",
            "ghg-sector-hierarchy": "carbon-scenarios/ies/v1/hierarchy/ghg-sector",
            "ghg-source-hierarchy": "carbon-scenarios/ies/v1/hierarchy/ghg-source",
        }

        if dataset not in dataset_to_path:
            valid = "\n".join(dataset_to_path.keys())
            print(f"Dataset '{dataset}' not found. Valid Datasets:\n", valid)
            raise ValueError(
                f"dataset '{dataset}' not found ",
            )
            return
        else:
            path = dataset_to_path[dataset]

        col_value = ", ".join(columns) if isinstance(columns, list) else columns or ""
        params = {"GroupBy": col_value, "pageSize": 5000}

        def to_df(resp: Response):
            j = resp.json()
            return DataFrame(j["aggResultValue"])

        return get_data(path, params, to_df, paginate=True)

    def get_coal_market(
        self,
        *,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        subsector: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
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
        The Coal Markets dataset contains the outlooks to 2050 for S&P Global’s scenarios for coal demand by sectors. Data is provided in million tonne of oil equivalent (mtoe) for 1990-2050 timeframe in annual granularity for selected geographies.

        Parameters
        ----------

        scenario: Optional[Union[list[str], Series[str], str]]
            S&P Global's Energy and Climate Scenarios: Green Rules, Discord and Inflections (base planning scenario), and low emission cases: Accelerated CCS and Multitech Mitigation., by default None
        subsector: Optional[Union[list[str], Series[str], str]]
            Coal consumption sectors: power and heat, gas works, own use and other, industry, feedstocks, rail transport, other transport, residential, agricultural, and commercial., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast., by default None
        year: Optional[int], optional
            Forecast year, includes actuals for historic values., by default None
        year_gt: Optional[int], optional
            filter by `year > x`, by default None
        year_gte: Optional[int], optional
            filter by `year >= x`, by default None
        year_lt: Optional[int], optional
            filter by `year < x`, by default None
        year_lte: Optional[int], optional
            filter by `year <= x`, by default None
        modified_date: Optional[datetime], optional
            The last modified date for the corresponding record., by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("subsector", subsector))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/coal-market",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_employment(
        self,
        *,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        theme: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
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
        The Employment dataset contains the outlooks to 2050 for S&P Global’s scenarios for employment. Data is provided in million persons for 1990-2050 timeframe in annual granularity for selected geographies.

        Parameters
        ----------

        scenario: Optional[Union[list[str], Series[str], str]]
            S&P Global's Energy and Climate Scenarios: Green Rules, Discord and Inflections (base planning scenario), and low emission cases: Accelerated CCS and Multitech Mitigation., by default None
        theme: Optional[Union[list[str], Series[str], str]]
            Set to employment., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast., by default None
        year: Optional[int], optional
            Forecast year, includes actuals for historic values., by default None
        year_gt: Optional[int], optional
            filter by `year > x`, by default None
        year_gte: Optional[int], optional
            filter by `year >= x`, by default None
        year_lt: Optional[int], optional
            filter by `year < x`, by default None
        year_lte: Optional[int], optional
            filter by `year <= x`, by default None
        modified_date: Optional[datetime], optional
            The last modified date for the corresponding record., by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("theme", theme))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/employment",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_final_energy_consumption(
        self,
        *,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        energy_type: Optional[Union[list[str], Series[str], str]] = None,
        subsector: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
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
        The Final Energy Consumption dataset contains the outlooks to 2050 for S&P Global’s scenarios for energy demand by sectors and energy type. Data is provided in million tonne of oil equivalent (mtoe) for 1990-2050 timeframe in annual granularity for selected geographies.

        Parameters
        ----------

        scenario: Optional[Union[list[str], Series[str], str]]
            S&P Global's Energy and Climate Scenarios: Green Rules, Discord and Inflections (base planning scenario), and low emission cases: Accelerated CCS and Multitech Mitigation., by default None
        energy_type: Optional[Union[list[str], Series[str], str]]
            Energy types: electricity, oil, natural gas, coal, hydrogen, and other energy., by default None
        subsector: Optional[Union[list[str], Series[str], str]]
            Final energy consumption sectors: residential, agricultural, commercial, industry, feedstocks, road transport, rail transport, aviation transport, shipping transport, and other transport., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast., by default None
        year: Optional[int], optional
            Forecast year, includes actuals for historic values., by default None
        year_gt: Optional[int], optional
            filter by `year > x`, by default None
        year_gte: Optional[int], optional
            filter by `year >= x`, by default None
        year_lt: Optional[int], optional
            filter by `year < x`, by default None
        year_lte: Optional[int], optional
            filter by `year <= x`, by default None
        modified_date: Optional[datetime], optional
            The last modified date for the corresponding record., by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("energyType", energy_type))
        filter_params.append(list_to_filter("subsector", subsector))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/final-energy-consumption",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_gdp(
        self,
        *,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
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
        The GDP (gross domestic product) dataset contains the outlooks to 2050 for S&P Global’s scenarios for the following GDP series in their respective units: Real GDP (billion real US dollars), Real GDP PPP basis (billion real US dollars), Nominal GDP (billion nominal US dollars), Nominal GDP PPP basis (billion nominal US dollars). Data is provided for 1990-2050 timeframe in annual granularity for selected geographies.

        Parameters
        ----------

        scenario: Optional[Union[list[str], Series[str], str]]
            S&P Global's Energy and Climate Scenarios: Green Rules, Discord and Inflections (base planning scenario), and low emission cases: Accelerated CCS and Multitech Mitigation., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast., by default None
        year: Optional[int], optional
            Forecast year, includes actuals for historic values., by default None
        year_gt: Optional[int], optional
            filter by `year > x`, by default None
        year_gte: Optional[int], optional
            filter by `year >= x`, by default None
        year_lt: Optional[int], optional
            filter by `year < x`, by default None
        year_lte: Optional[int], optional
            filter by `year <= x`, by default None
        modified_date: Optional[datetime], optional
            The last modified date for the corresponding record., by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/gdp",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_ghg_emission(
        self,
        *,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        ccus_savings: Optional[Union[list[str], Series[str], str]] = None,
        sector: Optional[Union[list[str], Series[str], str]] = None,
        source_of_emissions: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
        modified_date: Optional[datetime] = None,
        modified_date_lt: Optional[datetime] = None,
        modified_date_lte: Optional[datetime] = None,
        modified_date_gt: Optional[datetime] = None,
        modified_date_gte: Optional[datetime] = None,
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False,
    ) -> Union[DataFrame, Response]:
        """
        The GHG Emissions dataset contains the outlooks to 2050 for S&P Global’s scenarios for energy related GHG emissions (including carbon dioxide emissions and methane emissions) from the sources of oil, natural gas, and coal by sectors; And for non-energy related greenhouse gas emissions by it's categories. Data is provided for three options in regard to CCUS: Including CCUS, Excluding CCUS, CCUS savings. Data is provided in million tonne of CO2 equivalent for 1990-2050 timeframe in annual granularity for selected geographies.

        Parameters
        ----------

        scenario: Optional[Union[list[str], Series[str], str]]
            S&P Global's Energy and Climate Scenarios: Green Rules, Discord and Inflections (base planning scenario), and low emission cases: Accelerated CCS and Multitech Mitigation., by default None
        ccus_savings: Optional[Union[list[str], Series[str], str]]
            Carbon capture, utilization, and storage (CCUS). Options available: including CCUS, excluding CCUS, and CCUS savings., by default None
        sector: Optional[Union[list[str], Series[str], str]]
            Sectors: power generation, district heating, refining, other sectors, residential, agricultural, commercial, industry, road and rail transport, international and domestic aviation and shipping transport, other transport, coke ovens, hydrogen generation, and non-energy from waste-related, agricultural, LULUCF, industrial processes and geo-engineering., by default None
        source_of_emissions: Optional[Union[list[str], Series[str], str]]
            Source of emissions: oil, natural gas, coal, biomass, undefined and non-energy., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast., by default None
        year: Optional[int], optional
            Forecast year, includes actuals for historic values., by default None
        year_gt: Optional[int], optional
            filter by `year > x`, by default None
        year_gte: Optional[int], optional
            filter by `year >= x`, by default None
        year_lt: Optional[int], optional
            filter by `year < x`, by default None
        year_lte: Optional[int], optional
            filter by `year <= x`, by default None
        modified_date: Optional[datetime], optional
            The last modified date for the corresponding record., by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("ccusSavings", ccus_savings))
        filter_params.append(list_to_filter("sector", sector))
        filter_params.append(list_to_filter("sourceOfEmissions", source_of_emissions))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/ghg-emission",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_natural_gas_market(
        self,
        *,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        subsector: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
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
        The Natural Gas Markets dataset contains the outlooks to 2050 for S&P Global’s scenarios for natural gas demand by sectors. Data is provided in million tonne of oil equivalent (mtoe) for 1990-2050 timeframe in annual granularity for selected geographies.

        Parameters
        ----------

        scenario: Optional[Union[list[str], Series[str], str]]
            S&P Global's Energy and Climate Scenarios: Green Rules, Discord and Inflections (base planning scenario), and low emission cases: Accelerated CCS and Multitech Mitigation., by default None
        subsector: Optional[Union[list[str], Series[str], str]]
            Natural gas consumption sectors: power and heat, own use and other, industry, feedstocks, road transport, rail transport, shipping transport, other transport, residential, agricultural, and commercial., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast., by default None
        year: Optional[int], optional
            Forecast year, includes actuals for historic values., by default None
        year_gt: Optional[int], optional
            filter by `year > x`, by default None
        year_gte: Optional[int], optional
            filter by `year >= x`, by default None
        year_lt: Optional[int], optional
            filter by `year < x`, by default None
        year_lte: Optional[int], optional
            filter by `year <= x`, by default None
        modified_date: Optional[datetime], optional
            The last modified date for the corresponding record., by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("subsector", subsector))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/natural-gas-market",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_oil_consumption_by_product(
        self,
        *,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        energy_type: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        unit: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
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
        The Oil Markets by Product dataset contains the outlooks to 2050 for S&P Global’s for total oil liquids demand by oil products. Data is provided in two units, in thousand barrel per day (kbbld) and in million tonne of oil equivalent (mtoe) for 1990-2050 timeframe in annual granularity for selected geographies.

        Parameters
        ----------

        scenario: Optional[Union[list[str], Series[str], str]]
            S&P Global's Energy and Climate Scenarios: Green Rules, Discord and Inflections (base planning scenario), and low emission cases: Accelerated CCS and Multitech Mitigation., by default None
        energy_type: Optional[Union[list[str], Series[str], str]]
            Energy products: gasoline, aviation gasoline, gas diesel oil, residual fuel oil, liquefied petroleum gas, jet fuel, kerosene, naphtha, other liquids, crude oil (direct), and refinery losses and adjustments., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast., by default None
        unit: Optional[Union[list[str], Series[str], str]]
            Unit of measurement. Ex: kbbld (thousand barrels per day) , mtoe (million tonnes of oil equivalent), by default None
        year: Optional[int], optional
            Forecast year, includes actuals for historic values., by default None
        year_gt: Optional[int], optional
            filter by `year > x`, by default None
        year_gte: Optional[int], optional
            filter by `year >= x`, by default None
        year_lt: Optional[int], optional
            filter by `year < x`, by default None
        year_lte: Optional[int], optional
            filter by `year <= x`, by default None
        modified_date: Optional[datetime], optional
            The last modified date for the corresponding record., by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("energyType", energy_type))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("unit", unit))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/oil-consumption-by-product",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_oil_consumption_by_sector(
        self,
        *,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        subsector: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        unit: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
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
        The Oil Markets by Sector dataset contains the outlooks to 2050 for S&P Global’s scenarios for total oil liquids demand by sectors. Data is provided in two units, in thousand barrel per day (kbbld) and in million tonne of oil equivalent (mtoe) for 1990-2050 timeframe in annual granularity for selected geographies.

        Parameters
        ----------

        scenario: Optional[Union[list[str], Series[str], str]]
            S&P Global's Energy and Climate Scenarios: Green Rules, Discord and Inflections (base planning scenario), and low emission cases: Accelerated CCS and Multitech Mitigation., by default None
        subsector: Optional[Union[list[str], Series[str], str]]
            Oil consumption sectors: residential, commercial, agricultural, industry, feedstocks, road transport, aviation transport, rail transport, shipping transport, other transport, power and heat, refinery, own use and other, and refinery losses and adjustments., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast., by default None
        unit: Optional[Union[list[str], Series[str], str]]
            Unit of measurement. Ex: kbbld (thousand barrels per day) , mtoe (million tonnes of oil equivalent), by default None
        year: Optional[int], optional
            Forecast year, includes actuals for historic values., by default None
        year_gt: Optional[int], optional
            filter by `year > x`, by default None
        year_gte: Optional[int], optional
            filter by `year >= x`, by default None
        year_lt: Optional[int], optional
            filter by `year < x`, by default None
        year_lte: Optional[int], optional
            filter by `year <= x`, by default None
        modified_date: Optional[datetime], optional
            The last modified date for the corresponding record., by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("subsector", subsector))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("unit", unit))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/oil-consumption-by-sector",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_population_by_age(
        self,
        *,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        series: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
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
        The Population by Age Group dataset contains the outlooks to 2050 for S&P Global’s scenarios for the following series: Age 0-14 (Youth), Age 15–64 (Working age), Age 64+ (Older). Data is provided in million persons for 1990-2050 timeframe in annual granularity for selected geographies.

        Parameters
        ----------

        scenario: Optional[Union[list[str], Series[str], str]]
            S&P Global's Energy and Climate Scenarios: Green Rules, Discord and Inflections (base planning scenario), and low emission cases: Accelerated CCS and Multitech Mitigation., by default None
        series: Optional[Union[list[str], Series[str], str]]
            Age groups: age 0-14 (youth), age 15-64 (working age), age 64+ (older)., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast., by default None
        year: Optional[int], optional
            Forecast year, includes actuals for historic values., by default None
        year_gt: Optional[int], optional
            filter by `year > x`, by default None
        year_gte: Optional[int], optional
            filter by `year >= x`, by default None
        year_lt: Optional[int], optional
            filter by `year < x`, by default None
        year_lte: Optional[int], optional
            filter by `year <= x`, by default None
        modified_date: Optional[datetime], optional
            The last modified date for the corresponding record., by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("series", series))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/population-by-age",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_population_urban_rural(
        self,
        *,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        series: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
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
        The Population - Urban and Rural dataset contains the outlooks to 2050 for S&P Global’s scenarios for the following population series: Urban population, Rural population. Data is provided in million persons for 1990-2050 timeframe in annual granularity for selected geographies.

        Parameters
        ----------

        scenario: Optional[Union[list[str], Series[str], str]]
            S&P Global's Energy and Climate Scenarios: Green Rules, Discord and Inflections (base planning scenario), and low emission cases: Accelerated CCS and Multitech Mitigation., by default None
        series: Optional[Union[list[str], Series[str], str]]
            Urban or rural population., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast., by default None
        year: Optional[int], optional
            Forecast year, includes actuals for historic values., by default None
        year_gt: Optional[int], optional
            filter by `year > x`, by default None
        year_gte: Optional[int], optional
            filter by `year >= x`, by default None
        year_lt: Optional[int], optional
            filter by `year < x`, by default None
        year_lte: Optional[int], optional
            filter by `year <= x`, by default None
        modified_date: Optional[datetime], optional
            The last modified date for the corresponding record., by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("series", series))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/population-urban-rural",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_power_market_by_technology(
        self,
        *,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        energy_or_technology_type: Optional[Union[list[str], Series[str], str]] = None,
        theme: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        unit: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
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
        The Power Markets by Technology dataset contains the outlooks to 2050 for S&P Global’s scenarios for the concepts like Installed capacity (GW), Fuel input into power generation (mtoe), Electricity generated (TWh) etc for the following technologies: Coal, Natural gas, Oil, Hydro, Nuclear, Onshore wind, Offshore wind, Solar PV, Solar CSP, Geothermal, Biomass and waste, Tidal, Battery storage, Hydrogen fuel cells. Data is provided for 1990-2050 timeframe in annual granularity for selected geographies.

        Parameters
        ----------

        scenario: Optional[Union[list[str], Series[str], str]]
            S&P Global's Energy and Climate Scenarios: Green Rules, Discord and Inflections (base planning scenario), and low emission cases: Accelerated CCS and Multitech Mitigation., by default None
        energy_or_technology_type: Optional[Union[list[str], Series[str], str]]
            Technology types: coal, natural gas, oil, hydro, nuclear, onshore wind, offshore wind, solar PV, solar CSP, geothermal, biomass and waste, tidal, battery storage, and hydrogen fuel cells., by default None
        theme: Optional[Union[list[str], Series[str], str]]
            Options available: installed capacity, fuel input into power generation, electricity generated, capacity net additions, capacity additions, capacity retirements., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast., by default None
        unit: Optional[Union[list[str], Series[str], str]]
            Unit of measurement. Ex: GW (gigawatt), mtoe (million tonnes of oil equivalent), TWh (terrawatt-hours), by default None
        year: Optional[int], optional
            Forecast year, includes actuals for historic values., by default None
        year_gt: Optional[int], optional
            filter by `year > x`, by default None
        year_gte: Optional[int], optional
            filter by `year >= x`, by default None
        year_lt: Optional[int], optional
            filter by `year < x`, by default None
        year_lte: Optional[int], optional
            filter by `year <= x`, by default None
        modified_date: Optional[datetime], optional
            The last modified date for the corresponding record., by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(
            list_to_filter("energyOrTechnologyType", energy_or_technology_type)
        )
        filter_params.append(list_to_filter("theme", theme))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("unit", unit))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/power-market-by-technology",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_power_market_demand(
        self,
        *,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        subsector: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
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
        The Power Markets Demand dataset contains the outlooks to 2050 for S&P Global’s scenarios for power demand by the sectors and for net trade. Data is provided in million tonne of oil equivalent (mtoe) for 1990-2050 timeframe in annual granularity for selected geographies.

        Parameters
        ----------

        scenario: Optional[Union[list[str], Series[str], str]]
            S&P Global's Energy and Climate Scenarios: Green Rules, Discord and Inflections (base planning scenario), and low emission cases: Accelerated CCS and Multitech Mitigation., by default None
        subsector: Optional[Union[list[str], Series[str], str]]
            Total consumption., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast., by default None
        year: Optional[int], optional
            Forecast year, includes actuals for historic values., by default None
        year_gt: Optional[int], optional
            filter by `year > x`, by default None
        year_gte: Optional[int], optional
            filter by `year >= x`, by default None
        year_lt: Optional[int], optional
            filter by `year < x`, by default None
        year_lte: Optional[int], optional
            filter by `year <= x`, by default None
        modified_date: Optional[datetime], optional
            The last modified date for the corresponding record., by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("subsector", subsector))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/power-market-demand",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_primary_energy_demand(
        self,
        *,
        scenario: Optional[Union[list[str], Series[str], str]] = None,
        energy_type: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        year: Optional[int] = None,
        year_lt: Optional[int] = None,
        year_lte: Optional[int] = None,
        year_gt: Optional[int] = None,
        year_gte: Optional[int] = None,
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
        The Primary Energy Demand dataset contains the outlooks to 2050 for S&P Global’s scenarios for primary energy demand by different energy types. Data is provided in million tonne of oil equivalent (mtoe) for 1990-2050 timeframe in annual granularity for selected geographies.

        Parameters
        ----------

        scenario: Optional[Union[list[str], Series[str], str]]
            S&P Global's Energy and Climate Scenarios: Green Rules, Discord and Inflections (base planning scenario), and low emission cases: Accelerated CCS and Multitech Mitigation., by default None
        energy_type: Optional[Union[list[str], Series[str], str]]
            Energy types: oil, natural gas, coal, hydro, nuclear, wind, solar, other renewables, traditional biomass, modern biomass, and other energy., by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast., by default None
        year: Optional[int], optional
            Forecast year, includes actuals for historic values., by default None
        year_gt: Optional[int], optional
            filter by `year > x`, by default None
        year_gte: Optional[int], optional
            filter by `year >= x`, by default None
        year_lt: Optional[int], optional
            filter by `year < x`, by default None
        year_lte: Optional[int], optional
            filter by `year <= x`, by default None
        modified_date: Optional[datetime], optional
            The last modified date for the corresponding record., by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("scenario", scenario))
        filter_params.append(list_to_filter("energyType", energy_type))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("year", year))
        if year_gt is not None:
            filter_params.append(f'year > "{year_gt}"')
        if year_gte is not None:
            filter_params.append(f'year >= "{year_gte}"')
        if year_lt is not None:
            filter_params.append(f'year < "{year_lt}"')
        if year_lte is not None:
            filter_params.append(f'year <= "{year_lte}"')
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/primary-energy-demand",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_hierarchy_demand_sector(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        source: Optional[Union[list[str], Series[str], str]] = None,
        consumption: Optional[Union[list[str], Series[str], str]] = None,
        main_sector: Optional[Union[list[str], Series[str], str]] = None,
        subsector: Optional[Union[list[str], Series[str], str]] = None,
        subsector_ranking: Optional[Union[list[str], Series[str], str]] = None,
        main_sector_ranking: Optional[Union[list[str], Series[str], str]] = None,
        ranking: Optional[Union[list[str], Series[str], str]] = None,
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
        This end point contains sectoral hierarchy for Gas, Coal, Oil and Final consumption data tables

        Parameters
        ----------

        id: Optional[int], optional
            Unique row identifier, by default None
        id_gt: Optional[int], optional
            filter by `id > x`, by default None
        id_gte: Optional[int], optional
            filter by `id >= x`, by default None
        id_lt: Optional[int], optional
            filter by `id < x`, by default None
        id_lte: Optional[int], optional
            filter by `id <= x`, by default None
        source: Optional[Union[list[str], Series[str], str]]
            source table name, by default None
        consumption: Optional[Union[list[str], Series[str], str]]
            Total consumption by coal,gas,oil and final consumption., by default None
        main_sector: Optional[Union[list[str], Series[str], str]]
            Main sectors for coal, gas, oil (Transformation and own use, Industry including Feedstocks, Domestic, Transport) and final sectors(Industry including Feedstocks, Domestic, Transport), by default None
        subsector: Optional[Union[list[str], Series[str], str]]
            Subsectors for coal, gas, oil and final sector consumption(Power and heat, Own use and other, Industry, Feedstocks, Commercial, Residential, Agricultural, Road transport, Rail transport, Shipping transport, Other transport etc.), by default None
        subsector_ranking: Optional[Union[list[str], Series[str], str]]
            Subsector ranking, by default None
        main_sector_ranking: Optional[Union[list[str], Series[str], str]]
            Main sector ranking, by default None
        ranking: Optional[Union[list[str], Series[str], str]]
            Ranking for coal, gas, oil and final consumption, by default None
        modified_date: Optional[datetime], optional
            Last modified date for this record, by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        if id_gt is not None:
            filter_params.append(f'id > "{id_gt}"')
        if id_gte is not None:
            filter_params.append(f'id >= "{id_gte}"')
        if id_lt is not None:
            filter_params.append(f'id < "{id_lt}"')
        if id_lte is not None:
            filter_params.append(f'id <= "{id_lte}"')
        filter_params.append(list_to_filter("source", source))
        filter_params.append(list_to_filter("consumption", consumption))
        filter_params.append(list_to_filter("mainSector", main_sector))
        filter_params.append(list_to_filter("subsector", subsector))
        filter_params.append(list_to_filter("subsectorRanking", subsector_ranking))
        filter_params.append(list_to_filter("mainSectorRanking", main_sector_ranking))
        filter_params.append(list_to_filter("ranking", ranking))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/hierarchy/demand-sector",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_hierarchy_geo(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        world: Optional[Union[list[str], Series[str], str]] = None,
        main_region: Optional[Union[list[str], Series[str], str]] = None,
        subregion: Optional[Union[list[str], Series[str], str]] = None,
        country: Optional[Union[list[str], Series[str], str]] = None,
        country_ranking: Optional[Union[list[str], Series[str], str]] = None,
        subregion_ranking: Optional[Union[list[str], Series[str], str]] = None,
        main_region_ranking: Optional[Union[list[str], Series[str], str]] = None,
        world_ranking: Optional[Union[list[str], Series[str], str]] = None,
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
        The Geography table contains seven main regions which are divided into subregions and countries. The seven main regions are Africa (North Africa, South Africa and Other Sub-Saharan Africa), Asia Pacific (Australia, Japan, South Korea, Other OECD Asia Pacific, China (mainland), India, Indonesia and Other Non-OECD Asia Pacific), CIS (Russia and Other CIS), Europe (France, Germany, Italy, Spain, Turkey, United Kingdom and Other Europe), Latin America (Argentina, Brazil and Other Latin America), Middle East (Iran, Saudi Arabia and Other Middle East) and North America (Canada, Mexico and United States).

        Parameters
        ----------

        id: Optional[int], optional
            Unique row identifier, by default None
        id_gt: Optional[int], optional
            filter by `id > x`, by default None
        id_gte: Optional[int], optional
            filter by `id >= x`, by default None
        id_lt: Optional[int], optional
            filter by `id < x`, by default None
        id_lte: Optional[int], optional
            filter by `id <= x`, by default None
        world: Optional[Union[list[str], Series[str], str]]
            World, by default None
        main_region: Optional[Union[list[str], Series[str], str]]
            This contains seven main regions(Africa, Asia Pacific, CIS, Europe, Latin America, Middle East, North America), by default None
        subregion: Optional[Union[list[str], Series[str], str]]
            Subregion ranking column, by default None
        country: Optional[Union[list[str], Series[str], str]]
            Geography for which data is forecast, by default None
        country_ranking: Optional[Union[list[str], Series[str], str]]
            Countries ranking column, by default None
        subregion_ranking: Optional[Union[list[str], Series[str], str]]
            Subregion ranking column, by default None
        main_region_ranking: Optional[Union[list[str], Series[str], str]]
            Main region ranking column, by default None
        world_ranking: Optional[Union[list[str], Series[str], str]]
            World ranking, by default None
        modified_date: Optional[datetime], optional
            Last modified date for this record, by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        if id_gt is not None:
            filter_params.append(f'id > "{id_gt}"')
        if id_gte is not None:
            filter_params.append(f'id >= "{id_gte}"')
        if id_lt is not None:
            filter_params.append(f'id < "{id_lt}"')
        if id_lte is not None:
            filter_params.append(f'id <= "{id_lte}"')
        filter_params.append(list_to_filter("world", world))
        filter_params.append(list_to_filter("mainRegion", main_region))
        filter_params.append(list_to_filter("subregion", subregion))
        filter_params.append(list_to_filter("country", country))
        filter_params.append(list_to_filter("countryRanking", country_ranking))
        filter_params.append(list_to_filter("subregionRanking", subregion_ranking))
        filter_params.append(list_to_filter("mainRegionRanking", main_region_ranking))
        filter_params.append(list_to_filter("worldRanking", world_ranking))
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/hierarchy/geo",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_hierarchy_ghg_sector(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        emissions: Optional[Union[list[str], Series[str], str]] = None,
        total_consumption_level: Optional[Union[list[str], Series[str], str]] = None,
        main_sector_level: Optional[Union[list[str], Series[str], str]] = None,
        subsector_level: Optional[Union[list[str], Series[str], str]] = None,
        energy_and_non_energy_related_ranking: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        total_consumption_level_ranking: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        main_sector_level_ranking: Optional[Union[list[str], Series[str], str]] = None,
        subsector_level_ranking: Optional[Union[list[str], Series[str], str]] = None,
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
        The Sectors of emissions are classified into Non-energy related GHG emission and Energy related GHG emissions. The origin of emissions are categorized into multiple consumption (and Main Sector) levels such as Transformation and other (Power generation, District heating, Hydrogen generation, Refining, Coke ovens and Other Sectors), Final Sectors (Industry, Transport and Domestic sectors) and Non-energy (from waste-related, agricultural, LULUCF, industrial process and geo-engineering). Some of the Main sectors are further divided into subsector levels such as Transport (Road transport, Rail transport, International aviation transport, Domestic aviation transport, International shipping transport, Domestic shipping transport and Other transport) and Domestic sectors (Residential, Commercial and Agricultural).

        Parameters
        ----------

        id: Optional[int], optional
            Unique row identifier, by default None
        id_gt: Optional[int], optional
            filter by `id > x`, by default None
        id_gte: Optional[int], optional
            filter by `id >= x`, by default None
        id_lt: Optional[int], optional
            filter by `id < x`, by default None
        id_lte: Optional[int], optional
            filter by `id <= x`, by default None
        emissions: Optional[Union[list[str], Series[str], str]]
            Energy related emissions and Non-energy related emissions, by default None
        total_consumption_level: Optional[Union[list[str], Series[str], str]]
            Non-energy from waste-related, Non-energy from agricultural, Non-energy from LULUCF, Non-energy from industrial processes, Non-energy from geo-engineering, Transformation and other, Final sectors, by default None
        main_sector_level: Optional[Union[list[str], Series[str], str]]
            Non-energy from waste-related, Non-energy from agricultural, Non-energy from LULUCF, Non-energy from industrial processes, Non-energy from geo-engineering, Power generation, District heating, Hydrogen generation, Refining, Coke ovens, Other sectors, Industry, Transport, Domestic sectors, by default None
        subsector_level: Optional[Union[list[str], Series[str], str]]
            Non-energy from waste-related, Non-energy from agricultural, Non-energy from LULUCF, Non-energy from industrial processes, Non-energy from geo-engineering, Power generation, District heating, Hydrogen generation, Refining, Coke ovens, Other sectors, Industry, Road transport, Rail transport, International aviation transport, Domestic aviation transport, International shipping transport, Domestic shipping transport, Other transport, by default None
        energy_and_non_energy_related_ranking: Optional[Union[list[str], Series[str], str]]
            Energy and non energy related ranking, by default None
        total_consumption_level_ranking: Optional[Union[list[str], Series[str], str]]
            Total consumption level ranking, by default None
        main_sector_level_ranking: Optional[Union[list[str], Series[str], str]]
            Main sector level ranking, by default None
        subsector_level_ranking: Optional[Union[list[str], Series[str], str]]
            Subsector level ranking, by default None
        modified_date: Optional[datetime], optional
            Last modified date for this record, by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        if id_gt is not None:
            filter_params.append(f'id > "{id_gt}"')
        if id_gte is not None:
            filter_params.append(f'id >= "{id_gte}"')
        if id_lt is not None:
            filter_params.append(f'id < "{id_lt}"')
        if id_lte is not None:
            filter_params.append(f'id <= "{id_lte}"')
        filter_params.append(list_to_filter("emissions", emissions))
        filter_params.append(
            list_to_filter("totalConsumptionLevel", total_consumption_level)
        )
        filter_params.append(list_to_filter("mainSectorLevel", main_sector_level))
        filter_params.append(list_to_filter("subsectorLevel", subsector_level))
        filter_params.append(
            list_to_filter(
                "energyAndNonEnergyRelatedRanking",
                energy_and_non_energy_related_ranking,
            )
        )
        filter_params.append(
            list_to_filter(
                "totalConsumptionLevelRanking", total_consumption_level_ranking
            )
        )
        filter_params.append(
            list_to_filter("mainSectorLevelRanking", main_sector_level_ranking)
        )
        filter_params.append(
            list_to_filter("subsectorLevelRanking", subsector_level_ranking)
        )
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/hierarchy/ghg-sector",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    def get_hierarchy_ghg_source(
        self,
        *,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_lte: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_gte: Optional[int] = None,
        emissions: Optional[Union[list[str], Series[str], str]] = None,
        energy_and_non_energy: Optional[Union[list[str], Series[str], str]] = None,
        source_of_emission: Optional[Union[list[str], Series[str], str]] = None,
        total_ghg_emissions_ranking: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        energy_and_non_energy_ranking: Optional[
            Union[list[str], Series[str], str]
        ] = None,
        source_of_emissions_ranking: Optional[
            Union[list[str], Series[str], str]
        ] = None,
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
        The Sources of GHG emissions are classified into Energy related emissions and Non-energy related emissions. The Energy-related emissions are further identified based on the source of emissions - oil, natural gas, coal, biomass and undefined (methane emissions).

        Parameters
        ----------

        id: Optional[int], optional
            Unique row identifier, by default None
        id_gt: Optional[int], optional
            filter by `id > x`, by default None
        id_gte: Optional[int], optional
            filter by `id >= x`, by default None
        id_lt: Optional[int], optional
            filter by `id < x`, by default None
        id_lte: Optional[int], optional
            filter by `id <= x`, by default None
        emissions: Optional[Union[list[str], Series[str], str]]
            Total GHG emissions, by default None
        energy_and_non_energy: Optional[Union[list[str], Series[str], str]]
            Energy related emissions and Non-energy related emissions, by default None
        source_of_emission: Optional[Union[list[str], Series[str], str]]
            Energy related emissions from oil, natural gas, coal, biomass and undefined (methane), and Non-energy related emissions, by default None
        total_ghg_emissions_ranking: Optional[Union[list[str], Series[str], str]]
            Total GHG emissions ranking, by default None
        energy_and_non_energy_ranking: Optional[Union[list[str], Series[str], str]]
            Energy and non energy ranking, by default None
        source_of_emissions_ranking: Optional[Union[list[str], Series[str], str]]
            Source of emissions ranking, by default None
        modified_date: Optional[datetime], optional
            Last modified date for this record, by default None
        modified_date_gt: Optional[datetime], optional
            filter by `modified_date > x`, by default None
        modified_date_gte: Optional[datetime], optional
            filter by `modified_date >= x`, by default None
        modified_date_lt: Optional[datetime], optional
            filter by `modified_date < x`, by default None
        modified_date_lte: Optional[datetime], optional
            filter by `modified_date <= x`, by default None
        filter_exp: Optional[str] = None,
        page: int = 1,
        page_size: int = 1000,
        raw: bool = False,
        paginate: bool = False

        """

        filter_params: List[str] = []
        filter_params.append(list_to_filter("id", id))
        if id_gt is not None:
            filter_params.append(f'id > "{id_gt}"')
        if id_gte is not None:
            filter_params.append(f'id >= "{id_gte}"')
        if id_lt is not None:
            filter_params.append(f'id < "{id_lt}"')
        if id_lte is not None:
            filter_params.append(f'id <= "{id_lte}"')
        filter_params.append(list_to_filter("emissions", emissions))
        filter_params.append(
            list_to_filter("energyAndNonEnergy", energy_and_non_energy)
        )
        filter_params.append(list_to_filter("sourceOfEmission", source_of_emission))
        filter_params.append(
            list_to_filter("totalGhgEmissionsRanking", total_ghg_emissions_ranking)
        )
        filter_params.append(
            list_to_filter("energyAndNonEnergyRanking", energy_and_non_energy_ranking)
        )
        filter_params.append(
            list_to_filter("sourceOfEmissionsRanking", source_of_emissions_ranking)
        )
        filter_params.append(list_to_filter("modifiedDate", modified_date))
        if modified_date_gt is not None:
            filter_params.append(f'modifiedDate > "{modified_date_gt}"')
        if modified_date_gte is not None:
            filter_params.append(f'modifiedDate >= "{modified_date_gte}"')
        if modified_date_lt is not None:
            filter_params.append(f'modifiedDate < "{modified_date_lt}"')
        if modified_date_lte is not None:
            filter_params.append(f'modifiedDate <= "{modified_date_lte}"')

        filter_params = [fp for fp in filter_params if fp != ""]

        if filter_exp is None:
            filter_exp = " AND ".join(filter_params)
        elif len(filter_params) > 0:
            filter_exp = " AND ".join(filter_params) + " AND (" + filter_exp + ")"

        params = {"page": page, "pageSize": page_size, "filter": filter_exp}

        response = get_data(
            path=f"/carbon-scenarios/ies/v1/hierarchy/ghg-source",
            params=params,
            df_fn=self._convert_to_df,
            raw=raw,
            paginate=paginate,
        )
        return response

    @staticmethod
    def _convert_to_df(resp: Response) -> DataFrame:
        j = resp.json()
        df = DataFrame(j["results"])
        date_columns = [
            "modifiedDate",
        ]

        for column in date_columns:
            if column in df.columns:
                if parse(pd.__version__) >= parse("2"):
                    df[column] = pd.to_datetime(
                        df[column], utc=True, format="ISO8601", errors="coerce"
                    )
                else:
                    df[column] = pd.to_datetime(df[column], errors="coerce", utc=True)  # type: ignore

        return df
