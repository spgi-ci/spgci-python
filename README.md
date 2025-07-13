# SPGCI Python Library

Python Client for the [S&P Global Commodity Insights API](https://developer.platts.com).

Looking for more examples? Check out our [Notebook Gallery](https://developer.spglobal.com/commodityinsights/tools/notebooks).

## Installation

Requires Python >= 3.9.0.

```bash
pip install spgci
```

## Getting Started

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/achristie/images/blob/master/readme.ipynb)

```python
    import spgci as ci

    ci.set_credentials(<username>, <password>)
    mdd = ci.MarketData()

    symbols = ["PCAAS00", "PCAAT00"]
    mdd.get_assessments_by_symbol_current(symbol=symbols)
```

## ![SPGCI](https://raw.githubusercontent.com/achristie/images/master/getting_started.gif)

---

Alternatively, you can set your credentials via Environment Variables and _omit_ the `set_credentials` call:
| Environment Variable | Description |
| :------------------- | :----------------|
| SPGCI_USERNAME | Your Username |
| SPGCI_PASSWORD | Your Password |

## Features

- Automatically generates token prior to making request.
- Returns data as a pandas DataFrame (set `raw=False` to get the raw `request.response` object).
- Can auto-paginate response and concatenates into a single DataFrame (set `paginate=True` to enable).
- Sets datatype for `date` and `datetime` fields in DataFrame.
- Composes nicely with native python/pandas types. Arguments support `lists` and `pd.Series` which are automatically converted into filter expressions.

## Datasets Supported

### Market Data

```python
import spgci as ci

mdd = ci.MarketData()

mdd.get_symbols(commodity="Crude oil")
# DataFrame of symbols with commodity = "Crude oil".

mdd.get_mdcs(subscribed_only=True)
# DataFrame of all Market Data Categories you are subscribed to.

mdd.get_assessments_by_mdc_current(mdc="ET")
# DataFrame of current assessments for all symbols in the Market Data Category "ET".
```

### Forward Curves

```python
import spgci as ci

fc = ci.ForwardCurves()

fc.get_curves(
    commodity=["Benzene", "Crude oil"],
    derivative_maturity_frequency="Month"
    )
# DataFrame of all curves with commodity in ("Benzene", "Crude Oil") and have a Monthly frequency.

fc.get_assessments(curve_code=["CN003", "CN006"])
# DataFrame of the latest assessments for all symbols in the curves ("CN003", "CN006").
```

### Energy Price Forecast

```python
import spgci as ci

epf = ci.EnergyPriceForecast()

epf.get_prices_shortterm(symbol="PCAAS00", month=[10, 11, 12])
# DataFrame of monthly forecasts for the symbol "PCAAS00" in the last 3 months of the year.

epf.get_prices_longterm(year=[2020, 2021], sector="Energy Transition", delivery_region="Europe")
# DataFrame of the annual forecasts for the years in ("2020", "2021"), where the sector is "Energy Transition" and the delivery region is "Europe".
```

### EWindow Market Data

```python
import spgci as ci
from datetime import date

ewmd = ci.EWindowMarketData()

ewmd.get_markets()
# DataFrame of Markets.

d = date(2023,2,13)
ewmd.get_botes(market=["EU BFOE", "US MidWest"], order_time=d)
# DataFrame of all BOTes in the markets ("EU BFOE", "US MidWest") on Feb 13, 2023.

```

### World Oil Supply

```python
import spgci as ci

wos = ci.WorldOilSupply()

countries = wos.get_reference_data(type=wos.RefTypes.Countries)
# DataFrame of all countries.

wos.get_ownership(country=countries['countryName'][:3], year=2040)
# DataFrame of Ownership for the first three countries from the countries endpoint and year "2040".

```

### World Refinery Database

```python
import spgci as ci

wrd = ci.WorldRefineryData()

wrd.get_yields(year=2020, owner="BP")
# DataFrame of yields for the year "2020" where "BP" is the refinery owner.

ref = wrd.get_reference_data(type=wrd.RefTypes.Refineries)
# DataFrame of all refineries.

az = ref[ref['Name'].str.contains("Al-Zour")]
wrd.get_runs(refinery_id=az["Id"])
# DataFrame of runs for the refineries with "Al-Zour" in the name.

wrd.get_outages(refinery_id=245)
# DataFrame of outages for refineryId 245.

```

### Insights

```python
import spgci as ci

ni = ci.Insights()

ni.get_stories(q="Suez", content_type=ni.ContentType.MarketCommentary)
# DataFrame of articles related to "Suez" where the content type is "Market Commentary".

ni.get_subscriber_notes(q="Naptha")
# DataFrame of all subscriber notes related to "Naptha".

ni.get_heards(q="Steel", content_type=ni.HeardsContentType.Heard, geography=['Europe', 'Middle East'], strip_html=True)
# DataFrame of all Heards related to "Steel" where the geography is in ("Europe", "Middle East") with HTML Tags removed from the headline and body.
```

### Global Oil Demand

```python
import spgci as ci

od = ci.GlobalOilDemand()

od.get_demand(country="Cambodia", product=["Naphtha", "Ethane"])
# DataFrame of forecast monthly demand for ("Naphtha", "Ethane") for Cambodia.

products = od.get_reference_data(type=od.RefTypes.Products)
# DataFrame of all "products" covered by Global Oil Demand dataset.

od.get_demand(product=products["productName"][:3], year_gte=2023)
# DataFrame of forecast monthly demand for the first 3 products in the previous DataFrame and the year >= 2023.

od.get_demand_archive(scenario_id=150, country="Norway")
# DataFrame of an archived (March 2023) forecast of monthly oil demand for Norway.
```

### North America Natural Gas Analytics

```python
import spgci as ci
from datetime import date

ng = ci.NANaturalGasAnalytics()

ng.get_pipelines(state="NJ", facility_type="Interconnect")
# DataFrame of pipelines in "NJ" with facility type "Interconnect"

ng.get_pipelines(pipeline_name="Algonquin")
# DataFrame of pipelines with name "Algonquin"

ng.get_pipeline_flows(pipeline_id=32)
# DataFrame of flows for pipeline_id 32 (Algonquin) for last 2 days.

d = date(2023, 7, 24)
ng.get_pipeline_flows(nomination_cycle="I2", gasdate=d)
# DataFrame of all pipeline flows during the I2 nomination cycle on gas date 2023-07-24

```

### Global Integrated Energy Model

```python
import spgci as ci

giem = ci.GlobalIntegratedEnergyModel()

giem.get_demand(country="Cambodia", product=["Naphtha", "Ethane"])
# DataFrame of energy demand for ("Naphtha", "Ethane") for Cambodia.

giem.get_demand_archive(scenario_id=559, country="Cambodia", product=["Naphtha", "Ethane"])
# DataFrame of an archived demand data of giem for Cambodia.

giem.get_reference_data(type=giem.RefTypes.Products)
# DataFrame of all "products" covered by Global Oil Demand dataset.

```

### Refining Margins & Crude Arbitrage

```python
import spgci as ci

af = ci.Arbflow()

af.get_margins_catalog(location_id = 34, crude_symbol="AAQZB00")
# DataFrame of refining margins catalog for ("AAQZB00") for Location Id 34.

af.get_margins_data(margin_id=229, margin_date='2023-08-16')
# DataFrame of refining margins data of arbflow for '2023-08-16'.

af.get_arbitrage(margin_id=[220,330], base_margin_id=1514, frequency_id=2)
# DataFrame of arbitrage data with frequencyId = 2 (Monthly).

af.get_reference_data(type=af.RefTypes.Locations)
# DataFrame of all "locations" covered by Refining Margins & Crude Arbitrage dataset.

```

### LNG Global Analytics

```python
import spgci as ci

lng = ci.LNGGlobalAnalytics()

lng.get_tenders(country_name="United States", paginate=True)
# DataFrame of tenders with country = 'United States'.

lng.get_tenders(contract_type="FOB", contract_option="Sell")
# DataFrame of tenders with ContractType = "FOB" and ContractOption = "Sell".

lng.get_liquefaction_projects()
# DataFrame of liquefaction projects.

lng.get_outages(liquefaction_project_name="ADNOC LNG")
# DataFrame of all LNG outages tied to the liquefaction project "ADNOC LNG".

lng.get_netbacks(date_gt="2024-01-01", import_geography="Brazil")
# DataFrame of all LNG Netbacks where import geography is 'Brazil' since Jan 1, 2024.

lng.get_supply_forecast_current(export_market="Indonesia")
# DataFrame of the current supply forecast for Indonesia

lng.get_demand_forecast_history(import_market="Japan", month="2024-12-01")
# DataFrame of all historical forecasts (one per month) of the LNG demand for Japan in December 2024.

lng.get_cargo_trips(trade_route="Suez Canal", date_loaded_gte="2024-01-01")
# DataFrame of trips through the Suez Canaal that loaded since 2024.

lng.get_cargo_events_partial_reexport()
# DataFrame of partial re-exports (a fraction of the delivered LNG is re-exported during transportation)

lng.get_assets_contracts_offtake_contracts(assumed_destination="Japan")
# DataFrame of offtake contracts headed to 'Japan'

lng.get_assets_contracts_liquefaction_trains(train_status=["Existing","Under Construction"])
# DataFrame of liquefaction trains which are either 'Existing' or 'Under Construction'

lng.get_gas_demand(market=["United States", "Mexico"], period_type="Monthly")
# DataFrame of gas demand for the different categories in the U.S. and Mexico on a monthly basis.

lng.get_power_generation(
    market=["India", "Pakistan", "Bangladesh"],
    source="Power Generation By Fuel",
    period_type="Monthly",
    paginate=True,
)
# DataFrame of power generation for India, Pakistan, Bangladesh by fuel type on a monthly basis.

lng.get_demand_forecast_short_term_current(import_market="France",month_gte="2025-01-01")
# DataFrame of short term demand forecast for import market of France starting January 1st 2025

lng.get_supply_forecast_short_term_current(export_market="Canada",month_gte="2025-01-01")
# DataFrame of short term supply forecast for export market of Canada starting January 1st 2025

lng.get_events_trade_route(trade_route="Cape of Good Hope", vessel_direction="Westbound")
# DataFrame for events recording key information, e.g Westbound vessels passing throught the Cape of Good Hope

lng.get_events_diversion(diversion_location="Indian Ocean")
# DataFrame for events of diversion, e.g. in the Indian Ocean

```

### Crude Analytics

```python
import spgci as ci

ca = ci.CrudeAnalytics()

ca.get_country_scores(status="Current")
# DataFrame of latest scores for all countries.

ca.get_country_scores(country="United States")
# DataFrame of all (historical and current) scores for country = "United States".

ca.get_country_total_scores()
# DataFrame of aggregated scores, supply and capacity per date.

```

### Weather

```python
import spgci as ci

w = ci.Weather()

w.get_forecast(city="Boston")
# DataFrame of forecasts for Boston

w.get_forecast(market="United States", weather_date_gte="2024-01-01", weather_date_lte="2024-01-31")
# DateFrame of forecasts in the United States in January 2024.

w.get_actual(market="Hong Kong", paginate=True)
# DataFrame of actual weather in Hong Kong, paginate=True to get full history.

```

### Structured Heards

```python
import spgci as ci

sh = ci.StructuredHeards()

sh.get_markets()
# DataFrame of the list of markets that have structured heards.

sh.get_heards(market="Americas crude oil", heard_type="trade")
# DateFrame of heards in the Americas crude oil market that are of type 'trade'.

```

### Chemicals Analysis and Forecasts

```python
import spgci as ci

chem = ci.Chemicals()

chem.get_unique_values(dataset="production", columns=["commodity"])
# See the unique values for each field per dataset. Useful for finding out the exact values to fitler on for fields like commodity, region, etc..

chem.get_capacity_events(
    event_begin_date_gt="2023-01-01", country="China", event_type=["expand", "startup"]
)
# DataFrame of capacity events in plants in China, that are either "expand" or "startup" since 2023-01-01

chem.get_capacity_to_consume(is_active=[True, False], commodity="Polybutadiene rubber")
# DateFrame of capacities to consume. Setting `is_active` to [True, False] allows you to include records that were corrected or removed which is useful for Point-In-Time analysis.

chem.get_short_term_prices(commodity="Acetone", delivery_region="US Gulf Coast")
# DataFrame of short-term (monthly) price forecasts for Acetone delivered to the US Gulf Coast.

chem.get_outages(
    start_date_gte="2023-01-01",
    capacity_gte=500,
    alert_status=['Confirmed']
)
# DataFrame of chemical plant outages since Jan 1, 2023 where the capacity impacted is >= 500 and the status is `Confirmed`.

```

### European Gas Analytics

```python
import spgci as ci

egp = ci.EUGasAnalytics()

egp.get_daily_flow_point_selection(
    from_country="Norway",
    gas_day_gte="2024-10-01",
    gas_day_lte="2024-10-31",
    uom="MCM",
    direction="Net",
)
# DataFrame of all net flows out of Norway in October 2024 in Million Cubic Meters.


df = egp.get_overview_hub_balance(
    gas_day_gte="2024-10-01",
    gas_day_lte="2024-10-31",
    average_type="Daily value",
    hub="PEG",
    uom="MCM",
)
df.pivot_table(
    index=["hubFlowType", "hubSubFlowType"],
    columns="dayMonthOrdinal",
    values="quantity",
)
# DataFrame of hub balances for October 2024 for PEG in Million Cubic Meters
# Pivot Table shows this grouped by FlowType (demand, supply, storage, etc...) and +/- N Days.

egp.get_daily_country_overview(country="Netherlands", uom="MCM", gas_day_gte="2025-05-01", gas_day_lte="2025-05-31")
# DataFrame of the daily supply and demand of a specified country

```

### Oil and NGL Analytics

```python
import spgci as ci

rp = ci.OilNGLAnalytics()

rp.get_arbflow_arbitrage(from_region="Persian Gulf", to_region="Japan")
# DataFrame of arbitrage opportunities for commodities from the Persian Gulf to Japan by concept, commodity and date.

rp.get_arbflow_arbitrage(commodity="Jet Fuel", concept="incentive")
# DataFrame of arbitrage opportunities for Jet Fuel using the `incentive` concept.

rp.get_refinery_runs_latest(outlook_horizon="Short-Term", region="Latin America", report_for_date='2026-04-01')
# DataFrame of forecast monthly runs per country in Latin America for April 2026 for the most recent forecast.

rp.get_oil_inventory(
    commodity="Naphtha",
    outlook_horizon="Short-Term",
    geography="Amsterdam, Rotterdam and Antwerp",
    frequency='Monthly',
)
# DataFrame of oil inventory for ARA for Naptha, including the latest and historical forecasts.
```

### Integrated Energy Scenarios

```python
import spgci as ci

ies = ci.IntegratedEnergyScenarios()

ies.get_gdp(country="United States", scenario="Inflections 2024", year=2030)
# DataFrame of GDP forecasts for 2030, using the Inflections 2024 scenario, in both Nominal and Real and a PPP basis and not.

ies.get_unique_values(dataset="gdp", columns="scenario")
# DataFrame of unique values for `scenario` in the `get_gdp` method.
```

### Agri and Food

```python
import spgci as ci

agri = ci.AgriAndFood()

agri.get_cost_of_production(commodity="wheat",geography="India", year=2025)
# DataFrame of the cost of production of wheat in India for the year 2025.

agri.get_unique_values(dataset="cost-of-production", columns="commodity")
# DataFrame of unique values for `commodity` in the `get_cost_of_production` method.

agri.get_price_purchase_forecast(commodity="corn", reporting_region="United States", currency="USD",report_for_date_gte="2023-01-01", report_for_date_lte="2024-12-31")
# DataFrame of the price purchase forecast for corn in the United States in USD for the period from January 1, 2023 to December 31, 2024.
```

### Americas Gas

```python
import spgci as ci

na = ci.AmericasGas()

na.get_pipeline_flows(date_frequency="Monthly", subregion="New England", meter_type_primary="Power")
# DataFrame of monthly pipeline flows for Power as a primary meter type in New England

na.get_modeled_demand_actual(flow_date="2025-07-07")
# DataFrame of actual modeled demand for a specific date

na.get_outlook_marketbalances_prices(
    date_frequency="Summer",
    domain="Canada",
    category="LNG exports",
    vintage_type="Long Term Outlook",
    vintage="2025.01"
)
# DataFrame of market balances and prices outlook for Canadian LNG exports for the 2025 Long Term Outlook

na.get_unique_values("outlook-marketbalances-prices", "region")
# DataFrame the unique values in a dataset and column, e.g. the regions available in outlook market balances prices dataset
```
