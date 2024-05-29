# SPGCI Python Library

Python Client for the [S&P Global Commodity Insights API](https://developer.platts.com).

Looking for more examples? Check out our [Notebook Gallery](https://developer.spglobal.com/commodityinsights/tools/notebooks).

## Installation

Requires Python >= 3.7.0.

```bash
pip install spgci
```

## Getting Started

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/achristie/images/blob/master/readme.ipynb)

```python
    import spgci as ci

    ci.set_credentials(<username>, <password>, <appkey>)
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
| SPGCI_APPKEY | Your AppKey |

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

lng.get_reference_data(type=lng.RefTypes.LiquefactionProjects)
# DataFrame of liquefaction projects.

lng.get_outages(liquefaction_project_name="ADNOC LNG")
# DataFrame of all LNG outages tied to "ADNOC LNG".

lng.get_netbacks(date_gt="2024-01-01", import_geography="Brazil")
# DataFrame of all LNG Netbacks where import geography is 'Brazil' since Jan 1, 2024.

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
