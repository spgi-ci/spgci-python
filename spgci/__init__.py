# Copyright 2023 S&P Global Commodity Insights

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
S&P Global Commodity Insights API\n
More details available here: https://developer.platts.com

Getting Started
---------------
>>> import spgci as ci
>>> ci.set_credentials(``username``, ``password``, ``appkey``)
>>> ci.MarketData().get_assessments_by_symbol(symbol="PCAAS00")
\n
"""

from .market_data import MarketData
from .forward_curves import ForwardCurves
from .ewindow_md import EWindowMarketData
from .wos import WorldOilSupply
from .wrd import WorldRefineryData
from .oil_demand import GlobalOilDemand
from .api_client import get_token
from .epf import EnergyPriceForecast
from .insights import Insights
from .na_gas import NANaturalGasAnalytics
from .crude_supply_risk import CrudeAnalytics
from .giem import GlobalIntegratedEnergyModel
from .lng_analytics import LNGGlobalAnalytics
from .arbflow import Arbflow
from .weather import Weather


from .config import username, password, set_credentials, appkey, version

__version__ = version


__all__ = [
    "MarketData",
    "username",
    "password",
    "appkey",
    "set_credentials",
    "ForwardCurves",
    "EWindowMarketData",
    "WorldOilSupply",
    "WorldRefineryData",
    "get_token",
    "EnergyPriceForecast",
    "Insights",
    "LNGGlobalAnalytics",
    "GlobalOilDemand",
    "NANaturalGasAnalytics",
    "CrudeAnalytics",
    "Arbflow",
    "GlobalIntegratedEnergyModel",
    "Weather",
]
