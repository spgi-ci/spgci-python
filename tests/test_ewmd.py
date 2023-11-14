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

import unittest
import pytest
from datetime import date, datetime
from spgci import EWindowMarketData


class EWMDTest(unittest.TestCase):
    ew = EWindowMarketData()

    @pytest.mark.integtest
    def test_simple(self):
        ew = self.ew.get_botes(
            product="Platts Gasoline*",
            order_state=["consummated"],
            order_time_gt=date(2022, 1, 1),
            page_size=10,
        )
        # print(fc.info())
        self.assertGreater(len(ew), 1)  # type: ignore

    @pytest.mark.integtest
    def test_enum_orderstate(self):
        ew = self.ew.get_botes(
            product="Platts Gasoline*",
            order_state=[self.ew.OrderState.Consummated],
            page_size=10,
        )
        # print(fc.info())
        self.assertGreater(len(ew), 0)  # type: ignore

    @pytest.mark.integtest
    def test_datetime(self):
        ew = self.ew.get_botes(
            product="Platts Gasoline*",
            order_state=["consummated"],
            order_time_gt=datetime(2023, 2, 10, 19, 29, 00),
            page_size=10,
        )
        # print(fc.info())
        self.assertGreater(len(ew), 0)  # type: ignore

    @pytest.mark.integtest
    def test_get_products(self):
        ew = self.ew.get_products()
        self.assertGreater(len(ew), 0)  # type: ignore

    @pytest.mark.integtest
    def test_series(self):
        mkts = self.ew.get_markets()
        ew = self.ew.get_botes(market=mkts["market"][:2], page_size=20)  # type: ignore
        self.assertGreater(len(ew), 0)  # type: ignore
