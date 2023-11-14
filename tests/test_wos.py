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
from spgci.wos import WorldOilSupply
from pandas import Series, DataFrame
from typing import cast


class WOSTest(unittest.TestCase):
    wos = WorldOilSupply()

    @pytest.mark.integtest
    def test_simple(self):
        df = cast(
            DataFrame,
            self.wos.get_production(
                scenario_term_id=2,
                production_type="Production",
                grade_element=["Alaska Condensate"],
                page_size=10,
            ),
        )
        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_series(self):
        lst: list[str] = ["Heavy Sour", "Light Sour"]
        ser: Series[str] = Series(lst)
        df = cast(
            DataFrame,
            self.wos.get_production(
                scenario_term_id=2,
                grade=ser,
                page_size=10,
            ),
        )
        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_ownership(self):
        df = cast(
            DataFrame,
            self.wos.get_ownership(
                year=[2022, 2023, 2024],
                page_size=10,
            ),
        )
        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_cost_of_supplies(self):
        lst: list[str] = ["Conventional"]
        ser: Series[str] = Series(lst)

        df = cast(
            DataFrame,
            self.wos.get_cost_of_supplies(
                ref_year=[2014, 2023, 2024],
                reserve_type=ser,
                country="Algeria",
                page_size=10,
            ),
        )
        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_production_archive(self):
        lst: list[str] = ["Conventional"]
        ser: Series[str] = Series(lst)

        df = cast(
            DataFrame,
            self.wos.get_production_archive(
                scenario_term_id=1,
                scenario_id=2243,
                year=[2014, 2050, 2024],
                reserve_type=ser,
                country="Albania",
                units="MBD",
                page_size=10,
            ),
        )
        self.assertGreater(len(df), 1)

    def test_ref(self):
        for t in self.wos.RefTypes:
            df = cast(DataFrame, self.wos.get_reference_data(type=t))
            self.assertGreater(len(df), 1)
