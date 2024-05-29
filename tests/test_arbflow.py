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
from spgci.arbflow import Arbflow
from pandas import DataFrame
from typing import cast
from datetime import date


class CrudeArbitrageTest(unittest.TestCase):
    arbflow = Arbflow()

    ## Simple testcase for margins_catalog endpoint
    @pytest.mark.integtest
    def test_simple(self):
        df = self.arbflow.get_margins_catalog(location_id=34, crude_symbol="AAQZB00")
        self.assertGreater(len(df), 1)  # type: ignore

    ## Paging testcase for margins_data endpoint
    @pytest.mark.integtest
    def test_paging(self):
        paged = self.arbflow.get_margins_data(
            frequency_id=3, page_size=500, paginate=True
        )
        df = self.arbflow.get_margins_data(frequency_id=3, paginate=True)
        self.assertEqual(len(df), len(paged))  # type: ignore

    # Simple testcase for arbitrage endpoint
    @pytest.mark.integtest
    def test_arbitrage(self):
        df = cast(
            DataFrame,
            self.arbflow.get_arbitrage(
                margin_id=[229, 1457],
                base_margin_id=330,
                frequency_id=2,
                page_size=100,
            ),
        )
        self.assertGreater(len(df), 1)

    ## Testcase with multiple filters for margins_data endpoint
    @pytest.mark.integtest
    def test_get_margins_data(self):
        df = cast(
            DataFrame,
            self.arbflow.get_margins_data(
                frequency_id=1,
                margin_date=date(2023, 8, 16),
                page_size=100,
            ),
        )
        self.assertGreater(len(df), 1)

    def test_ref(self):
        for t in self.arbflow.RefTypes:
            df = cast(DataFrame, self.arbflow.get_reference_data(type=t))
            self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_get_margins_data_with_dates(self):
        df = cast(
            DataFrame,
            self.arbflow.get_margins_data(
                frequency_id=1,
                margin_id=229,
                margin_date_lte=date(2023, 12, 31),
                margin_date_gte=date(2023, 8, 16),
                page_size=100,
            ),
        )
        self.assertGreater(len(df), 1)
