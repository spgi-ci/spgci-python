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
from datetime import date
import pytest
from spgci import MarketData


class MddTest(unittest.TestCase):
    mdd = MarketData()

    @pytest.mark.integtest
    def test_simple(self):
        df = self.mdd.get_assessments_by_symbol_current(symbol=["PCAAS00", "PCAAT00"])
        self.assertGreater(len(df), 1)  # type: ignore

    @pytest.mark.integtest
    def test_paging(self):
        df = MarketData().get_assessments_by_symbol_current(
            symbol=["PCAAS00", "PCAAT00"]
        )
        df_paged = self.mdd.get_assessments_by_symbol_current(
            symbol=["PCAAS00", "PCAAT00"], page_size=3, paginate=True
        )

        self.assertEqual(len(df), len(df_paged))  # type: ignore

    @pytest.mark.integtest
    def test_historical(self):
        df = self.mdd.get_assessments_by_symbol_historical(
            symbol=["PCAAS00", "PCAAT00"],
            assess_date_gte=date(2022, 1, 1),
            assess_date_lte=date(2023, 1, 1),
        )
        self.assertGreater(len(df), 100)  # type: ignore

    @pytest.mark.integtest
    def test_mdc(self):
        df = self.mdd.get_assessments_by_mdc_current(mdc="ET")
        self.assertGreater(len(df), 100)  # type: ignore

    @pytest.mark.integtest
    def test_mdc_historical(self):
        df = self.mdd.get_assessments_by_mdc_historical(
            mdc="ET", assess_date=date(2022, 12, 1)
        )
        self.assertGreater(len(df), 100)  # type: ignore

    @pytest.mark.integtest
    def test_series(self):
        sym = self.mdd.get_symbols(q="Brent", page_size=10)
        df = self.mdd.get_assessments_by_symbol_current(
            symbol=sym["symbol"], page_size=10  # type: ignore
        )
        self.assertGreater(len(df), 0)  # type: ignore
