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
from typing import List
from spgci import MarketData


class SymbolTest(unittest.TestCase):
    mdd = MarketData()

    @pytest.mark.integtest
    def test_simple(self):
        df = self.mdd.get_symbols(symbol=["PCAAS00", "PCAAT00"])
        self.assertGreater(len(df), 1)  # type: ignore

    @pytest.mark.integtest
    def test_complicated(self):
        df = self.mdd.get_symbols(
            q="Brent", commodity="Crude Oil", symbol=["PCAAS00", "PCAAT00"]
        )
        self.assertGreater(len(df), 0)  # type: ignore

    @pytest.mark.integtest
    def test_mdcs(self):
        df = self.mdd.get_mdcs()
        self.assertGreater(len(df), 10)  # type: ignore

    @pytest.mark.integtest
    def test_enums(self):
        df = self.mdd.get_symbols(
            q="Brent",
            commodity="Crude Oil",
            contract_type=[self.mdd.ContractType.Future, self.mdd.ContractType.Spot],
            assessment_frequency=[self.mdd.AssessmentFrequency.DailyWeekday],
        )
        self.assertGreater(len(df), 0)  # type: ignore

    @pytest.mark.integtest
    def test_reorder(self):
        expected = ["symbol", "description"]
        df = self.mdd.get_symbols(
            q="Brent",
            currency="USD",
            commodity="Crude Oil",
            contract_type=[self.mdd.ContractType.Future, self.mdd.ContractType.Spot],
            assessment_frequency=[self.mdd.AssessmentFrequency.DailyWeekday],
        )
        actual: List[str] = df.columns.to_list()[:2]  # type: ignore
        self.assertEqual(expected, actual)  # type: ignore
