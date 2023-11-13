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
