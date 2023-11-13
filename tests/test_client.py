import unittest
import pytest
import asyncio
from pandas import DataFrame, Series
from spgci import MarketData
from typing import cast


class APIClientTest(unittest.TestCase):
    mdd = MarketData()

    @pytest.mark.integtest
    def test_throttle(self):
        async def api(symbol: str):
            return self.mdd.get_assessments_by_symbol_current(symbol=symbol)
            # return (
            #     j.headers.get("Date"),
            #     j.headers.get("x-ratelimit-remaining-second", 0),
            # )

        async def call_api():
            df = cast(
                DataFrame,
                self.mdd.get_symbols(
                    mdc="ET",
                    page_size=60,
                    assessment_frequency=self.mdd.AssessmentFrequency.Daily,
                ),
            )
            sym: Series[str] = df["symbol"]

            tasks = [api(s) for s in sym]

            return await asyncio.gather(*tasks)

        result = asyncio.run(call_api())
        self.assertEqual(len(result), 60)
