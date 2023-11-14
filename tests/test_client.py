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
