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
from spgci.oil_demand import GlobalOilDemand
from pandas import Series

# from spgci.types import WOSRefType


class OilDemandTest(unittest.TestCase):
    od = GlobalOilDemand()

    @pytest.mark.integtest
    def test_simple(self):
        df = self.od.get_demand(year=2022, country="Norway")
        self.assertGreater(len(df), 1)  # type: ignore

    @pytest.mark.integtest
    def test_series(self):
        ser: Series[str] = Series(["Naphtha"])  # type: ignore
        df = self.od.get_demand(product=ser, page_size=10)
        self.assertGreater(len(df), 1)  # type: ignore

    @pytest.mark.integtest
    def test_paging(self):
        paged = self.od.get_demand(
            year=2023, country="Norway", page_size=50, paginate=True
        )
        df = self.od.get_demand(year=2023, country="Norway")
        self.assertEqual(len(df), len(paged))  # type: ignore
