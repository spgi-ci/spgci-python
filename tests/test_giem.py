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
from spgci.giem import GlobalIntegratedEnergyModel
from pandas import Series

# from spgci.types import WOSRefType


class OilDemandTest(unittest.TestCase):
    giem = GlobalIntegratedEnergyModel()

    ## Simple testcase for demand endpoint
    @pytest.mark.integtest
    def test_simple(self):
        df = self.giem.get_demand(year=2023, country="Norway")
        self.assertGreater(len(df), 1)  # type: ignore

    ## Series testcase for demand endpoint
    @pytest.mark.integtest
    def test_series(self):
        ser: Series[str] = Series(["Naphtha"])  # type: ignore
        df = self.giem.get_demand(product=ser, page_size=10)
        self.assertGreater(len(df), 1)  # type: ignore

    ## Simple testcase for demand-archive endpoint
    @pytest.mark.integtest
    def test_simple_for_demand_archive(self):
        df = self.giem.get_demand_archive(scenario_id=559, country="Cambodia")
        self.assertGreater(len(df), 1)  # type: ignore

    ## Simple testcase for metadata endpoint called Countries
    @pytest.mark.integtest
    def test_simple_for_metadata_api(self):
        df = self.giem.get_reference_data(type=self.giem.RefTypes.Countries)
        self.assertGreater(len(df), 1)  # type: ignore

    @pytest.mark.integtest
    def test_paging(self):
        paged = self.giem.get_demand(
            year=2023, country="Norway", page_size=50, paginate=True
        )
        df = self.giem.get_demand(year=2023, country="Norway")
        self.assertEqual(len(df), len(paged))  # type: ignore
