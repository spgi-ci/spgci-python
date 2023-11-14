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
from spgci.na_gas import NANaturalGasAnalytics
from pandas import DataFrame
from typing import cast


class NAGASTest(unittest.TestCase):
    na_gas = NANaturalGasAnalytics()

    @pytest.mark.integtest
    def test_simple(self):
        df = cast(
            DataFrame,
            self.na_gas.get_pipeline_flows(
                pipeline_id=2,
                nomination_cycle="I1",
                location_type="R",
                page_size=10,
            ),
        )
        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_paging(self):
        df = self.na_gas.get_pipelines(point_type=["Point", "Segment"], state="NJ")
        df_paged = self.na_gas.get_pipelines(
            state="NJ", point_type=["Point", "Segment"], page_size=100, paginate=True
        )

        self.assertEqual(len(df), len(df_paged))  # type: ignore
