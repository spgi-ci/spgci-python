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
from spgci import Weather


class WeatherTest(unittest.TestCase):
    w = Weather()

    @pytest.mark.integtest
    def test_simple(self):
        df = self.w.get_actual()
        self.assertGreater(len(df), 1)  # type: ignore

    @pytest.mark.integtest
    def test_paginate(self):
        df = self.w.get_actual(
            market="Hong Kong",
            weather_date_gt="2023-01-10",
            weather_date_lt="2023-01-20",
            paginate=True,
            page_size=5,
        )
        df_nopage = self.w.get_actual(
            market="Hong Kong",
            weather_date_gt="2023-01-10",
            weather_date_lt="2023-01-20",
        )
        self.assertGreater(len(df), 0)  # type: ignore
        self.assertEqual(len(df), len(df_nopage))  # type: ignore

    @pytest.mark.integtest
    def test_simple_forecast(self):
        df = self.w.get_forecast()
        self.assertGreater(len(df), 1)  # type: ignore

    @pytest.mark.integtest
    def test_paginate_forecast(self):
        df = self.w.get_forecast(
            market="Hong Kong",
            recorded_date="2024-01-01",
            paginate=True,
            page_size=5,
        )
        df_nopage = self.w.get_forecast(
            market="Hong Kong",
            recorded_date="2024-01-01",
        )
        self.assertGreater(len(df), 0)  # type: ignore
        self.assertEqual(len(df), len(df_nopage))  # type: ignore
