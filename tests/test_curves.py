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
from datetime import date
from spgci import ForwardCurves


class CurvesTest(unittest.TestCase):
    fc = ForwardCurves()

    @pytest.mark.integtest
    def test_simple(self):
        d = date(2022, 12, 1)
        fc = self.fc.get_assessments(curve_code=["CN002"], assess_date=d, raw=False)
        # print(fc.info())
        self.assertGreater(len(fc), 1)  # type: ignore

    @pytest.mark.integtest
    def test_filter_param(self):
        d = date(2022, 12, 1)
        filt = 'bate: "c"'
        fc = self.fc.get_assessments(
            curve_code=["CN002"], assess_date=d, filter_exp=filt, page_size=10
        )
        # print(fc.info())
        self.assertGreater(len(fc), 1)  # type: ignore

    @pytest.mark.integtest
    def test_between(self):
        d = date(2022, 12, 1)
        d2 = date(2022, 12, 10)
        fc = self.fc.get_assessments(
            curve_code=["CN002"], assess_date_gte=d, assess_date_lte=d2, page_size=10
        )
        self.assertGreater(len(fc), 1)  # type: ignore

    @pytest.mark.integtest
    def test_auto_pagination(self):
        d = date(2022, 12, 1)
        filt = 'bate: "c"'
        fc_paged = self.fc.get_assessments(
            curve_code=["CN002"],
            assess_date=d,
            filter_exp=filt,
            page_size=100,
            paginate=True,
        )

        fc_notpaged = self.fc.get_assessments(
            curve_code=["CN002"], assess_date=d, filter_exp=filt
        )

        self.assertEqual(len(fc_paged), len(fc_notpaged))  # type: ignore

    @pytest.mark.integtest
    def test_ref_simple(self):
        fc = self.fc.get_curves(
            commodity="Benzene",
            subscribed_only=False,
            curve_type="Relative Forward Curve",
        )
        # print(fc.info())
        self.assertGreater(len(fc), 0)  # type: ignore

    @pytest.mark.integtest
    def test_curve_enum(self):
        fc = self.fc.get_curves(
            derivative_maturity_frequency=self.fc.MatFrequency.Month,
            subscribed_only=False,
            curve_type="Relative Forward Curve",
            contract_type=[
                self.fc.ContractType.Spot,
                self.fc.ContractType.Forward,
                self.fc.ContractType.Swap,
            ],
        )
        # print(fc.info())
        self.assertGreater(len(fc), 0)  # type: ignore

    @pytest.mark.integtest
    def test_assessment_enum(self):
        fc = self.fc.get_assessments(
            curve_code=["CN003", "CN005"],
            derivative_maturity_frequency=[
                self.fc.MatFrequency.Year,
                self.fc.MatFrequency.Quarter,
            ],
        )
        # print(fc.info())
        self.assertGreater(len(fc), 0)  # type: ignore

    # behavior changed in API to return an error if the curve code does not exist
    # @pytest.mark.integtest
    # def test_empty_curve(self):
    #     fc = self.fc.get_assessments(
    #         curve_code=["CN0P8"],
    #     )
    #     # print(fc.info())
    #     self.assertEqual(len(fc), 0)  # type: ignore

    @pytest.mark.integtest
    def test_series(self):
        rfc = self.fc.get_curves(curve_code="CN006")
        fc = self.fc.get_assessments(curve_code=rfc["curve_code"], page_size=10)  # type: ignore
        # print(fc.info())
        self.assertGreater(len(fc), 0)  # type: ignore
