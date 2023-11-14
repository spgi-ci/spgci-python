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
from datetime import datetime
from pandas import DataFrame
from typing import cast
from spgci import Insights


class InsightsTest(unittest.TestCase):
    ni = Insights()

    @pytest.mark.integtest
    def test_simple(self):
        df = cast(DataFrame, self.ni.get_stories(q="Suez"))
        self.assertGreater(len(df), 0)

    @pytest.mark.integtest
    def test_simple_spotlight(self):
        df = cast(DataFrame, self.ni.get_spotlights(q="Suez"))
        self.assertGreater(len(df), 0)

    @pytest.mark.integtest
    def test_simple_latest_news(self):
        df = cast(DataFrame, self.ni.get_latest_news(q="Suez"))
        self.assertGreater(len(df), 0)

    @pytest.mark.integtest
    def test_simple_top_news(self):
        df = cast(DataFrame, self.ni.get_top_news(q="Suez"))
        self.assertGreater(len(df), 0)

    @pytest.mark.integtest
    def test_notes(self):
        df = cast(
            DataFrame,
            self.ni.get_subscriber_notes(
                content_type=self.ni.SubscriberNotesContentType.MethodologyNote,
                strip_html=True,
            ),
        )
        self.assertGreater(len(df), 0)

    @pytest.mark.integtest
    def test_dates(self):
        df = cast(
            DataFrame,
            self.ni.get_stories(q="Suez", updated_date_gt=datetime(2023, 1, 1)),
        )
        df2 = cast(
            DataFrame,
            self.ni.get_subscriber_notes(
                q="Suez", updated_date_gt=datetime(2023, 1, 1)
            ),
        )
        self.assertGreater(len(df), 0)
        self.assertGreater(len(df2), 0)

    @pytest.mark.integtest
    def test_complex(self):
        df = cast(
            DataFrame,
            self.ni.get_stories(
                q="Suez",
                content_type=[self.ni.ContentType.Analytics, self.ni.ContentType.News],
                commodity="Crude",
            ),
        )
        df2 = cast(
            DataFrame,
            self.ni.get_subscriber_notes(
                q="Suez",
                content_type=[
                    self.ni.SubscriberNotesContentType.DataCorrection,
                    self.ni.SubscriberNotesContentType.MethodologyNote,
                ],
            ),
        )
        self.assertGreater(len(df), 0)
        self.assertGreater(len(df2), 0)

    @pytest.mark.integtest
    def test_pagination(self):
        df = cast(
            DataFrame,
            self.ni.get_stories(
                q="suez canal march",
                updated_date_gt=datetime(2023, 3, 1),
                updated_date_lt=datetime(2023, 3, 20),
            ),
        )
        df_paged = cast(
            DataFrame,
            self.ni.get_stories(
                q="suez canal march",
                updated_date_gt=datetime(2023, 3, 1),
                updated_date_lt=datetime(2023, 3, 20),
                paginate=True,
                page_size=2,
            ),
        )
        df_len = cast(
            DataFrame,
            self.ni.get_stories(
                q="suez canal march",
                updated_date_gt=datetime(2023, 3, 1),
                updated_date_lt=datetime(2023, 3, 20),
                paginate=False,
                page_size=2,
            ),
        )
        self.assertEqual(len(df), len(df_paged))
        self.assertEqual(len(df_len), 2)

    @pytest.mark.integtest
    def test_heards(self):
        df = cast(
            DataFrame,
            self.ni.get_heards(
                q="Suez",
                content_type=[
                    self.ni.HeardsContentType.Tenders,
                    self.ni.HeardsContentType.AssessmentSummary,
                ],
                commodity="Crude oil",
            ),
        )
        df2 = cast(
            DataFrame,
            self.ni.get_heards(
                page_number=["0100", "0200", "0300"],
                geography="Middle East",
                strip_html=True,
            ),
        )
        self.assertGreater(len(df), 0)
        self.assertGreater(len(df2), 0)
