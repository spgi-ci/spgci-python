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
        df = cast(DataFrame, self.ni.get_top_news(q="Rice"))
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
            self.ni.get_stories(q="Suez", updated_date_gt=datetime(2026, 7, 21)),
        )
        df2 = cast(
            DataFrame,
            self.ni.get_subscriber_notes(
                q="Emissions", updated_date_gt=datetime(2026, 7, 16)
            ),
        )
        self.assertGreater(len(df), 0)
        self.assertGreater(len(df2), 0)

    @pytest.mark.integtest
    def test_complex(self):
        df = cast(
            DataFrame,
            self.ni.get_stories(
                q="Emissions",
                content_type=[self.ni.ContentType.Briefing, self.ni.ContentType.News],
                commodity="Polymers",
            ),
        )
        df2 = cast(
            DataFrame,
            self.ni.get_subscriber_notes(
                q="Emissions",
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
                q="Emissions",
                updated_date_gt=datetime(2026, 7, 11),
                updated_date_lt=datetime(2026, 7, 16),
            ),
        )
        df_paged = cast(
            DataFrame,
            self.ni.get_stories(
                q="Emissions",
                updated_date_gt=datetime(2026, 7, 11),
                updated_date_lt=datetime(2026, 7, 16),
                paginate=True,
                page_size=2,
            ),
        )
        df_len = cast(
            DataFrame,
            self.ni.get_stories(
                q="Emissions",
                updated_date_gt=datetime(2026, 7, 11),
                updated_date_lt=datetime(2026, 7, 16),
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
                commodity="Crude oil",
            ),
        )
        df2 = cast(
            DataFrame,
            self.ni.get_heards(
                geography="Bulgaria",
                strip_html=True,
            ),
        )
        self.assertGreater(len(df), 0)
        self.assertGreater(len(df2), 0)

    @pytest.mark.integtest
    def test_sector(self):
        df = cast(
            DataFrame,
            self.ni.get_heards(
                sector='Agriculture',
                strip_html=True,
            ),
        )
        self.assertGreater(len(df), 0)

    @pytest.mark.integtest
    def test_sector_list(self):
        df = cast(
            DataFrame,
            self.ni.get_heards(
                sector=['Agriculture','Coal','EnergyTransition'],
                strip_html=True,
            ),
        )
        self.assertGreater(len(df), 0)
