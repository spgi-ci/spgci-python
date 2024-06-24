import unittest
import pytest
from datetime import datetime
from spgci import StructuredHeards


class StructuredHeardsTest(unittest.TestCase):
    sh = StructuredHeards()

    @pytest.mark.integtest
    def test_heards_data_Markets(self):
        sh = self.sh.get_data(
            page_size=10,
            filter_exp='Market IN ("US DDGS")',
        )
        self.assertGreater(len(sh), 1)  # type: ignore

    @pytest.mark.integtest
    def test_heards_markets(self):
        sh = self.sh.get_markets(
            page_size=10,
            filter_exp='Market IN ("Americas crude oil")',
        )
        self.assertEqual(len(sh), 1)  # type: ignore

    @pytest.mark.integtest
    def test_heards_markets_data(self):
        sh = self.sh.get_data(
            page_size=10,
            filter_exp='Market IN ("Americas crude oil") and rtpTimestamp: "2024-05-29T14:14:09.985Z"',
        )
        self.assertEqual(len(sh), 3)  # type: ignore

    @pytest.mark.integtest
    def test_heards_markets_data_heard_type(self):
        sh = self.sh.get_data(
            page_size=10,
            filter_exp='Market IN ("Americas crude oil") and heard_type: "Trade"',
        )
        self.assertGreater(len(sh), 1)  # type: ignore

