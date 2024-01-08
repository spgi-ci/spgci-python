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
            filter_exp='Market IN ("US DDGS") and rtpTimestamp: "2023-12-18T10:24:14.161Z"',
        )
        self.assertEqual(len(sh), 1)  # type: ignore


