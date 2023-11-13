import unittest
import pytest
from datetime import date, datetime
from spgci import EWindowMarketData


class EWMDTest(unittest.TestCase):
    ew = EWindowMarketData()

    @pytest.mark.integtest
    def test_simple(self):
        ew = self.ew.get_botes(
            product="Platts Gasoline*",
            order_state=["consummated"],
            order_time_gt=date(2022, 1, 1),
            page_size=10,
        )
        # print(fc.info())
        self.assertGreater(len(ew), 1)  # type: ignore

    @pytest.mark.integtest
    def test_enum_orderstate(self):
        ew = self.ew.get_botes(
            product="Platts Gasoline*",
            order_state=[self.ew.OrderState.Consummated],
            page_size=10,
        )
        # print(fc.info())
        self.assertGreater(len(ew), 0)  # type: ignore

    @pytest.mark.integtest
    def test_datetime(self):
        ew = self.ew.get_botes(
            product="Platts Gasoline*",
            order_state=["consummated"],
            order_time_gt=datetime(2023, 2, 10, 19, 29, 00),
            page_size=10,
        )
        # print(fc.info())
        self.assertGreater(len(ew), 0)  # type: ignore

    @pytest.mark.integtest
    def test_get_products(self):
        ew = self.ew.get_products()
        self.assertGreater(len(ew), 0)  # type: ignore

    @pytest.mark.integtest
    def test_series(self):
        mkts = self.ew.get_markets()
        ew = self.ew.get_botes(market=mkts["market"][:2], page_size=20)  # type: ignore
        self.assertGreater(len(ew), 0)  # type: ignore
