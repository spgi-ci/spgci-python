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
