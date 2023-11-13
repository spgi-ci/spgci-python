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
        df = self.giem.get_reference_data(type = self.giem.RefTypes.Countries)
        self.assertGreater(len(df), 1)  # type: ignore

    @pytest.mark.integtest
    def test_paging(self):
        paged = self.giem.get_demand(
            year=2023, country="Norway", page_size=50, paginate=True
        )
        df = self.giem.get_demand(year=2023, country="Norway")
        self.assertEqual(len(df), len(paged))  # type: ignore
