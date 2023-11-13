import unittest
import pytest
from spgci.wos import WorldOilSupply
from pandas import Series, DataFrame
from typing import cast


class WOSTest(unittest.TestCase):
    wos = WorldOilSupply()

    @pytest.mark.integtest
    def test_simple(self):
        df = cast(
            DataFrame,
            self.wos.get_production(
                scenario_term_id=2,
                production_type="Production",
                grade_element=["Alaska Condensate"],
                page_size=10,
            ),
        )
        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_series(self):
        lst: list[str] = ["Heavy Sour", "Light Sour"]
        ser: Series[str] = Series(lst)
        df = cast(
            DataFrame,
            self.wos.get_production(
                scenario_term_id=2,
                grade=ser,
                page_size=10,
            ),
        )
        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_ownership(self):
        df = cast(
            DataFrame,
            self.wos.get_ownership(
                year=[2022, 2023, 2024],
                page_size=10,
            ),
        )
        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_cost_of_supplies(self):
        lst: list[str] = ["Conventional"]
        ser: Series[str] = Series(lst)

        df = cast(
            DataFrame,
            self.wos.get_cost_of_supplies(
                ref_year=[2014, 2023, 2024],
                reserve_type=ser,
                country="Algeria",
                page_size=10,
            ),
        )
        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_production_archive(self):
        lst: list[str] = ["Conventional"]
        ser: Series[str] = Series(lst)

        df = cast(
            DataFrame,
            self.wos.get_production_archive(
                scenario_term_id=1,
                scenario_id=2243,
                year=[2014, 2050, 2024],
                reserve_type=ser,
                country="Albania",
                units="MBD",
                page_size=10,
            ),
        )
        self.assertGreater(len(df), 1)

    def test_ref(self):
        for t in self.wos.RefTypes:
            df = cast(DataFrame, self.wos.get_reference_data(type=t))
            self.assertGreater(len(df), 1)
