import unittest
import pytest
from spgci.epf import EnergyPriceForecast
import spgci.config
from pandas import Series, DataFrame
from typing import cast
from datetime import date

# from spgci.types import WOSRefType


class EPFTest(unittest.TestCase):
    epf = EnergyPriceForecast()

    @pytest.mark.integtest
    def test_simple(self):
        df = cast(DataFrame, self.epf.get_prices_shortterm(year=[2020, 2021]))

        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_complex(self):
        df = cast(
            DataFrame,
            self.epf.get_prices_shortterm(
                year_gte=2020, units="BBL", sector="Refined Products", category="NGL"
            ),
        )

        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_series(self):
        ser: Series["str"] = Series(["USD", "EUR", "JPY"])
        df = cast(
            DataFrame,
            self.epf.get_prices_shortterm(year_gte=2020, currency=ser),
        )

        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_paginate(self):
        df = cast(
            DataFrame,
            self.epf.get_prices_longterm(
                year=[2020, 2021],
            ),
        )
        df_paged = cast(
            DataFrame,
            self.epf.get_prices_longterm(
                year=[2020, 2021],
                page_size=100,
                paginate=True,
            ),
        )

        self.assertGreater(len(df), 0)
        self.assertGreater(len(df_paged), 0)

        self.assertEqual(len(df), len(df_paged))

    @pytest.mark.integtest
    def test_ref_data(self):
        spgci.config.sleep_time = 0.4
        for t in self.epf.RefTypes:
            df = cast(DataFrame, self.epf.get_reference_data(type=t))
            self.assertGreater(len(df), 0)

    @pytest.mark.integtest
    def test_lt(self):
        df = cast(
            DataFrame,
            self.epf.get_prices_longterm(
                year=[2020, 2021], delivery_region="Europe", sector="Energy Transition"
            ),
        )

        self.assertGreater(len(df), 0)

    @pytest.mark.integtest
    def test_archive_dates(self):
        df_st = cast(
            DataFrame,
            self.epf.get_archive_dates(category="LNG", term_type="short"),
        )

        self.assertGreater(len(df_st), 0)

        df_lt = cast(
            DataFrame,
            self.epf.get_archive_dates(category="LNG", term_type="long"),
        )
        self.assertGreater(len(df_lt), 0)

    @pytest.mark.integtest
    def test_lt_archive(self):
        df = cast(
            DataFrame,
            self.epf.get_prices_longterm_archive(
                modified_date=date(2021, 1, 1), category_id=1, year=[2040, 2030, 2035]
            ),
        )

        self.assertGreater(len(df), 0)

    @pytest.mark.integtest
    def test_st_archive(self):
        df = cast(
            DataFrame,
            self.epf.get_prices_shortterm_archive(
                modified_date=date(2021, 1, 1), category_id=1, month=[10, 11, 12]
            ),
        )

        self.assertGreater(len(df), 0)
