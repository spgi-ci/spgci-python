import unittest
import pytest
from spgci.wrd import WorldRefineryData
import spgci.config
from pandas import Series, DataFrame
from typing import cast
from datetime import date


class WRDTest(unittest.TestCase):
    wrd = WorldRefineryData()

    @pytest.mark.integtest
    def test_simple(self):
        df: DataFrame = cast(
            DataFrame, self.wrd.get_capacity(year=[2012, 2019], page_size=10)
        )
        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_years(self):
        df = cast(
            DataFrame, self.wrd.get_capacity(year_gt=2012, year_lt=2019, page_size=10)
        )
        self.assertGreater(len(df), 1)
        df2 = cast(
            DataFrame, self.wrd.get_capacity(year_gte=2019, year_lte=2021, page_size=10)
        )
        self.assertGreater(len(df2), 1)

    @pytest.mark.integtest
    def test_other_fields(self):
        df = cast(
            DataFrame, self.wrd.get_capacity(capacity_id=[29422, 1200], page_size=10)
        )
        self.assertGreater(len(df), 1)
        df2 = cast(
            DataFrame,
            self.wrd.get_capacity(
                quarter=[2, 3, 4],
                process_unit="Atmos Distillation",
                owner="Sonatrach ",
                page_size=10,
            ),
        )
        self.assertGreater(len(df2), 1)

    @pytest.mark.integtest
    def test_series(self):
        ser: "Series[int]" = Series([29422, 1200])
        df = cast(DataFrame, self.wrd.get_capacity(capacity_id=ser, page_size=10))
        self.assertGreater(len(df), 1)
        df2 = cast(
            DataFrame,
            self.wrd.get_capacity(
                quarter=[2, 3, 4],
                process_unit="Atmos Distillation",
                owner="bp",
                page_size=10,
            ),
        )
        self.assertGreater(len(df2), 1)

    @pytest.mark.integtest
    def test_pagination(self):
        df: DataFrame = cast(
            DataFrame,
            self.wrd.get_capacity(
                year=[2012, 2019], owner="Angolan Govt.", page_size=10, paginate=True
            ),
        )

        df_unpaged = cast(
            DataFrame,
            self.wrd.get_capacity(
                year=[2012, 2019], owner="Angolan Govt.", page_size=1000
            ),
        )
        self.assertEqual(len(df), len(df_unpaged))

    @pytest.mark.integtest
    def test_runs(self):
        df: DataFrame = cast(
            DataFrame, self.wrd.get_runs(year=[2012, 2019], page_size=10)
        )
        self.assertGreater(len(df), 1)

    @pytest.mark.integtest
    def test_yields(self):
        df: DataFrame = cast(
            DataFrame, self.wrd.get_yields(year_gte=2019, page_size=10)
        )

        df2 = cast(
            DataFrame,
            self.wrd.get_yields(quarter=4, capacity_status_id=100, page_size=10),
        )
        self.assertGreater(len(df), 1)
        self.assertGreater(len(df2), 1)

    @pytest.mark.integtest
    def test_outages(self):
        ser1: "Series[str]" = Series(["Cenovus", "BP"])
        df: DataFrame = cast(
            DataFrame,
            self.wrd.get_outages(date_gte=date(2021, 1, 1), owner=ser1, page_size=10),
        )

        df2 = cast(
            DataFrame,
            self.wrd.get_outages(quarter=4, process_unit=["CDU"], page_size=10),
        )
        df3 = cast(
            DataFrame,
            self.wrd.get_outages(
                date_gte=date(2022, 12, 2), page_size=10, refinery_id=[13, 14, 19, 1002]
            ),
        )
        df4 = cast(
            DataFrame,
            self.wrd.get_outages(outage_vol_gte=50, page_size=10),
        )
        ser: "Series[float]" = Series([20, 30, 40, 50, 60])
        serd: "Series[date]" = Series([date(2022, 1, 1), date(2022, 2, 2)])
        df_ser = cast(DataFrame, self.wrd.get_outages(outage_vol=ser, date=serd))

        self.assertGreater(len(df), 1)
        self.assertGreater(len(df2), 1)
        self.assertGreater(len(df3), 1)
        self.assertGreater(len(df4), 1)
        self.assertGreater(len(df_ser), 1)

    @pytest.mark.integtest
    def test_ownership(self):
        ser1: "Series[str]" = Series(["Saudi Aramco", "BP"])
        df: DataFrame = cast(
            DataFrame,
            self.wrd.get_ownership(year=2022, owner=ser1, page_size=10),
        )

        df2 = cast(
            DataFrame,
            self.wrd.get_ownership(
                quarter=3, refinery_id=[1, 2, 19, 1002], page_size=10
            ),
        )
        df3 = cast(
            DataFrame,
            self.wrd.get_ownership(
                year_lte=2021, page_size=10, refinery_id=[13, 14, 19, 1002]
            ),
        )

        df_paged = cast(
            DataFrame,
            self.wrd.get_ownership(
                year=2008, refinery_id=1, page_size=2, paginate=True
            ),
        )
        df_all = cast(
            DataFrame, self.wrd.get_ownership(year=2008, refinery_id=1, page_size=100)
        )

        self.assertGreater(len(df), 1)
        self.assertGreater(len(df2), 1)
        self.assertGreater(len(df3), 1)
        self.assertEqual(len(df_paged), len(df_all))

    @pytest.mark.integtest
    def test_margins(self):
        ser1: "Series[str]" = Series(["Dated Brent NWE Cracking"])
        df: DataFrame = cast(
            DataFrame,
            self.wrd.get_margins(margin_type=ser1, page_size=10),
        )

        df2 = cast(
            DataFrame,
            self.wrd.get_margins(date=date(2022, 2, 16), page_size=10),
        )
        df3 = cast(
            DataFrame,
            self.wrd.get_margins(
                date_gte=date(2022, 1, 1),
                page_size=10,
                margin_type="Dated Brent NWE Cracking",
            ),
        )

        df_paged = cast(
            DataFrame,
            self.wrd.get_margins(date=date(2022, 2, 16), page_size=6, paginate=True),
        )
        df_all = cast(
            DataFrame, self.wrd.get_margins(date=date(2022, 2, 16), page_size=100)
        )

        self.assertGreater(len(df), 1)
        self.assertGreater(len(df2), 1)
        self.assertGreater(len(df3), 1)
        self.assertEqual(len(df_paged), len(df_all))

    @pytest.mark.integtest
    def test_ref(self):
        spgci.config.sleep_time = 0.4

        for t in self.wrd.RefTypes:
            df = cast(DataFrame, self.wrd.get_reference_data(type=t))
            self.assertGreater(len(df), 1)
            # df = self.wos.get_reference_data(type=WOSRefType.Regions)
            # print(df)

    @pytest.mark.integtest
    def test_ref_pagination(self):
        df = cast(DataFrame, self.wrd.get_reference_data(type=self.wrd.RefTypes.Owners))
        self.assertGreater(len(df), 1000)

    @pytest.mark.integtest
    def test_outages_ts(self):
        print()
        df = cast(DataFrame, self.wrd.get_outages_grouped(country="United States"))
        self.assertGreater(len(df), 1)
