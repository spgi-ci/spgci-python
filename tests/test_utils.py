import unittest
from spgci import utilities, market_data
from pandas import Series
from datetime import date


class UtilitiesTest(unittest.TestCase):
    def test_list_to_filter(self):
        expected = 'curve_codes in ("ABC","XYZ")'
        actual = utilities.list_to_filter("curve_codes", ["ABC", "XYZ"])

        self.assertEqual(expected, actual)

    def test_str_to_filter(self):
        expected = 'curve_codes: "ABC"'
        actual = utilities.list_to_filter("curve_codes", "ABC")

        self.assertEqual(expected, actual)

    def test_int_to_filter(self):
        expected = "curve_codes: 123"
        actual = utilities.list_to_filter("curve_codes", 123)

        self.assertEqual(expected, actual)

    def test_list_enum_to_filter(self):
        expected = 'curve_codes in ("future","swap")'
        actual = utilities.list_to_filter(
            "curve_codes",
            [
                market_data.MarketData.ContractType.Future,
                market_data.MarketData.ContractType.Swap,
            ],
        )

        self.assertEqual(expected, actual)

    def test_enum_to_filter(self):
        expected = 'curve_codes: "future"'
        actual = utilities.list_to_filter(
            "curve_codes", market_data.MarketData.ContractType.Future
        )

        self.assertEqual(expected, actual)

    def test_odata_date_to_filter(self):
        expected = "date eq 2022-12-01"
        actual = utilities.odata_list_to_filter("date", date(2022, 12, 1))

        self.assertEqual(expected, actual)

    def test_odata_datelist_to_filter(self):
        expected = "date in ('2022-12-01','2021-12-01')"
        actual = utilities.odata_list_to_filter(
            "date", [date(2022, 12, 1), date(2021, 12, 1)]
        )

        self.assertEqual(expected, actual)

    def test_odata_dateseries_to_filter(self):
        ser: "Series[date]" = Series([date(2022, 12, 1), date(2021, 12, 1)])
        expected = "date in ('2022-12-01','2021-12-01')"
        actual = utilities.odata_list_to_filter("date", ser)

        self.assertEqual(expected, actual)

    def test_list_int_to_filter(self):
        expected = "curve_codes in (1,2,3)"
        actual = utilities.list_to_filter("curve_codes", [1, 2, 3])

        self.assertEqual(expected, actual)

    def test_tuple_int_to_filter(self):
        expected = "curve_codes in (1,2,3)"
        actual = utilities.list_to_filter("curve_codes", (1, 2, 3))

        self.assertEqual(expected, actual)

    def test_series_int_to_filter(self):
        expected = "curve_codes in (1,2,3)"
        ser: Series[int] = Series([1, 2, 3])
        actual = utilities.list_to_filter("curve_codes", ser)

        self.assertEqual(expected, actual)

    def test_series_str_to_filter(self):
        expected = 'curve_codes in ("abc","def","ghi")'
        ser: Series[int] = Series(["abc", "def", "ghi"])
        actual = utilities.list_to_filter("curve_codes", ser)

        self.assertEqual(expected, actual)
