import unittest
import pytest
from datetime import datetime
from spgci import LNGGlobalAnalytics


class LNGTendersTest(unittest.TestCase):
    lg = LNGGlobalAnalytics()

    @pytest.mark.integtest
    def test_tenders_All(self):
        lg = self.lg.get_tenders(
            page_size=10,
        )
        self.assertGreater(len(lg), 1)  # type: ignore

    @pytest.mark.integtest
    def test_tendersWithMultipleCargoType(self):
        lg = self.lg.get_tenders(
            filter_exp='cargoType in ("Single Cargo","Multiple Cargo")',
            page_size=10,
        )

        self.assertGreater(len(lg), 1)  # type: ignore

    @pytest.mark.integtest
    def test_tendersWithCountryName(self):
        lg = self.lg.get_tenders(
            filter_exp='countryName:"Australia"',
            page_size=10,
        )
        self.assertGreater(len(lg), 1)  # type: ignore

    @pytest.mark.integtest
    def test_tendersWithCountryNameParam(self):
        lg = self.lg.get_tenders(
            country_name="Australia",
            page_size=10,
        )
        self.assertGreater(len(lg), 1)  # type: ignore

    @pytest.mark.integtest
    def test_simple(self):
        lg = self.lg.get_tenders(
            filter_exp='tenderStatus:"Awarded"',
            page_size=10,
        )
        self.assertGreater(len(lg), 1)  # type: ignore

    @pytest.mark.integtest
    def test_tendersWithDate(self):
        lg = self.lg.get_tenders(
            lifting_delivery_period_from_gt=datetime(2019, 2, 10, 19, 29, 00),
            page_size=10,
        )
        self.assertGreater(len(lg), 1)  # type: ignore

    @pytest.mark.integtest
    def test_tendersWithDateLessThan(self):
        lg = self.lg.get_tenders(
            lifting_delivery_period_from_lt=datetime(2019, 2, 10, 19, 29, 00),
            page_size=10,
        )
        self.assertGreater(len(lg), 1)  # type: ignore

    @pytest.mark.integtest
    def test_tendersWithDateGreaterThanEqual(self):
        lg = self.lg.get_tenders(
            lifting_delivery_period_from_gte=datetime(2019, 2, 10, 19, 29, 00),
            page_size=10,
        )
        self.assertGreater(len(lg), 1)  # type: ignore

    @pytest.mark.integtest
    def test_lng_data_start_date_greater(self):
        lg = self.lg.get_outages(
            start_date_gte=datetime(2014, 4, 29, 00, 00, 00),
            page_size=10,
        )
        self.assertGreater(len(lg), 1)  # type: ignore

    @pytest.mark.integtest
    def test_lng_data_start_date_equal(self):
        lg = self.lg.get_outages(
            start_date=datetime(2014, 5, 15, 00, 00, 00),
            page_size=10,
        )
        self.assertGreater(len(lg), 0)  # type: ignore

    @pytest.mark.integtest
    def test_lng_data_start_date_lesser(self):
        lg = self.lg.get_outages(
            start_date_lt=datetime(2020, 9, 10, 00, 00, 00),
            page_size=10,
        )
        self.assertGreater(len(lg), 0)  # type: ignore

    @pytest.mark.integtest
    def test_lng_data_with_commodity_name(self):
        lg = self.lg.get_outages(
            commodity_name="LNG",
            page_size=10,
        )
        self.assertGreater(len(lg), 1)  # type: ignore

    @pytest.mark.integtest
    def test_lng_data_with_filter_exp(self):
        lg = self.lg.get_outages(
            filter_exp='commodityName:"LNG"',
            page_size=10,
        )
        self.assertGreater(len(lg), 1)  # type: ignore

    def test_lng_data_with_filter_exp_capacity(self):
        lg = self.lg.get_outages(
            filter_exp="totalCapacity > 4",
            page_size=10,
        )
        self.assertGreater(len(lg), 1)  # type: ignore

    def test_lng_get_reference_data_confidence(self):
        lg = self.lg.get_reference_data(type=self.lg.RefTypes.ConfidenceLevel)
        self.assertGreater(len(lg), 0)  # type: ignore

    def test_lng_get_reference_data_liquefaction_trains(self):
        lg = self.lg.get_reference_data(type=self.lg.RefTypes.LiquefactionTrains)
        self.assertGreater(len(lg), 0)  # type: ignore

    def test_lng_get_reference_data_liquefaction_projects(self):
        lg = self.lg.get_reference_data(type=self.lg.RefTypes.LiquefactionProjects)
        self.assertGreater(len(lg), 0)  # type: ignore
