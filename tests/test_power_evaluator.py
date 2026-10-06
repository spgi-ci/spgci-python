import pytest
from spgci.power_evaluator import PowerEvaluator

pe = PowerEvaluator()


def test_requires_measure_or_series():
    with pytest.raises(ValueError):
        pe.submit(grain="Asset", dry_run=True)


def test_fixed_fields_and_scope():
    body = pe.submit(
        scope=pe.scope(state="California", fuel=["Solar"]),
        grain="Asset",
        measure="irr",
        prime_mover="Solar",
        dry_run=True,
    )
    assert body == {
        "SCOPE": [{"state": ["California"], "fuel": ["Solar"]}],
        "GRAIN": "Asset",
        "MEASURE": ["irr"],
        "PRIME_MOVER": "Solar",
    }


def test_override_nesting():
    curve = pe.Curve("annual", {2027: 48.0, 2028: 52.5})
    block = {
        "temporal_resolution": "annual",
        "values": [
            {"timestamp": "2027", "value": 48.0},
            {"timestamp": "2028", "value": 52.5},
        ],
    }
    body = pe.submit(
        measure=["irr"],
        tax_fed=0.18,
        coe=["default", 0.09],
        energy_price=curve,
        capacity_price=["default", 50, curve, [curve, curve]],
        dry_run=True,
    )
    assert body["TAX_FED"] == 0.18
    assert body["COE"] == ["default", 0.09]
    assert body["ENERGY_PRICE"] == [[block]]
    assert body["CAPACITY_PRICE"] == ["default", 50, [block], [block, block]]


def test_site_parameter_overrides():
    s = pe.site("a", "POINT(-117.8 34.6)", project="p", battery_duration=4)
    assert s == {
        "object_name": ["a"],
        "geometry": ["POINT(-117.8 34.6)"],
        "project_name": ["p"],
        "parameter_overrides": {"battery_duration": 4},
    }


def test_payload_passthrough():
    raw = {"SERIES": ["metric"], "API_TYPE": "async"}
    assert pe.submit(payload=raw, dry_run=True) == raw


# --- response handling (mocked) -------------------------------------------
from unittest import mock

import pandas as pd


class _Resp:
    def __init__(self, j=None, content=b""):
        self._j, self.content = j, content

    def json(self):
        return self._j

    def raise_for_status(self):
        pass


def test_submit_sync_returns_dataframe():
    j = {"Message": "Data Extracted Successfully", "Data": [{"scenario": "A", "vintage": "V"}]}
    with mock.patch("spgci.power_evaluator.post_data", return_value=_Resp(j)):
        df = pe.submit(series=["Scenario"])
    assert list(df.columns) == ["scenario", "vintage"]


def test_submit_async_returns_run_id():
    j = {"run_id": 82373844772840, "Message": "Execution is currently in process"}
    with mock.patch("spgci.power_evaluator.post_data", return_value=_Resp(j)):
        assert pe.submit(series=["metric"], run_async=True) == "82373844772840"


def test_get_status_unwraps_data():
    j = {"Message": "Result Retrieved", "Data": [{"run_id": "1", "Message": "InProgress"}]}
    with mock.patch("spgci.power_evaluator.get_data", return_value=_Resp(j)):
        assert pe.get_status("1") == {"run_id": "1", "Message": "InProgress"}


def test_get_result_requires_completed():
    j = {"Data": [{"run_id": "1", "Message": "InProgress", "presigned_url": None}]}
    with mock.patch("spgci.power_evaluator.get_data", return_value=_Resp(j)):
        with pytest.raises(RuntimeError):
            pe.get_result("1")


def test_get_result_parses_escaped_quotes():
    csv = b'metric,desc\nm,"say \\"hi\\", ok"\n'
    j = {"Data": [{"run_id": "1", "Message": "Completed", "presigned_url": "https://s3/x"}]}
    with mock.patch("spgci.power_evaluator.get_data", return_value=_Resp(j)), mock.patch(
        "spgci.power_evaluator.requests.get", return_value=_Resp(content=csv)
    ):
        df = pe.get_result("1")
    assert df.shape == (1, 2) and df.loc[0, "metric"] == "m"


def test_run_async_maps_to_api_type():
    assert "API_TYPE" not in pe.submit(measure="irr", dry_run=True)
    assert pe.submit(measure="irr", run_async=True, dry_run=True)["API_TYPE"] == "async"
    assert pe.submit(measure="irr", run_async=False, dry_run=True)["API_TYPE"] == "sync"
