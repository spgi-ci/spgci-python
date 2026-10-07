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


# --- get_reference_data (mocked network, temp cache) ------------------------
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path


class _Http:
    def __init__(self, payload=None, text=""):
        self._payload, self.content = payload, text.encode()

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


def _manifest(age_days=1, datasets=("cases",)):
    ts = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat()
    return {"generated_at": ts, "datasets": {d: {} for d in datasets}}


def _fake_get(manifest, csv="scenario,vintage,method\nA,V1,Nodal\n"):
    def get(url, **kwargs):
        if url.endswith("manifest.json"):
            return _Http(manifest)
        return _Http(text=csv)

    return get


def _ref(fn):
    """Run a test body with an isolated cache dir."""

    def wrapper():
        with tempfile.TemporaryDirectory() as d:
            with mock.patch(
                "spgci.power_evaluator._reference_cache_dir", return_value=Path(d)
            ):
                fn(Path(d))

    return wrapper


@_ref
def test_reference_unknown_dataset(cache):
    with pytest.raises(ValueError):
        pe.get_reference_data("node")


@_ref
def test_reference_github_then_cache(cache):
    get = mock.Mock(side_effect=_fake_get(_manifest()))
    with mock.patch("spgci.power_evaluator.requests.get", get):
        df = pe.get_reference_data("cases")
        assert df.attrs["source"] == "github" and df.loc[0, "vintage"] == "V1"
        df2 = pe.get_reference_data("cases")
    assert df2.attrs["source"] == "cache"
    assert get.call_count == 2  # manifest + csv once; second call served from disk


@_ref
def test_reference_stale_manifest_falls_back_to_api(cache):
    api = pd.DataFrame({"scenario": ["B"], "vintage": ["V2"], "method": ["Zonal"]})
    with mock.patch(
        "spgci.power_evaluator.requests.get", _fake_get(_manifest(age_days=30))
    ), mock.patch.object(PowerEvaluator, "submit", return_value=api) as sub:
        df = pe.get_reference_data("cases")
    assert df.attrs["source"] == "api" and df.loc[0, "vintage"] == "V2"
    assert sub.call_args.kwargs["run_async"] is False


@_ref
def test_reference_github_failure_falls_back_to_api(cache):
    import requests as rq

    api = pd.DataFrame({"state": ["Texas"]})
    with mock.patch(
        "spgci.power_evaluator.requests.get", side_effect=rq.exceptions.ConnectTimeout()
    ), mock.patch.object(PowerEvaluator, "submit", return_value=api):
        df = pe.get_reference_data("state")
    assert df.attrs["source"] == "api"


@_ref
def test_reference_use_github_false_and_refresh(cache):
    api = pd.DataFrame({"state": ["Texas"]})
    get = mock.Mock()
    with mock.patch("spgci.power_evaluator.requests.get", get), mock.patch.object(
        PowerEvaluator, "submit", return_value=api
    ) as sub:
        pe.get_reference_data("state", use_github=False)
        assert pe.get_reference_data("state").attrs["source"] == "cache"
        pe.get_reference_data("state", refresh=True, use_github=False)
    assert get.call_count == 0 and sub.call_count == 2


@_ref
def test_reference_expired_cache_is_refetched(cache):
    with mock.patch("spgci.power_evaluator.requests.get", _fake_get(_manifest())):
        pe.get_reference_data("cases")
        meta_path = cache / "cases.json"
        meta = json.loads(meta_path.read_text())
        meta["fetched_at"] = (
            datetime.now(timezone.utc) - timedelta(days=2)
        ).isoformat()
        meta_path.write_text(json.dumps(meta))
        assert pe.get_reference_data("cases").attrs["source"] == "github"


@_ref
def test_reference_cached_snapshot_ages_out(cache):
    api = pd.DataFrame({"scenario": ["B"], "vintage": ["V2"], "method": ["Zonal"]})
    with mock.patch("spgci.power_evaluator._REFERENCE_MAX_AGE", timedelta(days=14)):
        with mock.patch("spgci.power_evaluator.requests.get", _fake_get(_manifest())):
            pe.get_reference_data("cases")
        meta_path = cache / "cases.json"
        meta = json.loads(meta_path.read_text())
        meta["generated_at"] = (
            datetime.now(timezone.utc) - timedelta(days=20)
        ).isoformat()
        meta_path.write_text(json.dumps(meta))
        with mock.patch(
            "spgci.power_evaluator.requests.get", _fake_get(_manifest(age_days=20))
        ), mock.patch.object(PowerEvaluator, "submit", return_value=api):
            assert pe.get_reference_data("cases").attrs["source"] == "api"


@_ref
def test_reference_api_non_dataframe_raises(cache):
    with mock.patch.object(PowerEvaluator, "submit", return_value="123"):
        with pytest.raises(RuntimeError):
            pe.get_reference_data("state", use_github=False)
