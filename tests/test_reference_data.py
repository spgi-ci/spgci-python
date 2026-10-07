from __future__ import annotations

import importlib.util
import json
import sys
import typing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal, Optional, Union
from unittest import mock

import pandas as pd
import pytest
import requests as rq
import spgci
import spgci.config
from pandas import Series
from spgci import reference_data as rd
from spgci.agriculture_and_food import AgriAndFood
from spgci.chemicals import Chemicals
from spgci.oil_ngl_analytics import OilNGLAnalytics

NS = "chemicals"
NOW = datetime.now(timezone.utc)

SERIES = pd.DataFrame(
    {
        "commodity": ["Ethylene", "Ethylene", "Propylene"],
        "concept": ["Capacity", "Production", "Capacity"],
        "uom": ["kt", "kt", "kt"],
    }
)
# one row per combination of every dimension: what a ``full`` snapshot holds
WIDE = pd.DataFrame(
    {
        "commodity": ["Ethylene", "Ethylene", "Propylene", "Propylene"],
        "region": ["Asia", "Europe", "Asia", "Europe"],
        "concept": ["Capacity", "Capacity", "Capacity", "Production"],
        "uom": ["kt", "kt", "kt", "t"],
    }
)


class _Resp:
    def __init__(self, content: bytes):
        self.content = content

    def raise_for_status(self):
        pass


class FakeGitHub:
    """Stands in for ``requests.get``: serves ``root/<ns>/<file>`` and counts calls."""

    def __init__(self, root: Path):
        self.root, self.calls, self.down = root, [], False

    def __call__(self, url, **kwargs):
        self.calls.append(url)
        if self.down:
            raise rq.exceptions.ConnectTimeout()
        path = self.root / url[len(rd.base_url()) + 1 :]
        if not path.exists():
            raise rq.exceptions.HTTPError("404")
        return _Resp(path.read_bytes())


def publish(root: Path, snapshots: dict, generated_at=NOW, namespace=NS):
    """``snapshots``: name -> (dataset, columns, df[, layout]). Writes files + manifest."""
    directory = root / namespace
    entries = {
        name: rd.write_snapshot(
            directory, name, spec[0], spec[1], spec[2], generated_at, *spec[3:]
        )
        for name, spec in snapshots.items()
    }
    rd.write_manifest(directory, entries, generated_at)


def edit_manifest(root: Path, fn, namespace=NS):
    path = root / namespace / "manifest.json"
    manifest = json.loads(path.read_text())
    fn(manifest)
    path.write_text(json.dumps(manifest))


def age_local_manifest(cache: Path, delta: timedelta):
    path = cache / NS / "manifest.json"
    wrapper = json.loads(path.read_text())
    wrapper["fetched_at"] = (NOW - delta).isoformat()
    path.write_text(json.dumps(wrapper))


@pytest.fixture
def remote(tmp_path, monkeypatch):
    root = tmp_path / "remote"
    publish(root, {"capacity__series": ("capacity", list(SERIES.columns), SERIES)})
    gh = FakeGitHub(root)
    monkeypatch.setattr(rd.requests, "get", gh)
    monkeypatch.setenv("SPGCI_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.delenv("SPGCI_REFERENCE_LAYOUT", raising=False)
    monkeypatch.setattr(rd, "_stores", {})
    gh.cache = tmp_path / "cache"
    return gh


@pytest.fixture
def store(remote):
    return rd.ReferenceStore(NS)


@pytest.fixture
def both_layouts(remote):
    """``grouped`` (small, hand-picked) and ``full`` (everything) side by side."""
    grouped = WIDE[["commodity", "concept"]]
    publish(
        remote.root,
        {
            "capacity__series": (
                "capacity",
                ["commodity", "concept"],
                grouped,
                "grouped",
            ),
            "capacity__all": ("capacity", list(WIDE.columns), WIDE, "full"),
        },
    )


# --- ReferenceStore ---------------------------------------------------------


def test_github_then_cache(remote, store):
    df = store.lookup("capacity", "commodity")
    assert df.attrs["source"] == "github" and df.attrs["snapshot"] == "capacity__series"
    assert len(remote.calls) == 2  # manifest + file
    assert store.lookup("capacity", "commodity").attrs["source"] == "cache"
    assert len(remote.calls) == 2  # second answer came from disk


def test_subset_is_deduplicated_in_requested_order(remote, store):
    df = store.lookup("capacity", ["uom", "commodity"])
    assert list(df.columns) == ["uom", "commodity"]
    assert df.values.tolist() == [["kt", "Ethylene"], ["kt", "Propylene"]]
    assert len(store.lookup("capacity", list(SERIES.columns))) == 3


def test_columns_accepted_as_str_list_or_comma_string(remote, store):
    a = store.lookup("capacity", "commodity, uom")
    b = store.lookup("capacity", ["commodity", "uom"])
    assert a.equals(b)


@pytest.mark.parametrize(
    "dataset, columns",
    [
        ("capacity", ["commodity", "region"]),  # column outside the snapshot
        ("trade", "commodity"),  # no snapshot for this dataset
        ("capacity", None),
        ("capacity", ""),
    ],
)
def test_miss_returns_none(remote, store, dataset, columns):
    assert store.lookup(dataset, columns) is None


def test_old_snapshot_is_ignored_but_fresh_one_is_used(remote, store):
    wide = SERIES.assign(extra="x")
    publish(
        remote.root,
        {
            "capacity__series": ("capacity", list(SERIES.columns), SERIES),
            "capacity__wide": ("capacity", list(wide.columns), wide),
        },
    )
    old = (NOW - timedelta(days=20)).isoformat()
    edit_manifest(
        remote.root,
        lambda m: m["snapshots"]["capacity__series"].update(generated_at=old),
    )
    df = store.lookup("capacity", "commodity")
    assert df is not None and len(df) == 2  # served by the fresh, wider snapshot
    edit_manifest(
        remote.root,
        lambda m: m["snapshots"]["capacity__wide"].update(generated_at=old),
    )
    store.clear_local()
    assert store.lookup("capacity", "commodity") is None


def test_smallest_covering_snapshot_wins(remote, store):
    wide = SERIES.assign(extra="x")
    publish(
        remote.root,
        {
            "capacity__wide": ("capacity", list(wide.columns), wide),
            "capacity__series": ("capacity", list(SERIES.columns), SERIES),
        },
    )
    store.lookup("capacity", "commodity")
    assert (remote.cache / NS / "capacity__series.csv.gz").exists()
    assert not (remote.cache / NS / "capacity__wide.csv.gz").exists()


def test_github_down_falls_through_and_is_not_retried(remote, store):
    remote.down = True
    assert store.lookup("capacity", "commodity") is None
    assert store.lookup("capacity", "commodity") is None
    assert len(remote.calls) == 1  # cooldown: no timeout paid per call
    store.clear_local()
    assert store.lookup("capacity", "commodity") is None
    assert len(remote.calls) == 2


def test_stale_manifest_still_serves_when_github_is_down(remote, store):
    store.lookup("capacity", "commodity")
    age_local_manifest(remote.cache, timedelta(days=2))
    store._memory.clear()  # as if a day later, in a new process
    store._frames.clear()
    remote.down = True
    assert store.lookup("capacity", "commodity").attrs["source"] == "cache"


def test_republished_snapshot_replaces_local_copy(remote, store):
    store.lookup("capacity", "commodity")
    new = pd.concat([SERIES, SERIES.assign(commodity="Benzene")])
    publish(remote.root, {"capacity__series": ("capacity", list(new.columns), new)})
    age_local_manifest(remote.cache, timedelta(days=2))
    store._memory.clear()  # as if a day later, in a new process
    store._frames.clear()
    df = store.lookup("capacity", "commodity")
    assert df.attrs["source"] == "github"
    assert set(df["commodity"]) == {"Ethylene", "Propylene", "Benzene"}


def test_fresh_manifest_is_not_refetched(remote, store):
    store.lookup("capacity", "commodity")
    publish(
        remote.root, {"capacity__series": ("capacity", list(SERIES.columns), SERIES)}
    )
    store.lookup("capacity", "commodity")
    assert len(remote.calls) == 2  # still trusting the local manifest


def test_checksum_mismatch_is_rejected_and_not_cached(remote, store):
    (remote.root / NS / "capacity__series.csv.gz").write_bytes(
        b"not what was published"
    )
    assert store.lookup("capacity", "commodity") is None
    assert not (remote.cache / NS / "capacity__series.csv.gz").exists()
    assert store.lookup("capacity", "commodity") is None
    assert len(remote.calls) == 2  # cooldown: not re-downloaded on every call


def test_corrupt_local_files_are_replaced(remote, store):
    store.lookup("capacity", "commodity")
    (remote.cache / NS / "capacity__series.csv.gz").write_text("garbage")
    (remote.cache / NS / "manifest.json").write_text("{not json")
    store._memory.clear()  # a new process finds only the damaged files
    store._frames.clear()
    df = store.lookup("capacity", "commodity")
    assert df.attrs["source"] == "github" and len(df) == 2


def test_dtypes_survive_the_csv_round_trip(remote, store):
    df = pd.DataFrame(
        {
            "code": ["NA", "007", "ZA"],  # "NA" is Namibia, not a missing value
            "flag": [True, False, True],
            "n": [1, 2, 3],
            "ratio": [0.5, 1.5, 2.5],
            "note": ["a", None, "c"],
        }
    )
    publish(remote.root, {"d__all": ("d", list(df.columns), df)})
    out = store.lookup("d", list(df.columns)).set_index("code")  # rows are sorted
    assert out.index.tolist() == ["007", "NA", "ZA"]  # no lost zeros, no NaN
    assert out["flag"].dtype == bool and out["n"].dtype == "int64"
    assert out["ratio"].dtype == "float64"
    assert out["note"].isna().to_dict() == {"007": True, "NA": False, "ZA": False}


def test_date_columns_are_parsed_like_the_api_path(remote, store):
    df = pd.DataFrame(
        {
            "commodity": ["Ethylene"],
            "vintageDate": pd.to_datetime(["2026-03-01T00:00:00Z"], utc=True),
        }
    )
    publish(remote.root, {"d__v": ("d", list(df.columns), df)})
    out = store.lookup("d", ["commodity", "vintageDate"], date_columns=["vintageDate"])
    assert str(out["vintageDate"].dtype) == "datetime64[ns, UTC]"
    assert out["vintageDate"].iloc[0] == df["vintageDate"].iloc[0]


def test_read_only_cache_is_served_from_memory(remote, store, monkeypatch):
    monkeypatch.setattr(rd, "_atomic_write", mock.Mock(side_effect=OSError("ro")))
    assert store.lookup("capacity", "commodity").attrs["source"] == "github"
    store._frames.clear()  # force a re-read: it must come from memory, not the network
    assert store.lookup("capacity", "commodity").attrs["source"] == "cache"
    assert len(remote.calls) == 2  # nothing re-downloaded


def test_parsed_frames_are_reused(remote, store):
    with mock.patch.object(store, "_csv", wraps=store._csv) as csv:
        store.lookup("capacity", "commodity")
        store.lookup("capacity", ["commodity", "uom"])
        store.lookup("capacity", "concept")
    assert csv.call_count == 1  # read and parsed once, then served from memory


def test_lookup_results_do_not_alias_the_cached_frame(remote, store):
    first = store.lookup("capacity", "commodity")
    first.loc[0, "commodity"] = "tampered"
    assert "tampered" not in store.lookup("capacity", "commodity").values


def test_snapshots_are_gzipped_and_byte_stable(tmp_path):
    big = pd.DataFrame({"a": ["x"] * 2000, "b": [f"v{i % 50}" for i in range(2000)]})
    one = rd.write_snapshot(tmp_path / "1", "d__a", "d", ["a", "b"], big, NOW)
    two = rd.write_snapshot(
        tmp_path / "2", "d__a", "d", ["a", "b"], big.iloc[::-1], NOW
    )
    assert one["file"] == "d__a.csv.gz" and one["sha256"] == two["sha256"]
    assert one["bytes"] < 400 < len(big.to_csv(index=False))
    assert (tmp_path / "1" / "d__a.csv.gz").read_bytes()[:2] == b"\x1f\x8b"
    with pytest.raises(ValueError, match="cap"):
        rd.write_snapshot(
            tmp_path / "3", "d__a", "d", ["a", "b"], big, NOW, max_bytes=10
        )
    assert not (tmp_path / "3" / "d__a.csv.gz").exists()


def test_plain_csv_entries_without_a_file_field_still_load(remote, store):
    directory = remote.root / NS
    (directory / "old__x.csv").write_text("commodity\nEthylene\nPropylene\n")
    import hashlib

    sha = hashlib.sha256((directory / "old__x.csv").read_bytes()).hexdigest()
    edit_manifest(
        remote.root,
        lambda m: m["snapshots"].update(
            old__x=dict(
                dataset="old",
                columns=["commodity"],
                rows=2,
                sha256=sha,
                generated_at=NOW.isoformat(),
            )
        ),
    )
    assert store.lookup("old", "commodity")["commodity"].tolist() == [
        "Ethylene",
        "Propylene",
    ]


# --- layouts side by side -----------------------------------------------------


def test_auto_prefers_the_smallest_snapshot_and_falls_back_to_the_full_one(
    remote, store, both_layouts
):
    assert store.lookup("capacity", "commodity").attrs["snapshot"] == "capacity__series"
    cross = store.lookup("capacity", ["region", "concept"])  # spans neither group
    assert cross.attrs["snapshot"] == "capacity__all"
    assert len(cross) == 3  # (Asia, Capacity) appears twice in WIDE


def test_layout_can_be_forced(remote, store, both_layouts, monkeypatch):
    monkeypatch.setenv("SPGCI_REFERENCE_LAYOUT", "grouped")
    assert store.lookup("capacity", "commodity").attrs["snapshot"] == "capacity__series"
    assert store.lookup("capacity", ["region", "concept"]) is None
    monkeypatch.setenv("SPGCI_REFERENCE_LAYOUT", "full")
    assert store.lookup("capacity", "commodity").attrs["snapshot"] == "capacity__all"
    assert store.lookup("capacity", ["region", "concept"]) is not None
    monkeypatch.setenv("SPGCI_REFERENCE_LAYOUT", "bogus")
    assert rd.layout() == "auto"


def test_the_full_snapshot_answers_the_same_as_the_grouped_one(
    remote, store, both_layouts, monkeypatch
):
    monkeypatch.setenv("SPGCI_REFERENCE_LAYOUT", "grouped")
    grouped = store.lookup("capacity", ["commodity", "concept"])
    monkeypatch.setenv("SPGCI_REFERENCE_LAYOUT", "full")
    full = store.lookup("capacity", ["commodity", "concept"])
    key = ["commodity", "concept"]
    assert (
        grouped.sort_values(key)
        .reset_index(drop=True)
        .equals(full.sort_values(key).reset_index(drop=True))
    )


# --- filters -------------------------------------------------------------------

EXPRESSIONS = [
    ('commodity: "Ethylene"', [("commodity", ["Ethylene"])]),
    ('commodity:"Ethylene"', [("commodity", ["Ethylene"])]),
    ('commodity in ("A","B")', [("commodity", ["A", "B"])]),
    ('commodity in ( "A" , "B" )', [("commodity", ["A", "B"])]),
    (
        'commodity: "A" AND region in ("x","y")',
        [("commodity", ["A"]), ("region", ["x", "y"])],
    ),
    ('commodity: "Oil AND Gas"', [("commodity", ["Oil AND Gas"])]),
    ('commodity: "a,b (c)"', [("commodity", ["a,b (c)"])]),
]


@pytest.mark.parametrize("expression, expected", EXPRESSIONS)
def test_parse_filter(expression, expected):
    assert rd.parse_filter(expression) == expected


@pytest.mark.parametrize(
    "expression",
    [
        "",
        'commodity eq "x"',  # the odata strategy
        'a: "x" OR b: "y"',
        'a: "x" and b: "y"',
        'a > "2020-01-01"',
        'a >= "2020-01-01" AND b: "x"',
        "a: x",
        "a: 1",
        "a in ()",
        'a: "x" AND',
        'a: "x" b: "y"',
        'a: "x"; DROP TABLE',
        '(a: "x")',
        'a: "x" AND (b: "y")',
    ],
)
def test_parse_filter_rejects_everything_else(expression):
    assert rd.parse_filter(expression) is None


def test_parse_filter_reads_what_build_filter_expression_writes():
    expression = spgci.utilities.build_filter_expression(
        {"commodity": ["Jet fuel", "Jet/Kero"], "region": ["Europe"], "uom": "kt"}
    )
    assert rd.parse_filter(expression) == [
        ("commodity", ["Jet fuel", "Jet/Kero"]),
        ("region", ["Europe"]),
        ("uom", ["kt"]),
    ]


@pytest.fixture
def wide_store(remote, store, both_layouts):
    return store


def test_filter_restricts_rows_before_deduplicating(remote, wide_store):
    df = wide_store.lookup("capacity", "commodity", filter_exp='region: "Asia"')
    assert sorted(df["commodity"]) == ["Ethylene", "Propylene"]
    df = wide_store.lookup("capacity", "region", filter_exp='concept: "Production"')
    assert df["region"].tolist() == ["Europe"]
    df = wide_store.lookup(
        "capacity", ["commodity", "uom"], filter_exp='region in ("Asia","Europe")'
    )
    assert len(df) == 3  # (Ethylene, kt) (Propylene, kt) (Propylene, t)


def test_filters_combine_with_and(remote, wide_store):
    df = wide_store.lookup(
        "capacity",
        "uom",
        filter_exp='commodity: "Propylene" AND region in ("Europe")',
    )
    assert df["uom"].tolist() == ["t"]


def test_filter_matches_what_pandas_says(remote, wide_store):
    expected = WIDE[WIDE["region"].isin(["Europe"]) & (WIDE["commodity"] == "Ethylene")]
    got = wide_store.lookup(
        "capacity",
        ["concept", "uom"],
        filter_exp='region: "Europe" AND commodity: "Ethylene"',
    )
    assert got.values.tolist() == expected[["concept", "uom"]].values.tolist()


def test_filter_can_use_a_column_that_is_not_requested(remote, wide_store):
    df = wide_store.lookup("capacity", "commodity", filter_exp='uom: "t"')
    assert df["commodity"].tolist() == ["Propylene"]
    assert df.attrs["snapshot"] == "capacity__all"  # only the full snapshot has uom


@pytest.mark.parametrize(
    "expression",
    [
        'commodity: "ethylene"',  # wrong case: the server might accept it, we can't know
        'commodity in ("Ethylene","Benzene")',  # one value we can't vouch for
        'commodity: "Ethylene" AND region: "Mars"',
        'commodity: "Ethylene" AND region: "Asia" AND concept: "Production"',  # no rows
        'country: "France"',  # a column no snapshot has
        'commodity: "Ethyl*"',
        'commodity: "Ethylene" OR region: "Asia"',
    ],
)
def test_filters_that_cannot_be_answered_exactly_go_to_the_api(
    remote, wide_store, expression
):
    assert wide_store.lookup("capacity", "commodity", filter_exp=expression) is None


def test_filter_on_a_non_string_column_goes_to_the_api(remote, store):
    df = pd.DataFrame({"commodity": ["a", "b"], "n": [1, 2]})
    publish(remote.root, {"d__all": ("d", ["commodity", "n"], df, "full")})
    assert store.lookup("d", "commodity", filter_exp='n: "1"') is None


def test_filters_follow_the_layout_setting(remote, wide_store, monkeypatch):
    monkeypatch.setenv("SPGCI_REFERENCE_LAYOUT", "grouped")
    assert (
        wide_store.lookup("capacity", "commodity", filter_exp='region: "Asia"') is None
    )
    assert (
        wide_store.lookup("capacity", "commodity", filter_exp='concept: "Capacity"')
        is not None
    )


# --- lookup_unique_values gating -------------------------------------------


def test_only_in_agent_mode(remote, monkeypatch):
    monkeypatch.setattr(spgci.config, "is_agent", False)
    assert rd.lookup_unique_values(NS, "capacity", "commodity") is None
    assert rd.lookup_unique_values(NS, "capacity", "commodity", 'uom: "kt"') is None
    assert remote.calls == []
    monkeypatch.setattr(spgci.config, "is_agent", True)
    assert rd.lookup_unique_values(NS, "capacity", "commodity") is not None
    assert rd.lookup_unique_values(NS, "capacity", "commodity", 'uom: "kt"') is not None
    assert rd.lookup_unique_values(NS, "capacity", "commodity", 'uom > "kt"') is None


def test_unexpected_errors_warn_and_fall_through(remote, monkeypatch):
    monkeypatch.setattr(spgci.config, "is_agent", True)
    with mock.patch.object(rd.ReferenceStore, "lookup", side_effect=RuntimeError("x")):
        with pytest.warns(UserWarning, match="using the API"):
            assert rd.lookup_unique_values(NS, "capacity", "commodity") is None


# --- what gets published ------------------------------------------------------

CLASSES = [Chemicals, OilNGLAnalytics, AgriAndFood]


@pytest.mark.parametrize("cls", CLASSES)
def test_specs_cover_every_dataset_in_both_layouts(cls):
    specs = rd.snapshot_specs(cls)
    datasets = set(typing.get_args(cls._datasets))
    assert set(specs) == set(rd.LAYOUTS)
    assert set(specs["grouped"]) == datasets and set(specs["full"]) == datasets


@pytest.mark.parametrize("cls", CLASSES)
def test_full_snapshot_is_a_superset_of_every_grouped_one(cls):
    """The invariant that makes ``full`` complete: it can answer anything ``grouped`` can."""
    specs = rd.snapshot_specs(cls)
    for dataset, groups in specs["grouped"].items():
        full = specs["full"][dataset]["all"]
        for group, columns in groups.items():
            assert columns and len(columns) == len(set(columns)), (dataset, group)
            assert set(columns) <= set(full), (dataset, group, set(columns) - set(full))


@pytest.mark.parametrize("cls", CLASSES)
def test_full_snapshot_has_every_dimension_and_no_measures_or_dates(cls):
    date_columns = set(sys.modules[cls.__module__]._DATE_COLUMNS)
    measures = {"value", "date", "year", "capacity", "capacityDown", "runRate"}
    exclude = set(getattr(cls, "_REFERENCE_FULL_EXCLUDE", ()))
    for dataset, spec in rd.snapshot_specs(cls)["full"].items():
        columns = spec["all"]
        assert columns and len(columns) == len(set(columns)), dataset
        assert not (date_columns | measures | exclude) & set(columns), dataset
        method = getattr(
            cls,
            getattr(cls, "_REFERENCE_METHODS", {}).get(
                dataset, f"get_{dataset.replace('-', '_')}"
            ),
        )
        assert set(columns) == set(rd.dimension_columns(method)) - exclude, dataset


def test_grouped_snapshots_hold_no_volatile_columns():
    for cls in CLASSES:
        volatile = set(sys.modules[cls.__module__]._DATE_COLUMNS) | {
            "isActive",
            "value",
            "date",
            "year",
            "outageId",
        }
        for dataset, groups in cls._REFERENCE_SNAPSHOTS.items():
            for group, columns in groups.items():
                assert not volatile & set(columns), (dataset, group)


def test_snapshot_specs_derive_the_full_layout_from_method_signatures():
    class Toy:
        _datasets = Literal["alpha-beta", "gamma"]
        _REFERENCE_SNAPSHOTS = {"alpha-beta": {"g": ["topRegion"]}, "gamma": {}}
        _REFERENCE_FULL_EXCLUDE = {"outageId"}
        _REFERENCE_METHODS = {"gamma": "get_other"}

        def get_alpha_beta(
            self,
            *,
            top_region: Optional[Union[list[str], Series[str], str]] = None,
            outage_id: Optional[Union[list[str], Series[str], str]] = None,
            uom_name: Optional[Union[list[str], Series[str], str]] = None,
            value: Optional[float] = None,
            start_date: Optional[datetime] = None,
            page: int = 1,
        ):
            pass

        def get_other(
            self,
            *,
            data_series_short: Optional[Union[list[str], Series[str], str]] = None,
        ):
            pass

    specs = rd.snapshot_specs(Toy)
    assert specs["full"] == {
        "alpha-beta": {"all": ["topRegion", "uomName"]},
        "gamma": {"all": ["dataSeriesShort"]},
    }
    assert specs["grouped"] is Toy._REFERENCE_SNAPSHOTS


# --- the client classes ------------------------------------------------------


def importlib_attr(cls, name):
    return getattr(sys.modules[cls.__module__], name)


def _first_snapshots(cls):
    """A dataset whose full snapshot is wider than its first grouped one."""
    specs = rd.snapshot_specs(cls)
    for dataset, groups in specs["grouped"].items():
        group, columns = next(iter(groups.items()))
        full = specs["full"][dataset]["all"]
        if set(full) - set(columns):
            break
    frame = lambda cols: pd.DataFrame({c: [f"{c}-1", f"{c}-2"] for c in cols})
    return dataset, group, columns, frame(columns), full, frame(full)


@pytest.fixture(params=CLASSES, ids=lambda c: c.__name__)
def client(request, remote, monkeypatch):
    cls = request.param
    dataset, group, columns, df, full, full_df = _first_snapshots(cls)
    publish(
        remote.root,
        {
            f"{dataset}__{group}": (dataset, columns, df, "grouped"),
            f"{dataset}__all": (dataset, full, full_df, "full"),
        },
        namespace=cls._REFERENCE_NAMESPACE,
    )
    api = pd.DataFrame({columns[0]: ["from-api"]})
    with mock.patch(f"{cls.__module__}.get_data", return_value=api) as get_data:
        c = cls()
        c.dataset, c.columns, c.full = dataset, columns, full
        c.get_data = get_data
        yield c


def test_agent_mode_serves_the_snapshot_without_calling_the_api(client, monkeypatch):
    monkeypatch.setattr(spgci.config, "is_agent", True)
    df = client.get_unique_values(client.dataset, client.columns[0])
    assert df.attrs["source"] == "github" and len(df) == 2
    client.get_data.assert_not_called()


def test_not_agent_mode_always_uses_the_api(client, remote, monkeypatch):
    monkeypatch.setattr(spgci.config, "is_agent", False)
    df = client.get_unique_values(client.dataset, client.columns[0])
    assert df[client.columns[0]].tolist() == ["from-api"]
    assert df.attrs["source"] == "api"
    assert remote.calls == []


def test_a_filter_built_by_the_sdk_is_served_from_the_snapshot(client, monkeypatch):
    monkeypatch.setattr(spgci.config, "is_agent", True)
    column = client.columns[0]
    expression = spgci.utilities.build_filter_expression({column: [f"{column}-1"]})
    df = client.get_unique_values(client.dataset, column, filter_exp=expression)
    assert df[column].tolist() == [f"{column}-1"] and df.attrs["source"] == "github"
    client.get_data.assert_not_called()


def test_columns_across_groups_come_from_the_full_snapshot(client, monkeypatch):
    monkeypatch.setattr(spgci.config, "is_agent", True)
    outside = [c for c in client.full if c not in client.columns]
    df = client.get_unique_values(client.dataset, [client.columns[0], outside[0]])
    assert df.attrs["snapshot"] == f"{client.dataset}__all"
    client.get_data.assert_not_called()


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(use_snapshot=False),
        dict(filter_exp='notAColumn: "x"'),
        dict(filter_exp="notAFilterTheSdkWrites > 3"),
    ],
)
def test_everything_else_uses_the_api(client, monkeypatch, kwargs):
    monkeypatch.setattr(spgci.config, "is_agent", True)
    df = client.get_unique_values(client.dataset, client.columns[0], **kwargs)
    assert df.attrs["source"] == "api"
    client.get_data.assert_called_once()


def test_a_value_the_snapshot_lacks_goes_to_the_api(client, monkeypatch):
    monkeypatch.setattr(spgci.config, "is_agent", True)
    column = client.columns[0]
    expression = spgci.utilities.build_filter_expression({column: ["brand new value"]})
    df = client.get_unique_values(client.dataset, column, filter_exp=expression)
    assert df.attrs["source"] == "api"


def test_uncovered_columns_use_the_api(client, monkeypatch):
    monkeypatch.setattr(spgci.config, "is_agent", True)
    df = client.get_unique_values(client.dataset, ["notACovered", client.columns[0]])
    assert df.attrs["source"] == "api"


def test_invalid_dataset_still_raises(client, monkeypatch):
    monkeypatch.setattr(spgci.config, "is_agent", True)
    with pytest.raises(ValueError):
        client.get_unique_values("not-a-dataset", "commodity")


# --- refresh script -> client round trip -------------------------------------


@pytest.fixture
def refresher(tmp_path, monkeypatch):
    path = (
        Path(__file__).resolve().parent.parent
        / "reference"
        / "refresh_unique_values.py"
    )
    spec = importlib.util.spec_from_file_location("refresh_unique_values", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "HERE", tmp_path / "published")
    return mod


def _table(cls, dataset, n=12):
    """What the API holds: one row per distinct combination of every dimension."""
    columns = rd.snapshot_specs(cls)["full"][dataset]["all"]
    df = pd.DataFrame(
        {c: [f"{c}-{i % (2 + j % 4)}" for i in range(n)] for j, c in enumerate(columns)}
    )
    return df.drop_duplicates().reset_index(drop=True)


def _fake_api(
    cls, fail=(), big=(), drop_empty=False, lists=None, fail_wide=False, calls=None
):
    """Stands in for ``get_unique_values(use_snapshot=False)``. With ``drop_empty``
    it behaves like a server that omits rows with an empty value in any grouped column;
    ``lists`` ({dataset: [columns]}) returns those columns as lists; ``fail_wide``
    times out any lookup of more than 8 columns."""
    tables = {d: _table(cls, d) for d in rd.snapshot_specs(cls)["full"]}
    if drop_empty:
        t = tables["capacity"]
        t.loc[0, "commodity"] = "only-with-an-empty-dataType"
        t.loc[0, "dataType"] = None

    def get_unique_values(self, dataset, columns, filter_exp=None, use_snapshot=True):
        assert use_snapshot is False and filter_exp is None
        if calls is not None:
            calls.append((dataset, tuple(columns)))
        if dataset in fail:
            raise RuntimeError("HTTP 400")
        if fail_wide and len(columns) > 8:
            raise rq.exceptions.ReadTimeout("Read timed out. (read timeout=60.0)")
        df = tables[dataset][columns]
        if drop_empty:
            df = df.dropna()
        if dataset in big:
            df = pd.concat([df.assign(**{columns[0]: f"extra-{i}"}) for i in range(30)])
        df = df.drop_duplicates().iloc[::-1]  # unsorted, to exercise sorting
        for c in (lists or {}).get(dataset, []):
            if c in df.columns:
                df = df.assign(**{c: df[c].map(lambda v: [v])})
        return df

    return get_unique_values


def _manifest(refresher, namespace="chemicals"):
    path = refresher.HERE / namespace / "manifest.json"
    return json.loads(path.read_text())["snapshots"]


def test_refresh_publishes_both_layouts_the_client_can_read(
    refresher, remote, monkeypatch
):
    monkeypatch.setattr(Chemicals, "get_unique_values", _fake_api(Chemicals))
    assert refresher.refresh(Chemicals, ["capacity", "trade"], 50_000, 2) == 0
    out = refresher.HERE / "chemicals"
    snapshots = _manifest(refresher)
    specs = rd.snapshot_specs(Chemicals)
    expected = {
        f"{d}__{g}": layout
        for layout in rd.LAYOUTS
        for d in ("capacity", "trade")
        for g in specs[layout][d]
    }
    assert {n: e["layout"] for n, e in snapshots.items()} == expected
    assert all((out / f"{n}.csv.gz").exists() for n in expected)
    assert all(
        e["bytes"] == (out / e["file"]).stat().st_size for e in snapshots.values()
    )

    remote.root = refresher.HERE  # serve what the script just wrote
    store = rd.ReferenceStore(NS)
    table = _table(Chemicals, "capacity")
    cross = store.lookup("capacity", ["region", "concept"])  # spans two groups
    assert cross.attrs["snapshot"] == "capacity__all"
    assert len(cross) == len(table[["region", "concept"]].drop_duplicates())
    single = store.lookup("capacity", "commodity")
    assert single.attrs["snapshot"] != "capacity__all" and len(single) > 0


def test_full_snapshot_matches_the_grouped_ones(refresher, monkeypatch):
    """Whatever a grouped file says, the full file says the same."""
    monkeypatch.setattr(Chemicals, "get_unique_values", _fake_api(Chemicals))
    refresher.refresh(Chemicals, ["capacity"], 50_000, 2)
    out = refresher.HERE / "chemicals"
    snapshots = _manifest(refresher)
    full = pd.read_csv(out / "capacity__all.csv.gz")
    for name, entry in snapshots.items():
        if entry["layout"] == "grouped":
            group = pd.read_csv(out / entry["file"])
            cols = entry["columns"]
            assert refresher.row_set(full, cols) == refresher.row_set(group, cols)


def test_full_snapshot_is_withheld_when_the_api_drops_rows(
    refresher, monkeypatch, capsys
):
    monkeypatch.setattr(Chemicals, "get_unique_values", _fake_api(Chemicals))
    refresher.refresh(Chemicals, ["capacity"], 50_000, 2)
    before = _manifest(refresher)["capacity__all"]

    monkeypatch.setattr(
        Chemicals, "get_unique_values", _fake_api(Chemicals, drop_empty=True)
    )
    failed = refresher.refresh(Chemicals, ["capacity"], 50_000, 2)
    after = _manifest(refresher)
    assert failed == 1
    assert after["capacity__all"] == before  # incomplete data never replaces it
    assert "disagrees with" in capsys.readouterr().out
    assert after["capacity__series"]["generated_at"] > before["generated_at"]


def test_full_snapshots_refreshed_alone_are_flagged_unchecked(
    refresher, monkeypatch, capsys
):
    monkeypatch.setattr(Chemicals, "get_unique_values", _fake_api(Chemicals))
    refresher.refresh(Chemicals, ["capacity"], 50_000, 2, layouts=["full"])
    assert "not cross-checked" in capsys.readouterr().out
    assert set(_manifest(refresher)) == {"capacity__all"}


def test_refresh_skips_failures_and_oversize_but_keeps_the_old_snapshot(
    refresher, monkeypatch
):
    monkeypatch.setattr(Chemicals, "get_unique_values", _fake_api(Chemicals))
    refresher.refresh(Chemicals, ["capacity", "trade"], 50_000, 2)
    before = _manifest(refresher)

    monkeypatch.setattr(
        Chemicals,
        "get_unique_values",
        _fake_api(Chemicals, fail=["capacity"], big=["trade"]),
    )
    failed = refresher.refresh(Chemicals, ["capacity", "trade", "production"], 40, 2)
    after = _manifest(refresher)

    specs = rd.snapshot_specs(Chemicals)
    count = lambda d: sum(len(specs[layout][d]) for layout in rd.LAYOUTS)
    assert failed == count("capacity") + count("trade")
    for name in before:  # failed lookups keep their previous entry untouched
        assert after[name] == before[name]
    assert sum(n.startswith("production__") for n in after) == count("production")


def test_refresh_never_publishes_a_file_over_the_size_cap(refresher, monkeypatch):
    monkeypatch.setattr(Chemicals, "get_unique_values", _fake_api(Chemicals))
    failed = refresher.refresh(Chemicals, ["trade"], 50_000, 2, max_mb=0.00001)
    assert failed > 0 and _manifest(refresher) == {}
    assert not list((refresher.HERE / "chemicals").glob("*.csv.gz"))


def test_refresh_removes_snapshots_no_longer_declared(refresher, monkeypatch):
    monkeypatch.setattr(Chemicals, "get_unique_values", _fake_api(Chemicals))
    refresher.refresh(Chemicals, ["capacity"], 50_000, 2)
    out = refresher.HERE / "chemicals"
    (out / "gone__old.csv.gz").write_bytes(b"x")
    (out / "capacity__series.csv").write_text("a\n1\n")  # an older, uncompressed format
    refresher.refresh(Chemicals, ["capacity"], 50_000, 2)
    assert not (out / "gone__old.csv.gz").exists()
    assert not (out / "capacity__series.csv").exists()
    assert "gone__old" not in _manifest(refresher)


def test_refresh_is_byte_stable_when_data_is_unchanged(refresher, monkeypatch):
    monkeypatch.setattr(Chemicals, "get_unique_values", _fake_api(Chemicals))
    refresher.refresh(Chemicals, ["capacity"], 50_000, 2)
    first = {n: e["sha256"] for n, e in _manifest(refresher).items()}
    refresher.refresh(Chemicals, ["capacity"], 50_000, 2)
    second = {n: e["sha256"] for n, e in _manifest(refresher).items()}
    assert first == second and len(first) > 1


def test_refresh_other_class_uses_its_own_specs(refresher, monkeypatch):
    monkeypatch.setattr(
        OilNGLAnalytics, "get_unique_values", _fake_api(OilNGLAnalytics)
    )
    assert refresher.refresh(OilNGLAnalytics, ["demand"], 50_000, 2) == 0
    assert {
        e["layout"] for e in _manifest(refresher, "oil_ngl_analytics").values()
    } == {
        "grouped",
        "full",
    }


def test_list_valued_columns_are_left_out_and_reported(
    refresher, remote, monkeypatch, capsys
):
    api = _fake_api(Chemicals, lists={"capacity": ["dataType"]})
    monkeypatch.setattr(Chemicals, "get_unique_values", api)
    assert refresher.refresh(Chemicals, ["capacity"], 50_000, 2) == 0
    snapshots = _manifest(refresher)
    assert snapshots["capacity__all"]["columns"] and all(
        "dataType" not in e["columns"] for e in snapshots.values()
    )
    out = capsys.readouterr().out
    assert "left out list-valued ['dataType']" in out
    assert "not cross-checked" not in out  # the remaining columns were still compared

    remote.root = refresher.HERE
    store = rd.ReferenceStore(NS)
    assert store.lookup("capacity", ["commodity", "dataType"]) is None  # the API's job
    assert store.lookup("capacity", "commodity") is not None


def test_progress_is_printed_as_each_lookup_finishes(refresher, monkeypatch, capsys):
    monkeypatch.setattr(Chemicals, "get_unique_values", _fake_api(Chemicals))
    refresher.refresh(Chemicals, ["trade"], 50_000, 2)
    out = capsys.readouterr().out
    assert "fetching 4 lookups" in out
    assert "fetched trade__series:" in out and "published trade__series:" in out
    assert "published trade__all:" in out


def test_a_timed_out_full_lookup_costs_nothing_that_finished(
    refresher, monkeypatch, capsys
):
    monkeypatch.setattr(
        Chemicals, "get_unique_values", _fake_api(Chemicals, fail_wide=True)
    )
    assert refresher.refresh(Chemicals, ["capacity"], 50_000, 2) == 1
    snapshots = _manifest(refresher)
    assert set(snapshots) == {
        f"capacity__{g}" for g in Chemicals._REFERENCE_SNAPSHOTS["capacity"]
    }
    assert "FAILED capacity__all" in capsys.readouterr().out


def test_missing_fetches_only_what_is_not_published_and_still_cross_checks(
    refresher, monkeypatch, capsys
):
    monkeypatch.setattr(
        Chemicals, "get_unique_values", _fake_api(Chemicals, fail_wide=True)
    )
    refresher.refresh(Chemicals, ["capacity"], 50_000, 2)
    before = _manifest(refresher)
    capsys.readouterr()

    calls = []
    monkeypatch.setattr(
        Chemicals, "get_unique_values", _fake_api(Chemicals, calls=calls)
    )
    assert refresher.refresh(Chemicals, ["capacity"], 50_000, 2, missing=True) == 0
    full_columns = rd.snapshot_specs(Chemicals)["full"]["capacity"]["all"]
    assert calls == [("capacity", tuple(full_columns))]  # nothing else was asked
    out = capsys.readouterr().out
    assert "3 already published, 1 to fetch" in out
    assert "not cross-checked" not in out  # compared with the published grouped files
    after = _manifest(refresher)
    assert {n: e for n, e in after.items() if n in before} == before
    assert "capacity__all" in after


def test_a_full_only_refresh_is_checked_against_the_published_grouped_files(
    refresher, monkeypatch, capsys
):
    api = _fake_api(Chemicals, drop_empty=True)
    monkeypatch.setattr(Chemicals, "get_unique_values", api)
    refresher.refresh(Chemicals, ["capacity"], 50_000, 2, layouts=["grouped"])
    capsys.readouterr()
    assert refresher.refresh(Chemicals, ["capacity"], 50_000, 2, layouts=["full"]) == 1
    assert "WITHHELD capacity__all: disagrees with" in capsys.readouterr().out
    assert "capacity__all" not in _manifest(refresher)


def test_a_cross_check_that_cannot_run_withholds_the_full_snapshot_only(
    refresher, monkeypatch, capsys
):
    monkeypatch.setattr(Chemicals, "get_unique_values", _fake_api(Chemicals))
    monkeypatch.setattr(
        refresher, "cross_check", mock.Mock(side_effect=RuntimeError("boom"))
    )
    assert refresher.refresh(Chemicals, ["capacity"], 50_000, 2) == 1
    assert (
        "WITHHELD capacity__all: cross-check or write failed: RuntimeError: boom"
        in (capsys.readouterr().out)
    )
    assert len(_manifest(refresher)) == 3  # the grouped ones are safe


def test_main_applies_the_timeout_and_page_workers(refresher, monkeypatch):
    for name, value in (("timeout", 60), ("parallelism", 8), ("is_agent", False)):
        monkeypatch.setattr(spgci.config, name, value, raising=False)
    monkeypatch.setattr(spgci.config, "username", "u")
    monkeypatch.setattr(spgci.config, "password", "p")
    seen = []

    def fake_refresh(cls, *args):
        seen.append((cls, spgci.config.timeout, spgci.config.parallelism, args[-1]))
        return 0

    monkeypatch.setattr(refresher, "refresh", fake_refresh)
    argv = ["x", "--only", "chemicals", "--timeout", "123", "--page-workers", "3"]
    monkeypatch.setattr(sys, "argv", argv + ["--missing"])
    with pytest.raises(SystemExit) as exit_:
        refresher.main()
    assert exit_.value.code == 0
    assert seen == [(Chemicals, 123, 3, True)]


def test_an_interrupted_refresh_keeps_every_dataset_that_finished(
    refresher, monkeypatch, capsys
):
    base = _fake_api(Chemicals)

    def api(self, dataset, columns, filter_exp=None, use_snapshot=True):
        if dataset == "trade":
            raise KeyboardInterrupt  # the user gives up part way through
        return base(self, dataset, columns, filter_exp, use_snapshot)

    monkeypatch.setattr(Chemicals, "get_unique_values", api)
    with pytest.raises(KeyboardInterrupt):
        refresher.refresh(Chemicals, ["capacity", "trade"], 50_000, 1)
    snapshots = _manifest(refresher)
    assert "capacity__all" in snapshots  # published as soon as capacity was complete
    assert {n for n in snapshots if n.startswith("capacity__")} == {
        f"capacity__{g}"
        for layout in rd.LAYOUTS
        for g in rd.snapshot_specs(Chemicals)[layout]["capacity"]
    }
    assert not any(n.startswith("trade__") for n in snapshots)


def test_lookups_are_ordered_dataset_by_dataset(refresher):
    names = [j[1] for j in refresher.jobs(Chemicals, ["capacity", "trade"])]
    assert [n.split("__")[0] for n in names] == ["capacity"] * 4 + ["trade"] * 4
    assert names[3].endswith("__all")  # each dataset's wide lookup comes last


@pytest.mark.parametrize("cls", CLASSES, ids=lambda c: c.__name__)
def test_every_class_round_trips_through_refresh_and_client(
    cls, refresher, remote, monkeypatch
):
    """Refresh one dataset of the class, then read it back the way an agent would."""
    dataset = next(iter(cls._REFERENCE_SNAPSHOTS))
    monkeypatch.setattr(cls, "get_unique_values", _fake_api(cls))
    assert refresher.refresh(cls, [dataset], 50_000, 2) == 0
    remote.root = refresher.HERE
    monkeypatch.setattr(spgci.config, "is_agent", True)
    table = _table(cls, dataset)
    full = rd.snapshot_specs(cls)["full"][dataset]["all"]
    store = rd.ReferenceStore(cls._REFERENCE_NAMESPACE)
    got = store.lookup(dataset, full)
    assert got.attrs["snapshot"] == f"{dataset}__all" and len(got) == len(table)
    one = store.lookup(dataset, full[0])
    assert sorted(one[full[0]]) == sorted(table[full[0]].unique())
