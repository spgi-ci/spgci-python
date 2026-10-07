"""Refresh the Power Evaluator reference snapshots in this folder.

Runs every lookup in ``spgci.power_evaluator._REFERENCE_LOOKUPS`` live (sync),
rewrites the CSVs, and writes ``manifest.json``. ``PowerEvaluator.get_reference_data``
ignores a manifest older than 14 days, so run this at least weekly.

    cd <repo root>
    python3 reference/pe/refresh_reference_data.py                 # refresh all
    python3 reference/pe/refresh_reference_data.py --manifest-only # rebuild manifest

Requires SPGCI_USERNAME / SPGCI_PASSWORD in the environment (except with
--manifest-only). Never commit credentials.
"""

import hashlib
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.getcwd())

import pandas as pd  # noqa: E402
import spgci as ci  # noqa: E402
from spgci.power_evaluator import _REFERENCE_LOOKUPS  # noqa: E402

HERE = Path(__file__).resolve().parent
WORKERS = 4


def fetch(item):
    name, kwargs = item
    df = ci.PowerEvaluator().submit(run_async=False, **kwargs)
    if not isinstance(df, pd.DataFrame):
        raise RuntimeError(f"{name}: lookup did not return rows: {df!r}")
    (HERE / f"{name}.csv").write_text(df.to_csv(index=False), encoding="utf-8")
    return name


def write_manifest(generated_at: datetime) -> dict:
    datasets = {}
    for name in _REFERENCE_LOOKUPS:
        path = HERE / f"{name}.csv"
        df = pd.read_csv(path)
        datasets[name] = {
            "rows": len(df),
            "columns": list(df.columns),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    manifest = {"generated_at": generated_at.isoformat(), "datasets": datasets}
    (HERE / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    return manifest


def main():
    if "--manifest-only" in sys.argv:
        newest = max((HERE / f"{n}.csv").stat().st_mtime for n in _REFERENCE_LOOKUPS)
        generated_at = datetime.fromtimestamp(newest, tz=timezone.utc)
    else:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for name in pool.map(fetch, _REFERENCE_LOOKUPS.items()):
                print("refreshed", name)
        generated_at = datetime.now(timezone.utc)

    manifest = write_manifest(generated_at)
    print(f"manifest generated_at={manifest['generated_at']}")
    for name, info in manifest["datasets"].items():
        print(f"  {name}: {info['rows']} rows")


if __name__ == "__main__":
    main()
