"""Deterministic offline experiment. Invoke from the repository root."""

import json
import platform
import random
import tempfile
import time
from pathlib import Path

import duckdb

from filing_time_lab.core import as_of, connect, ingest, state_checksum

SOURCE = "https://data.sec.gov/api/xbrl/companyfacts/CIK0001637459.json"
CAPTURE = "2026-10-27T05:59:32Z"


def run():
    source = Path("data/khc-selected-facts.json")
    data = json.loads(source.read_text())
    result = {
        "fixture": str(source),
        "python": platform.python_version(),
        "duckdb": duckdb.__version__,
        "replay_trials": 50,
    }
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        con = connect(root / "baseline.duckdb")
        t = time.perf_counter()
        result["ingest"] = ingest(con, source, SOURCE, CAPTURE, root / "archive")
        result["ingest_seconds"] = round(time.perf_counter() - t, 6)
        result["fact_checksum"] = state_checksum(con)
        key = [
            "0001637459",
            "us-gaap",
            "NetIncomeLoss",
            "USD",
            "2016-01-04",
            "2016-12-31",
        ]
        result["case_study"] = {}
        for date in ["2018-01-01", "2020-01-01"]:
            result["case_study"][date] = next(
                r for r in as_of(con, date, concept="NetIncomeLoss") if r["key"] == key
            )
        con.close()
        matched = 0
        for seed in range(50):
            shuffled = json.loads(json.dumps(data))
            rng = random.Random(seed)
            for concepts in shuffled["facts"].values():
                for detail in concepts.values():
                    for rows in detail["units"].values():
                        rows.extend(rows[:5])
                        rng.shuffle(rows)
            path = root / f"delivery-{seed}.json"
            path.write_text(json.dumps(shuffled))
            c = connect(":memory:")
            try:
                ingest(c, path, SOURCE, CAPTURE, root / "archive")
                matched += state_checksum(c) == result["fact_checksum"]
            finally:
                c.close()
        result["matching_replay_checksums"] = matched
    return result


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
