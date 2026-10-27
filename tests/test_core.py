import copy
import json
import random
import tempfile
import unittest
from pathlib import Path

from filing_time_lab.core import (
    AmbiguousFact,
    as_of,
    connect,
    ingest,
    normalize,
    state_checksum,
    verify_archive,
)

ROW = {
    "start": "2020-01-01",
    "end": "2020-12-31",
    "filed": "2021-02-01",
    "accn": "a",
    "form": "10-K",
    "val": 100,
}


def doc(rows):
    return {"cik": 1, "facts": {"us-gaap": {"Profit": {"units": {"USD": rows}}}}}


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.con = connect(":memory:")
        self.addCleanup(self.con.close)
        self.addCleanup(self.tmp.cleanup)

    def load(self, data, name="in.json", captured="2026-10-27T06:00:00Z", **kw):
        path = self.root / name
        path.write_text(json.dumps(data))
        return ingest(
            self.con,
            path,
            "https://example.invalid/fixture",
            captured,
            self.root / "archive",
            **kw,
        )

    def count(self, table):
        return self.con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]

    def test_duplicate_replay(self):
        self.load(doc([ROW, ROW]))
        checksum = state_checksum(self.con)
        self.load(doc([ROW, ROW]))
        self.assertEqual(self.count("facts"), 1)
        self.assertEqual(self.count("snapshots"), 1)
        self.assertEqual(self.count("lineage"), 1)
        self.assertEqual(state_checksum(self.con), checksum)

    def test_as_of_and_local_capture_are_distinct(self):
        revision = dict(ROW, val=90, filed="2022-02-01", accn="b")
        self.load(doc([ROW, revision]))
        self.assertEqual(as_of(self.con, "2021-12-31")[0]["value"], "100")
        self.assertEqual(as_of(self.con, "2022-12-31")[0]["value"], "90")
        self.assertEqual(as_of(self.con, "2022-12-31", "2025-01-01T00:00:00Z"), [])
        self.assertEqual(len(as_of(self.con, "2022-12-31", "2027-01-01T00:00:00Z")), 1)

    def test_late_delivery_and_order_independent_rebuild(self):
        original = copy.deepcopy(doc([ROW]))
        revised = doc([dict(ROW, val=95, filed="2022-01-01", accn="b")])
        self.load(revised, "new.json")
        self.load(original, "old.json")
        checksum = state_checksum(self.con)
        expected = as_of(self.con, "2023-01-01")
        other = connect(":memory:")
        try:
            for filename in ["old.json", "new.json"]:
                ingest(
                    other,
                    self.root / filename,
                    "https://example.invalid/fixture",
                    "2026-10-27T06:00:00Z",
                    self.root / "archive",
                )
            self.assertEqual(checksum, state_checksum(other))
            self.assertEqual(expected, as_of(other, "2023-01-01"))
        finally:
            other.close()

    def test_crash_rolls_back_every_table_then_replays(self):
        with self.assertRaises(RuntimeError):
            self.load(doc([ROW, dict(ROW, accn="b")]), fail_after=1)
        for table in ["facts", "lineage", "quarantine", "snapshots"]:
            self.assertEqual(self.count(table), 0)
        self.load(doc([ROW]))
        self.assertEqual(self.count("facts"), 1)
        self.assertEqual(verify_archive(self.con), [])

    def test_malformed_rows_quarantined_without_hiding_valid_row(self):
        self.load(doc([ROW, dict(ROW, filed="nonsense"), dict(ROW, val=True)]))
        self.assertEqual(self.count("facts"), 1)
        self.assertEqual(self.count("quarantine"), 2)

    def test_same_day_conflict_is_not_arbitrarily_ranked(self):
        self.load(doc([ROW, dict(ROW, accn="b", val=99)]))
        with self.assertRaises(AmbiguousFact):
            as_of(self.con, "2022-01-01")

    def test_equal_same_day_versions_retain_both_accessions(self):
        self.load(doc([ROW, dict(ROW, accn="b")]))
        self.assertEqual(as_of(self.con, "2022-01-01")[0]["accessions"], ["a", "b"])

    def test_exact_period_and_unit_not_collapsed(self):
        data = doc([ROW, dict(ROW, start="2020-07-01")])
        data["facts"]["us-gaap"]["Profit"]["units"]["EUR"] = [ROW]
        self.load(data)
        self.assertEqual(len(as_of(self.con, "2022-01-01")), 3)

    def test_decimal_preserved_beyond_default_context_precision(self):
        text = "123456789012345678901234567890.123456789"
        record = normalize(1, "us-gaap", "Profit", "USD", dict(ROW, val=text))
        self.assertEqual(record[-1], text)

    def test_zero_and_trailing_zero_identity(self):
        self.assertEqual(
            normalize(1, "a", "b", "c", dict(ROW, val="1.00")),
            normalize(1, "a", "b", "c", dict(ROW, val=1)),
        )
        self.assertEqual(normalize(1, "a", "b", "c", dict(ROW, val="-0.00"))[-1], "0")

    def test_timezone_required(self):
        with self.assertRaises(ValueError):
            self.load(doc([ROW]), captured="2026-10-27T06:00:00")
        self.assertEqual(self.count("snapshots"), 0)

    def test_archive_tampering_detected_and_reingest_rejected(self):
        self.load(doc([ROW]))
        next((self.root / "archive").iterdir()).write_text("tampered")
        self.assertEqual(len(verify_archive(self.con)), 1)
        with self.assertRaises(ValueError):
            self.load(doc([ROW]))

    def test_randomized_delivery_50_seeded_orders(self):
        rows = [
            dict(ROW, filed=f"202{i}-02-01", accn=str(i), val=100 - i)
            for i in range(1, 5)
        ]
        identities = []
        for seed in range(50):
            shuffled = rows * 2
            random.Random(seed).shuffle(shuffled)
            self.load(doc(shuffled), f"shuffle-{seed}.json")
            identities.append(state_checksum(self.con))
        self.assertEqual(len(set(identities)), 1)
        self.assertEqual(self.count("facts"), 4)
        self.assertEqual(as_of(self.con, "2025-01-01")[0]["value"], "96")

    def test_schema_drift_is_fatal_without_partial_commit(self):
        for data in [
            [],
            {"cik": 1, "facts": {"a": []}},
            {"cik": 1, "facts": {"a": {"b": {}}}},
            {"cik": "bad", "facts": {}},
        ]:
            with self.assertRaises(ValueError):
                self.load(data)
            self.assertEqual(self.count("snapshots"), 0)

    def test_non_object_row_is_quarantined(self):
        self.load(doc([ROW, None, [], "bad"]))
        self.assertEqual(self.count("quarantine"), 3)
        self.assertEqual(self.count("facts"), 1)

    def test_concept_filter_avoids_unrelated_ambiguity(self):
        data = doc([ROW])
        data["facts"]["us-gaap"]["Other"] = {"units": {"USD": [ROW, dict(ROW, val=99)]}}
        self.load(data)
        with self.assertRaises(AmbiguousFact):
            as_of(self.con, "2022-01-01")
        self.assertEqual(len(as_of(self.con, "2022-01-01", concept="Profit")), 1)

    def test_missing_lineage_not_in_query(self):
        self.load(doc([ROW]))
        self.con.execute("DELETE FROM lineage")
        self.assertEqual(as_of(self.con, "2022-01-01"), [])

    def test_nonfinite_value_quarantined(self):
        self.load(doc([dict(ROW, val="NaN"), dict(ROW, val="Infinity")]))
        self.assertEqual(self.count("facts"), 0)
        self.assertEqual(self.count("quarantine"), 2)

    def test_conflicts_report_keeps_all_candidates(self):
        self.load(doc([ROW, dict(ROW, val=99, accn="b")]))
        result = as_of(self.con, "2022-01-01", include_conflicts=True)
        self.assertEqual(result[0]["status"], "conflict")
        self.assertEqual({r["value"] for r in result[0]["candidates"]}, {"99", "100"})

    def test_atomic_raw_copy_has_no_staging_file(self):
        self.load(doc([ROW]))
        self.assertEqual(len(list((self.root / "archive").iterdir())), 1)
        self.assertTrue(next((self.root / "archive").iterdir()).name.endswith(".json"))

    def test_persistent_database_reopen(self):
        path = self.root / "persist.duckdb"
        con = connect(path)
        try:
            incoming = self.root / "in.json"
            incoming.write_text(json.dumps(doc([ROW])))
            ingest(
                con, incoming, "fixture", "2026-01-01T00:00:00Z", self.root / "archive"
            )
            before = state_checksum(con)
        finally:
            con.close()
        con = connect(path)
        try:
            self.assertEqual(state_checksum(con), before)
            self.assertEqual(as_of(con, "2022-01-01")[0]["value"], "100")
        finally:
            con.close()

    def test_real_fixture_matches_independent_filing_table(self):
        # SEC filing table R35, 2016 Net income attributable to Kraft Heinz:
        # previously 3,632m, restated 3,596m. Not total consolidated income.
        # https://www.sec.gov/Archives/edgar/data/1637459/000163745919000049/R35.htm
        fixture = Path(__file__).parents[1] / "data" / "khc-selected-facts.json"
        ingest(
            self.con,
            fixture,
            "SEC selected fixture",
            "2026-10-27T05:59:32Z",
            self.root / "archive",
        )
        key = [
            "0001637459",
            "us-gaap",
            "NetIncomeLoss",
            "USD",
            "2016-01-04",
            "2016-12-31",
        ]
        for cutoff, value in [
            ("2018-01-01", "3632000000"),
            ("2020-01-01", "3596000000"),
        ]:
            row = next(
                r
                for r in as_of(self.con, cutoff, concept="NetIncomeLoss")
                if r["key"] == key
            )
            self.assertEqual(row["value"], value)

    def test_fixture_hash_matches_manifest(self):
        import hashlib

        root = Path(__file__).parents[1]
        manifest = json.loads((root / "data/provenance.json").read_text())
        actual = hashlib.sha256(
            (root / "data/khc-selected-facts.json").read_bytes()
        ).hexdigest()
        self.assertEqual(actual, manifest["selected_sha256"])
