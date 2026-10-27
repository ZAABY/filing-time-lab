import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from filing_time_lab.cli import main


class CliTests(unittest.TestCase):
    def invoke(self, argv):
        output = io.StringIO()
        with (
            patch.object(sys, "argv", ["filing-time", *argv]),
            contextlib.redirect_stdout(output),
        ):
            main()
        return json.loads(output.getvalue())

    def test_full_cli_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "demo.duckdb")
            source = str(Path(__file__).parents[1] / "data/khc-selected-facts.json")
            loaded = self.invoke(
                [
                    "--db",
                    db,
                    "ingest",
                    source,
                    "--source-url",
                    "fixture",
                    "--captured-at",
                    "2026-10-27T05:59:32Z",
                    "--archive",
                    tmp + "/raw",
                ]
            )
            self.assertEqual(loaded["rows_seen"], 299)
            audit = self.invoke(["--db", db, "audit"])
            self.assertEqual(audit["facts"], 299)
            self.assertEqual(audit["archive_errors"], [])
            rows = self.invoke(
                ["--db", db, "as-of", "2018-01-01", "--concept", "NetIncomeLoss"]
            )
            self.assertTrue(all(r["key"][2] == "NetIncomeLoss" for r in rows))
            empty = self.invoke(
                [
                    "--db",
                    db,
                    "as-of",
                    "2018-01-01",
                    "--known-at",
                    "2020-01-01T00:00:00Z",
                ]
            )
            self.assertEqual(empty, [])


class DownloaderTests(unittest.TestCase):
    def setUp(self):
        script = Path(__file__).parents[1] / "scripts/download.py"
        spec = importlib.util.spec_from_file_location("download", script)
        self.mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.mod)

    def test_rejects_bad_identity_before_request(self):
        with (
            patch.object(sys, "argv", ["download", "../bad", "--contact", "no-email"]),
            patch.object(self.mod.urllib.request, "urlopen") as request,
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit),
        ):
            self.mod.main()
        request.assert_not_called()

    def test_one_request_records_hash_capture_and_bytes(self):
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def read(self, limit):
                return b'{"cik":1637459,"facts":{}}'

        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(
                sys,
                "argv",
                [
                    "download",
                    "1637459",
                    "--contact",
                    "test@example.invalid",
                    "--out",
                    tmp,
                ],
            ),
            patch.object(
                self.mod.urllib.request, "urlopen", return_value=Response()
            ) as request,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            self.mod.main()
            request.assert_called_once()
            self.assertEqual(len(list(Path(tmp).glob("*.manifest.json"))), 1)
            manifests = list(Path(tmp).glob("*.manifest.json"))
            manifest = json.loads(manifests[0].read_text())
            self.assertEqual(
                Path(manifest["path"]).read_bytes(), b'{"cik":1637459,"facts":{}}'
            )
            self.assertTrue(manifest["captured_at"].endswith("+00:00"))
