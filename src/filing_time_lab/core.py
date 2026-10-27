"""Atomic, content-addressed ingestion and conservative as-of selection.

A fact's economic period, filing date and local observation time are distinct.
No claim is made that a current SEC archive is a historical live capture.
"""

import datetime as dt
import hashlib
import json
import os
import tempfile
from decimal import Decimal, InvalidOperation
from pathlib import Path

import duckdb

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id VARCHAR PRIMARY KEY, source_url VARCHAR NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL, raw_path VARCHAR NOT NULL,
    raw_sha256 VARCHAR NOT NULL, byte_count BIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS facts (
    fact_id VARCHAR PRIMARY KEY, cik VARCHAR NOT NULL,
    taxonomy VARCHAR NOT NULL, concept VARCHAR NOT NULL, unit VARCHAR NOT NULL,
    period_start DATE, period_end DATE NOT NULL, filed DATE NOT NULL,
    accession VARCHAR NOT NULL, form VARCHAR NOT NULL, value_decimal VARCHAR NOT NULL);
CREATE TABLE IF NOT EXISTS lineage (
    fact_id VARCHAR NOT NULL, snapshot_id VARCHAR NOT NULL,
    PRIMARY KEY (fact_id, snapshot_id));
CREATE TABLE IF NOT EXISTS quarantine (
    snapshot_id VARCHAR NOT NULL, row_path VARCHAR NOT NULL,
    reason VARCHAR NOT NULL, payload VARCHAR NOT NULL,
    PRIMARY KEY (snapshot_id, row_path));
"""


class AmbiguousFact(ValueError):
    """Two values share the latest filing day; do not invent an ordering."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def utc_timestamp(text):
    parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("captured_at must include a timezone")
    return parsed.astimezone(dt.timezone.utc).isoformat()


def connect(path):
    con = duckdb.connect(str(path))
    con.execute(SCHEMA)
    return con


def normalize(cik, taxonomy, concept, unit, row):
    if not isinstance(row, dict):
        raise ValueError("fact row must be an object")
    start = dt.date.fromisoformat(row["start"]) if row.get("start") else None
    end = dt.date.fromisoformat(row["end"])
    filed = dt.date.fromisoformat(row["filed"])
    if start and start > end:
        raise ValueError("period starts after end")
    if not row.get("accn") or not row.get("form"):
        raise ValueError("missing accession or form")
    if isinstance(row["val"], bool):
        raise ValueError("boolean is not a financial value")
    value = Decimal(str(row["val"]))
    if not value.is_finite():
        raise ValueError("non-finite value")
    # format(..., 'f') keeps exact decimal digits, never binary floats in storage.
    value_text = format(value, "f")
    if "." in value_text:
        value_text = value_text.rstrip("0").rstrip(".")
    if value == 0:
        value_text = "0"
    record = [
        str(cik).zfill(10),
        taxonomy,
        concept,
        unit,
        str(start) if start else None,
        str(end),
        str(filed),
        row["accn"],
        row["form"],
        value_text,
    ]
    return [digest(canonical(record).encode()), *record]


def ingest(con, raw_path, source_url, captured_at, archive_dir, fail_after=None):
    """Copy immutable bytes, then load facts/lineage/quarantine in one transaction.

    A simulated/real failure leaves at most an unreferenced raw object, safe to
    replay. Snapshot identity includes source + capture time + content digest.
    """
    captured_at = utc_timestamp(captured_at)
    raw = Path(raw_path).read_bytes()
    data = json.loads(raw, parse_float=Decimal)
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("facts"), dict)
        or "cik" not in data
    ):
        raise ValueError("not a companyfacts document")
    # Structural drift fails the whole document before any bytes are archived.
    if not str(data["cik"]).isdigit() or not 1 <= len(str(data["cik"])) <= 10:
        raise ValueError("invalid CIK")
    for taxonomy, concepts in data["facts"].items():
        if not isinstance(concepts, dict):
            raise ValueError(f"invalid taxonomy structure: {taxonomy}")
        for concept, detail in concepts.items():
            if not isinstance(detail, dict) or not isinstance(
                detail.get("units"), dict
            ):
                raise ValueError(f"invalid concept units: {taxonomy}/{concept}")
            if any(not isinstance(rows, list) for rows in detail["units"].values()):
                raise ValueError(f"invalid unit rows: {taxonomy}/{concept}")
    sha = digest(raw)
    archive = Path(archive_dir)
    archive.mkdir(parents=True, exist_ok=True)
    target = archive / (sha + ".json")
    if target.exists() and digest(target.read_bytes()) != sha:
        raise ValueError("raw archive hash mismatch")
    if not target.exists():
        # Publish a fully flushed object atomically. Hard-link creation will not
        # replace another writer's object. A crash can leave only an orphan temp.
        fd, staging = tempfile.mkstemp(prefix=".raw-", dir=archive)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(staging, target)
            except FileExistsError:
                if digest(target.read_bytes()) != sha:
                    raise ValueError("concurrent archive hash mismatch")
        finally:
            Path(staging).unlink(missing_ok=True)
    sid = digest(canonical([source_url, captured_at, sha]).encode())
    counts = {"snapshot_id": sid, "rows_seen": 0, "quarantined": 0}
    records = []
    rejected = []
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            "INSERT INTO snapshots VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
            [sid, source_url, captured_at, str(target), sha, len(raw)],
        )
        for taxonomy, concepts in data["facts"].items():
            for concept, detail in concepts.items():
                for unit, rows in detail.get("units", {}).items():
                    for index, row in enumerate(rows):
                        row_path = canonical([taxonomy, concept, unit, index])
                        try:
                            record = normalize(
                                data["cik"], taxonomy, concept, unit, row
                            )
                        except (
                            KeyError,
                            ValueError,
                            TypeError,
                            InvalidOperation,
                        ) as exc:
                            rejected.append([sid, row_path, str(exc), canonical(row)])
                            counts["quarantined"] += 1
                        else:
                            records.append(record)
                        counts["rows_seen"] += 1
                        if fail_after is not None and counts["rows_seen"] >= fail_after:
                            raise RuntimeError(
                                "injected failure before transaction commit"
                            )
        # One vectorized insert per table, not a network/statement round-trip per fact.
        columns = ", ".join(f"json_extract_string(value, '$[{i}]')" for i in range(11))
        con.execute(
            f"INSERT INTO facts SELECT {columns} FROM json_each(?) ON CONFLICT DO NOTHING",
            [canonical(records)],
        )
        con.execute(
            "INSERT INTO lineage SELECT json_extract_string(value, '$[0]'), ? FROM json_each(?) ON CONFLICT DO NOTHING",
            [sid, canonical(records)],
        )
        columns = ", ".join(f"json_extract_string(value, '$[{i}]')" for i in range(4))
        con.execute(
            f"INSERT INTO quarantine SELECT {columns} FROM json_each(?) ON CONFLICT DO NOTHING",
            [canonical(rejected)],
        )
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return counts


def as_of(con, cutoff, known_at=None, concept=None, include_conflicts=False):
    """Return all exact-period keys, latest filed date <= cutoff, or reject ties.

    known_at additionally limits local snapshot capture time. With no known_at,
    this is retrospective filing-date reconstruction, not a live-capture claim.
    Multiple accessions with the same value on the same day remain in lineage.
    """
    cutoff = dt.date.fromisoformat(cutoff)
    params = [cutoff]
    predicate = ""
    if known_at:
        predicate = " AND s.captured_at <= ?"
        params.append(utc_timestamp(known_at))
    if concept:
        predicate += " AND f.concept = ?"
        params.append(concept)
    rows = con.execute(
        """
        SELECT DISTINCT f.* FROM facts f
        JOIN lineage l USING (fact_id) JOIN snapshots s USING (snapshot_id)
        WHERE f.filed <= ?"""
        + predicate
        + " ORDER BY f.fact_id",
        params,
    ).fetchall()
    selected = {}
    for row in rows:
        key = row[1:7]  # CIK, taxonomy, concept, unit, exact start/end
        if key not in selected or row[7] > selected[key][0][7]:
            selected[key] = [row]
        elif row[7] == selected[key][0][7]:
            selected[key].append(row)
    out = []
    for key, versions in selected.items():
        if len({r[10] for r in versions}) > 1:
            if include_conflicts:
                out.append(
                    {
                        "key": [str(x) if x is not None else None for x in key],
                        "filed": str(versions[0][7]),
                        "status": "conflict",
                        "candidates": [
                            {"value": r[10], "accession": r[8], "fact_id": r[0]}
                            for r in versions
                        ],
                    }
                )
                continue
            raise AmbiguousFact(f"conflicting values on latest filing day for {key}")
        out.append(
            {
                "key": [str(x) if x is not None else None for x in key],
                "filed": str(versions[0][7]),
                "value": versions[0][10],
                "fact_ids": sorted(r[0] for r in versions),
                "accessions": sorted({r[8] for r in versions}),
            }
        )
    return sorted(out, key=canonical)


def state_checksum(con):
    """Canonical semantic state, excluding file paths and delivery order."""
    rows = con.execute("SELECT * FROM facts ORDER BY fact_id").fetchall()
    return digest(canonical(rows).encode())


def verify_archive(con):
    problems = []
    for sid, path, expected in con.execute(
        "SELECT snapshot_id, raw_path, raw_sha256 FROM snapshots"
    ).fetchall():
        if not Path(path).exists() or digest(Path(path).read_bytes()) != expected:
            problems.append(sid)
    return problems
