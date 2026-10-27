# Filing Time Lab

**Historical financial data should keep its history.** This ETL lab preserves SEC fact versions, separates filing dates from local capture time, and tests whether repeated, shuffled and interrupted deliveries change the result.

It is a local data-engineering experiment, not a trading system or a claim to reconstruct everything investors knew at a historical instant.

## A real example

Kraft Heinz's 2016 income attributable to Kraft Heinz was reported as **$3,632 million**, then restated to **$3,596 million**. The original [SEC restatement table](https://www.sec.gov/Archives/edgar/data/1637459/000163745919000049/R35.htm) independently confirms the change. The selected SEC companyfacts fixture preserves the original filing on 2017-02-23 and later filing on 2019-06-07.

A latest-value-only dataset would replace the original number even in a query about 2018. This project returns the earlier version for a filing-date cutoff of 2018-01-01 and the later version for 2020-01-01. Those are **retrospective, day-level filing-date queries**, not proof that this machine observed either filing in 2018.

## Run offline

Python 3.10+; no API key, cloud account or paid service required.

```bash
uv venv
source .venv/bin/activate
uv pip install -e .
python -m unittest discover -s tests -v
filing-time --db filings.duckdb ingest data/khc-selected-facts.json \
  --source-url https://data.sec.gov/api/xbrl/companyfacts/CIK0001637459.json \
  --captured-at 2026-10-27T05:59:32Z
filing-time --db filings.duckdb audit
filing-time --db filings.duckdb as-of 2018-01-01 --concept NetIncomeLoss
python scripts/benchmark.py
```

On Windows activate `.venv\Scripts\activate` instead. `pip install -e .` also works in an existing environment. The default commands never access the network.

## Pipeline and data model

```text
SEC JSON or pinned offline fixture
  -> validate document shape
  -> SHA-256 raw object + capture manifest
  -> one DuckDB transaction
       facts (exact decimal strings, economic period, filing/accession)
       lineage (fact <-> source snapshot)
       quarantine (invalid row + reason + source location)
  -> conservative as-of query / integrity audit / replay checks
```

- Identity includes CIK, taxonomy, concept, unit, exact period, filing date, accession, form and normalized exact value. No cross-unit conversion or quarter/year guessing.
- Duplicate delivery does not duplicate facts. Different captures retain separate lineage.
- Database writes are atomic. An interrupted run can leave an unreferenced raw object; rerun the same delivery safely. Raw files are flushed and atomically linked into the content-addressed archive.
- Invalid rows go to quarantine; structural schema drift rejects the entire document. Never treat a partly loaded document as a silent success: inspect quarantine counts.
- Two different values on the latest filing day raise an error rather than ranking accessions lexically. `--include-conflicts` emits the competing values and source IDs instead.
- `--known-at 2026-10-27T06:00:00Z` additionally limits which locally captured snapshots may contribute. A current archive is not eligible for an earlier local-capture query.

## Reproducible checks, not promises

The included fixture has 299 fact versions from two concepts in one company archive. The benchmark rebuilds it from 50 seeded shuffled/duplicate deliveries and compares semantic checksums. See [machine-readable results](docs/benchmark.json) for the environment, local runtime, exact hash and case-study accessions. Tests also cover rollback, persistence, malformed rows, ambiguous values, unit/period separation and source tampering. Runtime is a single local measurement, not a scale or availability guarantee.

## Source and limitations

- [Fixture provenance](data/provenance.json) records the original response hash, selected fixture hash, capture time and selection. Selection removes concepts; it does not edit values or invent historic snapshots.
- SEC companyfacts aggregates standard-taxonomy, entity-wide facts. Custom-taxonomy and dimensional context are outside this lab. Exact-period matching does not establish accounting comparability.
- Changed values are not automatically restatements. Only the selected KHC example was checked against the original filing table. Other changes may be recasts or other accounting changes.
- Dates do not capture time-of-day availability. Later non-reliance notices, vendor delays and original archive omissions are not modeled. This is not a point-in-time investment database.
- One company and two fixture concepts are a demonstration, not a representative benchmark. Full archives can contain genuine same-day conflicts; inspect them rather than silently choosing.
- Single-process, single-writer local DuckDB; not a distributed scheduler. SHA-256 checks detect changed bytes, not a malicious database editor who can rewrite the manifest too.

## Optional fresh download

```bash
python scripts/download.py 1637459 --contact YOUR_REAL_EMAIL --out data/raw
```

This performs one explicit request and stops on HTTP errors, including throttling. Do not loop it or run concurrent copies. SEC asks clients to identify themselves, download only what they need, and stay below its aggregate 10 requests/second maximum. This project does not need anything close to that rate. Follow the [SEC developer policy](https://www.sec.gov/about/developer-resources) and [User-Agent guidance](https://www.sec.gov/about/webmaster-frequently-asked-questions). Use the returned source URL and actual capture time when ingesting a fresh file; do not reuse the bundled fixture's timestamp.

[API documentation](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) explains companyfacts, frames, units and update behavior. Frames selects last-filed values and must not be substituted for a history-preserving source.

## License

Code: MIT. SEC source content remains attributed to its original source; the code license does not assert ownership over filing content. No investment advice.
