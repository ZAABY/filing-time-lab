import argparse
import json

from .core import as_of, connect, ingest, state_checksum, verify_archive


def main():
    parser = argparse.ArgumentParser(
        description="Replay SEC fact versions; no investment advice"
    )
    parser.add_argument("--db", default="filings.duckdb")
    commands = parser.add_subparsers(dest="command", required=True)
    load = commands.add_parser("ingest")
    load.add_argument("path")
    load.add_argument("--source-url", required=True)
    load.add_argument("--captured-at", required=True)
    load.add_argument("--archive", default="data/archive")
    query = commands.add_parser("as-of")
    query.add_argument("date")
    query.add_argument("--known-at")
    query.add_argument("--concept")
    query.add_argument("--include-conflicts", action="store_true")
    commands.add_parser("audit")
    args = parser.parse_args()
    con = connect(args.db)
    try:
        if args.command == "ingest":
            result = ingest(
                con, args.path, args.source_url, args.captured_at, args.archive
            )
        elif args.command == "as-of":
            result = as_of(
                con, args.date, args.known_at, args.concept, args.include_conflicts
            )
        else:
            result = {
                "fact_checksum": state_checksum(con),
                "archive_errors": verify_archive(con),
                "facts": con.execute("SELECT count(*) FROM facts").fetchone()[0],
                "quarantine": con.execute("SELECT count(*) FROM quarantine").fetchone()[
                    0
                ],
            }
        print(json.dumps(result, indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()
