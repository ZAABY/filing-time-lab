"""One explicit SEC download, no crawler, no retry loop or embedded identity."""

import argparse
import datetime as dt
import hashlib
import json
import os
import urllib.request
from pathlib import Path


def main():
    p = argparse.ArgumentParser()
    p.add_argument("cik", help="1-10 digit company CIK")
    p.add_argument(
        "--contact", required=True, help="Your real contact email for SEC User-Agent"
    )
    p.add_argument("--out", default="data/raw")
    args = p.parse_args()
    if (
        not args.cik.isdigit()
        or not 1 <= len(args.cik) <= 10
        or "@" not in args.contact
    ):
        p.error("valid numeric CIK and contact email required")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{args.cik.zfill(10)}.json"
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": f"FilingTimeLab {args.contact}",
            "Accept": "application/json",
        },
    )
    # Fail on 403/429 instead of escalating traffic. One invocation = one request.
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read(50_000_001)
    if len(raw) > 50_000_000:
        raise ValueError("response exceeds 50MB safety limit")
    parsed = json.loads(raw)
    if str(parsed.get("cik")).zfill(10) != args.cik.zfill(10):
        raise ValueError("response CIK mismatch")
    captured = dt.datetime.now(dt.timezone.utc).isoformat()
    sha = hashlib.sha256(raw).hexdigest()
    path = out / f"{sha}.json"
    if not path.exists():
        with path.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    manifest = {
        "source_url": url,
        "captured_at": captured,
        "sha256": sha,
        "bytes": len(raw),
        "path": str(path),
    }
    sidecar = (
        out
        / f"{sha}-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}.manifest.json"
    )
    sidecar.write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
