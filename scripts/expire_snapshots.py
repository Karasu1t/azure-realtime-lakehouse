"""Iceberg keeps every metadata.json/manifest/data file version instead of
overwriting them -- with a 30s checkpoint interval, a multi-hour demo
session can accumulate hundreds of snapshot versions. This expires
everything older than RETENTION_HOURS, freeing the manifests and data
files no live snapshot references any more.

Run manually during an active session, same port-forward requirement as
verify_stock_status.py. This is intentionally not on a schedule -- see
the README ADR for why a cron job doesn't fit this project's lifecycle
(ADLS2 itself gets destroyed with the rest of the stack between
sessions, so there's nothing for a recurring job to clean up between
runs).

pyiceberg's expire-snapshots API surface has changed across releases --
verify Table.expire_snapshots() (or wherever it currently lives) against
the pinned pyiceberg version before relying on this.
"""

import os
from datetime import datetime, timedelta, timezone

from catalog import load_polaris_catalog

RETENTION_HOURS = int(os.environ.get("SNAPSHOT_RETENTION_HOURS", "24"))


def main() -> None:
    catalog = load_polaris_catalog()
    table = catalog.load_table("inventory.stock_status")

    cutoff = datetime.now(timezone.utc) - timedelta(hours=RETENTION_HOURS)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    before = len(list(table.snapshots()))
    table.expire_snapshots().expire_older_than(cutoff_ms).commit()
    after = len(list(table.snapshots()))

    print(f"snapshots: {before} -> {after} (expired anything older than {cutoff.isoformat()})")


if __name__ == "__main__":
    main()
