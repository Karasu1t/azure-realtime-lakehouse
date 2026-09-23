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

pyiceberg 0.12.0's expire-snapshots API lives at Table.maintenance
.expire_snapshots() (returns an ExpireSnapshots builder), not
Table.expire_snapshots() directly -- that method doesn't exist on Table
at all. Confirmed against the installed 0.12.0 by inspecting
pyiceberg/table/maintenance.py and pyiceberg/table/update/snapshot.py;
verify again if the pinned version changes.
"""

import os
from datetime import datetime, timedelta, timezone

from catalog import load_polaris_catalog

RETENTION_HOURS = int(os.environ.get("SNAPSHOT_RETENTION_HOURS", "24"))


def main() -> None:
    catalog = load_polaris_catalog()
    table = catalog.load_table("inventory.stock_status")

    cutoff = datetime.now(timezone.utc) - timedelta(hours=RETENTION_HOURS)

    before = len(list(table.snapshots()))
    table.maintenance.expire_snapshots().older_than(cutoff).commit()
    table.refresh()
    after = len(list(table.snapshots()))

    print(f"snapshots: {before} -> {after} (expired anything older than {cutoff.isoformat()})")


if __name__ == "__main__":
    main()
