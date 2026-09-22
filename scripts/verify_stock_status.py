"""Reads inventory.stock_status straight from Polaris/ADLS2 -- the
verification path chosen in the README ADR instead of standing up Trino.

Polaris's Service is ClusterIP-only (k8s/polaris/03_service.yaml), so
this has to run against a port-forward from outside the cluster:

    kubectl port-forward svc/polaris 8181:8181 -n flink

03_sink.sql's write.upsert.enabled=true makes Flink's IcebergSink write
equality-delete files on every update to an existing product_id.
PyIceberg (as of 0.12.0, the latest release) can't merge those into a
scan yet: https://github.com/apache/iceberg/issues/6568. table.scan()
raises ValueError for any snapshot that has one. This falls back to
listing snapshots/manifests directly (metadata-only, no delete-merge
needed) to confirm real commits landed, rather than switching the sink
off upsert or adding a second query engine just for this script.
"""

from catalog import load_polaris_catalog


def main() -> None:
    catalog = load_polaris_catalog()
    table = catalog.load_table("inventory.stock_status")

    try:
        df = table.scan().to_pandas()
    except ValueError as e:
        if "equality deletes" not in str(e):
            raise
        print("table.scan() unsupported (pyiceberg can't merge equality deletes yet); "
              "falling back to snapshot/manifest metadata to confirm commits landed.\n")
        snapshots = table.metadata.snapshots
        print(f"{len(snapshots)} snapshot(s) committed for inventory.stock_status\n")
        for s in snapshots:
            manifests = s.manifests(table.io)
            added_data = sum(m.added_files_count or 0 for m in manifests if m.content.name == "DATA")
            added_deletes = sum(m.added_files_count or 0 for m in manifests if m.content.name == "DELETES")
            print(f"  snapshot {s.snapshot_id}: {s.summary.operation.value}, "
                  f"+{added_data} data file(s), +{added_deletes} delete file(s)")
        return

    print(f"{len(df)} products tracked\n")
    print(df.sort_values("product_id").to_string(index=False))

    low_stock = df[df["low_stock"]]
    print(f"\n{len(low_stock)} product(s) below threshold:")
    print(low_stock.to_string(index=False) if len(low_stock) else "(none)")


if __name__ == "__main__":
    main()
