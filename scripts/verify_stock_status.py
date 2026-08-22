"""Reads inventory.stock_status straight from Polaris/ADLS2 -- the
verification path chosen in the README ADR instead of standing up Trino.

Polaris's Service is ClusterIP-only (k8s/polaris/03_service.yaml), so
this has to run against a port-forward from outside the cluster:

    kubectl port-forward svc/polaris 8181:8181 -n flink

Property names for the ADLS2 file IO in catalog.py (adls.account-name /
adls.account-key) haven't been checked against the currently installed
pyiceberg version's docs -- verify before relying on this.
"""

from catalog import load_polaris_catalog


def main() -> None:
    catalog = load_polaris_catalog()
    table = catalog.load_table("inventory.stock_status")
    df = table.scan().to_pandas()

    print(f"{len(df)} products tracked\n")
    print(df.sort_values("product_id").to_string(index=False))

    low_stock = df[df["low_stock"]]
    print(f"\n{len(low_stock)} product(s) below threshold:")
    print(low_stock.to_string(index=False) if len(low_stock) else "(none)")


if __name__ == "__main__":
    main()
