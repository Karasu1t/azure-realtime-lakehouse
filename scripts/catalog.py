"""Shared Polaris catalog connection, used by verify_stock_status.py and
expire_snapshots.py. See either script's docstring for the port-forward
requirement (Polaris's Service is ClusterIP-only).
"""

import os

from pyiceberg.catalog import load_catalog


def load_polaris_catalog():
    # 'credential' (client_id:client_secret), not a static 'token' -- the
    # Iceberg REST client fetches and auto-refreshes an OAuth2 token
    # itself. See flink-jobs/inventory-monitor/01_catalog.sql for the
    # same reasoning on the Flink side.
    return load_catalog(
        "polaris_catalog",
        **{
            "uri": os.environ.get("POLARIS_URI", "http://localhost:8181/api/catalog"),
            "credential": f"{os.environ['POLARIS_CLIENT_ID']}:{os.environ['POLARIS_CLIENT_SECRET']}",
            # Polaris rejects pyiceberg's default OAuth2 scope 'catalog' with
            # invalid_scope; same fix as 01_catalog.sql's 'scope' property.
            "scope": "PRINCIPAL_ROLE:ALL",
            "warehouse": "lakehouse",
            "adls.account-name": os.environ["ADLS_ACCOUNT_NAME"],
            "adls.account-key": os.environ["ADLS_ACCOUNT_KEY"],
        },
    )
