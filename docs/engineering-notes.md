# Engineering Notes

Detail that didn't fit in README.md — for anyone digging deeper during an interview, or as reference for a similar build later.

---

## Directory Layout

```
.
├── terraform/
│   ├── modules/
│   │   ├── aks/                # AKS cluster itself
│   │   ├── event_hubs/         # Event Hubs namespace + Kafka-compatible config
│   │   ├── adls2/              # Storage account + container
│   │   ├── networking/         # VNet, subnet
│   │   └── acr/                # sql-runner image registry, grants AcrPull to AKS
│   └── env/
│       └── dev/                # `terraform output` surfaces the connection info below
├── k8s/
│   ├── flink-operator/         # Helm install of the Flink Kubernetes Operator
│   ├── polaris/                # Polaris itself (Deployment + Service, in-memory)
│   └── flink-deployment/       # FlinkDeployment CRD + envsubst from secrets & apply
├── flink-jobs/
│   ├── inventory-monitor/      # The detection job (4 Flink SQL files)
│   └── sql-runner/             # Custom Java runner that executes them in order
├── simulator/
│   └── inventory-event-producer/  # Synthetic inventory-change event generator (Python)
├── scripts/                    # Verification and maintenance helpers (below)
└── .github/workflows/          # terraform apply/destroy, Iceberg maintenance
```

---

## What Each Script Actually Does

Everything runs from a workstation and drives AKS via `kubectl`/`helm`/`az`. **Nothing here runs inside AKS itself.**

What ends up running on AKS (all Pods):

```
flink namespace
 ├─ flink-kubernetes-operator      watches and manages Flink jobs
 ├─ polaris                        Iceberg REST Catalog server
 └─ inventory-monitor              JobManager / TaskManager (created by the Operator from the FlinkDeployment)
cert-manager (3 Pods)              issues the Operator's webhook certificate
```

| Script | What it does | Result |
|---|---|---|
| `k8s/flink-operator/install.sh` | Installs cert-manager and the Flink Kubernetes Operator | Operator Pod, `FlinkDeployment` CRD |
| `flink-jobs/sql-runner/build-and-push.sh` | Builds sql-runner and pushes it to ACR | An image in ACR (no Pod yet) |
| `k8s/flink-deployment/01_render-and-deploy.sh` | Fills secrets into the SQL files, applies the ConfigMap and `FlinkDeployment` | Operator spins up JobManager/TaskManager Pods |
| `scripts/setup-polaris.sh` | Registers the catalog and Flink's principal against a freshly started Polaris | No new Pods — populates Polaris's own state |
| `scripts/setup-oidc.sh` | One-time registration of GitHub Actions' OIDC trust with Azure AD | An Azure AD app registration |

Polaris itself isn't deployed via a script — its YAML under `k8s/polaris/` is applied directly with `kubectl apply`. Note "flink" is overloaded three ways here: the Kubernetes namespace (which Polaris also lives in), the Flink software itself, and the `flink-jobs/` directory.

---

## Running It End to End

Infra is stood up and torn down per verification session, for cost control. Order, as run against real Azure:

1. **Apply the infra**
   ```bash
   cd terraform/env/dev
   cp dev.tfvars.example dev.tfvars   # fill in your own IP
   terraform apply -var-file=dev.tfvars
   ```
2. **Point kubectl at the cluster**
   ```bash
   az aks get-credentials --resource-group $(terraform output -raw resource_group_name) \
     --name $(terraform output -raw aks_cluster_name)
   ```
3. **Install the Flink Kubernetes Operator** (`k8s/flink-operator/install.sh`, cert-manager included)
4. **Deploy Polaris**: copy `k8s/polaris/02_secret.example.yaml` to `02_secret.yaml` and fill in real values, then set `POLARIS_WORKLOAD_IDENTITY_CLIENT_ID` (`terraform output -raw polaris_workload_identity_client_id`) and run
   ```bash
   POLARIS_WORKLOAD_IDENTITY_CLIENT_ID=$(terraform output -raw polaris_workload_identity_client_id) \
     k8s/polaris/00_render-and-deploy.sh
   ```
5. **Bootstrap Polaris**: with `kubectl port-forward svc/polaris 8181:8181 -n flink` open, run `scripts/setup-polaris.sh`. Creates the `lakehouse` catalog and Flink's dedicated principal (`flink_app`), printing its credentials. Re-run this after every Polaris Pod restart — it's in-memory.
6. **Build and push the SQL runner**: `ACR_NAME=... SQL_RUNNER_TAG=<unique tag> flink-jobs/sql-runner/build-and-push.sh` (a fresh tag every time)
7. **Deploy the FlinkDeployment**: copy `k8s/flink-deployment/00_secrets.example.env` to `00_secrets.env`, fill in the values from `terraform output` (`ADLS_ACCOUNT_NAME`, `flink_workload_identity_client_id`), step 5's credentials, and step 6's tag, then run `k8s/flink-deployment/01_render-and-deploy.sh`
8. **Send events**: `simulator/inventory-event-producer/producer.py` (or `send_demo_events.py <product_id>` to target a single product)
9. **Verify**: with the port-forward from step 5 still open, run `scripts/verify_stock_status.py`, or `scripts/verify_stock_status_duckdb.sh` to see the actual row data
10. **Tear down**: `terraform destroy -var-file=dev.tfvars`

Steps 1 and 10 can also run via GitHub Actions (`terraform_apply.yml`/`terraform_destroy.yml`, workflow_dispatch), using OIDC auth set up once via `scripts/setup-oidc.sh`.

---

## Bugs and Gotchas, One by One

**Why Event Hubs via the Kafka protocol, not Azure's native SDK**
Lets Flink's stock Kafka connector work unmodified, no custom connector needed. Polaris was chosen for exit-cost reasons, but not every layer needs to avoid vendor lock-in — messaging is a layer where the managed-service benefit clearly outweighs that concern.

**How environments are separated**
A real production org would split subscriptions across dev/stg/prd. This portfolio, being single-environment, uses resource-group separation within one subscription instead — a deliberate simplification, with the `env/` structure in Terraform ready to extend if more environments were ever needed.

**Handling Iceberg's unbounded metadata/manifest/data file growth**
Iceberg appends a new `metadata.json` on every checkpoint rather than overwriting, so an unattended production table can easily accumulate thousands of files. `scripts/expire_snapshots.py` and `.github/workflows/iceberg_maintenance.yml` implement snapshot expiration for this. In this project's actual usage pattern (destroy the whole environment, ADLS2 included, after every session), accumulation never spans more than a single session — the maintenance script exists to demonstrate awareness of the problem and its fix, not because it's load-bearing here.

**How production-equivalent is the ADLS2 network path and auth**
ADLS2 keeps `public_network_access_enabled = true`, with the firewall's `default_action` set to `Deny` and only the AKS subnet (via service endpoint) and a verification workstation's IP allowed through. Setting it to `false` would force Private Endpoint-only access, which would also cut off Flink running on AKS. A real production setup should close the public endpoint entirely via Private Endpoint; this is a deliberate simplification made to keep verification scripts runnable from a workstation.

**Choosing a VM size under Azure pay-as-you-go (quota and SKU restrictions)**
Ended up on `Standard_D2as_v7`. The original `Standard_B2s_v2` failed with `ErrCode_InsufficientVCPUQuota` — vCPU quota is allocated **per VM family**, separately from the regional total, and this subscription had zero quota for both Bsv2 and Dsv5. Even where quota exists, some sizes are blocked outright by `NotAvailableForSubscription` (Dsv6, for example) — **both** conditions have to be satisfied. None of this showed up during the free trial, so `terraform plan` gave no warning either. Cross-referencing `az vm list-usage` against `az vm list-skus` is what actually found a working size.

**Why the container image tag changes on every build**
A mutable tag like `:latest` gets cached by nodes under `imagePullPolicy: IfNotPresent`, so re-pushing to ACR silently keeps the old image running (this actually happened, and cost real time to diagnose). `build-and-push.sh` now refuses to run without an explicit, unique tag (`SQL_RUNNER_TAG`).

**What to verify locally before spending money on Azure**
Polaris's whole configuration (startup, auth, catalog creation, namespace creation) was walked through on local Docker before ever touching AKS. That surfaced undocumented behavior for free — a realm name mismatch just returns `unauthorized_client` with no further detail, and `default-base-location` has to be the container root or namespace creation fails with a 400. Flink had a similar trap: `Could not find any factory for identifier 'iceberg'` looks like a missing factory, but the actual cause was that a jar was unreadable — the classpath listing still showed it. The Dockerfile's `ADD` from a URL wrote the jar as root, mode 600, unreadable by the `flink` user the JobManager actually runs as. Since the error message alone can't distinguish these cases, the working rule became: don't grind on this in a billed environment — reduce to a minimal local repro first.

**Why Polaris's OAuth call failed with `invalid_scope`**
The Iceberg REST client sends scope `catalog` by default; Polaris rejects that and wants `PRINCIPAL_ROLE:ALL` (or a specific principal role). Flink's SQL sets `'scope' = 'PRINCIPAL_ROLE:ALL'` explicitly.

**Why `event_time` is `TIMESTAMP_LTZ(3)`, and why the simulator sends a `Z` suffix**
Flink's `json.timestamp-format.standard = 'ISO-8601'` expects a timezone-aware value for a `TIMESTAMP_LTZ` column, and **only accepts a `Z` suffix**. A numeric offset like Python's default `+00:00` from `datetime.isoformat()` **silently parses to `NULL`**, no error — invisible unless you turn on `json.ignore-parse-errors` and go looking. Found by standing up a real local JobManager+TaskManager pair and trying several formats.

**Why `verify_stock_status.py` sometimes can't read the table**
`03_sink.sql`'s `write.upsert.enabled=true` makes Flink's `IcebergSink` write an equality-delete file every time an existing `product_id` gets updated. PyIceberg (0.12.0, the latest release as of writing) can't merge that delete format yet ([apache/iceberg#6568](https://github.com/apache/iceberg/issues/6568)), so `table.scan()` raises a `ValueError`. The data itself is committed correctly (verifiable via `table.metadata.snapshots`); `verify_stock_status.py` catches the exception and falls back to snapshot/manifest metadata instead.

**Why the CI Service Principal's IAM needs User Access Administrator, not just Contributor**
`setup-oidc.sh` originally granted the CI Service Principal subscription-scoped `Contributor` only. Running `terraform apply` from GitHub Actions failed in two places: ① `terraform init` hit `AuthorizationPermissionMismatch` against the tfstate backend (`use_azuread_auth = true`) — `Contributor` is a management-plane role, and Azure AD-based Blob data access needs a separate data-plane role like `Storage Blob Data Contributor` (the same shape of bug as Polaris's own IAM issue). ② Creating the `azurerm_role_assignment` for the AKS kubelet identity failed with `AuthorizationFailed` — `Contributor` deliberately excludes `Microsoft.Authorization/roleAssignments/write`, so a Terraform run that itself grants IAM roles needs `User Access Administrator` (or `Owner`) on top.

**Migrating off shared keys hit a library version wall**
Flink's own checkpoint/HA storage (via the Hadoop ABFS driver, not Iceberg's own client) stays on the shared key. Found without touching Azure, just by pulling apart the actual jar: the bundled `flink-azure-fs-hadoop-1.20.5.jar` ships a 2022-vintage Hadoop-Azure driver with no `WorkloadIdentityTokenProvider` class (confirmed present in the current `hadoop-azure:3.4.1` from Maven Central). Simply swapping in a newer jar risks losing Flink's own glue classes bundled in the same file, so this one piece stayed keyed rather than risk breaking it blind.

**Why `upgradeMode: last-state` needed `high-availability` alongside it**
`upgradeMode: stateless` discards the last checkpoint and starts from zero on every redeploy. Repeated `kubectl delete flinkdeployment && kubectl apply` cycles during debugging made this very visible — the running stock total reset every time. Switching to `last-state` resumes from the previous run instead, but it silently behaves like `stateless` unless Flink's own HA mechanism (`high-availability.type: kubernetes`) is also configured — hence `high-availability.storageDir` alongside it. Verified live: changing `execution.checkpointing.interval` and reapplying (without deleting) produced `Restoring job <same jobId> from Checkpoint 35` in the JobManager log, with checkpoint numbering, job ID, and the Kafka source's offset position all carried through.

**Where pyiceberg's `expire_snapshots` API actually lives**
`Table.expire_snapshots()` doesn't exist (as of 0.12.0). The real entry point is `Table.maintenance.expire_snapshots()` (returns an `ExpireSnapshots` builder), and `.older_than(dt)` wants a `datetime`, not an epoch-millis integer — found by reading the installed package (`pyiceberg/table/maintenance.py`) directly rather than trusting the docs.

**Switching the read tool for the demo, instead of redesigning Flink**
The equality-delete limitation above led to considering a rewrite of the aggregation from `GROUP BY` upsert to append-only `OVER`-window output — but that's a real change to the write pipeline. The actual ask for the demo was just "show the table's contents," so the fix was switching the reader from pyiceberg to DuckDB instead (`scripts/verify_stock_status_duckdb.sh`) — its `iceberg` extension does merge equality deletes. The Flink SQL (`01_catalog.sql`/`03_sink.sql`/`04_pipeline.sql`) was never touched. Live, it hit one snag: the default Azure SDK transport failed with `Problem with the SSL CA cert`, even though the system CA bundle was fine and `curl` reached the same endpoint without issue. `SET azure_transport_option_type = 'curl';` fixed it.

**Why the Iceberg maintenance workflow_dispatch fails**
`iceberg_maintenance.yml` gets through `az aks get-credentials` fine, then fails at `kubectl port-forward` with `ConnectionRefusedError`. The cause is the AKS API server's own `authorized_ip_ranges` (a home IP only) — `az aks get-credentials` is an ARM (management-plane) call and passes regardless, but `kubectl` needs a direct connection to the API server, and GitHub-hosted runners use a different IP on every run, so they never match the allowlist. The same issue doesn't affect `terraform_apply.yml`/`terraform_destroy.yml`, since Terraform only ever talks to ARM APIs and never connects to the AKS API server directly. Fixing this properly would mean widening `authorized_ip_ranges` for the run's duration or standing up a self-hosted runner inside the VNet; given this maintenance path was already low priority, `scripts/expire_snapshots.py` is run by hand instead.
