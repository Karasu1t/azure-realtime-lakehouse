# Azure Realtime Lakehouse

[![terraform apply](https://github.com/Karasu1t/azure-realtime-lakehouse/actions/workflows/terraform_apply.yml/badge.svg)](https://github.com/Karasu1t/azure-realtime-lakehouse/actions/workflows/terraform_apply.yml)
[![terraform destroy](https://github.com/Karasu1t/azure-realtime-lakehouse/actions/workflows/terraform_destroy.yml/badge.svg)](https://github.com/Karasu1t/azure-realtime-lakehouse/actions/workflows/terraform_destroy.yml)

A streaming platform that detects inventory shortages in real time. Azure Event Hubs → Flink on AKS → Apache Iceberg (ADLS2 + Apache Polaris), built to close the gap where daily-batch inventory checks don't surface a stockout until the next morning.

Author: [@Karasu1t](https://github.com/Karasu1t)

---

## Why This Exists

Inventory management is typically built on daily batch aggregation, so a stockout that happens today isn't detected until the next batch run. In the meantime, this creates a trade-off on both sides: **lost sales** (revenue that would have happened if the item had been in stock) and **excess inventory risk** (overstocking as a safety margin against the fear of running out). Detecting inventory changes in real time shrinks the detection lag, which is the hypothesis this project tests directly.

This repo implements the **streaming detection layer, from real-time inventory-change detection through to storage in Iceberg tables**. Downstream automated reordering logic and integration with supplier systems are out of scope. The goal is to prove that a real-time detection layer can be built with a production-grade setup (IaC, Kubernetes-native operations) — not to quantify business impact, since the data is synthetic.

---

## Architecture

![Architecture](img/architecture.png)

- **Event Hubs**: exposed via its Kafka-protocol-compatible endpoint, so Flink connects to it as an ordinary Kafka source.
- **Flink on AKS**: managed as a `FlinkDeployment` CRD via the Flink Kubernetes Operator. Keeps current stock per product as Flink state, updating and threshold-checking on every event, then sinks the result to Iceberg.
- **ADLS2 + Polaris**: table data lives in ADLS2; Polaris tracks each table's current state (a pointer to its latest `metadata.json`) via the vendor-neutral Iceberg REST Catalog spec, avoiding lock-in to a single catalog implementation.

---

## Demo

Captured against a real, running Azure deployment.

**① Events flowing through Flink (Web UI)**

![Flink processing events](img/demo01_send_event.gif)

As the simulator sends events to Event Hubs, the Flink job graph's per-operator record counts climb in real time.

**② Iceberg table contents (before)**

![Stock data before](img/demo02_before.png)

Reading the Iceberg table directly via Polaris's REST Catalog, using DuckDB. `P006` is at 205 units.

**③ Iceberg table contents (after)**

![Stock data after](img/demo03_after.png)

After sending one SALE of 4 units and one RESTOCK of 10 units for `P006`. `205 - 4 + 10 = 211`, correctly reflected, with every other product untouched.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Messaging | Azure Event Hubs (Kafka-protocol compatible) |
| Stream processing | Apache Flink (Flink Kubernetes Operator) |
| Container platform | Azure Kubernetes Service (AKS) |
| Storage | Azure Data Lake Storage Gen2 (ADLS2) |
| Table format | Apache Iceberg |
| Catalog | Apache Polaris (Iceberg REST Catalog) |
| Auth | Azure AD Workload Identity (dedicated identities for Flink and Polaris, no shared keys) |
| Infrastructure | Terraform |
| CI/CD | GitHub Actions (OIDC) |

---

## Verification Status

Every layer below was confirmed against a real, billed Azure deployment.

| Layer | Status |
|---|---|
| Terraform (17 resources, including Workload Identity) | Verified |
| Event Hubs ⇔ simulator | Verified |
| AKS / Flink Kubernetes Operator | Verified |
| Polaris (catalog + dedicated principal bootstrap) | Verified |
| FlinkDeployment (Kafka → Iceberg on Polaris) | Verified |
| Iceberg writes / snapshot commits | Verified |
| Workload Identity (dedicated identities for Flink and Polaris) | Verified |
| `upgradeMode: last-state` (state carried across redeploys) | Verified |
| CI/CD (terraform apply/destroy via OIDC) | Verified |
| Iceberg maintenance (expiring old snapshots) | Verified (a known limitation on the GitHub Actions path, see below) |

Debugging detail and individual gotchas are in [docs/engineering-notes.md](docs/engineering-notes.md).

---

## Design Decisions (Selected)

**Why Apache Polaris instead of a native Azure catalog**
The goal isn't "vendor neutrality" for its own sake — it's migration cost. Putting the catalog on an Azure-proprietary spec would lock the single most important asset (the pointer to "what's the current state of this table") to one vendor. Polaris, as a REST Catalog spec implementation, keeps that migration path open.

**Why the Flink Kubernetes Operator**
The `FlinkDeployment` CRD lets the job be managed natively through Kubernetes — deploy, scale, and recover via kubectl/Terraform. Given the goal of demonstrating AKS operational skill, a Kubernetes-native workflow makes the stronger case.

**How exactly-once is achieved**
Flink's checkpoint mechanism keeps the Kafka offset snapshot and the Iceberg snapshot commit in sync, effectively a two-phase commit. If a checkpoint fails, processing rewinds to that checkpoint's offset, so no partial commit is ever left in Iceberg.

**Why no Postgres or Trino**
Iceberg is self-contained with just storage + catalog, no dedicated DB server. Verification reads go straight through the catalog via `pyiceberg`/DuckDB, so a standing query engine wasn't worth the added operational cost.

**Why stateful processing (`GROUP BY`), not windowed aggregation**
Stock level is "the value right now," not "change over a window" — windowed aggregation doesn't fit the shape of the problem, and waiting for a window to close would reintroduce the detection lag this project is meant to eliminate.

**Polaris needs its own Azure IAM permissions — a finding, not a given**
Separately from Flink's own credentials, Polaris itself calls out to Azure with its own identity to validate a table's storage location during `CREATE TABLE`. With no credential configured explicitly, it silently fell back to the AKS node's Managed Identity, which had no relevant permissions — causing an authorization failure that took real investigation to trace. This is exactly the kind of concern a managed catalog service hides from you, and matches this project's underlying goal of stress-testing a fully self-hosted stack (see [docs/engineering-notes.md](docs/engineering-notes.md) for the full trace).

**Migrating off shared keys / the node's Managed Identity to Workload Identity**
Following that finding, Flink and Polaris were each given a dedicated Azure identity, federated directly to a Kubernetes ServiceAccount. Iceberg's actual data reads/writes are now fully keyless (Flink's checkpoint driver alone still needs a shared key, due to a library version constraint).

**Cost posture**
Portfolio-scale, so infra is applied and destroyed per verification session rather than run continuously. A budget alert is configured; nothing stays up idle.

---

## Read More

- Debugging detail, individual gotchas, and the full run-through instructions: [docs/engineering-notes.md](docs/engineering-notes.md)
