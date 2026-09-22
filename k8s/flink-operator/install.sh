#!/usr/bin/env bash
set -euo pipefail

# Installs the Flink Kubernetes Operator via its official Helm chart.
# Run after `az aks get-credentials` has pointed kubectl at the cluster.
# Kept out of Terraform on purpose: this installs application-layer
# software into the cluster, a different concern from provisioning the
# cluster itself, and mixing the two would make `terraform plan` noisy
# with every operator version bump.

# Apache's release archive only keeps the latest patch of each minor line
# (1.16.0 was pruned in favor of 1.16.1 between sessions), so this default
# will go stale again -- check `curl https://downloads.apache.org/flink/`
# if this 404s.
FLINK_OPERATOR_VERSION="${FLINK_OPERATOR_VERSION:-1.16.1}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.20.4}"

# The operator's admission webhook gets its TLS certificate from
# cert-manager, so the chart fails without it (see the operator quick start).
kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
kubectl rollout status deployment/cert-manager-webhook -n cert-manager --timeout=180s

helm repo add flink-operator-repo \
  "https://downloads.apache.org/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/"
helm repo update

kubectl create namespace flink --dry-run=client -o yaml | kubectl apply -f -

helm upgrade --install flink-kubernetes-operator \
  flink-operator-repo/flink-kubernetes-operator \
  --namespace flink \
  --version "${FLINK_OPERATOR_VERSION}"

kubectl rollout status deployment/flink-kubernetes-operator -n flink --timeout=180s
