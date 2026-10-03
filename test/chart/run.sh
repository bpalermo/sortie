#!/bin/bash
# Installs the chart for real. Needs a kind cluster (KIND_CLUSTER, default
# sortie-e2e) that kubectl and helm already point at; builds the sortie and
# engine images from this checkout, loads them into the cluster's node,
# installs the chart with its engine on and a plan aimed at a test server, and
# checks the verdict both ways: a plan within its thresholds completes the Job with the
# expected request count in its report, and one that breaches fails it.
#
# Extra arguments go to the bazel commands (CI passes its remote-execution
# config). test/e2e covers the same path without a cluster; this is the only
# test that renders the chart and runs what it rendered.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "${here}/../.." && pwd)"
cluster="${KIND_CLUSTER:-sortie-e2e}"
release=sortie
chart="${root}/bazel-bin/charts/sortie/sortie.tgz"

cd "${root}"
# The images go in as OCI archives, straight from Bazel to the node: rules_img
# writes them as the `tarball` output group of the load targets. Not through
# the docker daemon, whose `docker save` of a multi-platform image references
# the other platform's blobs without carrying them, which the node's import
# then rejects.
bazel build //charts/sortie:sortie //:image_load //engine:image_load \
  --output_groups=default,tarball "$@"
kind load image-archive --name "${cluster}" "${root}/bazel-bin/image_load_docker.tar"
kind load image-archive --name "${cluster}" "${root}/bazel-bin/engine/image_load_docker.tar"

# The tags are fixed and the pull policy is Never, so a pod already running
# from an earlier run keeps the earlier image: restart whatever is there, so
# the run exercises what was just loaded. Before the install, so the Job --
# created with the engine -- meets the restarted engine, not the old one.
kubectl apply -f "${here}/target.yaml"
kubectl rollout restart deployment/target
kubectl rollout status deployment/target --timeout=120s
if kubectl get "deployment/${release}-sortie-engine" >/dev/null 2>&1; then
  kubectl rollout restart "deployment/${release}-sortie-engine"
  kubectl rollout status "deployment/${release}-sortie-engine" --timeout=120s
fi

# The Job's name is suffixed with a digest of its pod template, so each plan
# below is a new Job and the newest one is the run just made.
latest_job() {
  kubectl get jobs -l "app.kubernetes.io/instance=${release}" \
    --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1:].metadata.name}'
}

echo "== a plan within its thresholds completes the Job"
helm upgrade --install "${release}" "${chart}" -f "${here}/values.yaml" \
  --wait --wait-for-jobs --timeout 5m
job="$(latest_job)"
kubectl logs "job/${job}" | tee "${root}/bazel-bin/chart-e2e.log"
if ! grep -Eq '^\s+\S+: 500 requests in \S+$' "${root}/bazel-bin/chart-e2e.log"; then
  echo "expected a report of exactly 500 requests from job/${job}" >&2
  exit 1
fi

echo "== a plan that breaches a threshold fails the Job"
if helm upgrade "${release}" "${chart}" -f "${here}/values.yaml" -f "${here}/values-breach.yaml" \
  --wait --wait-for-jobs --timeout 5m; then
  echo "helm upgrade succeeded on a breached threshold; the Job should have failed" >&2
  exit 1
fi
job="$(latest_job)"
kubectl logs "job/${job}" || true
failed="$(kubectl get "job/${job}" -o jsonpath='{.status.failed}')"
if [ "${failed:-0}" -lt 1 ]; then
  echo "job/${job} did not fail" >&2
  exit 1
fi
# The engine survived its tenant's failure: it is still serving.
kubectl rollout status "deployment/${release}-sortie-engine" --timeout=60s
echo "chart e2e: ok"
