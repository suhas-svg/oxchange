#!/usr/bin/env bash
# Runs the whole system in a local Kubernetes cluster (k3d), the setup the
# spec uses for failure and load tests.
#
#   scripts/k8s.sh up [helm args]     create the cluster if needed, build and load the
#                                     images, and install or upgrade the Helm chart
#   scripts/k8s.sh status             pods and jobs
#   scripts/k8s.sh audit [--traffic-stopped]
#                                     run the auditor now and print its report;
#                                     exits non-zero if a rule fails
#   scripts/k8s.sh smoke              trade once through the dashboard's address
#   scripts/k8s.sh down               delete the cluster and its data
#
# Examples:
#   scripts/k8s.sh up --set redpanda.replicas=3     # for the broker-loss test
#   SKIP_BUILD=1 scripts/k8s.sh up                  # reuse the images already built
#   IMAGE=ghcr.io/me/oxchange:abc123 WEB_IMAGE=ghcr.io/me/oxchange-web:abc123 #     SKIP_BUILD=1 scripts/k8s.sh up                # deploy images built elsewhere (CI)
#
# Needs Docker, kubectl and helm. k3d itself runs from its container image
# when it is not installed. The kubeconfig is written to .k8s/kubeconfig and
# your own ~/.kube/config is left alone:
#   export KUBECONFIG=$PWD/.k8s/kubeconfig
#
# Stop the Compose stack first (docker compose stop): both use port 3000 and
# a laptop rarely has memory for the two together.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=${CLUSTER:-oxchange}
NS=${NAMESPACE:-oxchange}
WEB_PORT=${WEB_PORT:-3000}
API_PORT=${API_PORT:-6550}
K3D_IMAGE=${K3D_IMAGE:-ghcr.io/k3d-io/k3d:5.7.5}
# The images to run, as name:tag. They are loaded into the cluster from
# this machine's Docker, never pulled.
IMAGE=${IMAGE:-oxchange:dev}
WEB_IMAGE=${WEB_IMAGE:-oxchange-web:dev}

mkdir -p .k8s
# kubectl and helm are native programs on Windows, so give them a native path.
KUBECONFIG="$(cygpath -m "$PWD/.k8s/kubeconfig" 2>/dev/null || echo "$PWD/.k8s/kubeconfig")"
export KUBECONFIG

k3d() {
  # type -P: an installed program, not this function.
  if type -P k3d >/dev/null 2>&1; then
    command k3d "$@"
  else
    # MSYS_NO_PATHCONV keeps Git Bash from rewriting the socket path.
    MSYS_NO_PATHCONV=1 docker run --rm -v /var/run/docker.sock:/var/run/docker.sock "$K3D_IMAGE" "$@"
  fi
}

kc() { kubectl -n "$NS" "$@"; }

up() {
  if ! k3d cluster list "$CLUSTER" >/dev/null 2>&1; then
    echo "== creating cluster $CLUSTER"
    # Traefik is off: the dashboard is reached through a NodePort that k3d
    # maps to localhost.
    k3d cluster create "$CLUSTER" --servers 1 --agents 0 \
      --api-port "127.0.0.1:$API_PORT" \
      -p "$WEB_PORT:30080@server:0" \
      --k3s-arg "--disable=traefik@server:0" \
      --kubeconfig-update-default=false --wait
  fi
  # Written by a container when k3d runs from its image, so point it at
  # this machine's side of the published API port.
  k3d kubeconfig get "$CLUSTER" | sed -E "s#server: https://[^:]+:[0-9]+#server: https://127.0.0.1:$API_PORT#" > .k8s/kubeconfig
  kubectl wait --for=condition=Ready node --all --timeout=120s >/dev/null

  if [ -z "${SKIP_BUILD:-}" ]; then
    echo "== building images"
    docker build -q -t "$IMAGE" -f deploy/docker/Dockerfile . >/dev/null
    docker build -q -t "$WEB_IMAGE" web >/dev/null
  fi
  echo "== loading images into the cluster"
  k3d image import "$IMAGE" "$WEB_IMAGE" -c "$CLUSTER" >/dev/null

  kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  if ! kc get secret oxchange-jwt >/dev/null 2>&1; then
    echo "== generating the login signing keys"
    # Made inside a container and copied straight into a Secret; the files
    # are removed again and never enter the repository.
    rm -rf .k8s/keys
    docker rm -f oxchange-keygen >/dev/null 2>&1 || true
    MSYS_NO_PATHCONV=1 docker run --name oxchange-keygen "$IMAGE" accounts keygen /keys >/dev/null
    docker cp oxchange-keygen:/keys .k8s/keys >/dev/null
    docker rm oxchange-keygen >/dev/null
    kc create secret generic oxchange-jwt \
      --from-file=jwt-private.pem=.k8s/keys/jwt-private.pem \
      --from-file=jwt-public.pem=.k8s/keys/jwt-public.pem >/dev/null
    rm -rf .k8s/keys
  fi

  echo "== installing the chart"
  helm upgrade --install oxchange deploy/helm/oxchange -n "$NS" \
    -f deploy/helm/oxchange/values-local.yaml     --set "image.repository=${IMAGE%:*}" --set "image.tag=${IMAGE##*:}"     --set "web.image.repository=${WEB_IMAGE%:*}" --set "web.image.tag=${WEB_IMAGE##*:}"     --wait --timeout 10m "$@"
  # Pods of an unchanged Deployment keep running the old image after a
  # rebuild under the same tag, so restart everything built from it.
  if [ -z "${SKIP_BUILD:-}" ] && [ "$(helm -n "$NS" history oxchange -o json | grep -o '"revision"' | wc -l)" -gt 1 ]; then
    kc rollout restart deployment statefulset/matching >/dev/null
    kc rollout status statefulset/matching --timeout=180s >/dev/null
    for d in $(kc get deployment -o name); do kc rollout status "$d" --timeout=180s >/dev/null; done
  fi
  echo "== ready: http://localhost:$WEB_PORT"
}

audit() {
  local name="audit-$(date +%s)"
  local patch='[]'
  if [ "${1:-}" = "--traffic-stopped" ]; then
    patch='[{"op":"add","path":"/spec/template/spec/containers/0/args","value":["--traffic-stopped"]}]'
  fi
  # The same pod the CronJob runs, started now.
  kc create job "$name" --from=cronjob/auditor --dry-run=client -o json \
    | kubectl patch --local -f - --type=json -p "$patch" -o json \
    | kc apply -f - >/dev/null
  local done=""
  for _ in $(seq 1 150); do
    done=$(kc get job "$name" -o jsonpath='{.status.succeeded}/{.status.failed}')
    [ "$done" != "/" ] && break
    sleep 2
  done
  kc logs "job/$name" || true
  case "$done" in
    1/*) echo "== auditor passed" ;;
    *) echo "== auditor FAILED (or did not finish)"; exit 1 ;;
  esac
}

case "${1:-}" in
  up) shift; up "$@" ;;
  status) kc get pods,jobs,cronjobs -o wide ;;
  audit) shift; audit "${1:-}" ;;
  smoke)
    # The first Python that runs (on Windows "python3" may be a Store stub).
    for py in python3 python; do
      if "$py" -c '' >/dev/null 2>&1; then exec "$py" scripts/smoke.py "http://localhost:$WEB_PORT"; fi
    done
    echo "smoke needs Python 3" >&2; exit 1 ;;
  down) k3d cluster delete "$CLUSTER"; rm -f .k8s/kubeconfig ;;
  *) sed -n '2,24p' "$0"; exit 2 ;;
esac
