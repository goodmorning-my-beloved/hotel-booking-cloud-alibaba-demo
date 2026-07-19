#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

ROUNDS="${ROUNDS:-5}"
TASKS="${TASKS:-32}"
BUSY_MS="${BUSY_MS:-8000}"
GATEWAY_URL="${GATEWAY_URL:-http://127.0.0.1:8080/api/lab-chain/thread-test}"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://127.0.0.1:9091}"

echo "Generating thread metrics through the BFF route:"
echo "  $GATEWAY_URL?tasks=$TASKS&busyMs=$BUSY_MS"

for i in $(seq 1 "$ROUNDS"); do
  curl -sS --max-time 5 "$GATEWAY_URL?tasks=$TASKS&busyMs=$BUSY_MS"
  echo
  sleep 1
done

echo
echo "Waiting for Prometheus to scrape the latest samples..."
sleep 6

query_prometheus() {
  local title="$1"
  local query="$2"

  echo
  echo "$title"
  curl -sS --get "$PROMETHEUS_URL/api/v1/query" --data-urlencode "query=$query" \
    | sed 's/},{/},\n{/g' \
    | head -n 20
  echo
}

query_prometheus \
  "Thread pool saturation metrics:" \
  '{__name__=~"executor_(active_threads|queued_tasks|queue_remaining_tasks|pool_size_threads|pool_core_threads|pool_max_threads)", application="sentinel-bff-service", name="lab-thread-pool"}'

query_prometheus \
  "Thread pool throughput metrics:" \
  'label_replace(rate(lab_thread_test_requests_total{application="sentinel-bff-service"}[1m]), "metric", "requests/s", "instance", ".*") or label_replace(rate(lab_thread_test_tasks_total{application="sentinel-bff-service"}[1m]), "metric", "submitted/s", "instance", ".*") or label_replace(rate(executor_completed_tasks_total{application="sentinel-bff-service", name="lab-thread-pool"}[1m]), "metric", "completed/s", "instance", ".*")'

query_prometheus \
  "Thread pool rejection metrics:" \
  'lab_thread_test_rejected_tasks_total{application="sentinel-bff-service"} or rate(lab_thread_test_rejected_tasks_total{application="sentinel-bff-service"}[1m])'

query_prometheus \
  "Thread task duration metrics:" \
  'histogram_quantile(0.95, sum by (le, instance) (rate(lab_thread_test_task_duration_seconds_bucket{application="sentinel-bff-service"}[1m]))) or lab_thread_test_task_duration_seconds_max{application="sentinel-bff-service"}'

query_prometheus \
  "JVM live threads:" \
  'jvm_threads_live_threads{application="sentinel-bff-service"}'

cat <<EOF

Open Grafana:
  http://127.0.0.1:3000  admin/admin

Dashboard:
  Hotel Demo / Hotel Demo JVM and Thread Metrics
EOF
