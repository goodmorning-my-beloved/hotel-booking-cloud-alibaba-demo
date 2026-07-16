#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

NACOS_URL="${NACOS_URL:-http://127.0.0.1:8848}"
NACOS_GROUP="${NACOS_GROUP:-SENTINEL_GROUP}"
CONFIG_DIR="${CONFIG_DIR:-$PWD/nacos-config/sentinel}"

publish_config() {
  local data_id="$1"
  local file="$2"

  echo "Publishing $data_id to Nacos group $NACOS_GROUP"
  curl -fsS -X POST "$NACOS_URL/nacos/v1/cs/configs" \
    --data-urlencode "dataId=$data_id" \
    --data-urlencode "group=$NACOS_GROUP" \
    --data-urlencode "type=json" \
    --data-urlencode "content@$file" >/dev/null
}

publish_config "sentinel-bff-service-flow-rules.json" "$CONFIG_DIR/sentinel-bff-service-flow-rules.json"
publish_config "sentinel-bff-service-degrade-rules.json" "$CONFIG_DIR/sentinel-bff-service-degrade-rules.json"
publish_config "sentinel-a-service-degrade-rules.json" "$CONFIG_DIR/sentinel-a-service-degrade-rules.json"

echo "Sentinel rules have been published to Nacos."
