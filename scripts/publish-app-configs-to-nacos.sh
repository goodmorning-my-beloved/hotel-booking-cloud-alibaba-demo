#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

NACOS_URL="${NACOS_URL:-http://127.0.0.1:8848}"
NACOS_NAMESPACE="${NACOS_NAMESPACE:-pro}"
NACOS_GROUP="${NACOS_GROUP:-DEFAULT_GROUP}"
CONFIG_DIR="${CONFIG_DIR:-$PWD/nacos-config/apps}"

ensure_namespace() {
  local response

  response="$(curl -fsS "$NACOS_URL/nacos/v1/console/namespaces")"
  if printf '%s' "$response" | grep -qE "\"namespace(Id)?\":\"$NACOS_NAMESPACE\""; then
    echo "Nacos namespace $NACOS_NAMESPACE already exists."
    return
  fi

  echo "Creating Nacos namespace $NACOS_NAMESPACE"
  curl -fsS -X POST "$NACOS_URL/nacos/v1/console/namespaces" \
    --data-urlencode "customNamespaceId=$NACOS_NAMESPACE" \
    --data-urlencode "namespaceName=$NACOS_NAMESPACE" \
    --data-urlencode "namespaceDesc=Hotel booking demo pro namespace" >/dev/null
}

publish_config() {
  local file="$1"
  local data_id

  data_id="$(basename "$file")"
  echo "Publishing $data_id to Nacos namespace $NACOS_NAMESPACE group $NACOS_GROUP"
  curl -fsS -X POST "$NACOS_URL/nacos/v1/cs/configs" \
    --data-urlencode "tenant=$NACOS_NAMESPACE" \
    --data-urlencode "dataId=$data_id" \
    --data-urlencode "group=$NACOS_GROUP" \
    --data-urlencode "type=yaml" \
    --data-urlencode "content@$file" >/dev/null
}

ensure_namespace

for file in "$CONFIG_DIR"/*.yaml; do
  publish_config "$file"
done

echo "Application configs have been published to Nacos namespace $NACOS_NAMESPACE."
