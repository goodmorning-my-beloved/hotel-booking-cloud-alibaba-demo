#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
docker compose up -d nacos sentinel-dashboard rabbitmq kafka kafka-ui seata-server

echo "Nacos: http://127.0.0.1:8848/nacos"
echo "Sentinel Dashboard: http://127.0.0.1:8858"
echo "RabbitMQ Management: http://127.0.0.1:15672  guest/guest"
echo "Kafka UI: http://127.0.0.1:8090"
echo "Seata console: http://127.0.0.1:7091"
