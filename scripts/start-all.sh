#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

"$PWD/scripts/build-frontend.sh"

if [[ -x /opt/codex-runner/workspace/.local-tools/maven/bin/mvn && -x /opt/codex-runner/workspace/.local-tools/jdk/bin/java ]]; then
  export JAVA_HOME=/opt/codex-runner/workspace/.local-tools/jdk
  export PATH="/opt/codex-runner/workspace/.local-tools/maven/bin:$JAVA_HOME/bin:$PATH"
  mvn -B -DskipTests package
else
  "$PWD/scripts/build-with-docker.sh"
fi

docker compose up -d nacos sentinel-dashboard rabbitmq kafka kafka-ui seata-server
echo "Waiting a bit for middleware to accept connections..."
sleep "${HOTEL_DEMO_INFRA_WAIT_SECONDS:-25}"
docker compose up -d --build gateway-service user-service hotel-service order-service payment-service message-service

echo "Hotel Web: http://127.0.0.1:8080"
echo "Gateway API: http://127.0.0.1:8080/api/hotels"
echo "Nacos: http://127.0.0.1:8848/nacos"
echo "RabbitMQ Management: http://127.0.0.1:15672  guest/guest"
echo "Kafka UI: http://127.0.0.1:8090"
echo "Try: ./scripts/smoke-test.sh"
