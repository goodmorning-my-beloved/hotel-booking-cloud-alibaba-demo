#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

REQUESTS="${REQUESTS:-20}"
BFF_1="${BFF_1:-http://127.0.0.1:8086/lab-chain/entry}"
BFF_2="${BFF_2:-http://127.0.0.1:8186/lab-chain/entry}"
OUTPUT_DIR="$(mktemp -d)"
OUTPUT="$OUTPUT_DIR/responses.txt"
trap 'rm -rf "$OUTPUT_DIR"' EXIT

echo "Sending $REQUESTS concurrent requests to two BFF instances:"
echo "  BFF 1: $BFF_1"
echo "  BFF 2: $BFF_2"

for i in $(seq 1 "$REQUESTS"); do
  if (( i % 2 == 0 )); then
    url="$BFF_2"
  else
    url="$BFF_1"
  fi
  (
    curl -sS --max-time 5 "$url" >"$OUTPUT_DIR/$i.json" || true
  ) &
done
wait

: >"$OUTPUT"
for file in "$OUTPUT_DIR"/*.json; do
  if [[ -s "$file" ]]; then
    tr -d '\n' <"$file" >>"$OUTPUT"
    printf '\n' >>"$OUTPUT"
  fi
done

responses="$(grep -c '.' "$OUTPUT" || true)"
passed="$(grep -c '"success":true' "$OUTPUT" || true)"
blocked="$(grep -c 'Sentinel blocked chainBffEntry: FlowException' "$OUTPUT" || true)"
failed="$(grep -c '"success":false' "$OUTPUT" || true)"

cat <<EOF

Result:
  total requests:  $REQUESTS
  responses:       $responses
  passed:          $passed
  blocked:         $blocked
  failed total:    $failed

The rule count is 3 QPS for chainBffEntry across both BFF instances.
Seeing blocked responses here means both BFF instances are sharing the independent token server.
EOF

echo
echo "Sample blocked response:"
grep -m 1 'Sentinel blocked chainBffEntry' "$OUTPUT" || echo "No blocked response captured. Run again or increase REQUESTS."
