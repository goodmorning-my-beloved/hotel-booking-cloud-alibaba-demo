#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

REQUESTS="${REQUESTS:-20}"
BFF_1="${BFF_1:-http://127.0.0.1:8086/lab-chain/entry}"
BFF_2="${BFF_2:-http://127.0.0.1:8186/lab-chain/entry}"
A_URL="${A_URL:-http://127.0.0.1:8087/a/work}"
B_URL="${B_URL:-http://127.0.0.1:8088/b/work}"
OUTPUT_DIR="$(mktemp -d)"
trap 'rm -rf "$OUTPUT_DIR"' EXIT

send_requests() {
  local name="$1"
  local output="$2"
  shift 2
  local urls=("$@")

  echo
  echo "Sending $REQUESTS concurrent requests for $name:"
  for url in "${urls[@]}"; do
    echo "  $url"
  done

  local phase_dir="$OUTPUT_DIR/$name"
  mkdir -p "$phase_dir"

  for i in $(seq 1 "$REQUESTS"); do
    local index=$(( (i - 1) % ${#urls[@]} ))
    local url="${urls[$index]}"
    (
      curl -sS --max-time 5 "$url" >"$phase_dir/$i.json" || true
    ) &
  done
  wait

  : >"$output"
  for file in "$phase_dir"/*.json; do
    if [[ -s "$file" ]]; then
      tr -d '\n' <"$file" >>"$output"
      printf '\n' >>"$output"
    fi
  done
}

count_lines() {
  local pattern="$1"
  local file="$2"
  grep -c "$pattern" "$file" || true
}

print_phase_result() {
  local title="$1"
  local output="$2"

  local responses top_level_passed top_level_failed bff_blocked a_blocked b_blocked
  responses="$(count_lines '.' "$output")"
  top_level_passed="$(count_lines '^{"success":true' "$output")"
  top_level_failed="$(count_lines '^{"success":false' "$output")"
  bff_blocked="$(count_lines 'Sentinel blocked chainBffEntry: FlowException' "$output")"
  a_blocked="$(count_lines 'Sentinel blocked chainAWork: FlowException' "$output")"
  b_blocked="$(count_lines 'Sentinel blocked chainBWork: FlowException' "$output")"

  cat <<EOF

$title:
  responses:            $responses
  top-level passed:     $top_level_passed
  top-level failed:     $top_level_failed
  BFF blocked:          $bff_blocked
  A service blocked:    $a_blocked
  B service blocked:    $b_blocked
EOF
}

BFF_OUTPUT="$OUTPUT_DIR/bff-responses.txt"
A_OUTPUT="$OUTPUT_DIR/a-responses.txt"
B_OUTPUT="$OUTPUT_DIR/b-responses.txt"

send_requests "BFF cluster" "$BFF_OUTPUT" "$BFF_1" "$BFF_2"
sleep 2
send_requests "A service cluster" "$A_OUTPUT" "$A_URL"
sleep 2
send_requests "B service cluster" "$B_OUTPUT" "$B_URL"

cat <<EOF

Cluster flow rules in Nacos namespace pro:
  chainBffEntry: 6 QPS across both BFF instances
  chainAWork:   4 QPS
  chainBWork:   2 QPS
EOF

print_phase_result "BFF phase" "$BFF_OUTPUT"
print_phase_result "A phase" "$A_OUTPUT"
print_phase_result "B phase" "$B_OUTPUT"

echo
echo "Sample BFF blocked response:"
grep -m 1 'Sentinel blocked chainBffEntry' "$BFF_OUTPUT" || echo "No BFF blocked response captured. Run again or increase REQUESTS."
echo
echo "Sample A blocked response:"
grep -m 1 'Sentinel blocked chainAWork' "$A_OUTPUT" || echo "No A blocked response captured. Run again or increase REQUESTS."
echo
echo "Sample B blocked response:"
cat "$B_OUTPUT" "$A_OUTPUT" | grep -m 1 'Sentinel blocked chainBWork' || echo "No B blocked response captured. Run again or increase REQUESTS."
