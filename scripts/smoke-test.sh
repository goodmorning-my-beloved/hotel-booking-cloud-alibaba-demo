#!/usr/bin/env bash
set -euo pipefail

base="${1:-http://127.0.0.1:8080}"

echo "List rooms"
curl -sS "$base/api/hotels" | sed 's/,/,\n/g'

echo
echo "Create booking"
curl -sS -X POST "$base/api/orders/book" \
  -H 'Content-Type: application/json' \
  -d '{"userId":1,"roomId":101,"checkIn":"2026-07-01","checkOut":"2026-07-03"}' | sed 's/,/,\n/g'

echo
echo "Consumed booking events"
curl -sS "$base/api/messages/booking-events" | sed 's/,/,\n/g'
