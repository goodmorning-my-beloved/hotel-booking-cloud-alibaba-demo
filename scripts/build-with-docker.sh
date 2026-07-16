#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
"$PWD/scripts/build-frontend.sh"
docker run --rm \
  -v "$PWD":/workspace \
  -v "${HOME}/.m2":/root/.m2 \
  -w /workspace \
  maven:3.9.9-eclipse-temurin-17 \
  mvn -B -DskipTests package
