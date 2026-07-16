#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../frontend"

NODE_HOME="${NODE_HOME:-/opt/codex-runner/workspace/tools/node-v22.12.0-linux-x64}"
if [[ -x "$NODE_HOME/bin/npm" ]]; then
  export PATH="$NODE_HOME/bin:$PATH"
fi

if [[ ! -d node_modules ]]; then
  npm install
fi

npm run build
