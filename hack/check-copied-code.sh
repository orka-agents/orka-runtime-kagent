#!/usr/bin/env bash
# Checks that the packages copied from orka-runtime-foundry (see NOTICE) still
# match their source, apart from the module path. Test files may differ.
#
# usage: hack/check-copied-code.sh <orka-runtime-foundry checkout>
set -euo pipefail

source_dir=${1:?usage: hack/check-copied-code.sh <orka-runtime-foundry checkout>}
repo_dir=$(cd "$(dirname "$0")/.." && pwd)
packages=(internal/durablestore internal/strictjson)

status=0
for package in "${packages[@]}"; do
  ours=$(cd "$repo_dir/$package" && find . -name '*.go' ! -name '*_test.go' | sort)
  theirs=$(cd "$source_dir/$package" && find . -name '*.go' ! -name '*_test.go' | sort)
  if [ "$ours" != "$theirs" ]; then
    echo "$package: file set differs from orka-runtime-foundry" >&2
    diff <(echo "$theirs") <(echo "$ours") >&2 || true
    status=1
    continue
  fi
  for file in $ours; do
    if ! diff -u \
      <(sed 's#github.com/orka-agents/orka-runtime-foundry/#github.com/orka-agents/orka-runtime-kagent/#' "$source_dir/$package/$file") \
      "$repo_dir/$package/$file" >&2; then
      echo "$package/${file#./}: differs from orka-runtime-foundry" >&2
      status=1
    fi
  done
done
if [ "$status" -eq 0 ]; then
  echo "copied packages match orka-runtime-foundry"
fi
exit "$status"
