#!/usr/bin/env sh
# .zed/build-tests.sh
#
# Compiles the lib test binary and creates a stable symlink at
#   target/debug/nu_plugin_topology-tests
# so that debug.json can reference a fixed "program" path regardless of the
# content-hash cargo appends to the real binary name.
#
# Usage (called automatically by Zed's "build" step):
#   sh .zed/build-tests.sh [-- <extra cargo args>]

set -e

MANIFEST="$(dirname "$0")/../Cargo.toml"
SYMLINK="$(dirname "$0")/../target/debug/nu_plugin_topology-tests"

# Build the lib test binary and capture the executable path from JSON output.
BINARY=$(
  cargo test \
    --manifest-path "$MANIFEST" \
    --lib \
    --no-run \
    --message-format=json \
    "$@" \
    2>&1 |
    grep -o '"executable":"[^"]*nu_plugin_topology[^"]*"' |
    grep -v '\.d"' |
    head -1 |
    sed 's/"executable":"//;s/"//'
)

if [ -z "$BINARY" ]; then
  echo "build-tests.sh: could not locate test binary — cargo output follows" >&2
  exit 1
fi

echo "build-tests.sh: test binary -> $BINARY"

# Atomically replace the symlink so the path in debug.json stays valid.
ln -sf "$BINARY" "$SYMLINK"
echo "build-tests.sh: symlink     -> $SYMLINK"
