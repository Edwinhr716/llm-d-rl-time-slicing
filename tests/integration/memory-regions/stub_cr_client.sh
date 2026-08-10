#!/bin/sh
# Stub cr_client for cross-language integration tests. Simulates GPU-CR for
# a single workload: "device memory" is the file $EXPORT_FILE_PATH/device;
# -c dumps it into the dump buffer $EXPORT_FILE_PATH/42, -r loads it back.
set -eu

ctl="${EXPORT_FILE_PATH:?EXPORT_FILE_PATH must be set}"
echo "$@" >>"$ctl/cr_client_calls.log"

mode=""
for arg in "$@"; do
  case "$arg" in
    -c) mode=checkpoint ;;
    -r) mode=restore ;;
  esac
done

case "$mode" in
  checkpoint) cp "$ctl/device" "$ctl/42" ;;
  restore) cp "$ctl/42" "$ctl/device" ;;
  *)
    echo "stub cr_client: no -c/-r flag in: $*" >&2
    exit 1
    ;;
esac
