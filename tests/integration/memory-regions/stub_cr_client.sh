#!/bin/sh
# Stub cr_client for cross-language integration tests. Simulates GPU-CR for
# a single workload: "device memory" is the file $EXPORT_FILE_PATH/device;
# -c dumps it into the -o destination (GEP-0001 destination-path
# checkpoints), -r loads the destination back. Falls back to the legacy
# in-place dump buffer $EXPORT_FILE_PATH/42 when no -o is given.
set -eu

ctl="${EXPORT_FILE_PATH:?EXPORT_FILE_PATH must be set}"
echo "$@" >>"$ctl/cr_client_calls.log"

mode=""
dest=""
prev=""
for arg in "$@"; do
  if [ "$prev" = "-o" ]; then
    dest="$arg"
  fi
  case "$arg" in
    -c) mode=checkpoint ;;
    -r) mode=restore ;;
  esac
  prev="$arg"
done

case "$mode" in
  checkpoint) cp "$ctl/device" "${dest:-$ctl/42}" ;;
  restore) cp "${dest:-$ctl/42}" "$ctl/device" ;;
  *)
    echo "stub cr_client: no -c/-r flag in: $*" >&2
    exit 1
    ;;
esac
