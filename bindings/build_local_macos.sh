#!/bin/bash
set -e
cd "$(dirname "$0")/main"
export ONLY_MACOS=1
./build.sh
