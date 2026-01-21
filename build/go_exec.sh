#!/bin/bash
set -e

# This script builds a go application using 'go build'
# It requires exactly two arguments:
# 1. The target directory to output the executable
# 2. The relative path to the main package within the repo

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <output_directory> <main_package_path>"
  exit 1
fi
outpath="$1"
entrypoint="$2"

# Build the Go executable
CGO_ENABLED=0
go build \
    -trimpath \
    -ldflags "-extldflags '-static'" \
    -o $outpath $entrypoint
