#!/usr/bin/env bash

set -eux -o pipefail

SPARK_VERSION="3.0.0-preview2"

SPARK_BUILD="spark-${SPARK_VERSION}-bin-hadoop2.7"

_script_dir_="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

spark_tarball="${SPARK_BUILD}.tgz"

function try_download_latest_snapshot {
    local spark_url="https://archive.apache.org/dist/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop2.7.tgz"
    echo "Spark build URL = $spark_url"
    wget --tries=3 ${spark_url}
}

SPARK_DIR="$HOME/spark"

mkdir -p "$SPARK_DIR"
cd "$SPARK_DIR"

try_download_latest_snapshot
tar -zxf "${spark_tarball}"

echo "Content of directory:"
cd "${SPARK_BUILD}"
pwd
ls -la
