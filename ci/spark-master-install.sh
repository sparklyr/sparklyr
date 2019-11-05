#!/usr/bin/env bash

set -eux -o pipefail

SPARK_VERSION="3.0.0-preview"

SPARK_BUILD="spark-${SPARK_VERSION}-bin-hadoop2.7"

_script_dir_="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

spark_tarball="${SPARK_BUILD}.tgz"

function try_download_latest_snapshot {
    local spark_url="https://archive.apache.org/dist/spark/spark-3.0.0-preview/spark-3.0.0-preview-bin-hadoop2.7.tgz"
    echo "Spark build URL = $spark_url"
    wget --tries=3 ${spark_url}
}

mkdir -p "/home/travis/spark"

cd "/home/travis/spark"

try_download_latest_snapshot
tar -zxf "${spark_tarball}"

echo "Content of directory:"
cd "${SPARK_BUILD}"
pwd
ls -la
