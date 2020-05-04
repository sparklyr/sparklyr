ARROW_VERSION=$1

pushd ..

TMP_DIR=/tmp
BUILD_DIR="$TMP_DIR/apache-arrow-$ARROW_VERSION-build"

cd "$TMP_DIR"

sudo apt install -y -V autoconf automake cmake bison flex libboost-all-dev libevent-dev libssl-dev libtool pkg-config

if [[ $ARROW_VERSION == "devel" ]]; then
  git clone https://github.com/apache/arrow
  SRC_DIR="$TMP_DIR/arrow/cpp"
else
  wget https://archive.apache.org/dist/arrow/arrow-$ARROW_VERSION/apache-arrow-$ARROW_VERSION.tar.gz
  tar -xvzf apache-arrow-$ARROW_VERSION.tar.gz
  SRC_DIR="$TMP_DIR/apache-arrow-$ARROW_VERSION/cpp"
fi

mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"
cmake -DCMAKE_BUILD_TYPE=release \
      -DARROW_BUILD_TESTS=FALSE \
      -DARROW_WITH_SNAPPY=FALSE \
      -DARROW_WITH_ZSTD=FALSE \
      -DARROW_WITH_LZ4=FALSE \
      -DARROW_JEMALLOC=FALSE \
      -DARROW_WITH_ZLIB=FALSE \
      -DARROW_WITH_BROTLI=FALSE \
      -DARROW_USE_GLOG=FALSE \
      -DARROW_BUILD_UTILITIES=FALSE \
      -DARROW_COMPUTE=ON \
      -DARROW_CSV=ON \
      -DARROW_DATASET=ON \
      -DARROW_FILESYSTEM=ON \
      -DARROW_JSON=ON \
      -DARROW_PARQUET=ON \
      "$SRC_DIR"

make -j$(($(nproc) + 1)) arrow
sudo make install

popd
