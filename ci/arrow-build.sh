ARROW_VERSION=$1

curl https://dist.apache.org/repos/dist/dev/arrow/KEYS | sudo apt-key add -
sudo tee /etc/apt/sources.list.d/apache-arrow.list <<APT_LINE
deb [arch=amd64] https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/ $(lsb_release --codename --short) main
deb-src https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/ $(lsb_release --codename --short) main
APT_LINE
sudo apt update

pushd ..

TMP_DIR=/tmp
BUILD_DIR="$TMP_DIR/apache-arrow-$ARROW_VERSION-build"

cd "$TMP_DIR"

sudo apt install -y -V cmake
sudo apt install -y -V libboost-all-dev
sudo apt install -y -V autoconf
sudo apt-get build-dep libarrow-dev libarrow-glib-dev |:

if [[ $ARROW_VERSION == "devel" ]]; then
  git clone https://github.com/apache/arrow
  SRC_DIR="$TMP_DIR/arrow/cpp"
else
  if [[ $ARROW_VERSION == "0.13.0" ]]; then
    wget http://archive.apache.org/dist/arrow/arrow-0.13.0/apache-arrow-0.13.0.tar.gz
  else
    wget https://arrowlib.rstudio.com/dist/arrow/arrow-$ARROW_VERSION/apache-arrow-$ARROW_VERSION.tar.gz
  fi
  tar -xvzf apache-arrow-$ARROW_VERSION.tar.gz
  SRC_DIR="$TMP_DIR/apache-arrow-$ARROW_VERSION/cpp"
fi

mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"
cmake -DARROW_BUILD_TESTS=FALSE \
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
