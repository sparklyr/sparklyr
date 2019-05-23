ARROW_VERSION=$1

pushd .

sudo apt install -y -V cmake
sudo apt install -y -V libboost-all-dev
sudo apt install -y -V autoconf

if [[ $ARROW_VERSION == "devel" ]]; then
  git clone https://github.com/apache/arrow
  cd arrow/cpp
  # arrow@master has a broken R package, pending https://issues.apache.org/jira/browse/ARROW-5361
  # Remove this version pin after that issue is resolved
  git checkout 1ed608aa94f0d22cfa69c81687006b4ebaefa258
else
  wget https://arrowlib.rstudio.com/dist/arrow/arrow-$ARROW_VERSION/apache-arrow-$ARROW_VERSION.tar.gz
  tar -xvzf apache-arrow-$ARROW_VERSION.tar.gz
  cd apache-arrow-$ARROW_VERSION/cpp
fi

mkdir release
cd release
cmake -DARROW_BUILD_TESTS=FALSE \
      -DARROW_WITH_SNAPPY=FALSE \
      -DARROW_WITH_ZSTD=FALSE \
      -DARROW_WITH_LZ4=FALSE \
      -DARROW_JEMALLOC=FALSE \
      -DARROW_WITH_ZLIB=FALSE \
      -DARROW_WITH_BROTLI=FALSE \
      -DARROW_USE_GLOG=FALSE \
      -DARROW_BUILD_UTILITIES=FALSE \
      -DARROW_PARQUET=ON \
      ..

make arrow
sudo make install

popd
