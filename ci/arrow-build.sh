ARROW_VERSION=$1

pushd ..

sudo apt install -y -V cmake
sudo apt install -y -V libboost-all-dev
sudo apt install -y -V autoconf

if [[ $ARROW_VERSION == "devel" ]]; then
  git clone https://github.com/apache/arrow
  cd arrow/cpp
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
