ARROW_VERSION="0.12.0"

pushd .

sudo apt install -y -V cmake
sudo apt install -y -V libboost-all-dev
sudo apt install -y -V autoconf
sudo apt install -y -V flex
sudo apt install -y -V bison

wget https://arrowlib.rstudio.com/dist/arrow/arrow-$ARROW_VERSION/apache-arrow-$ARROW_VERSION.tar.gz
tar -xvzf apache-arrow-$ARROW_VERSION.tar.gz
cd apache-arrow-$ARROW_VERSION/cpp
mkdir release
cd release
cmake -DARROW_BUILD_TESTS=ON -DARROW_PARQUET=ON ..
make arrow
sudo make install

popd
