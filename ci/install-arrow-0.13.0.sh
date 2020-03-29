
sudo apt update
sudo apt install -y -V apt-transport-https lsb-release
curl https://dist.apache.org/repos/dist/dev/arrow/KEYS | sudo apt-key add -
sudo tee /etc/apt/sources.list.d/apache-arrow.list <<APT_LINE
deb [arch=amd64] https://dl.bintray.com/apache/arrow/ubuntu/ xenial main
deb-src https://dl.bintray.com/apache/arrow/ubuntu/ xenial main
APT_LINE
sudo apt update
sudo apt install -y libarrow16 libarrow-glib-dev libarrow-dev
