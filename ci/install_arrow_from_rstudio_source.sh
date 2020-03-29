#!/bin/bash

curl https://arrowlib.rstudio.com/ubuntu/red-data-tools-keyring.gpg | sudo apt-key add -
sudo apt install -y -V apt-transport-https lsb-release
sudo tee /etc/apt/sources.list.d/apache-arrow.list <<APT_LINE
deb https://arrowlib.rstudio.com/$(lsb_release --id --short | tr 'A-Z' 'a-z')/ $(lsb_release --codename --short) universe
APT_LINE
sudo apt update
sudo apt install -y -V libarrow-dev # For C++
sudo apt install -y -V libarrow-glib-dev # For GLib (C)
sudo apt install -y -V libparquet-dev # For Apache Parquet C++
sudo apt install -y -V libparquet-glib-dev # For Apache Parquet GLib (C)
