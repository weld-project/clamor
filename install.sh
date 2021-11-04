#!/bin/bash
echo 0 | sudo tee /proc/sys/kernel/randomize_va_space
export LD_LIBRARY_PATH=/usr/local/lib
sudo apt-get update
sudo apt-get install git build-essential autoconf libtool pkg-config libgflags-dev libgtest-dev clang libc++-dev unzip libcurl4-openssl-dev libboost-dev cargo zlib1g-dev llvm-6.0 llvm-6.0-dev clang-6.0
curl https://sh.rustup.rs -sSf | sh
source $HOME/.cargo/env
rustup update stable
sudo ln -s /usr/bin/llvm-config-6.0 /usr/local/bin/llvm-config
git clone -b v1.15.1 https://github.com/grpc/grpc
cd grpc
git submodule update --init
cd third_party/protobuf
./autogen.sh
./configure CFLAGS="-m64 -fPIC -DNDEBUG" CXXFLAGS="-m64 -fPIC -DNDEBUG" LDFLAGS=-m64
make
sudo make install
cd ../../
make -j
sudo make install
cd ../
git clone http://github.com/weld-project/clamor
git clone http://github.com/pratiksha/weld
cd clamor
git pull
cd smalloc
make
cd ~/weld
git pull
git checkout clamor-ll
echo "export CLAMOR_HOME=~/clamor" >> ~/.local_vars
echo "export WELD_HOME=~/weld" >> ~/.local_vars
echo "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:~/clamor/smalloc:~/clamor/clamor:~/weld/target/debug" >> ~/.local_vars
source ~/.local_vars
cargo build --release
cd ~/clamor/clamor
make clean
make
