#!/bin/bash
echo 0 | sudo tee /proc/sys/kernel/randomize_va_space
export LD_LIBRARY_PATH=/usr/local/lib
sudo apt-get update
yes Y | sudo apt-get install emacs git build-essential autoconf libtool pkg-config libgflags-dev libgtest-dev clang libc++-dev unzip libcurl4-openssl-dev htop zlib1g-dev python3.6-config python3.6-dev
curl -sSf https://static.rust-lang.org/rustup.sh | sh
rustup update stable
sudo apt-get install llvm-6.0
sudo apt-get install llvm-6.0-dev
sudo apt-get install clang-6.0
sudo ln -s /usr/bin/llvm-config-6.0 /usr/local/bin/llvm-config
git clone -b $(curl -L https://grpc.io/release) https://github.com/grpc/grpc
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
echo "github.com ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAq2A7hRGmdnm9tUDbO9IDSwBK6TbQa+PXYPCPy6rbTrTtw7PHkccKrpp0yVhp5HdEIcKr6pLlVDBfOLX9QUsyCOV0wzfjIJNlGEYsdlLJizHhbn2mUjvSAHQqZETYP81eFzLQNnPHt4EVVUh7VfDESU84KezmD5QlWpXLmvU31/yMf+Se8xhHTvKSCZIFImWwoG6mbUoWf9nzpIoaSjB+weqqUUmpaaasXVal72J+UX2B+2RPW3RcT0eOzQgqlJL3RKrTJvdsjE3JEAvGq3lGHSZXy28G3skua2SmVi/w4yCE6gbODqnTWlg7+wC604ydGXA8VJiS5ap43JXiUFFAaQ==" >> ~/.ssh/known_hosts
git clone git@github.com:pratiksha/clamor.git
git clone git@github.com:pratiksha/weld.git
cd clamor
git pull
cd smalloc
make
cd ~/weld
git pull
git checkout clamor
git checkout 309c2672b0264f57fd2446e8d77e7d50503fc956
echo "export CLAMOR_HOME=~/clamor" >> ~/.local_vars
echo "export WELD_HOME=~/weld" >> ~/.local_vars
echo "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:~/clamor/smalloc:~/clamor/clamor:~/weld/target/debug" >> ~/.local_vars
source ~/.local_vars
cargo build
cd ~/clamor/clamor
make clean
make
