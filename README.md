# Clamor

Clamor is a framework that augments parallel data analytics frameworks with
fine-grained access to large global variables.
This allows significant performance improvements for workloads with sparse accesses,
where a Spark-based solution would require broadcasting the entire variable.

Clamor was published in SoCC 2021. The [paper](https://dl.acm.org/doi/abs/10.1145/3472883.3486996)
describes Clamor's architecture and demonstrates its performance improvements over comparable frameworks.

## Disclaimer

We appreciate your interest in using Clamor. This is a research prototype and as such there may be issues
preventing it from being used out of the box for new applications. We cannot promise to provide extensive
technical support, but we appreciate issues, pull requests, or comments/questions to prthaker@stanford.edu.

## Dependencies

The script `install.sh` downloads the required repositories and compiles gRPC, Weld, and Clamor.

Clamor uses the `spdlog` logging library, which we package with the repository.

Clamor depends on the following libraries, which are available through `apt-get`:
```
build-essential autoconf libtool pkg-config libgflags-dev libgtest-dev clang libc++-dev unzip libcurl4-openssl-dev libboost-dev 
```

Weld (and the Clamor-enabled Rust libraries) additionally require the following:
```
cargo llvm-6.0 llvm-6.0-dev clang-6.0 zlib1g-dev
```

Finally, Clamor uses `grpc` and `protobuf` for RPCs:
```
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
```
Note the compiler flags for `./configure`.

## Installation

Clone Weld from the Clamor fork:
```
git clone https://github.com/pratiksha/weld/tree/clamor-ll
cd weld/
git checkout clamor-ll
export WELD_HOME=`pwd`
cd ..
```

Clone the Clamor repository:
```
git clone https://github.com/pratiksha/clamor
cd clamor
export CLAMOR_HOME=`pwd`
```

If running locally with small memory for testing, use the `LOCAL` environment variable
to allocate a smaller shared address space at compile time:
```
export LOCAL=true
```

Build the memory allocation library (needed to link with Weld):
```
cd smalloc/
make
cd ../..
```

Build Weld:
```
cd weld/
cargo build --release
cd ..
```

Build Clamor:
```
cd clamor/clamor
make
cd ..
```

Clamor requires global configuration changes to run:
```
./scripts/disable-aslr.sh # disable ASLR
ulimit -s unlimited # disable stack space limits
```

## Running Clamor

To build a simple example:
```
cd experiments/cpp/simple_sum
make
```

This example downloads some integers from an Amazon S3 bucket and sums them.
We can try to run the simple example locally with a single worker.

To run the simple example we first need to start the memory manager:
```
./simple_sum -i 0.0.0.0 -m manager -r 40000 -l simple_sum.weld -t 1 -w 0.0.0.0
```

We then connect a worker:
```
./simple_sum -i 0.0.0.0 -m worker -p 50000 -d 0.0.0.0 -l simple_sum.weld
```

Finally, we can start the driver, which runs the sum program:
```
./simple_sum -i 0.0.0.0 -m master -p 70000 -d 0.0.0.0 -l simple_sum.weld -t 1 -w 0.0.0.0
```

For a more complex example involving lookups into a large hash table, see `experiments/cpp/hashmap`.

## Clamor cluster management

[Cloister](https://github.com/pratiksha/cloister) provides some rudimentary functionality
to set up clusters using Amazon EC2 and a script to launch Clamor experiments using Cloister-created clusters.

Clamor does not require Cloister to run. Other options for launching clusters,
such as [Flintrock](https://github.com/nchammas/flintrock), may be better maintained.
