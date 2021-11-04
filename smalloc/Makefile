OS = $(shell uname -s)
LLVM_VERSION = $(shell llvm-config --version | cut -d . -f 1,2)

CXXFLAGS = -g -ggdb -O0 -Wall -fPIC -fnon-call-exceptions -mcmodel=large
CXX=g++ 

ifeq (${OS}, Darwin)
  # OS X
  CLANG ?= clang-${LLVM_VERSION}
	LIB = libsmalloc.so
else ifeq (${OS}, Linux)
  # Linux
  CLANG ?= clang-${LLVM_VERSION}
	LIB = libsmalloc.so
else
  $(error Unsupported platform: ${OS})
endif

# Allocate less memory for local testing
ifeq (${LOCAL}, true)
DLOCAL=-Dclamor_local
endif

all: ${LIB}

${LIB}: smalloc.o
	$(CXX) $(CXXFLAGS) $(DLOCAL) -shared -o $@ $^

%.o: %.cc
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) $(DLOCAL) -c -o $@ $< 

clean:
	rm -f *.o ${LIB}

.phony: all
