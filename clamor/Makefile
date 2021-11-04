OS = $(shell uname -s)
LLVM_VERSION = $(shell llvm-config --version | cut -d . -f 1,2)

CXX=g++
LANGFLAGS=-std=c++17 -mcmodel=large 
CXXFLAGS= -O0 -g -ggdb -I$(WELD_HOME)/c -I$(CLAMOR_HOME) -fPIC -fnon-call-exceptions
CXXFLAGS += `pkg-config --cflags protobuf grpc`
LDFLAGS=-L$(WELD_HOME)/target/release -L$(CLAMOR_HOME)/smalloc
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl
LIBS=-lpthread -lcurl -lweld -lsmalloc -lstdc++fs

PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

PROTOS_PATH = .

SOURCES := dsm.pb.cc dsm.grpc.pb.cc util-templates.cc $(wildcard *.cc) 
OBJECTS := $(patsubst %.cc,%.o,$(SOURCES))
DEPENDS := $(patsubst %.cc,%.d,$(SOURCES))

ifeq (${OS}, Darwin)
  # OS X
  CLANG ?= clang++-${LLVM_VERSION}
	LIB = libclamor.so
else ifeq (${OS}, Linux)
  # Linux
  CLANG ?= clang++-${LLVM_VERSION}
	LIB = libclamor.so
else
  $(error Unsupported platform: ${OS})
endif

# Allocate less memory for local testing
ifeq (${LOCAL}, true)
DLOCAL=-Dclamor_local
endif

vpath %.proto $(PROTOS_PATH)

all: ${LIB}

${LIB}: $(OBJECTS)
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) $(DLOCAL) -shared -o $@ $^ $(LDFLAGS) $(LIBS)

%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

-include $(DEPENDS)

%.o: %.cc 
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) $(DLOCAL) -MMD -MP -c -o $@ $< 

.PHONY: clean
clean:
	-rm -f ${LIB} *.o *.pb.cc *.pb.h *~
