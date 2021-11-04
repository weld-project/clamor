source = simple_sum.cc
objects = 
executables = simple_sum
CXX=g++
LANGFLAGS=-std=c++17 -mcmodel=large
CXXFLAGS=-O0 -I$(WELD_HOME)/c -I$(CLAMOR_HOME)/ -fnon-call-exceptions
CXXFLAGS += `pkg-config --cflags protobuf grpc`
LDFLAGS=-L$(WELD_HOME)/target/release -L$(CLAMOR_HOME)/smalloc -L$(CLAMOR_HOME)/clamor
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl
LIBS=-lclamor -lpthread -lcurl -lweld -lsmalloc -rdynamic

PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

PROTOS_PATH = .

vpath %.proto $(PROTOS_PATH)

all: $(executables)

simple_sum: simple_sum.o
	   $(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LDFLAGS) $(LIBS)

lookups: lookups.o
	   $(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LDFLAGS) $(LIBS)

%.o: %.cc
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -c -o $@ $< 

.PHONY: clean
clean:
	-rm -f $(executables) *.o *.pb.cc *.pb.h *~
