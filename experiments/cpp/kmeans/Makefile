source = kmeans.cc
objects = 
executables = kmeans
CXX=g++
LANGFLAGS=-std=c++17 -mcmodel=large
CXXFLAGS=-O3 -I$(WELD_HOME)/c -I$(CLAMOR_HOME)/ 
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

kmeans: kmeans-ec2.o argmin.o
	   $(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LDFLAGS) $(LIBS)

kmeans_local: kmeans_local.o
	   $(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LDFLAGS) $(LIBS)

%.o: %.cc
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -c -o $@ $< 

.PHONY: clean
clean:
	-rm -f $(executables) *.o *.pb.cc *.pb.h *~
