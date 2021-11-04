source = hashmap.cc
objects = 
executables = hashmap hashmap-worker
CXX=g++
LANGFLAGS=-std=c++17 -mcmodel=large
CXXFLAGS=-O0 -I$(WELD_HOME)/c -I$(CLAMOR_HOME)/ -I$(CLAMOR_HOME)/rust-libs/hashmap/
LDFLAGS=-L$(WELD_HOME)/target/release -L$(CLAMOR_HOME)/smalloc -L$(CLAMOR_HOME)/clamor -L$(CLAMOR_HOME)/rust-libs/hashmap/target/release
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl
LIBS= -lclamor -lpthread -lcurl -lweld -lsmalloc -rdynamic

all: $(executables)

hashmap: hashmap.o
	   $(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LDFLAGS) $(LIBS) -lclamor_hashmap-rsmalloc

hashmap-worker: hashmap.o
	   $(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LDFLAGS) $(LIBS) -lclamor_hashmap-malloc

%.o: %.cc
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -c -o $@ $< 

.PHONY: clean
clean:
	-rm -f $(executables) *.o *.pb.cc *.pb.h *~
