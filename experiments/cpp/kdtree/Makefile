source = kdtree.cc
objects = 
executables = kdtree kdtree-worker
CXX=g++
LANGFLAGS=-std=c++17 -mcmodel=large
CXXFLAGS=-O0 -g -ggdb -I$(WELD_HOME)/c -I$(CLAMOR_HOME)/ -I$(CLAMOR_HOME)/rust-libs/kdtree-rs/
LDFLAGS=-L$(WELD_HOME)/target/debug -L$(CLAMOR_HOME)/smalloc -L$(CLAMOR_HOME)/clamor -L$(CLAMOR_HOME)/rust-libs/kdtree-rs/target/release
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl
LIBS= -lclamor -lpthread -lcurl -lweld -lsmalloc -rdynamic

all: $(executables)

kdtree: kdtree.o
	   $(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LDFLAGS) $(LIBS) -lkdtree-rsmalloc 

kdtree-worker: kdtree.o
	   $(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LDFLAGS) $(LIBS) -lkdtree-malloc 

%.o: %.cc
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -c -o $@ $< 

.PHONY: clean
clean:
	-rm -f $(executables) *.o *.pb.cc *.pb.h *~
