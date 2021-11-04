executables = ps
CXX=g++
LANGFLAGS=-std=c++17 -mcmodel=large
CXXFLAGS=-O3 -g -ggdb -I$(WELD_HOME)/c -I$(CLAMOR_HOME)/ 
LDFLAGS=-L$(WELD_HOME)/target/debug -L$(CLAMOR_HOME)/smalloc -L$(CLAMOR_HOME)/clamor
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl -Wall
LIBS= -lclamor -lpthread -lcurl -lweld -lsmalloc -rdynamic -fopenmp 

all: $(executables)

ps: ps-array.o
	   $(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LDFLAGS) $(LIBS)

%.o: %.cc
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -c -o $@ $< 

.PHONY: clean
clean:
	-rm -f $(executables) *.o *.pb.cc *.pb.h *~
