CXX=g++
LANGFLAGS=-std=c++17 -mcmodel=large -rdynamic
CXXFLAGS= -O3

all: s3utils.o s3utils-templates.o

%.o: %.cpp
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -c -o $@ $< 

.PHONY: clean
clean:
	-rm -f *.o 
