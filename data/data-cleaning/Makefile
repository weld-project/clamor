source = 311-to-bin.cc
objects = 
executables = 311-to-bin
CXX = g++
LANGFLAGS = -std=c++14
CXXFLAGS = -g -ggdb -O3 -pedantic -Werror -Wall -Wextra -Weffc++
LIBS =

all: $(executables)

311-to-bin: 311-to-bin.o $(objects)
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LIBS)

%.o: %.cc
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -c -o $@ $<

.PHONY: clean
clean:
	-rm -f $(executables) *.o
