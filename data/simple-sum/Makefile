source = write-ints.cc read-ints.cc 
objects = 
executables = write-ints read-ints write-csv-ints
CXX = g++
LANGFLAGS = -std=c++14
CXXFLAGS = -g -ggdb -O3 -pedantic -Werror -Wall -Wextra -Weffc++
LIBS =

all: $(executables)

write-ints: write-ints.o $(objects)
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LIBS)

write-csv-ints: write-csv-ints.o $(objects)
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LIBS)

read-ints: read-ints.o $(objects)
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LIBS)

%.o: %.cc
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -c -o $@ $<

.PHONY: clean
clean:
	-rm -f $(executables) *.o
