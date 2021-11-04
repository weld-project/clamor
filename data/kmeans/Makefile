source = generate-data.cc
objects = 
executables = generate-data read-doubles
CXX = g++
LANGFLAGS = -std=c++14
CXXFLAGS = -g -ggdb -O3 -pedantic -Werror -Wall -Wextra -Weffc++
LIBS =

all: $(executables)

generate-data: generate-data.o $(objects)
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LIBS)

generate-csv-data: generate-csv-data.o $(objects)
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LIBS)

read-doubles: read-doubles.o $(objects)
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -o $@ $+ $(LIBS)

%.o: %.cc
	$(CXX) $(LANGFLAGS) $(CXXFLAGS) -c -o $@ $<

.PHONY: clean
clean:
	-rm -f $(executables) *.o
