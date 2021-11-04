EXECS=kmeans
MPICXX?=mpicxx
CXXFLAGS= -O3 -mcmodel=large
LDFLAGS=
LIBS= -lcurl

all: ${EXECS}

kmeans: curl-handle.o kmeans.o 
	$(MPICXX) $(CXXFLAGS) -o $@ $+ $(LDFLAGS) $(LIBS)

%.o: %.cc
	$(MPICXX) $(CXXFLAGS) -c -o $@ $< 

clean:
	rm ${EXECS} *.o *~
