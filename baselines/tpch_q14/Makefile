EXECS=tpch_q14
MPICXX?=mpicxx
CXXFLAGS= -O3 -mcmodel=large
LDFLAGS=
LIBS=

all: ${EXECS}

tpch_q14: tpch_q14.o ../../experiments/cpp/tpch_util/s3utils.o ../../experiments/cpp/tpch_util/s3utils-templates.o
	$(MPICXX) $(CXXFLAGS) -o $@ $+ $(LDFLAGS) $(LIBS)

%.o: %.cc
	$(MPICXX) $(CXXFLAGS) -c -o $@ $< 

clean:
	rm ${EXECS} *.o *~
