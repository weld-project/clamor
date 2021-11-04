EXECS=tpch_q1
MPICXX?=mpicxx
CXXFLAGS= -O3 -mcmodel=large
LDFLAGS=
LIBS=

all: ${EXECS}

tpch_q1: tpch_q1.o ../../experiments/cpp/tpch_util/s3utils.o ../../experiments/cpp/tpch_util/s3utils-templates.o
	$(MPICXX) $(CXXFLAGS) -o $@ $+ $(LDFLAGS) $(LIBS)

%.o: %.cc
	$(MPICXX) $(CXXFLAGS) -c -o $@ $< 

clean:
	rm ${EXECS} *.o *~
