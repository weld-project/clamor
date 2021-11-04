infile = 'queries-ca-restaurants-2_extended.csv'
#infile = 'rest-small.csv'
outfile = 'queries-ca-restaurants-full.csv'

inf = open(infile, 'r')
of = open(outfile, 'w') 
for line in inf:
    x = line.strip().split(',')[:2]
    f = (float(x[0]), float(x[1])) 
    of.write("{:=08.03f},{:=08.03f}\n".format(f[0], f[1]))
    of.flush()
