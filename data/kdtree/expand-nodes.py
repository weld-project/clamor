import numpy as np

base = 'queries-ca-restaurants-2'
infile = base + '.csv'
outfile = base + '_extended.csv'
infile_len = 6992

scale = 0.5
total_points = 1000000000 # 1 billion queries
points_per_node = int(np.ceil(total_points / infile_len))
print(points_per_node, points_per_node * infile_len)
gauss_x = np.random.normal(0, scale, size=points_per_node)
gauss_y = np.random.normal(0, scale, size=points_per_node)
points = list(zip(gauss_x, gauss_y))

inf = open(infile, 'r')
lines = [l.strip().split(',') for l in inf.readlines()]
nodes = [(float(x[0]), float(x[1])) for x in lines]

with open(outfile, 'w') as nf:
    for n in nodes:
        for p in points:
            nf.write('%f,%f\n' % (p[0] + n[0], p[1] + n[1]))
