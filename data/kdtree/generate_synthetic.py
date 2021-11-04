import numpy as np

nodes_outfile = 'nodes_synthetic.csv'
queries_outfile = 'queries_synthetic.csv'

scale = 100.0

gauss_x = np.random.normal(0, scale, size=4000000)
gauss_y = np.random.normal(0, scale, size=4000000)
points = list(zip(gauss_x, gauss_y))

with open(nodes_outfile, 'w') as nf:
    for p in points:
        nf.write('%f,%f\n' % (p[0], p[1]))

g_queries_outfile = 'queries_gaussian_%d.csv'

scales = [1.0, 2.0, 5.0, 10.0, 20.0, 50.0]

for s in scales:
    gauss_x = np.random.normal(0, s, size=4000)
    gauss_y = np.random.normal(0, s, size=4000)
    points = list(zip(gauss_x, gauss_y))
    
    with open(g_queries_outfile % s, 'w') as gf:
        for p in points:
            gf.write('%f,%f\n' % (p[0], p[1]))
            
