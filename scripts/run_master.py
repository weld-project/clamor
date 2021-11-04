import argparse
import csv
import itertools
import json
import math
import subprocess
import sys
import time

from clamor_util import *

def start_master(manager_ip, bench_name, nprocs, worker_ips):
    cmd = "./run-master.sh %s %d %s %s" % (bench_name, nprocs, manager_ip, worker_ips)
    output = subprocess.call(cmd, shell=True)
    return output
            
def main():
    parser = argparse.ArgumentParser(
        description="Run the performance suite for the passed in benchmarks"
    )

    parser.add_argument('-n', "--num_iterations", type=int, default=1,
                        help="Number of iterations to run each benchmark")
    parser.add_argument('-b', "--benchmark", type=str, default=None,
                        help="Benchmark to run")
    parser.add_argument('-s', "--server_names", type=str, default="../boto-scripts/servers.txt",
                        help="Filename containing list of server IPs")
    parser.add_argument('-m', "--master_name", type=str, default="../boto-scripts/master.txt",
                        help="Filename containing master IP")
    parser.add_argument('-k', "--nworkers", type=int, default=1,
                        help="Number of nodes to use")
    parser.add_argument('-p', "--nprocs", type=int, default=1,
                        help="Number of processes per worker")

    args = parser.parse_args()

    server_names = read_ips(args.server_names)
    master_name = read_ips(args.master_name)[0]

    assert args.nworkers <= (len(server_names))

    manager_name = master_name
    worker_names = server_names[:args.nworkers]

    # starts blocking server
    start_master(manager_name, args.benchmark, args.nprocs, ','.join(worker_names))
    
if __name__=="__main__":
    main()
