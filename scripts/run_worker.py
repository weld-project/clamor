import argparse
import csv
import itertools
import json
import math
import subprocess
import sys
import time

from clamor_util import *

aws_key = "prthaker-slate.pem"

def start_worker(bench_name, manager_name, worker_id):
    cmd = "./runserver.sh %s %d %s" % (bench_name, worker_id, manager_name)
    print cmd
    output = subprocess.call(cmd, shell=True)
    return output
    
def start_workers(worker_ips, bench_name, nprocs, manager_name):
    for ip in worker_ips:
        for i in range(nprocs):
            cmd = "runserver.sh %s %d %s & sleep 0.1" % (bench_name, i, manager_name)
            print cmd
            run_cmd_nonblock(ip, cmd)

def main():
    parser = argparse.ArgumentParser(
        description="Run the performance suite for the passed in benchmarks"
    )

    parser.add_argument('-n', "--num_iterations", type=int, default=1,
                        help="Number of iterations to run each benchmark")
    #parser.add_argument('-f', "--output_fname", type=str, required=True,
    #                    help="Name of CSV to dump output in")
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
    parser.add_argument('-i', "--worker-idx", type=int, default=0,
                        help="Worker process num on this machine")
    
    args = parser.parse_args()

    server_names = read_ips(args.server_names)
    master_name = read_ips(args.master_name)[0]

    assert args.nworkers <= (len(server_names))

    manager_name = master_name
    worker_names = server_names[:args.nworkers]

    # starts blocking server
    start_worker(args.benchmark, manager_name, args.worker_idx)
    
if __name__=="__main__":
    main()
