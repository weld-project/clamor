import argparse
import csv
import itertools
import json
import math
import numpy as np
import subprocess
import sys
import time

import clamor_util

aws_key = "prthaker-slate.pem"

def run_cmd(server_name, script):
    cmd = "ssh -A -o StrictHostKeyChecking=no -i %s ubuntu@%s 'bash -s' < %s" % (aws_key, server_name, script)
    output = subprocess.check_output(cmd, shell=True)
    return output

def run_cmd_nonblock(server_name, script):
    cmd = "ssh -A -o StrictHostKeyChecking=no -i %s ubuntu@%s 'bash -s' < %s" % (aws_key, server_name, script)
    print cmd
    subprocess.Popen(cmd, shell=True)

def start_manager(manager_ip, bench_name, nprocs, worker_ips):
    run_cmd_nonblock(manager_ip, "run-manager.sh %s %d %s" % (bench_name, nprocs, worker_ips))

def start_master(master_ip, bench_name, nprocs, manager_name, worker_ips):
    run_cmd_nonblock(master_ip, "run-master.sh %s %d %s %s" % (bench_name, nprocs, manager_name, worker_ips))

def start_workers(worker_ips, bench_name, nprocs, manager_name):
    for ip in worker_ips:
        for i in range(nprocs):
            cmd = "runserver.sh %s %d %s & sleep 0.1" % (bench_name, i, manager_name)
            print cmd
            run_cmd_nonblock(ip, cmd)

def kill_servers(server_ips, bench_name):
    for ip in server_ips:
        run_cmd(ip, "killserver.sh %s" % bench_name)
            
def read_ips(ip_fname):
    with open(ip_fname, 'r') as f:
        return [l.strip() for l in f.readlines()]
            
def main():
    parser = argparse.ArgumentParser(
        description="Run the performance suite for the passed in benchmarks"
    )

    #parser.add_argument('-n', "--num_iterations", type=int, default=1,
    #                    help="Number of iterations to run each benchmark")
    #parser.add_argument('-f', "--output_fname", type=str, required=True,
    #                    help="Name of CSV to dump output in")
    parser.add_argument('-b', "--benchmark", type=str, default=None,
                        help="Benchmark to run")
    parser.add_argument('-s', "--server_names", type=str, default='../boto-scripts/servers.txt',
                        help="Filename containing list of server IPs")
    parser.add_argument('-m', "--master_name", type=str, default='../boto-scripts/master.txt',
                        help="Filename containing master IP")
    parser.add_argument('-k', "--nworkers", type=int, default=1,
                        help="Number of nodes to use")
    parser.add_argument('-p', "--nprocs", type=int, default=1,
                        help="Number of processes per worker")
    parser.add_argument('-z', "--no-master", action='store_true',
                        help="Only set up servers without starting master")

    args = parser.parse_args()

    server_names = read_ips(args.server_names)
    master_name = read_ips(args.master_name)[0]

    assert args.nworkers <= (len(server_names))

    manager_name = master_name
    worker_names = server_names[:args.nworkers]

    # don't use this - SSH to master node to start memory manager for experiments, instead
    #    start_manager(manager_name, args.benchmark, args.nprocs, ','.join(worker_names))
    #    time.sleep(10)
    start_workers(worker_names, args.benchmark, args.nprocs, manager_name)
    time.sleep(10) # hack: wait for workers to come up

    if not args.no_master:
        start_master(master_name, args.benchmark, args.nprocs, manager_name, ','.join(worker_names))

    time.sleep(1000) # hack: wait for end
    #kill_servers(server_names, args.benchmark)
    
if __name__=="__main__":
    main()
