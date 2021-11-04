import argparse
import csv
import itertools
import json
import math
import subprocess
import sys
import time

aws_key = "prthaker-slate.pem"

def run_cmd(server_name, script):
    cmd = "ssh -A -o StrictHostKeyChecking=no -i %s ubuntu@%s 'bash -s' < %s" % (aws_key, server_name, script)
    output = subprocess.check_output(cmd, shell=True)
    return output

def read_ips(ip_fname):
    with open(ip_fname, 'r') as f:
        return [l.strip() for l in f.readlines()]
            
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

    args = parser.parse_args()

    server_names = read_ips(args.server_names)
    master_name = read_ips(args.master_name)[0]

    assert args.nworkers <= (len(server_names))

    for name in server_names + [master_name]:
        run_cmd(name, "make.sh")

    
if __name__=="__main__":
    main()
