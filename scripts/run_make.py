import argparse
import subprocess
import sys

aws_key = "prthaker.pem"

def run_cmd(server_name, script):
    cmd = "ssh -A -i %s ubuntu@%s 'bash -s' < %s" % (aws_key, server_name, script)
    output = subprocess.check_output(cmd, shell=True)
    return output

def run_cmd_nonblock(server_name, script):
    cmd = "ssh -A -i %s ubuntu@%s 'bash -s' < %s" % (aws_key, server_name, script)
    subprocess.Popen(cmd, shell=True)

def run_make(server_ips):
    for s in server_ips:
        run_cmd_nonblock(s, "make.sh")

def run_kill(server_ips, bench_name):
    for s in server_ips:
        run_cmd_nonblock(s, "killserver.sh %s" % bench_name)

def read_ips(ip_fname):
    with open(ip_fname, 'r') as f:
        return [l.strip() for l in f.readlines()]

def main():
    parser = argparse.ArgumentParser(
        description="Run the performance suite for the passed in benchmarks"
    )

    parser.add_argument('-s', "--server_names", type=str, default=None,
                        help="Filename containing list of server IPs")
    parser.add_argument('-k', "--kill_servers", action='store_true',
                        help="Kill servers")
    parser.add_argument('-b', "--benchmark", type=str, default=None,
                        help="Benchmark to run")

    args = parser.parse_args()
    
    server_names = read_ips(args.server_names)
    if args.kill_servers:
        run_kill(server_names, args.benchmark)
        return
    
    run_make(server_names)
    
if __name__=="__main__":
    main()