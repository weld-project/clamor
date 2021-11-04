import argparse
import csv
import itertools
import json
import math
import subprocess
import sys
import time

# Copy a file to a given host through scp, throwing an exception if scp fails
def scp(host, identity_file, user, local_file, dest_file):
  subprocess.check_call(
      "scp -q -o StrictHostKeyChecking=no -i %s '%s' '%s@%s:%s'" %
      (identity_file, local_file, user, host, dest_file), shell=True)

def ssh(host, identity_file, user):
    subprocess.check_call("ssh -o StrictHostKeyChecking=no -i %s %s@%s" %
                          (identity_file, user, host), shell=True)

def run_cmd(server_name, script):
    cmd = "ssh -A -o StrictHostKeyChecking=no -i %s ubuntu@%s 'bash -s' < %s" % (aws_key, server_name, script)
    output = subprocess.check_output(cmd, shell=True)
    return output

def run_cmd_nonblock(server_name, script):
    cmd = "ssh -A -o StrictHostKeyChecking=no -i %s ubuntu@%s 'bash -s' < %s" % (aws_key, server_name, script)
    print cmd
    subprocess.Popen(cmd, shell=True)
    
def read_ips(ip_fname):
    with open(ip_fname, 'r') as f:
        return [l.strip() for l in f.readlines()]

def read_config(config_file):
    with open(config_file, 'r') as f:
        return json.load(f)
      
