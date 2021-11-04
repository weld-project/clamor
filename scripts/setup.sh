#!/bin/bash
echo 0 | sudo tee /proc/sys/kernel/randomize_va_space
echo 1 | sudo tee /proc/sys/vm/overcommit_memory
ulimit -s unlimited
source ~/.local_vars
export PUBLIC_HOSTNAME="$(curl http://169.254.169.254/latest/meta-data/public-hostname 2>/dev/null)"
