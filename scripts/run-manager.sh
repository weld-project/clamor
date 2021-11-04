#!/bin/bash
echo 0 | sudo tee /proc/sys/kernel/randomize_va_space
echo 1 | sudo tee /proc/sys/vm/overcommit_memory
ulimit -s unlimited
source ~/.local_vars
export PUBLIC_HOSTNAME="$(curl http://169.254.169.254/latest/meta-data/public-hostname 2>/dev/null)"
cd $HOME/clamor/experiments/cpp/$1
curdate=$(date +"%m%d%Y_%H%M")
echo ./$1 -i $PUBLIC_HOSTNAME -m manager -r 40000 -t $2 -l $1.weld -w $3
./$1 -i $PUBLIC_HOSTNAME -m manager -r 40000 -t $2 -l $1.weld -w $3 > manager_$curdate.log
