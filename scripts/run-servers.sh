#!/bin/bash
for i in $(eval echo {0..$3});
do ssh -A -i $1 ubuntu@$2 'bash -s' < runserver.sh $i $i & sleep 0.1;
done