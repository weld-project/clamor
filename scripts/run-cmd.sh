#!/bin/bash
ssh -A -i $1 ubuntu@$2 'bash -s' < $3&