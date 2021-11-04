#!/bin/bash
sudo killall $1
sudo fuser -k 50000/tcp
sudo fuser -k 50001/tcp
sudo fuser -k 50002/tcp
sudo fuser -k 50003/tcp
sudo fuser -k 50004/tcp
sudo fuser -k 60000/tcp
sudo fuser -k 60001/tcp
sudo fuser -k 60002/tcp
sudo fuser -k 60003/tcp
sudo fuser -k 60004/tcp
sudo fuser -k 40000/tcp
sudo fuser -k 70000/tcp
