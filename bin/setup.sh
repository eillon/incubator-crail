#!/usr/bin/env bash

if [ `whoami` != "root" ]; then
	echo "RUN this script with root"
	exit
fi

# create data and cache path
if [ ! -d "/dev/hugepages/cache" ]; then
	mkdir /dev/hugepages/cache
fi

if [ ! -d "/dev/hugepages/data" ]; then
	mkdir /dev/hugepages/data
fi

# add executive authority to files in bin/ libexec/ conf/crail-env.sh 
chmod +x bin/*
chmod +x libexec/*
chmod +x conf/crail-env.sh

echo "CRAIL PREPARE DONE."