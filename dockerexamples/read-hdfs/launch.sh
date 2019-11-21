#!/bin/bash

kinit -kt /source/$1 $2
python /source/read-hdfs.py $3 $4