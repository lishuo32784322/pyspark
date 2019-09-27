#!/bin/bash
export PYSPARK_PYTHON=/usr/bin/python3;
/usr/bin/spark-submit --master yarn --deploy-mode client --driver-memory 2g --num-executors 3  --executor-memory 2g --executor-cores 2 /mnt/bigdata_workspace/pyspark/prod/report/count_top.py
