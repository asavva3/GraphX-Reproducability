#!/bin/bash

for i in {1..10}
do
    ./spark-3.2.0-bin-hadoop3.2/bin/spark-submit --class ConnectedComponents --master yarn --deploy-mode cluster  --num-executors 3 --executor-cores 2 --executor-memory 8G graphx-connected-compoenents_2.12-1.0.jar
    FOLDER=$(ls -1 hadoop/logs/userlogs | tail -n 1)
    LAST=$(ls -1 hadoop/logs/userlogs/$FOLDER | head -n 1)
    cp hadoop/logs/userlogs/$FOLDER/$LAST/stdout graphx2nodes/cc-citPatents2nodes/stdout$FOLDER
done
