#!/bin/bash

for file in graphx2nodes/cc-citPatents2nodes/*
do
  line=$(head -n 1 $file)
  result=$(echo $line | sed s/"Time taken: "// | sed s/" ms"//)
  echo "$result" >> "resultsCCcitPatents2nodes.txt"
done