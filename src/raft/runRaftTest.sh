#!/bin/bash

usage() {
  echo "Usage: $0 [ -n ITERATION ]" 1>&2 
}

exit_abnormal() {
  usage
  exit 1
}

while getopts ":n:" options; do
    case "${options}" in
    n)
        ITER=${OPTARG}
        ;;
    :)
        echo "Error: -${OPTARG} requires an argument."
        exit_abnormal
        ;;
    *)
        exit_abnormal
        ;;
    esac
done

if [ "$ITER" == "" ]; then
    echo "Must provide a -n to specify how many times to run the test!\n"
    exit_abnormal
fi

echo "Job started..."
echo "Start running raft tests ${ITER} times..." > testout.txt

MAX=$(($ITER))
for ((i=1; i<=$MAX;i++))
do
    echo "starting iteration ${i}..."
    echo "starting iteration ${i}: " >> testout.txt
    #go test -timeout 60s -race -run TestFigure8Unreliable2C >> testout.txt
    go test -run 2C -race >> testout.txt
    echo "" >> testout.txt
done

pass_num=$(cat testout.txt | grep ok | wc -l | xargs)
echo "Running the test a total of ${ITER} times, with ${pass_num} times passed." >> testout.txt

echo "Job done"
echo "Running the test a total of ${ITER} times, with ${pass_num} times passed." 

