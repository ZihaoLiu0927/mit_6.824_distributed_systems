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
ratio=0
scale=0
if [ $MAX -gt 10 ]; then
    CHECKPOINT=$(($MAX/10))
    scale=$(echo "scale=3; 1.0*$CHECKPOINT/$MAX*100"|bc)
else
    CHECKPOINT=$((10/$MAX))
    scale=$(echo "scale=3; 1.0/$MAX*100"|bc)
fi

for ((i=1; i<=$MAX;i++))
do
    if [ $MAX -gt 10 ]; then
        if !(( $i % $CHECKPOINT )); then
            printf "Test Progress: [%-20s]%.1f%%\r" "${mark}" "${ratio}"
            ratio=$(echo "scale=3; ${ratio}+5"|bc)
            mark="#${mark}"
        fi
    else
        printf "Test Progress: [%-20s]%.1f%%\r" "${mark}" "${ratio}"
        for ((k=0;k<$CHECKPOINT;k++))
        do
            mark="##${mark}"
        done
        ratio=$(echo "scale=3; ${ratio}+$scale"|bc)
    fi

    echo "starting iteration ${i}: " >> testout.txt
    go test -run 2A -race >> testout.txt
    echo "" >> testout.txt

done
printf "Test Progress: [%-20s]%.1f%%\n" "####################" "100"

pass_num=$(cat testout.txt | grep ok | wc -l | xargs)
echo "Running the test a total of ${ITER} times, with ${pass_num} times passed." >> testout.txt

echo "Job done!"
echo "Running the test a total of ${ITER} times, with ${pass_num} times passed."