#!/bin/bash

usage() {
  echo "Usage: $0 [ -n ITERATION ] [ -t WHICH-TEST ]" 1>&2 
}

exit_abnormal() {
  usage
  exit 1
}

while getopts ":n:t:" options; do
    case "${options}" in
    n)
        ITER=${OPTARG}
        ;;
    t)
        TEST=${OPTARG}
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

if [ "$TEST" == "" ]; then
    echo "Must provide a -t to specify which test to run! Can be 2A, 2B, 2C or 2D\n"
    exit_abnormal
fi

OUTFILE="testout_${TEST}.txt"

echo "Job started..."
echo "Start running raft tests ${ITER} times..." > ${OUTFILE}

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

start=`date +%s`

for ((i=1; i<=$MAX;i++))
do
    if [ $MAX -gt 10 ]; then
        if !(( $i % $CHECKPOINT )); then
            printf "Test Progress: [%-20s]%.1f%%\r" "${mark}" "${ratio}"
            ratio=$(echo "scale=3; ${ratio}+10"|bc)
            mark="##${mark}"
        fi
    else
        printf "Test Progress: [%-20s]%.1f%%\r" "${mark}" "${ratio}"
        for ((k=0;k<$CHECKPOINT;k++))
        do
            mark="##${mark}"
        done
        ratio=$(echo "scale=3; ${ratio}+$scale"|bc)
    fi

    echo "starting iteration ${i}: " >> ${OUTFILE}
    go test -run ${TEST} -race >> ${OUTFILE}
    echo "" >> ${OUTFILE}

done
printf "Test Progress: [%-20s]%.1f%%\n" "####################" "100"

end=`date +%s.%N`
runtime=$( echo "$end - $start" | bc -l )

pass_num=$(cat ${OUTFILE} | grep PASS$ | wc -l | xargs)
echo "Running the test a total of ${ITER} times, with ${pass_num} times passed. \nTotal time spent: ${runtime} seconds." >> ${OUTFILE}

echo "Job done!"
echo "Running the test a total of ${ITER} times, with ${pass_num} times passed. \nTotal time spent: ${runtime} seconds."
