#!/bin/bash


SCRIPT_DIR=$(readlink -f ${0%/*})
echo 'You are in directory: '$SCRIPT_DIR

##for local test
# startflink='../flink-1.3.2/bin/start-local.sh'
# stopflink='../flink-1.3.2/bin/stop-local.sh'

#startflink='../flink-1.3.2/bin/start-cluster.sh'
#stopflink='../flink-1.3.2/bin/stop-cluster.sh'

flink='../flink-1.3.2/bin/flink'
jarfile='./proteus-solma-development/target/proteus-solma_2.11-0.1.3-jar-with-dependencies.jar'
timefile='./data/TimeResults_GGMM.txt'





#GGMM Variables
GGMMClass='eu.proteus.solma.ggmm.GGMM_Cluster'
GGMMinput='/share/hadoop/tmp/cassini-bigtest-labels.csv'
GGMMOut='/share/flink/onlineML/data/GGMM/'

now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "----------$today $now------------" >> $timefile



echo "-----------Beginning of GGMM-----------" >> $timefile
for windowSize in 100 200
do
for parallelism in 20
do
for PSParallelism in 1
do
echo "GGMM_workerp"$parallelism"_W"$windowSize"_PSpara"$PSParallelism" start"
START=$(date +%s)
$flink run -c $GGMMClass $jarfile $parallelism $PSParallelism 1 $windowSize $GGMMinput $GGMMOut"parallelism"$parallelism"_W"$windowSize"_PSP"$PSParallelism".txt" 1
END=$(date +%s)
DIFF=$((END-START))
echo "GGMM_workerp"$parallelism"_W"$windowSize"_PSpara"$PSParallelism": "$DIFF"s" >> $timefile
done
done
done
echo "--------------End of GGMM---------------" >> $timefile
