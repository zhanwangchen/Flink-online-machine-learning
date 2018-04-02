#!/bin/bash
#SGD_Cluster Parameter:
#args0 = Parallelism Environment (Int)
#args1 = input file path
#args2 = WindowSize (Int)
#args3 = output file path
#args4 = InterationWaitTime
#args5 = PSParallelism
#args6 = WorkerParallelism
#ALS_Cluster Parameter:
#args0 = Parallelism Environment (Int)
#args1 = WindowSize (Int)
#args2 = setLabeledVectorOutput (bool)
#args3 = input file path
#args4 = output file path
#args5 = IterationWaitTime
#args6 = PSParallelism (Int)
#args7 = WorkerParallelism (Int)
#CluStream_Cluster Parameter:
#args0 = Parallelism Environment (Int)
#args1 = input file path
#args2 = WindowSize (Int)
#args3 = ClusterRange (Double)
#args4 = output file path
#HT Parameter:
#args0 = Parallelism Environment (Int)
#args1 = input file path
#args2 = WindowSize (Int)
#args3 = PSParallel
#args4 = Worker Parallel
#args5 = output file path
#args6 = LabeledVectorOutput (boolean)
#args7 = treebound (Double)

startflink='/share/flink/flink-1.1.3/bin/start-cluster.sh'
stopflink='/share/flink/flink-1.1.3/bin/stop-cluster.sh'
flink='/share/flink/flink-1.1.3/bin/flink'
jarfile='./proteus-solma-development/target/proteus-solma_2.11-0.1.3-jar-with-dependencies.jar'
timefile='/home/ziehn/results/TimeResults.txt'
bigDataset='/home/ziehn/data/TurboFanforSGDALS.txt'
#SGD variables
SGDstartClass='eu.proteus.solma.SGD.SGD_Cluster'
SGDpath='/home/ziehn/results/SGD/'
#ALS variables
ALSstartClass='eu.proteus.solma.ALS.ALS_Cluster'
ALSpath='/home/ziehn/results/ALS/'
#CluStream Variables
CluStreamStartClass='eu.proteus.solma.CluStream.CluStream_Cluster'
CluStreamInput='/home/ziehn/data/settings.csv'
CluStreamPath='/home/ziehn/results/CluStream/'
#HT Variables
HTstartClass='eu.proteus.solma.HoeffdingTree.HT_Cluster'
HTinput='/home/ziehn/data/HT_bigDS_AdultTree_Train.txt'
HTpath='/home/ziehn/results/HT/'
echo "Flink start"
$startflink
now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "Current time : $today $now" >> $timefile
echo "----------$today $now------------" >> $timefile
echo "-------Beginning of CluStream----------" >> $timef


for parallelism in 4 8 16 24 32
do
for windowSize in 100 200 600 1000 2000 5000 10000 50000
do
for maxClusterRange in 1.3 1.5 1.6
do
echo "CluStream_P"$parallelism"_W"$windowSize"_C"$maxClusterRange" start"
START=$(date +%s)
$flink run -c $CluStreamStartClass $jarfile $parallelism $CluStreamInput $windowSize
$maxClusterRange
$CluStreamPath"CluStream_P"$parallelism"_W"$windowSize"_C"$maxClusterRange".txt"
END=$(date +%s)
DIFF=$((END-START))
echo "CluStream_P"$parallelism"_W"$windowSize"_C"$maxClusterRange": "$DIFF"s" >> $timefile
done
done
done
echo "------------End of CluStream------------" >> $timefile
now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "----------$today $now------------" >> $timefile
echo "-------Beginning of HT----------" >> $timefile
for parallelism in 4 8 16
do
for windowSize in 100 200 600 1000 2000 5000 10000
do

for PSParallelism in 2 4 8
do
for WorkerParallelism in 2 4 8 16 32 40 48
do
for treebound in 0.05 0.1 0.15
do
echo "HT_P"$parallelism"_W"$windowSize"_PSP"$PSParallelism"_WP"$WorkerParallelism"_T"$treebound"
start"
START=$(date +%s)
$flink run -c $HTstartClass $jarfile $parallelism $HTinput $windowSize $PSParallelism
$WorkerParallelism
$HTpath"HT_P"$parallelism"_W"$windowSize"_PSP"$PSParallelism"_WP"$WorkerParallelism"_T"$treebound".txt"
true $treebound
END=$(date +%s)
DIFF=$((END-START))
echo "HT_P"$parallelism"_W"$windowSize"_PSP"$PSParallelism"_WP"$WorkerParallelism"_T"$treebound":
"$DIFF"s" >> $timefile
done
done
done
done
done
echo "------------End of HT------------" >> $timefile
echo "-----------Beginning of ALS-----------" >> $timefile
for parallelism in 4 8 16 24 32
do
for windowSize in 1 100 200 300 400 600 800 1000 2000 5000 7000 8000 9000 10000 50000
do
for iteration in 4000
do
for PSParallelism in 2 4 8
do
for WorkerParallelism in 2 4 8 16 32 40 48
do
echo
"ALS_P"$parallelism"_W"$windowSize"_I"$iteration"_False_PSP"$PSParallelism"_WP"$WorkerParallelism"
start"
START=$(date +%s)

$flink run -c $ALSstartClass $jarfile $parallelism $windowSize false $bigDataset
$ALSpath"ALS_P"$parallelism"_W"$windowSize"_I"$iteration"_False_PSP"$PSParallelism"_WP"$WorkerParallelism"
.txt" $iteration $PSParallelism $WorkerParallelism
END=$(date +%s)
DIFF=$((END-START))
echo
"ALS_P"$parallelism"_W"$windowSize"_I"$iteration"_False_PSP"$PSParallelism"_WP"$WorkerParallelism":
"$DIFF"s" >> $timefile
done
done
done
done
done
echo "--------------End of ALS---------------" >> $timefile
now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "----------$today $now------------" >> $timefile
echo "----------Beginning of SGD-------------" >> $timefile
for parallelism in 4 8 16
do
for windowSize in 1 100 200 600 1000 2000 5000 10000 50000
do



for iteration in 4000
do
for PSParallelism in 2 4 8
do
for WorkerParallelism in 2 4 8 16 32 40 48
do
echo "SGD_P"$parallelism"_W"$windowSize"_I"$iteration"_PSP"$PSParallelism"_WP"$WorkerParallelism"
start"
START=$(date +%s)
$flink run -c $SGDstartClass $jarfile $parallelism $bigDataset $windowSize
$SGDpath"SGD_P"$parallelism"_W"$windowSize"_I"$iteration"_PSP"$PSParallelism"_WP"$WorkerParallelism".txt"
$iteration $PSParallelism $WorkerParallelism
END=$(date +%s)
DIFF=$((END-START))
echo "SGD_P"$parallelism"_W"$windowSize"_I"$iteration"_PSP"$PSParallelism"_WP"$WorkerParallelism":
"$DIFF"s" >> $timefile
done
done
done
done
done
echo "--------------End of SGD--------------" >> $timefile
now=$(date +"%T")
today=$(date +%d.%m.%y)
$stopflink
echo "Tasks executed"
echo "------------ Flink stopped ------------" >> $timefile