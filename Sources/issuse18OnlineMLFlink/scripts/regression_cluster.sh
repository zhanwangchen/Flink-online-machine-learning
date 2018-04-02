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
timefile='./data/TimeResults_regression.txt'




#Ridge variables
RidgeRegressionClass='eu.proteus.solma.ridgeRegression.ORR_Cluster'
RidgeInput='/share/flink/onlineML/onlineML_regression20th.csv'
RidgeOut='/share/flink/onlineML/data/Ridge/'

#Slog Variables
SlogClass='eu.proteus.solma.SLOG.SLOG_Cluster'
SlogInput='/share/flink/onlineML/onlineML_regression20th.csv'
SlogOut='/share/flink/onlineML/data/Slog/'

now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "----------$today $now------------" >> $timefile


###########################################################
#exit
###########################################################

echo "-------Beginning of Ridge----------" >> $timefile

    # var WindowSize  = 50
    # var LabeledVectorOutput = true
    # var WorkerParallelism = 200
    # var PSParallelism = 3
    # var streamingEnvParallelism = 3
    # var filename = "/share/flink/onlineML/onlineML_regression20th.csv"
    #   //"hdfs:/onlineML/onlineMLBigSVm43.5.csv"
    # var out = "/share/flink/onlineML/ORR_resultWeights.24.12.txt"
    #   //"/share/flink/onlineML/svmOutOnRev43.5.txt"
    # var outParallelism = 5

for windowSize in 50 150 200 250 300 500 600
do
for parallelism in 5 10 15
do
for PSParallelism in 1 2
do
echo "Ridge_workerp"$parallelism"_W"$windowSize"_PSpara"$PSParallelism" start"
START=$(date +%s)
$flink run -c $RidgeRegressionClass $jarfile $parallelism $PSParallelism 1 $windowSize $RidgeInput $RidgeOut"parallelism"$parallelism"_W"$windowSize"_PSP"$PSParallelism".txt" 1
END=$(date +%s)
DIFF=$((END-START))
echo "Ridge_workerp"$parallelism"_W"$windowSize"_PSpara"$PSParallelism": "$DIFF"s" >> $timefile
done
done
done
echo "------------End of Ridge------------" >> $timefile


echo "-----------Beginning of Slog-----------" >> $timefile
for windowSize in 50 150 200 250 300 500 600
do
for parallelism in 5 10 15
do
for PSParallelism in 1 2
do
echo "Slog_workerp"$parallelism"_W"$windowSize"_PSpara"$PSParallelism" start"
START=$(date +%s)
$flink run -c $SlogClass $jarfile $parallelism $PSParallelism 1 $windowSize $SlogInput $SlogOut"parallelism"$parallelism"_W"$windowSize"_PSP"$PSParallelism".txt" 1
END=$(date +%s)
DIFF=$((END-START))
echo "Slog_workerp"$parallelism"_W"$windowSize"_PSpara"$PSParallelism": "$DIFF"s" >> $timefile
done
done
done
echo "--------------End of Slog---------------" >> $timefile
now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "----------$today $now------------" >> $timefile

echo 'Done!'
exit
