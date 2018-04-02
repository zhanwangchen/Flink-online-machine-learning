#!/bin/bash
#SGD_PSPluster Parameter:
#args0 = Parallelism Environment (Int)
#args1 = input file path
#args2 = WindowSize (Int)
#args3 = output file path
#args4 = InterationWaitTime
#args5 = PSParallelism
#args6 = WorkerParallelism
#ALS_PSPluster Parameter:
#args0 = Parallelism Environment (Int)
#args1 = WindowSize (Int)
#args2 = setLabeledVectorOutput (bool)
#args3 = input file path
#args4 = output file path
#args5 = IterationWaitTime
#args6 = PSParallelism (Int)
#args7 = WorkerParallelism (Int)
#CluStream_PSPluster Parameter:
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


SCRIPT_DIR=$(readlink -f ${0%/*})
echo 'You are in directory: 'SCRIPT_DIR

##for local test
# startflink='../flink-1.3.2/bin/start-local.sh'
# stopflink='../flink-1.3.2/bin/stop-local.sh'

startflink='../flink-1.3.2/bin/start-cluster.sh'
stopflink='../flink-1.3.2/bin/stop-cluster.sh'

flink='../flink-1.3.2/bin/flink'
jarfile='./proteus-solma-development/target/proteus-solma_2.11-0.1.3-jar-with-dependencies.jar'
timefile='./data/TimeResults.txt'




#SVM variables
SVMClass='eu.proteus.solma.svm.SVM_Cluster'
SVMinput='./data/SVMTest-small.csv'
SVMOut='./data/SVM/'
#SVMinput=$SCRIPT_DIR'/data/SVMTest.csv'
#SVMOut=$SCRIPT_DIR'/data/SVM/'

#Ridge variables
RidgeRegressionClass='eu.proteus.solma.ridgeRegression.ORR_Cluster'
RidgeInput='./data/RegressionTest.csv'
RidgeOut='./data/Ridge/'

#Slog Variables
SlogClass='eu.proteus.solma.SLOG.SLOG_Cluster'
SlogInput='./data/RegressionTest.csv'
SlogOut='./data/Slog/'

#ASVI Variables
SVIClass='eu.proteus.solma.ASYVI.ASYVI_Cluster'
SVIinput='./data/ldatext2.txt'
SVIOut='./data/ASVI/'
DictFile='./data/ldatext2.dict'
nb_documents=100
topicKNum=10

#GGMM Variables
GGMMClass='eu.proteus.solma.ggmm.GGMM_Cluster'
GGMMinput='./data/cassini-big.csv'
GGMMOut='./data/GGMM/'

echo "mvn package -DskipTests"
#mvn package -DskipTests
(cd ./proteus-solma-development/; mvn package -DskipTests)
# if [ -f $jarfile ]; then
#    echo "File $FILE exists."
# else
#    echo "File $FILE does not exist."
# fi
#mv $jarfile ./
#jarfile='./proteus-solma_2.11-0.1.3-jar-with-dependencies.jar'

echo "Flink start"
$stopflink
$startflink
now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "Current time : $today $now" >> $timefile
echo "----------$today $now------------" >> $timefile
echo "-------Beginning of SVM----------" >> $timefile



    # var WindowSize  = 50
    # var LabeledVectorOutput = true
    # var WorkerParallelism = 200
    # var PSParallelism = 3
    # var streamingEnvParallelism = 3
    # var filename = "hdfs:/onlineML/onlineMLBigSVm43.5.csv"
    # var out =  "/share/flink/onlineML/svmOutOnRev43.5.txt"
    # var outParallelism = 5
#40 1 2 50 /share/flink/onlineML/onlineMLBigSVm43.5v2.csv /share/flink/onlineML/svmOutOnRev43.5v2.txt 1

for windowSize in 1
do
for parallelism in 1
do
for PSParallelism in 1
do
echo "SVM_workerp"$parallelism"_W"$windowSize"_PSpara"$PSParallelism" start"
START=$(date +%s)
$flink run -c $SVMClass $jarfile $parallelism $PSParallelism 1 $windowSize $SVMinput $SVMOut"parallelism"$parallelism"_W"$windowSize"_PSP"$PSParallelism".txt" 1
END=$(date +%s)
DIFF=$((END-START))
echo "SVM_workerp"$parallelism"_W"$windowSize"_PSpara"$PSParallelism": "$DIFF"s" >> $timefile
done
done
done
echo "------------End of SVM------------" >> $timefile
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

for windowSize in 100 
do
for parallelism in 1
do
for PSParallelism in 1
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
for windowSize in 100 
do
for parallelism in 1
do
for PSParallelism in 1
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

echo "----------Beginning of SVI-------------" >> $timefile
for windowSize in 100 
do
for parallelism in 1
do
for PSParallelism in 1
do
echo "SVI_workerp"$parallelism"_W"$windowSize"_PSpara"$PSParallelism" start"
START=$(date +%s)
$flink run -c $SVIClass $jarfile $parallelism $PSParallelism 1 $windowSize $SVIinput $SVIOut"parallelism"$parallelism"_W"$windowSize"_PSP"$PSParallelism".txt" 1 $DictFile $nb_documents $topicKNum

END=$(date +%s)
DIFF=$((END-START))
echo "SVI_workerp"$parallelism"_W"$windowSize"_PSpara"$PSParallelism": "$DIFF"s" >> $timefile
done
done
done
echo "--------------End of SVI--------------" >> $timefile


now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "----------$today $now------------" >> $timefile


# echo "-----------Beginning of GGMM-----------" >> $timefile
# for windowSize in 100 200
# do
# for parallelism in 1
# do
# for PSParallelism in 1
# do
# echo "GGMM_workerp"$parallelism"_W"$windowSize"_PSpara"$PSParallelism" start"
# START=$(date +%s)
# $flink run -c $GGMMClass $jarfile $parallelism $PSParallelism 1 $windowSize $GGMMinput $GGMMOut"parallelism"$parallelism"_W"$windowSize"_PSP"$PSParallelism".txt" 1
# END=$(date +%s)
# DIFF=$((END-START))
# echo "GGMM_workerp"$parallelism"_W"$windowSize"_PSpara"$PSParallelism": "$DIFF"s" >> $timefile
# done
# done
# done
# echo "--------------End of GGMM---------------" >> $timefile


now=$(date +"%T")
today=$(date +%d.%m.%y)
$stopflink
echo "Tasks executed"
echo "------------ Flink stopped ------------" >> $timefile
