#!/bin/bash




flink='../flink-1.3.2/bin/flink'
jarfile='./proteus-solma-development/target/proteus-solma_2.11-0.1.3-jar-with-dependencies.jar'
timefile='./data/SVMTimeResults.txt'




#SVM variables
SVMClass='eu.proteus.solma.svm.SVM_Cluster'
SVMinput='hdfs://ibm-power-1.dima.tu-berlin.de:44000/onlineML/onlineMLBig3.csv'
SVMOut='/share/flink/onlineML/data/SVM/'
#SVMinput=$SCRIPT_DIR'/data/SVMTest.csv'
#SVMOut=$SCRIPT_DIR'/data/SVM/'


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
for windowSize in 500 1000 3000
do
for parallelism in 50 60 100
do
for PSParallelism in 1 4
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

echo "svm done" >> $timefile
exit
