#!/bin/bash




flink='../flink-1.3.2/bin/flink'
jarfile='./proteus-solma-development/target/proteus-solma_2.11-0.1.3-jar-with-dependencies.jar'
timefile='./data/ASVITimeResults.txt'



#ASVI Variables
SVIClass='eu.proteus.solma.ASYVI.ASYVI_Cluster'
SVIinput='/share/flink/onlineML/Online_FlinkML_Notes/data/ldatext2.txt'
SVIOut='/share/flink/onlineML/Online_FlinkML_Notes/data/ASVI/'
DictFile='/share/flink/onlineML/Online_FlinkML_Notes/data/ldatext2.dict'

nb_documents=100
topicKNum=10


now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "Current time : $today $now" >> $timefile
echo "----------$today $now------------" >> $timefile

echo "----------Beginning of SVI-------------" >> $timefile
for windowSize in 100 
do
for parallelism in 10 5
do
for PSParallelism in 1 2
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

echo "svm done" >> $timefile
exit
