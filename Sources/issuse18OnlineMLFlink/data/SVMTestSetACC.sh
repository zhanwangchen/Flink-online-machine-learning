
#!/bin/bash

flink='./flink-1.3.2/bin/flink'
jarfile='./Online_FlinkML_Notes/proteus-solma-development/target/proteus-solma_2.11-0.1.3-jar-with-dependencies.jar'
c = 0
#for filename in /share/flink/onlineML/data/SVM/para*.txt; do
#echo $filename
#c+=1
#$flink run -c eu.proteus.solma.ridgeRegression.Accuracy_Cluster $jarfile 20 $filename /home/zhanwang/onlineMLBig3_testSet_true.csv /share/flink/onlineML/data/"$c"acc1.txt
#done

for filename in /share/flink/onlineML/data/SVM/para*.txt; do
	echo $filename
	c+=1
	cat /share/flink/onlineML/data/"$c"acc1.txt
done
