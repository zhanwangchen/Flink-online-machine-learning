package eu.proteus.solma.ASYVI

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

// this class is used to test ALS with PS local
object ASYVI_Local extends  Serializable {

  /**
    * method that starts the computation
    * @param args not used
    * @see ALS_Cluster
    */
  def main(args: Array[String]) {

    //create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment




    var WorkerParallelism = 2
    var PSParallelism = 1
    var streamingEnvParallelism = 1
    //var filename = "/share/flink/onlineML/20news.txt"
    var WindowSize  = 50
    var filename = "src/main/resources/ldatext2.txt"
    //var out =  "/share/flink/onlineML/svi20newsv1.txt"
    //"/share/flink/onlineML/svmOutOnRev43.5.txt"
    var out =  "/tmp/ldatext2_out.txt"
    var outParallelism = 1
    var dictFile = "src/main/resources/ldatext2.dict"
    var LabeledVectorOutput = false

    var nb_documents = 100
    var topicKNum  = 10
    try{
      if(args.length==10){
        println("using given parameters!")
        // 1 1 1 50 hdfs:/onlineML/onlineMLBig3.csv
        //40 2 2 50 /share/flink/onlineML/onlineMLBigSVm43.5v2.csv /share/flink/onlineML/svmOutOnRev43.5v2.txt 1
        //40 2 2 50 /share/flink/onlineML/onlineMLBigSVm43.5.csv /share/flink/onlineML/svmOutOnRev43.5.txt 1
        //40 1 2 50 /share/flink/onlineML/onlineMLBigSVm43.5v2.csv /share/flink/onlineML/svmOutOnRev43.5v2.txt 1

        WorkerParallelism = args.apply(0).toInt
        PSParallelism = args.apply(1).toInt
        streamingEnvParallelism = args.apply(2).toInt
        WindowSize=args.apply(3).toInt
        filename = args.apply(4)
        out = args.apply(5)
        outParallelism = args.apply(6).toInt
        dictFile = args.apply(7)
        nb_documents = args.apply(8).toInt
        topicKNum = args.apply(9).toInt
      }else{
        println("using default parameters!")
      }
    }



    val ASYVI = new ASYVI_PS(dictFile)
      .setWindowSize(WindowSize)
      .setLabeledVectorOutput(LabeledVectorOutput)
      .setPSParallelism(PSParallelism)
      .setWorkerParallelism(WorkerParallelism)
      .setNb_documents(nb_documents)
      .setTopicKNum(topicKNum)


//ldatext1
    val labeledDataStream: DataStream[String] = streamingEnv.readTextFile(filename)


    val resultSGDPS = ASYVI.fitAndPredict(labeledDataStream)
    val Sink = resultSGDPS.map(x=>x).writeAsText(out, WriteMode.OVERWRITE).setParallelism(outParallelism)

    streamingEnv.execute()

  }

}

