package eu.proteus.solma.ggmm

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object GGMM_Cluster {

  def main(args: Array[String]) {

    //create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment



    var WorkerParallelism = 200
    var PSParallelism = 3
    var streamingEnvParallelism = 3
    var WindowSize  = 50
    var filename = "hdfs:/onlineML/onlineMLBigSVm43.5.csv"
    var out =  "/share/flink/onlineML/svmOutOnRev43.5.txt"
    var outParallelism = 1
    var dimension = 7
    var LabeledVectorOutput = true
    try{
      if(args.length==8){
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
        dimension = args.apply(7).toInt
      }else{
        println("using default parameters!")
      }
    }
    val pipline = new GGMM()
      .setWindowSize(WindowSize)
      .setLabeledVectorOutput(LabeledVectorOutput)
      .setWorkerParallelism(WorkerParallelism)
      .setPSParallelism(PSParallelism)
    val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile(filename) //SGDALSTurboFan.txt
      .map{ x: String =>
      val test = x.split(",")
      val rul = test.apply(0).toDouble
      val weights = new Array[Double](dimension)
      for (i<-1 to dimension){
        weights.update(i-1, test.apply(i).toDouble)
      }
      LabeledVector(rul,DenseVector(weights))
    }

//    val unlabeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("src/main/resources/cassini-very-small-test.csv") //SGDALSTurboFan.txt
//      .map{ x: String =>
//      val test = x.split(",")
//      val rul = test.apply(0).toDouble
//      val weights = new Array[Double](2)
//      weights.update(0, test.apply(1).toDouble)
//      weights.update(1, test.apply(2).toDouble)
//      LabeledVector(rul,DenseVector(weights))
//    }

//    pipline.train(labeledDataStream)
//    val result = pipline.predict(unlabeledDataStream)
    val result = pipline.predict(labeledDataStream)
    val Sink = result.map(x=>x).writeAsText(out, WriteMode.OVERWRITE).setParallelism(outParallelism)

    streamingEnv.execute()

  }

}