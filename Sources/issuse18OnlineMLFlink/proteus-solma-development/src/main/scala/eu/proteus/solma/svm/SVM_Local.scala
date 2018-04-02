package eu.proteus.solma.svm

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

//#import eu.proteus.solma.svm

// this class is used to test ALS with PS local
object SVM_Local {
  private val serialVersionUID = 6529685098267711691L


  /**
    * method that starts the computation
    * @param args not used
    * @see ALS_Cluster
    */
  def main(args: Array[String]) {

    //create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment

    var WindowSize  = 600
    var LabeledVectorOutput = true
    var WorkerParallelism = 1
    var PSParallelism = 1
    var streamingEnvParallelism = 1
    var filename = "/media/zhanwang/data/data/lab/flink_t1/Online_FlinkML_Notes/data/onlineMLBig3.csv"
    var out =  "/tmp/svmout.txt"
    var outParallelism = 1
    try{
      if(args.length==7){
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
      }else{
        println("using default parameters!")
      }
    }


    val pipeline = new OSVM()
      .setWindowSize(WindowSize)
      .setLabeledVectorOutput(LabeledVectorOutput)
      .setWorkerParallelism(WorkerParallelism)
      .setPSParallelism(PSParallelism)
    val dimension = 7
    //val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\SGDALSTurboFan.txt")
    //src/main/resources/SVMTest.csv
    //hdfs:/onlineML/onlineMLBig.csv
    streamingEnv.setParallelism(streamingEnvParallelism)
    val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile(filename) //SGDALSTurboFan.txt
      // val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("src/main/resources/SVMTest.csv") //SGDALSTurboFan.txt
      .map{ x: String =>
      val test = x.split(",")
      val rul = test.apply(0).toDouble
      val weights = new Array[Double](dimension)
      for (i<-1 to dimension){
        weights.update(i-1, test.apply(i).toDouble)
      }
      LabeledVector(rul,DenseVector(weights))
    }
    //      pipeline.train(labeledDataStream)
    val result = pipeline.predict(labeledDataStream)
    //    val resultSGDPS = pipeline.fitAndPredict(labeledDataStream)
    val Sink = result.map(x=>x).writeAsText(out, WriteMode.OVERWRITE).setParallelism(outParallelism)
    //val Sink = result.map(x=>x).writeAsText("/tmp/svm.txt", WriteMode.OVERWRITE).setParallelism(1)

    streamingEnv.execute()

  }

}