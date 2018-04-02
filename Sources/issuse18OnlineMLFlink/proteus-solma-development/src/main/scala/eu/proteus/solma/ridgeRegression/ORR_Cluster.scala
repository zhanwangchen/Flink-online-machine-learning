package eu.proteus.solma.ridgeRegression
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
object ORR_Cluster {

  /**
    * method that starts the computation
    * @param args not used
    * @see ALS_Cluster
    */
  def main(args: Array[String]) {

    //create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment



    var WorkerParallelism = 200
    var PSParallelism = 3
    var streamingEnvParallelism = 3
    var WindowSize  = 50
    var filename = "/share/flink/onlineML/onlineML_regression20th.csv"
      //"hdfs:/onlineML/onlineMLBigSVm43.5.csv"
    var out = "/share/flink/onlineML/ORR_resultWeights.24.12.txt"
      //"/share/flink/onlineML/svmOutOnRev43.5.txt"
    var outParallelism = 5
    var dimension = 7
    var LabeledVectorOutput = true
    try{
      if(args.length==8){
        println("using given parameters!")
        // 1 1 1 50 hdfs:/onlineML/onlineMLBig3.csv
        //40 2 2 50 /share/flink/onlineML/onlineMLBigSVm43.5v2.csv /share/flink/onlineML/svmOutOnRev43.5v2.txt 1
        //40 2 2 50 /share/flink/onlineML/onlineMLBigSVm43.5.csv /share/flink/onlineML/svmOutOnRev43.5.txt 1

        //40 2 2 50 /share/flink/onlineML/onlineML_regression20th.csv /share/flink/onlineML/ORR_resultWeights.24.12.txt 1
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

    val pipeline = new ORR()
      .setWindowSize(WindowSize)
      .setLabeledVectorOutput(true)
      .setWorkerParallelism(WorkerParallelism)
      .setPSParallelism(PSParallelism)

    //val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\SGDALSTurboFan.txt")
    val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile(filename) //SGDALSTurboFan.txt
      .map{ x: String =>
      // 0 = Label
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

    streamingEnv.execute()

  }

}