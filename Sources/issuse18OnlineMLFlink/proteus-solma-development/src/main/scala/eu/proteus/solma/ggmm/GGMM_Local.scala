package eu.proteus.solma.ggmm

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object GGMM_Local {

  def main(args: Array[String]) {

    //create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val dimension = 2
    val pipline = new GGMM()
      .setWindowSize(50)
      .setLabeledVectorOutput(true)
    val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("src/main/resources/cassini-very-small.csv") //SGDALSTurboFan.txt
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
    val Sink = result.map(x=>x).writeAsText("src/main/resources/ggmm-result-3.3.txt", WriteMode.OVERWRITE).setParallelism(1)

    streamingEnv.execute()

  }

}