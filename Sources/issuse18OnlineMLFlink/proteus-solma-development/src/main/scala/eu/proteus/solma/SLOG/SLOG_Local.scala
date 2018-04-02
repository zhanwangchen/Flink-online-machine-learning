package eu.proteus.solma.SLOG
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
object SLOG_Local {
  private val serialVersionUID = 6529647098267711690L

  def main(args: Array[String]) {


    //create Streaming Environment
    val streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val pipline = new SLOG()
      .setWindowSize(2000)
      .setLabeledVectorOutput(true)
      .setPenalization(0.1)
    val dimension = 7

    //val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("C:\\Users\\Ariane\\workspaceLuna\\proteus-solma\\src\\main\\resources\\SGDALSTurboFan.txt")  SGDALSTurboFan.txt
    val labeledDataStream: DataStream[LabeledVector] = streamingEnv.readTextFile("/media/zhanwang/data/data/lab/flink_t1/Online_FlinkML_Notes/proteus-solma-development/src/main/resources/regressionTest.csv")
      .map{ x: String =>
        val test = x.split(",")
        val rul = test.apply(0).toDouble
        val weights = new Array[Double](dimension)
        for (i<-1 to dimension){
          weights.update(i-1, test.apply(i).toDouble)
        }
        LabeledVector(rul,DenseVector(weights))
      }

    val result = pipline.predict(labeledDataStream)
    val Sink = result.map(x=>x).writeAsText("src/main/resources/SLOG_res.txt", WriteMode.OVERWRITE).setParallelism(1)

    streamingEnv.execute()

  }
}
