package eu.proteus.solma.ridgeRegression

import org.apache.flink.api.java.DataSet
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common._
import breeze.linalg._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.immutable.Range.Int

object Accuracy_Cluster {
  /** args0 = Parallelism Environment (Int)
    * args1   = svm_result path
    * args2   = svm_true path
    * args3   = input file path
    * @param args currently settable variables see above, add more if required
    */
def main(args:Array[String]){
  val env = ExecutionEnvironment.getExecutionEnvironment

  var p = 1

  var input1 = "/tmp/svmout.txt"
  var input2 = "/media/zhanwang/data/data/lab/flink_t1/Online_FlinkML_Notes/data/onlineMLBig3_true.csv"
  var output = "/tmp/accRes.txt"

  if(args.length==4){

    env.setParallelism(args.apply(0).toInt)
    p = args.apply(0).toInt
    input1 = args.apply(1)
    input2 = args.apply(2)
    output = args.apply(3)


    println("using given parameters!")
  }


  val result = env.readTextFile(input1).map { x: String =>
    val test = x.replaceAll(",", "").split(" ")
    // 0 = Label
    val rul = test.apply(0).replace("Left((LabeledVector(", "").replace(",","").replace(" ", "").toDouble
    val dense1 = test.apply(1).replace("DenseVector(", "").replace(",","").replace(" ", "").toDouble
    val a = Tuple2(dense1,rul.toInt)
    a
  }.setParallelism(p)
  val ytrue = env.readTextFile(input2).map { x: String =>
    val test = x.replaceAll(",", "").split(" ")
    // 0 = Label
    val rul = test.apply(0).replace("LabeledVector(", "").replace(",","").replace(" ", "").toDouble
    val dense1 = test.apply(1).replace("DenseVector(", "").replace(",","").replace(" ", "").toDouble
    val a = Tuple2(dense1,rul.toInt)
    a
  }.setParallelism(p)
  //val total = 12026925


  val co = result.leftOuterJoin(ytrue)
    .where(0)
    .equalTo(0){
      (left,right)=>
        if(left == right) DenseVector(1,0) else DenseVector(0,1)
    }.reduce{_ + _}.setParallelism(p)
  val a = co.collect()
  val t = a.toArray
  val res  = DenseVector(t(0))
  val sum  =res.data(0).data(0)+res.data(0).data(1)
  println("sum: "+sum)
  val truePositive = res.data(0).data(0)
  println("truePositive: "+truePositive)
  val accRes = "acc: "+ (1.0*truePositive/sum)
  println(accRes)
  val out2 = env.fromElements(accRes)


//  val sum  = t(0)+t(1)
//  val acc = t(0) / sum
//  println("sum:" + sum)
//  println("acc:" + acc)

  //    val Sink = accuracy.map(x=>x).writeAsText("src/main/resources/Ridge_resultWeights.txt", WriteMode.OVERWRITE).setParallelism(1)
  //val Sink1 = co.writeAsText(output, WriteMode.OVERWRITE).setParallelism(1)
  val Sink2 = out2.writeAsText(output, WriteMode.OVERWRITE).setParallelism(1)

  env.execute()
  }
}
