package eu.proteus.solma.test

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode


object svmout {
  def main(args: Array[String]) {





    var input = "/share/flink/onlineML/onlineMLBigSVm43.5.csv"
    var trueLabel =  "/share/flink/onlineML/svmOutOnRev43.5.txt"
    var out =  "/tmp/testout.txt"

    var parallelism = 200

    var total = 5677849
    if(args.length==5){
      println("using given parameters!")


      input = args.apply(0)
      trueLabel = args.apply(1)
      out = args.apply(2)

      total  = args.apply(3).toInt
      parallelism = args.apply(4).toInt

    }else{
      println("using default parameters!")
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val result = env.readTextFile(input).map { x: String =>
      val test = x.replaceAll(",", "").split(" ")
      // 0 = Label
      val rul = test.apply(0).replace("Left((LabeledVector(", "").replace(",","").replace(" ", "").toDouble
      val dense1 = test.apply(1).replace("DenseVector(", "").replace(",","").replace(" ", "").toDouble
      Tuple2(dense1,rul.toInt)
    }.setParallelism(parallelism)
    val ytrue = env.readTextFile(trueLabel).map { x: String =>
      val test = x.split(" ")
      // 0 = Label

      Tuple2(test(0).toDouble,test(7).head.toInt)
    }.setParallelism(parallelism)

    val co = result.leftOuterJoin(ytrue)
      .where(0)
      .equalTo(0){
        (left,right)=>
          if(left == right) 1 else 0
      }.reduce{_ + _}.setParallelism(parallelism)

    //    val Sink = accuracy.map(x=>x).writeAsText("src/main/resources/Ridge_resultWeights.txt", WriteMode.OVERWRITE).setParallelism(1)
    val Sink = co.map(x=>x/total).writeAsText(out, WriteMode.OVERWRITE).setParallelism(1)
    env.execute()

  }
}
