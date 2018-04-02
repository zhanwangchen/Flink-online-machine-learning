package eu.proteus.solma.ridgeRegression
import breeze.linalg.{DenseMatrix, Transpose}
import hu.sztaki.ilab.ps.FlinkParameterServer._
import hu.sztaki.ilab.ps._
import hu.sztaki.ilab.ps.entities.PullAnswer
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.ml.common.{WithParameters, _}
import org.apache.flink.ml.math
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{BLAS, DenseVector, SparseMatrix, SparseVector}
import org.apache.flink.ml.preprocessing.StandardScaler
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.mutable
import eu.proteus.solma.pipeline.{StreamEstimator, StreamFitOperation, StreamPredictor,PredictDataStreamOperation}
import breeze.linalg._
/*
 *Ariane 11.08.2017
 *Formular : weights = (sum_n(x_n*y_n) - n*avg_x*avg*y) / (sum_n(x^2) - n*(avg_x)^2)
 * sources: Parameter Server: https://github.com/gaborhermann/flink-parameter-server
*/
class ORR extends WithParameters with Serializable
  with StreamEstimator[ORR]
  with StreamPredictor[ORR]{
  // weights and the count of obs
  type P = (DenseMatrix[Double],WeightVector, Int)
  type WorkerIn = (Int, Int, (DenseMatrix[Double],WeightVector, Int)) // id, workerId, weigthvector
  type WorkerOut = (Boolean, Array[Int], (DenseMatrix[Double],WeightVector, Int))
  type WOut = (LabeledVector, Int)
  /** fit with labeled data, predicts unlabeled data
    * handeles both labeld and unlabeled data
    * @param evalDataStream datastream of LabeledVectors
    * @return either the model or the predicted LabelVectors
    */
  def fitAndPredict(evalDataStream : DataStream[LabeledVector]) : DataStream[String] = {
    //add a "problem" id
    //TODO in general this could be done in a more efficient way, i.e. solving several problems in the same time
    val partitionedStream = evalDataStream.map(x=>(x,0))
    // send the stream to the parameter server, define communication and Worker and Server instances
    val outputDS =
      transform(
        partitionedStream,
        RRWorkerLogic,
        RRParameterServerLogic,
        (x: (WorkerOut)) => x match {
          case (true, Array(partitionId, id), emptyVector) => Math.abs(id.hashCode())
          case (false, Array(partitionId, id), update) => Math.abs(id.hashCode())
        },
        (x: (WorkerIn)) => x._2,
        this.getWorkerParallelism(),
        this.getPSParallelism(),
        new WorkerReceiver[WorkerIn, P] {
          override def onPullAnswerRecv(msg: WorkerIn, pullHandler: PullAnswer[P] => Unit): Unit = {
            pullHandler(PullAnswer(msg._1, msg._3))
          }
        },
        new WorkerSender[WorkerOut, P] {
          // output of Worker
          override def onPull(id: Int, collectAnswerMsg: WorkerOut => Unit, partitionId: Int): Unit = {
            collectAnswerMsg((true, Array(partitionId, id), (DenseMatrix.eye(7), WeightVector(new DenseVector(Array(0.0)),0.0),0)))
          }

          override def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: WorkerOut => Unit, partitionId: Int): Unit = {
            collectAnswerMsg((false, Array(partitionId, id), deltaUpdate))
          }
        },
        new PSReceiver[(WorkerOut), P] {
          override def onWorkerMsg(msg: (WorkerOut), onPullRecv: (Int, Int) => Unit, onPushRecv: (Int, P) => Unit): Unit = {
            msg match {
              case (true, Array(partitionId, id), update) =>
                onPullRecv(id, partitionId)
              case (false, Array(partitionId, id), update) =>
                onPushRecv(id, update)
            }
          }
        },
        new PSSender[WorkerIn, P] {
          override def onPullAnswer(id: Int,
                                    value: P,
                                    workerPartitionIndex: Int,
                                    collectAnswerMsg: ((WorkerIn)) => Unit): Unit = {
            collectAnswerMsg(id, workerPartitionIndex, value)
          }
        },
        this.getIterationWaitTime()
      )
    val y = outputDS.flatMap(new FlatMapFunction[Either[(LabeledVector,Int),String], String] {
      override def flatMap(t: Either[(LabeledVector, Int), String], collector: Collector[String]): Unit = {
        if(getLabeledVectorOutput() == true && t.isLeft) collector.collect(t.toString)
        if(getLabeledVectorOutput() == false && t.isRight) collector.collect(t.toString)
      }
    })
    y
  }
  //--------------GETTER AND SETTER ----------------------------------------------------------------
  /** Get the WorkerParallism value as Int
    * @return The number of worker parallelism
    */
  def getWorkerParallelism() : Int  = {
    this.parameters.get(ORR.WorkerParallism).get
  }
  /**
    * Set the WorkerParallelism value as Int
    * @param workerParallism workerParallism as Int
    * @return
    */
  def setWorkerParallelism(workerParallism : Int): ORR = {
    this.parameters.add(ORR.WorkerParallism, workerParallism)
    this
  }
  /**
    * Get the SeverParallism value as Int
    * @return The number of server parallelism
    */
  def getPSParallelism() : Int  = {
    this.parameters.get(ORR.PSParallelism).get
  }
  /**
    * Set the SeverParallism value as Int
    * @param psParallelism serverparallelism as Int
    * @return
    */
  def setPSParallelism(psParallelism : Int): ORR = {
    parameters.add(ORR.PSParallelism, psParallelism)
    this
  }
  /**
    * Variable to stop the calculation in stream environment
    * @param iterationWaitTime time as ms (long)
    * @return
    */
  def setIterationWaitTime(iterationWaitTime: Long) : ORR = {
    parameters.add(ORR.IterationWaitTime, iterationWaitTime)
    this
  }
  /** Variable to stop the calculation in stream environment
    * get the waiting time
    * @return The waiting time as Long
    */
  def getIterationWaitTime() : Long  = {
    this.parameters.get(ORR.IterationWaitTime).get
  }
  /**
    * set a new windowsize
    * @param windowsize
    */
  def setWindowSize(windowsize : Int) : ORR = {
    this.parameters.add(ORR.WindowSize, windowsize)
    RRWorkerLogic.setWindowSize(this.getWindowSize())
    this
  }
  /**
    * Get the Windowsize
    * @return The windowsize
    */
  def getWindowSize(): Int = {
    this.parameters.get(ORR.WindowSize).get
  }
  /**
    * Get the Default Label which is used for unlabeld data points as double
    * @return The defaultLabel
    */
  def getDefaultLabel(): Double  = {
    this.parameters.get(ORR.DefaultLabel).get
  }
  /**Set the Default Label
    * Variable to define the unlabeled observation that can be predicted later
    * @param label the value of the defaul label
    * @return
    */
  def setDefaultLabel(label: Double) : ORR = {
    parameters.add(ORR.DefaultLabel, label)
    RRWorkerLogic.setLabel(label)
    this
  }
  /**
    * Set the required output format
    * @return either the Labeled datapoints (true) or the model (false)
    */
  def setLabeledVectorOutput(yes: Boolean): ORR = {
    this.parameters.add(ORR.LabeledVectorOutput, yes)
    this
  }
  /**
    * Get the current output format
    * @return
    */
  def getLabeledVectorOutput(): Boolean = {
    this.parameters.get(ORR.LabeledVectorOutput).get
  }
  //---------------------------------WORKER--------------------------------
  val RRWorkerLogic = new WorkerLogic[(LabeledVector, Int), P, WOut]{
    // Worker variables, have same default values as ALS
    private var window = 100
    private var label = -9.0
    private var doLabeling = true
    // free parameters
    private val dataFit = new mutable.Queue[(LabeledVector, Int)]()
    private val dataPredict = new mutable.Queue[(DenseVector, Int)]()
    private val dataPredictPull = new mutable.Queue[(DenseVector, Int)]()
    /**
      * Set Parameters
      */
    /**
      * Set the windowsize
      * @param windowsize
      * @return
      */
    def setWindowSize(windowsize : Int) : Int = {
      this.window = windowsize
      this.window
    }
    /**Set the Default Label
      * Variable to define the unlabeled observation that can be predicted later
      * @param label the value of the defaul label
      * @return the default label
      */
    def setLabel(label : Double) : Double = {
      this.label = label
      this.label
    }
    /**
      * set to true if prediction is required,else false
      * @param predictionOutput
      * @return
      */
    def setDoLabeling(predictionOutput : Boolean) : Boolean = {
      this.doLabeling = predictionOutput
      this.doLabeling
    }
    /**
      * Method handles data receives
      * @param data the datapoint arriving on the worker
      * @param ps parameter server client
      */
    override def onRecv(data: (LabeledVector, Int), ps: ParameterServerClient[P, WOut]): Unit = {
      // if for unlabeled data
      if(data._1.label == this.label && this.doLabeling){
        val dataDenseVector = data._1.vector match {
          case d: DenseVector => d
          case s: SparseVector => s.toDenseVector
        }
        this.dataPredict.enqueue((dataDenseVector, data._2))
        if(this.dataPredict.size >= this.window*0.5) {
          // already collected data is put into dataQPull-Queue so dataQ starts again with 0
          val dataPart = this.dataPredict.dequeueAll(x => x._2 == data._2)
          dataPart.map(x => this.dataPredictPull.enqueue(x))
          //sent pull request
          ps.pull(data._2)
        }
        // for labeled data
      } else{
        if(data._1.label != this.label)  this.dataFit.enqueue(data)
        if(this.dataFit.size >= this.window ) {
          // already collected data is put into dataQPull-Queue so dataQ starts again with 0
          val dataPart = this.dataFit.dequeueAll(x => x._2 == data._2 )
          val statistics = getStatistics(dataPart)
          val update = calcNewWeigths(statistics._1, statistics._2, statistics._3)
          ps.push(data._2,update)
          ps.pull(data._2)
        }
      }
    }
    /**
      * aggregates the values of a set of datapoints to calculate the LS weights
      * @param trainSet Set of labeled data points
      * @return tuple of aggregated values : (count, x           , xy     , x*x     , y)
      */
    def getStatistics(trainSet: mutable.Seq[(LabeledVector, Int)]) : (Int,breeze.linalg.DenseMatrix[Double], math.Vector) = {
      val statistics = trainSet.map { x =>
        val xori= x._1.vector.copy
        val m  = x._1.vector.outer(x._1.vector) //xt*xt'


        /**
          * x = a * x
          *  def scal(a: Double, x: Vector):
          */
        BLAS.scal(x._1.label,x._1.vector) //yt*xt

        val m2:Array[Double] = m.asBreeze.toDenseMatrix.data

        val mt2 = breeze.linalg.DenseMatrix(m2)
        //val sumM = tmp + tmp2
        val t1 =mt2.reshape(7,7)
        //(1,  yt*xt     ,  xt*xt' )
        (1, t1 ,  x._1.vector)
      }.reduce { (x, y) =>
        val count = x._1 + y._1
        /**
          * y += a * x
          * axpy(a: Double, x: Vector, y: Vector)
          */

        //BLAS.axpy(1.0, x._2, y._2)
        //BLAS.axpy(1.0, x._3, y._3)

        BLAS.axpy(1.0, x._3, y._3)


        val sumM = x._2 + y._2

        (count,sumM,y._3)
      }
      statistics
    }
    def calcNewWeigths(count : Int, xtxt : breeze.linalg.DenseMatrix[Double], x2 : math.Vector): (DenseMatrix[Double],WeightVector, Int) = {
      //val a = x1.outer(x1)
      //(a.asBreeze.toDenseMatrix,WeightVector(x2,0.0), count)
      (xtxt,WeightVector(x2,0.0), count)
    }
    /**
      * defines PullRecv from ParameterServer, labels the unseen datapoints
      * @param paramId problem id
      * @param paramValue current weights from PS
      * @param ps parameter server client
      */
    override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {
      // dequeue all with key == paramId  dequeueAll(x=> boolean)
      if(this.dataPredictPull.nonEmpty && (paramValue._3>50)) {
        val xPredict = this.dataPredictPull.dequeueAll(x => x._2 == paramId)
        val xs_Sized = xPredict.splitAt(this.window)
        xs_Sized._2.foreach(x => this.dataPredictPull.enqueue(x))
        if (xs_Sized._1.size > 0) {
          xs_Sized._1.foreach { x =>
            val WeightVector(weights, weight0) = paramValue._2
            val A = breeze.linalg.inv(paramValue._1)
            //            b*A^-1 *x
            val b = weights.asBreeze.toDenseVector
            val f = A * b
            val r = x._1.asBreeze.dot(f)
//            val dotProduct = x._1.asBreeze.dot(weights.asBreeze)
            ps.output(LabeledVector((r), x._1), paramId)
          }
        }
      }
      else{
        val release = this.dataPredictPull.dequeueAll(x => x._2 == paramId)
        release.foreach(x => this.dataPredict.enqueue(x))
      }
    }//onPull
  }//Worker
  //-----------------------------------------------------SERVER-----------------------------------------
  val RRParameterServerLogic = new ParameterServerLogic[P, String] {
    val params = new mutable.HashMap[Int, (DenseMatrix[Double],WeightVector, Int)]()
    /**
      * On Pull Receive this method searches for the current weight of this problem id and send them
      * to the worker
      * @param id problem id
      * @param workerPartitionIndex the worker id to remember where the answer needs to be send to
      * @param ps parameter server
      */
    override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, String]): Unit = {
      val param = this.params.getOrElseUpdate(id, (DenseMatrix.eye[Double](1), WeightVector(DenseVector(Array[Double](0.0)),0.0),0))
      ps.answerPull(id, param, workerPartitionIndex)
    }
    /**
      * this method merges the the current weights with the updated weights with regards to the number of
      * observation forming each weight
      * @param id problem id
      * @param deltaUpdate updated weights from a worker
      * @param ps parameter server
      */
    override def onPushRecv(id: Int, deltaUpdate: P, ps: ParameterServer[P, String]): Unit = {
      if(deltaUpdate._3 >= 0){
        val param = this.params.getOrElseUpdate(id, (DenseMatrix.eye[Double](1),WeightVector(DenseVector(Array[Double](0.0)),0.0),0))
        if(param._3 > 0){
          val newa = deltaUpdate._1 + param._1
          BLAS.axpy(1.0,param._2.weights,deltaUpdate._2.weights)

          val count = param._3 + deltaUpdate._3
          //scal down the current weights
          //x = a*x
          BLAS.scal(param._3.toDouble/count.toDouble, param._2.weights)
          val newmatrix1 =  param._1* param._3.toDouble/count.toDouble
          val newmatrix2 = deltaUpdate._3.toDouble/count.toDouble *newa
          this.params.update(id,(newmatrix1+newmatrix2,WeightVector(param._2.weights,0.0),count))
          ps.output(this.params(id).toString)
        }//if
        else{
          this.params.update(id, deltaUpdate)
          ps.output(this.params(id).toString)
        }
      }
    }//onPushRecv
  }//PS
  // equals constructor
  def apply() : ORR = new ORR()
} // end class
// Single instance
object ORR {
  case object IterationWaitTime extends Parameter[Long] {
    /**
      * Default time - needs to be set in any case to stop this calculation
      */
    private val DefaultIterationWaitTime: Long = 4000
    /**
      * Default value.
      */
    override val defaultValue: Option[Long] = Some(IterationWaitTime.DefaultIterationWaitTime)
  }
  case object WindowSize extends Parameter[Int] {
    /**
      * Default windowsize constant : taking each and every data point into account
      */
    private val DefaultWindowSize: Int = 100
    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(WindowSize.DefaultWindowSize)
  }
  case object WorkerParallism extends Parameter[Int] {
    /**
      * Default parallelism
      */
    private val DefaultWorkerParallism: Int = 4
    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(WorkerParallism.DefaultWorkerParallism)
  }
  case object PSParallelism extends Parameter[Int] {
    /**
      * Default server parallelism.
      */
    private val DefaultPSParallelism: Int = 4
    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(PSParallelism.DefaultPSParallelism)
  }
  case object DefaultLabel extends Parameter[Double] {
    /**
      * Default Label for unseen data
      */
    private val LabelDefault: Double = -1.0
    /**
      * Default value.
      */
    override val defaultValue: Option[Double] = Some(DefaultLabel.LabelDefault)
  }
  case object LabeledVectorOutput extends Parameter[Boolean] {
    /**
      * Default output are the Weights
      */
    private val DefaultLabeledVectorOutput: Boolean = false
    /**
      * Default value.
      */
    override val defaultValue: Option[Boolean] = Some(LabeledVectorOutput.DefaultLabeledVectorOutput)
  }
  def apply(): ORR = {
    new ORR()
  }
  implicit val fit =  new StreamFitOperation[ORR,LabeledVector,String] {
    override def fit(instance: ORR,
                     fitParameters: ParameterMap,
                     input: DataStream[LabeledVector]): DataStream[String] = {
      instance.fitAndPredict(input)
    }
  }
  implicit val prediction = new PredictDataStreamOperation[ORR,LabeledVector, String] {
    override def predictDataStream(instance: ORR,
                                   predictParameters: ParameterMap,
                                   input: DataStream[LabeledVector])
    : DataStream[String] = {
      instance.fitAndPredict(input)
    }
  }
}