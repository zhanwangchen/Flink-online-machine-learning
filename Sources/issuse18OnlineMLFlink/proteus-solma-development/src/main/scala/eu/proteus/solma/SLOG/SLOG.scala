package eu.proteus.solma.SLOG
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
import breeze.linalg._
import breeze.linalg.{DenseMatrix, det, eigSym, inv, max, norm, pinv, rank, sum, trace}
import breeze.stats.distributions._
import breeze.numerics._
import eu.proteus.solma.pipeline.{StreamEstimator, StreamFitOperation, StreamPredictor,PredictDataStreamOperation}

class SLOG extends WithParameters with Serializable
  with StreamEstimator[SLOG]
  with StreamPredictor[SLOG]{
  private val serialVersionUID = 6529645098267711690L

  // weights and the count of obs
  type P = (DenseMatrix[Double],WeightVector,Int)
  type WorkerIn = (Int, Int, (DenseMatrix[Double],WeightVector,Int)) // id, workerId, weigthvector
  type WorkerOut = (Boolean, Array[Int], (DenseMatrix[Double],WeightVector,Int))
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
        SLOGWorkerLogic,
        SLOGParameterServerLogic,
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
            collectAnswerMsg((true, Array(partitionId, id), (DenseMatrix.eye(7),WeightVector(new DenseVector(Array(0.0)),0.0),0)))
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
    this.parameters.get(SLOG.WorkerParallism).get
  }

  /**
    * Set the WorkerParallelism value as Int
    * @param workerParallism workerParallism as Int
    * @return
    */
  def setWorkerParallelism(workerParallism : Int): SLOG = {
    this.parameters.add(SLOG.WorkerParallism, workerParallism)
    this
  }
  /**
    * Get the SeverParallism value as Int
    * @return The number of server parallelism
    */
  def getPSParallelism() : Int  = {
    this.parameters.get(SLOG.PSParallelism).get
  }
  /**
    * Set the SeverParallism value as Int
    * @param psParallelism serverparallelism as Int
    * @return
    */
  def setPSParallelism(psParallelism : Int): SLOG = {
    parameters.add(SLOG.PSParallelism, psParallelism)
    this
  }
  /**
    * Variable to stop the calculation in stream environment
    * @param iterationWaitTime time as ms (long)
    * @return
    */
  def setIterationWaitTime(iterationWaitTime: Long) : SLOG = {
    parameters.add(SLOG.IterationWaitTime, iterationWaitTime)
    this
  }
  /** Variable to stop the calculation in stream environment
    * get the waiting time
    * @return The waiting time as Long
    */
  def getIterationWaitTime() : Long  = {
    this.parameters.get(SLOG.IterationWaitTime).get
  }
  /**
    * set a new windowsize
    * @param windowsize
    */
  def setWindowSize(windowsize : Int) : SLOG = {
    this.parameters.add(SLOG.WindowSize, windowsize)
    SLOGWorkerLogic.setWindowSize(this.getWindowSize())
    this
  }
  def setFeatureDimension( dimensions: Int ): SLOG = {
    this.parameters.add(SLOG.FeatureDimensions, dimensions)
    SLOGParameterServerLogic.setFeatureDimension(this.getFeatureDimension())
    this
  }
  def getFeatureDimension(): Int = {
    this.parameters.get(SLOG.FeatureDimensions).get
  }

  def setPenalization( dimensions: Double ): SLOG = {
    this.parameters.add(SLOG.Penalization, dimensions)
    SLOGWorkerLogic.setPenalization(this.getPenalization())
    this
  }
  def getPenalization(): Double = {
    this.parameters.get(SLOG.Penalization).get
  }
  /**
    * Get the Windowsize
    * @return The windowsize
    */
  def getWindowSize(): Int = {
    this.parameters.get(SLOG.WindowSize).get
  }
  /**
    * Get the Default Label which is used for unlabeld data points as double
    * @return The defaultLabel
    */
  def getDefaultLabel(): Double  = {
    this.parameters.get(SLOG.DefaultLabel).get
  }
  /**Set the Default Label
    * Variable to define the unlabeled observation that can be predicted later
    * @param label the value of the defaul label
    * @return
    */
  def setDefaultLabel(label: Double) : SLOG = {
    parameters.add(SLOG.DefaultLabel, label)
    SLOGWorkerLogic.setLabel(label)
    this
  }
  /**
    * Set the required output format
    * @return either the Labeled datapoints (true) or the model (false)
    */
  def setLabeledVectorOutput(yes: Boolean): SLOG = {
    this.parameters.add(SLOG.LabeledVectorOutput, yes)
    this
  }
  /**
    * Get the current output format
    * @return
    */
  def getLabeledVectorOutput(): Boolean = {
    this.parameters.get(SLOG.LabeledVectorOutput).get
  }
  //---------------------------------WORKER--------------------------------
  val SLOGWorkerLogic = new WorkerLogic[(LabeledVector, Int), P, WOut]{
    // Worker variables, have same default values as ALS
    private var window = 100
    private var label = -9.0
    private var doLabeling = true
    private var penalize = 0.1
    private  var w =new WeightVector(DenseVector(Array[Double](0.1)),0.0)





    // free parameters
    private val dataFit         = new mutable.Queue[(LabeledVector, Int)]()
    private val dataPredict     = new mutable.Queue[(DenseVector, Int)]()
    private val dataPredictPull = new mutable.Queue[(DenseVector, Int)]()
    private val dataQPull       = new mutable.Queue[(LabeledVector, Int)]()
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
    def setPenalization(dismension : Double) : Double = {
      this.penalize = dismension
      this.penalize
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
          val dataPart = dataFit.dequeueAll(x=>x._2== data._2)
          dataPart.map(x=> dataQPull.enqueue(x))
          ps.pull(data._2)
        }
      }
    }
    /**
      * aggregates the values of a set of datapoints to calculate the LS weights
      * @param trainSet Set of labeled data points
      * @return tuple of aggregated values : (count, x           , xy     , x*x     , y)
      */

    def calcNewWeigths(trainSet: mutable.Seq[(LabeledVector, Int)], xx: DenseMatrix[Double]) : (breeze.linalg.DenseMatrix[Double],breeze.linalg.DenseVector[Double],Int) = {

      //val out:P =
      var w = this.w.weights
      var historical_xx = xx

      var b = diag(w.asBreeze.toDenseVector)
      var lamda = 0.1
      if(this.w.weights.toArray.length==1){
        //        uniformly initialize vector, and transfrom it to diagnoal matrix
        val gau = Gaussian(0.0,1.0)
        val a = gau.sample(7).toArray
        b = diag(a.asBreeze.toDenseVector).map(x=>abs(x))
        historical_xx = DenseMatrix.zeros(7,7)
      }
      val tmp = trainSet.map{ datex =>
        var xt= datex._1.vector.copy
        //        divide total count to scal the output result
        val yt = datex._1.label
        //
        val middle = lamda * DenseMatrix.eye[Double](7)
        //        Done, accumulate all previous sample and current one to sum(x,x^T)
        val cxx = xt.outer(xt).asBreeze.toDenseMatrix
        val temp_xx = historical_xx + cxx
        val sqrtb = b.map(x=> sqrt(x))
        val bxxb = sqrtb * temp_xx * sqrtb
//        simple closed solution
        val dirw = inv(temp_xx + lamda * inv( b) )
//        val invbxxb = inv(bxxb)
//        val dirw = sqrtb * invbxxb * sqrtb
        val out = dirw * xt.asBreeze.toDenseVector * yt


        (1,out,cxx)

      }.reduce { (x, y) =>
        val count = x._1 + y._1
        val o = x._2 + y._2
        val xx = x._3 + y._3
        (count, o, xx)
      }
      (tmp._3,tmp._2,tmp._1)
    }
    /**
      * defines PullRecv from ParameterServer, labels the unseen datapoints
      * @param paramId problem id
      * @param paramValue current weights from PS
      * @param ps parameter server client
      */
    override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {
      // dequeue all with key == paramId  dequeueAll(x=> boolean)
      this.w = paramValue._2
      //      training
      val data_train = this.dataQPull.dequeueAll(x=> x._2==paramId)
      if(data_train.size>0){
        val ws = calcNewWeigths(data_train,paramValue._1)
        val v = ws._2.toVector
        val vv = org.apache.flink.ml.math.Vector.vectorConverter.convert(v)
        //val converter = new org.apache.flink.ml.math.Vector.vectorConverter
        //val v:org.apache.flink.ml.math.Vector = new org.apache.flink.ml.math.Vector.vectorConverter(ws._1.toArray)
        val topush = (ws._1,new WeightVector(vv,0.0), ws._3 )
        ps.push(paramId,topush)
      }

      if(this.dataPredictPull.nonEmpty && (paramValue._3>2000)) {
        val xPredict = this.dataPredictPull.dequeueAll(x => x._2 == paramId)
        val xs_Sized = xPredict.splitAt(this.window)
        xs_Sized._2.foreach(x => this.dataPredictPull.enqueue(x))
        if (xs_Sized._1.size > 0) {
          xs_Sized._1.foreach { x =>
            val WeightVector(weights, weight0) = paramValue._2
            val r = x._1.asBreeze.dot(weights.asBreeze.toVector)
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
  val SLOGParameterServerLogic = new ParameterServerLogic[P, String] {
    val params = new mutable.HashMap[Int, (DenseMatrix[Double],WeightVector, Int)]()
    private var featuredimension = 10

    def setFeatureDimension(dismension : Int) : Int = {
      this.featuredimension = dismension
      this.featuredimension
    }

    /**
      * On Pull Receive this method searches for the current weight of this problem id and send them
      * to the worker
      * @param id problem id
      * @param workerPartitionIndex the worker id to remember where the answer needs to be send to
      * @param ps parameter server
      */
    override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, String]): Unit = {
      val param = this.params.getOrElseUpdate(id, (DenseMatrix.eye[Double](1),WeightVector(DenseVector(Array[Double](0.0)),0.0),0))
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

          val count = param._3 + deltaUpdate._3
          val regularxx1 = param._3/count.toDouble * param._1
          val regularxx2 = deltaUpdate._3/count.toDouble * deltaUpdate._1


          this.params.update(id,(regularxx1+regularxx2,WeightVector(deltaUpdate._2.weights,0.0),count))
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
  def apply() : SLOG = new SLOG()
} // end class
// Single instance
object SLOG {
  case object FeatureDimensions extends Parameter[Int] {
    /**
      * Default time - needs to be set in any case to stop this calculation
      */
    private val DefaultFeatureDimensions: Int = 10
    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(FeatureDimensions.DefaultFeatureDimensions)
  }
  case object Penalization extends Parameter[Double] {
    /**
      * Default time - needs to be set in any case to stop this calculation
      */
    private val DefaultPenalization: Double= 0.1
    /**
      * Default value.
      */
    override val defaultValue: Option[Double] = Some(Penalization.DefaultPenalization)
  }
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
    private val DefaultWorkerParallism: Int = 1
    /**
      * Default value.
      */
    override val defaultValue: Option[Int] = Some(WorkerParallism.DefaultWorkerParallism)
  }
  case object PSParallelism extends Parameter[Int] {
    /**
      * Default server parallelism.
      */
    private val DefaultPSParallelism: Int = 1
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
  implicit val fit =  new StreamFitOperation[SLOG,LabeledVector,String] {
    override def fit(instance: SLOG,
                     fitParameters: ParameterMap,
                     input: DataStream[LabeledVector]): DataStream[String] = {
      instance.fitAndPredict(input)
    }
  }
  implicit val prediction = new PredictDataStreamOperation[SLOG,LabeledVector, String] {
    override def predictDataStream(instance: SLOG,
                                   predictParameters: ParameterMap,
                                   input: DataStream[LabeledVector])
    : DataStream[String] = {
      instance.fitAndPredict(input)
    }
  }
}
