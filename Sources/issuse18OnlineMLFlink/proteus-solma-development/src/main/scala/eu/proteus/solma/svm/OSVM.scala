package eu.proteus.solma.svm

//import breeze.linalg.{DenseMatrix, _}
import eu.proteus.solma.pipeline.{PredictDataStreamOperation, StreamEstimator, StreamFitOperation, StreamPredictor}
import hu.sztaki.ilab.ps.FlinkParameterServer._
import hu.sztaki.ilab.ps._
import hu.sztaki.ilab.ps.entities.PullAnswer
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.ml.common.{WithParameters, _}
import org.apache.flink.ml.math.{BLAS, DenseVector, SparseVector}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable


class OSVM extends WithParameters with Serializable
  with StreamEstimator[OSVM]
  with StreamPredictor[OSVM]{

  private val serialVersionUID = 6529685098267711690L
  // weights and the count of obs
  type P = (WeightVector,Double,Int)
  type WorkerIn = (Int, Int, (WeightVector,Double, Int)) // id, workerId, weigthvector
  type WorkerOut = (Boolean, Array[Int], (WeightVector,Double, Int))
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
        SVMWorkerLogic,
        SVMParameterServerLogic,
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
            collectAnswerMsg((true, Array(partitionId, id), (WeightVector(new DenseVector(Array(0.0)),0.0),0.0,0)))
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
    this.parameters.get(OSVM.WorkerParallism).get
  }
  /**
    * Set the WorkerParallelism value as Int
    * @param workerParallism workerParallism as Int
    * @return
    */
  def setWorkerParallelism(workerParallism : Int): OSVM = {
    this.parameters.add(OSVM.WorkerParallism, workerParallism)
    this
  }
  /**
    * Get the SeverParallism value as Int
    * @return The number of server parallelism
    */
  def getPSParallelism() : Int  = {
    this.parameters.get(OSVM.PSParallelism).get
  }
  /**
    * Set the SeverParallism value as Int
    * @param psParallelism serverparallelism as Int
    * @return
    */
  def setPSParallelism(psParallelism : Int): OSVM = {
    parameters.add(OSVM.PSParallelism, psParallelism)
    this
  }
  /**
    * Variable to stop the calculation in stream environment
    * @param iterationWaitTime time as ms (long)
    * @return
    */
  def setIterationWaitTime(iterationWaitTime: Long) : OSVM = {
    parameters.add(OSVM.IterationWaitTime, iterationWaitTime)
    this
  }
  /** Variable to stop the calculation in stream environment
    * get the waiting time
    * @return The waiting time as Long
    */
  def getIterationWaitTime() : Long  = {
    this.parameters.get(OSVM.IterationWaitTime).get
  }
  /**
    * set a new windowsize
    * @param windowsize
    */
  def setWindowSize(windowsize : Int) : OSVM = {
    this.parameters.add(OSVM.WindowSize, windowsize)
    SVMWorkerLogic.setWindowSize(this.getWindowSize())
    this
  }
  /**
    * Get the Windowsize
    * @return The windowsize
    */
  def getWindowSize(): Int = {
    this.parameters.get(OSVM.WindowSize).get
  }
  /**
    * Get the Default Label which is used for unlabeld data points as double
    * @return The defaultLabel
    */
  def getDefaultLabel(): Double  = {
    this.parameters.get(OSVM.DefaultLabel).get
  }
  /**Set the Default Label
    * Variable to define the unlabeled observation that can be predicted later
    * @param label the value of the defaul label
    * @return
    */
  def setDefaultLabel(label: Double) : OSVM = {
    parameters.add(OSVM.DefaultLabel, label)
    SVMWorkerLogic.setLabel(label)
    this
  }
  /**
    * Set the required output format
    * @return either the Labeled datapoints (true) or the model (false)
    */
  def setLabeledVectorOutput(yes: Boolean): OSVM = {
    this.parameters.add(OSVM.LabeledVectorOutput, yes)
    this
  }
  /**
    * Get the current output format
    * @return
    */
  def getLabeledVectorOutput(): Boolean = {
    this.parameters.get(OSVM.LabeledVectorOutput).get
  }
  //---------------------------------WORKER--------------------------------
  val SVMWorkerLogic = new WorkerLogic[(LabeledVector, Int), P, WOut]{
    // Worker variables, have same default values as ALS
    private var window = 100
    private var label = -9.0
    private var doLabeling = true
    private[this] var _stepSizeW: Double = 0.01
    private[this] var __stepSizeB: Double = 0.01



    private[this] var __c: Double = 1
    private  var w =new WeightVector(DenseVector(Array[Double](0.1,0.1,0.1,0.1,0.1,0.1,0.1)),0.0)
    private var b =0.1

    private var paramId = 0




    // free parameters
    private val dataFit         = new mutable.Queue[(LabeledVector, Int)]()
    private val dataPredict     = new mutable.Queue[(DenseVector, Int)]()

    /**
      * Set Parameters
      */


    private[this] def _c: Double = __c

    private[this] def _c_=(value: Double): Unit = {
      __c = value
    }

    private[this] def _stepSizeB: Double = __stepSizeB

    private[this] def _stepSizeB_=(value: Double): Unit = {
      __stepSizeB = value
    }

    private def stepSize: Double = _stepSizeW

    private def stepSize_=(value: Double): Unit = {
      _stepSizeW = value
    }
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

    private var noMoredata = false
     var workerpp:ParameterServerClient[P, WOut]=null
    /**
      * Method handles data receives
      * @param data the datapoint arriving on the worker
      * @param ps parameter server client
      */
    override def onRecv(data: (LabeledVector, Int), ps: ParameterServerClient[P, WOut]): Unit = {
      workerpp=ps
      //get gloable parameters from server
      // if for unlabeled data
      println("worker onRecv() ")
      if(data._1.label == this.label && this.doLabeling){
        val dataDenseVector = data._1.vector match {
          case d: DenseVector => d
          case s: SparseVector => s.toDenseVector
        }

        this.dataPredict.enqueue((dataDenseVector, data._2))

        if(this.dataPredict.size >= this.window || noMoredata) {
          ps.pull(data._2)

          if(this.dataPredict.nonEmpty ) {
            this.predict(dataPredict)


          }


        }

        // for labeled data , no need to predict, just for trainning
      } else{
        if(data._1.label != this.label)  this.dataFit.enqueue(data)



        if(this.dataFit.size >= this.window ||noMoredata) {
          // already collected data is put into dataQPull-Queue so dataQ starts again with 0

          val dataPart = dataFit.dequeueAll(x=>x._2== data._2)

          ps.pull(data._2)

          if(dataPart.size>0){
            val ws = calcNewWeigths(dataPart)

            val topush = (new WeightVector(ws._1, 0.0), ws._2, ws._3 )
            ps.push(paramId,topush)
          }

        }
      }










    }

    def predict(dataPredict:mutable.Queue[(DenseVector, Int)]): Unit ={
      val xPredict = this.dataPredict.dequeueAll(x => x._2 == paramId)
      val xs_Sized = xPredict.splitAt(this.window)
      xs_Sized._2.foreach(x => this.dataPredict.enqueue(x))
      if (xs_Sized._1.size > 0) {
        xs_Sized._1.foreach { x =>
          val wxb = x._1.dot(this.w.weights) + this.b
          var labelPredict = -1
          if(wxb >0.0){
            labelPredict = 1
          }

          workerpp.output(LabeledVector((labelPredict), x._1), paramId)
        }
      }
    }



    override def close(): Unit ={

      noMoredata=true

      if(this.dataFit.nonEmpty){
        this.onRecv(this.dataFit.front, this.workerpp)
      }

      if(this.dataPredict.nonEmpty){
        this.predict(this.dataPredict)
      }

      println("dataFit, dataPredict, size: "+this.dataFit.size+", "+this.dataPredict.size)
      println("worker onclose(): "+this.toString)




      //this.close()
    }

    /**
      * aggregates the values of a set of datapoints to calculate the LS weights
      * @param trainSet Set of labeled data points
      * @return tuple of aggregated values : (count, x           , xy     , x*x     , y)
      */
    def calcNewWeigths(trainSet: mutable.Seq[(LabeledVector, Int)]) : (org.apache.flink.ml.math.Vector,Double,Int) = {

      //val out:P =
      var w = this.w.weights
      if(this.w.weights.toArray.length==1){
        w = new DenseVector(Array[Double](0.1,0.1,0.1,0.1,0.1,0.1,0.1))
      }
      val tmp = trainSet.map{ datex =>
        var xt= datex._1.vector.copy
        val yt = datex._1.label
        //
        var sign = 1
        if (yt *( w.dot(xt) + this.b ) < 1){
          sign = -1
        }
        var dirw =w.copy
        //BLAS.scal(- yt, xt)
        /**
          * y += a * x
          *
          * def axpy(a: Double, x: Vector, y: Vector): Unit = {
          */
        //var t2: org.apache.flink.ml.math.Vector
        //axpy(yt, xt,)
        BLAS.scal(yt,     xt)
        BLAS.scal(sign,   xt)
        BLAS.scal(this.__c ,xt)
        BLAS.axpy(-1.0, xt , dirw)
        var dirb = yt
        if(sign == -1){
          dirb = -yt
        }else{
          dirb = 0
        }
        //axpy(-1.0,yt * xt,dirb)

        //val driw = w-xt
        //val dirw = w - this.__c * (yt * xt) * sign

        //
        //          var ou:P = (wtt,btt)
        (1,dirw,dirb)
        //(1,1,1)
      }.reduce { (x, y) =>
        val count = x._1 + y._1
        BLAS.axpy(1.0, x._2 , y._2)
        //axpy(1.0, x._3 , y._3)
        val dirbSum = x._3 + y._3

        (count, y._2, dirbSum)
        //
        //(count, y._2, y._3)
        //         (1,1,1)
      }
      BLAS.scal(1.0/tmp._1, tmp._2)
      //val dirw = tmp._2 /tmp._1
      val dirb = 1.0 * tmp._3 / tmp._1
      BLAS.scal(this._stepSizeW, tmp._2)
      BLAS.axpy(-1.0, w , tmp._2)
      val wtt = tmp._2.copy
      val btt = b - this.__stepSizeB * dirb

      (wtt,btt,tmp._1)
    }





    /**
      * defines PullRecv from ParameterServer, labels the unseen datapoints
      * @param paramId problem id
      * @param paramValue current weights from PS
      * @param ps parameter server client
      */
    override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {

      println("worker onPullRecv() "+paramId)
      if(this.paramId!=paramId) throw new IllegalArgumentException("paramId was wrong...")

      this.w = paramValue._1
      this.b = paramValue._2

      if(this.w.weights.toArray.length==1){
        this.w = new WeightVector(DenseVector(Array[Double](0.1,0.1,0.1,0.1,0.1,0.1,0.1)),0.0)

      }

    }//onPull
  }//Worker
  //-----------------------------------------------------SERVER-----------------------------------------
  val SVMParameterServerLogic = new ParameterServerLogic[P, String] {
    val params = new mutable.HashMap[Int, (WeightVector, Double, Int)]()
    /**
      * On Pull Receive this method searches for the current weight of this problem id and send them
      * to the worker
      * @param id problem id
      * @param workerPartitionIndex the worker id to remember where the answer needs to be send to
      * @param ps parameter server
      */
    override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, String]): Unit = {
      println("Server onPullRecv() "+id+" workerPartitionIndex: "+ workerPartitionIndex)
      val param = this.params.getOrElseUpdate(id, (WeightVector(DenseVector(Array[Double](0.0711)),0.0),0.0,0))
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
      println("Server onPushRecv() "+id)
      if(deltaUpdate._3 >= 0){
        val param = this.params.getOrElseUpdate(id, (WeightVector(DenseVector(Array[Double](0.091)),0.0),0.0,0))
        if(param._3 > 0){
          //          val newa = deltaUpdate._1 + param._1
          BLAS.axpy(1.0,param._1.weights,deltaUpdate._1.weights)

          val count = param._3 + deltaUpdate._3
          //scal down the current weights
          //x = a*x
          BLAS.scal(param._3.toDouble/count.toDouble, param._1.weights)
          BLAS.scal(deltaUpdate._3.toDouble/count.toDouble,deltaUpdate._1.weights)
          BLAS.axpy(1.0,deltaUpdate._1.weights,param._1.weights)
          val b1 = param._3.toDouble/count.toDouble*param._2
          val b2 = deltaUpdate._3.toDouble/count.toDouble*deltaUpdate._2
          val b = b1+b2

          this.params.update(id,(WeightVector(param._1.weights,0.0),b,count))
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
  def apply() : OSVM = new OSVM()
}

object OSVM {
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
    private val LabelDefault: Double = -9.0
    /**
      * Default value.
      */
    override val defaultValue: Option[Double] = Some(DefaultLabel.LabelDefault)
  }
  case object LabeledVectorOutput extends Parameter[Boolean] {
    /**
      * Default output are the Weights
      */
    private val DefaultLabeledVectorOutput: Boolean = true
    /**
      * Default value.
      */
    override val defaultValue: Option[Boolean] = Some(LabeledVectorOutput.DefaultLabeledVectorOutput)
  }
  def apply(): OSVM = {
    new OSVM()
  }
  implicit val fitSVM =  new StreamFitOperation[OSVM,LabeledVector,String] {
    override def fit(instance: OSVM,
                     fitParameters: ParameterMap,
                     input: DataStream[LabeledVector]): DataStream[String] = {
      instance.fitAndPredict(input)
    }
  }
  implicit val prediction = new PredictDataStreamOperation[OSVM,LabeledVector, String] {
    override def predictDataStream(instance: OSVM,
                                   predictParameters: ParameterMap,
                                   input: DataStream[LabeledVector])
    : DataStream[String] = {
      instance.fitAndPredict(input)
    }
  }
}