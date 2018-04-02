package eu.proteus.solma.ASYVI

import breeze.linalg.{DenseMatrix, Transpose}
import breeze.linalg._
import breeze.stats.distributions._
import breeze.numerics._

import hu.sztaki.ilab.ps.FlinkParameterServer._
import hu.sztaki.ilab.ps._
import hu.sztaki.ilab.ps.entities.PullAnswer
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.ml.common.{WithParameters, _}
import org.apache.flink.ml.math
import org.apache.flink.ml.math.Breeze._
//import org.apache.flink.ml.math.{BLAS, DenseVector, SparseMatrix, SparseVector}
import org.apache.flink.ml.preprocessing.StandardScaler
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.mutable


import scala.io.Source
//import org.reflections.Reflections


class ASYVI_PS(val dictFile:String="src/main/resources/20news.dict") extends WithParameters with Serializable{
  ASYVI_PS.initVocab(dictFile)
  // weights and the count of obs
  type P = (DenseMatrix[Double], Int)
  type WorkerIn = (Int, Int, P) // id, workerId, weigthvector
  type WorkerOut = (Boolean, Array[Int], P)
  type WOut = (LabeledVector, Int)
  /** fit with labeled data, predicts unlabeled data
    * handeles both labeld and unlabeled data
    * @param evalDataStream datastream of LabeledVectors
    * @return either the model or the predicted LabelVectors
    */
  def fitAndPredict(evalDataStream : DataStream[String]) : DataStream[String] = {
    //add a "problem" id
    //TODO in general this could be done in a more efficient way, i.e. solving several problems in the same time
    val partitionedStream = evalDataStream.map(x=>(x,0))
    // send the stream to the parameter server, define communication and Worker and Server instances
    val outputDS =
      transform(
        partitionedStream,
        ASYVIWorkerLogic,
        ASYVIParameterServerLogic,
        this.getWorkerParallelism(),
        this.getPSParallelism(),
        this.getIterationWaitTime()
      )

    val y = outputDS.flatMap(new FlatMapFunction[Either[(LabeledVector,Int),String], String] {
      override def flatMap(t: Either[(LabeledVector, Int), String], collector: Collector[String]): Unit = {
        if(t.isLeft) collector.collect(t.toString)
        if(t.isRight) collector.collect(t.right.get)
      }
    })
    y


  }
  //--------------GETTER AND SETTER ----------------------------------------------------------------
  /** Get the WorkerParallism value as Int
    * @return The number of worker parallelism
    */
  def getWorkerParallelism() : Int  = {
    this.parameters.get(ASYVI_PS.WorkerParallism).get
  }

  /**
    * Set the WorkerParallelism value as Int
    * @param workerParallism workerParallism as Int
    * @return
    */
  def setWorkerParallelism(workerParallism : Int): ASYVI_PS = {
    this.parameters.add(ASYVI_PS.WorkerParallism, workerParallism)
    this
  }

  /**
    * Get the SeverParallism value as Int
    * @return The number of server parallelism
    */
  def getPSParallelism() : Int  = {
    this.parameters.get(ASYVI_PS.PSParallelism).get
  }

  /**
    * Set the SeverParallism value as Int
    * @param psParallelism serverparallelism as Int
    * @return
    */
  def setPSParallelism(psParallelism : Int): ASYVI_PS = {
    parameters.add(ASYVI_PS.PSParallelism, psParallelism)
    this
  }

  def setNb_documents(Nb_documents : Int): ASYVI_PS = {
    ASYVI_PS._Nb_documents =  Nb_documents
    this
  }
  def getNb_documents(): Int = {
    ASYVI_PS._Nb_documents

  }

  /**
    * Variable to stop the calculation in stream environment
    * @param iterationWaitTime time as ms (long)
    * @return
    */
  def setIterationWaitTime(iterationWaitTime: Long) : ASYVI_PS = {
    parameters.add(ASYVI_PS.IterationWaitTime, iterationWaitTime)
    this
  }
  /** Variable to stop the calculation in stream environment
    * get the waiting time
    * @return The waiting time as Long
    */
  def getIterationWaitTime() : Long  = {
    this.parameters.get(ASYVI_PS.IterationWaitTime).get
  }
  /**
    * set a new windowsize
    * @param windowsize
    */
  def setWindowSize(windowsize : Int) : ASYVI_PS = {
    ASYVI_PS.setWindowSize_=(windowsize)
    this
  }

  def getWindowSize() : Int = {
    ASYVI_PS.getWindowSize
  }



  def getTopicKNum(): Int = {
    ASYVI_PS.topickK
  }
  def setTopicKNum(numK: Int): ASYVI_PS = {
    ASYVI_PS.topickK = numK
    this
  }




  /**
    * Get the Default Label which is used for unlabeld data points as double
    * @return The defaultLabel
    */
  def getDefaultLabel(): Double  = {
    this.parameters.get(ASYVI_PS.DefaultLabel).get
  }
  /**Set the Default Label
    * Variable to define the unlabeled observation that can be predicted later
    * @param label the value of the defaul label
    * @return
    */
  def setDefaultLabel(label: Double) : ASYVI_PS = {
    parameters.add(ASYVI_PS.DefaultLabel, label)
    ASYVIWorkerLogic.setLabel(label)
    this
  }
  /**
    * Set the required output format
    * @return either the Labeled datapoints (true) or the model (false)
    */
  def setLabeledVectorOutput(yes: Boolean): ASYVI_PS = {
    this.parameters.add(ASYVI_PS.LabeledVectorOutput, yes)
    this
  }
  /**
    * Get the current output format
    * @return
    */
  def getLabeledVectorOutput(): Boolean = {
    this.parameters.get(ASYVI_PS.LabeledVectorOutput).get
  }
  //---------------------------------WORKER--------------------------------
  val ASYVIWorkerLogic = new WorkerLogic[(String, Int), P, WOut]{

    private var window = ASYVI_PS.getWindowSize
    private var label = -1.0
    private var doLabeling = false

    private var topicKnum = ASYVI_PS.topickK
    //private  var lambda = Nd4j.randn(Array(3)).data().asDouble()
    private var  eta = DenseMatrix.rand[Double](topicKnum, ASYVI_PS.Vocab.vocab.size)
    //ASYvi_PS_Obj.Vocab.vocab.size
    //ASYVI_PS.Vocab.vocab.size

    private  var streamVI =  new StreamVI(data_from=false, K=topicKnum, dim=ASYVI_PS.Vocab.vocab.size,
      nu=0.001, covar_0=ASYVI_PS.Vocab.vocab,max_itr=5, v_0 = 0.01)

    //var max_itr:Int, var alpha_0: Double,var nu:Double,covar_0:collection.mutable.Map[String,Long])




    // free parameters
    private val dataFit         = new mutable.Queue[(String, Int)]()
    private val dataPredict     = new mutable.Queue[(String, Int)]()
    private val dataPredictPull = new mutable.Queue[(String, Int)]()
    private val dataQPull       = new mutable.Queue[(String, Int)]()
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
    override def onRecv(data: (String, Int), ps: ParameterServerClient[P, WOut]): Unit = {
      println("worker onRecv()")
      // if for unlabeled data
      //      if(this.doLabeling){
      //        val dataDenseVector = data._1
      //        this.dataPredict.enqueue((dataDenseVector, data._2))
      //        if(this.dataPredict.size >= this.window*0.5) {
      //          // already collected data is put into dataQPull-Queue so dataQ starts again with 0
      //          val dataPart = this.dataPredict.dequeueAll(x => x._2 == data._2)
      //          dataPart.map(x => this.dataPredictPull.enqueue(x))
      //          //sent pull request
      //          ps.pull(data._2)
      //        }
      //        // for labeled data
      //      } else{
      //      ps.pull(0)

      //      ps.pull(8)
      //      ps.push(0, (Array[Double](0.1,0.2,0.3), 1))
      this.dataFit.enqueue(data)
      if(this.dataFit.size >= this.window ) {
        // already collected data is put into dataQPull-Queue so dataQ starts again with 0

        ps.pull(1)


        //          ps.pull(data._2)
      }
      //      }


      val data_train = this.dataFit.dequeueAll(x=> true)
      if(data_train.size>0){
        //        todo


        val preplexity = 0
        val data_train2 = data_train.map(x=> x._1).toVector.toArray
        //preplexity,
        var grad = streamVI.find_lambda(data_train2,  ASYVI_PS.scale_of_step, etap =this.eta )
        ps.push(0, (grad, data_train.size))
        //        val update = (Nd4j.rand(3),0)
        //        ps.push(paramId,update)
      }


    }

    /**
      * defines PullRecv from ParameterServer, labels the unseen datapoints
      * @param paramId problem id
      * @param paramValue current weights from PS
      * @param ps parameter server client
      */
    override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {
      println("worker onPullRecv, paramValue_,2: +\n"+ paramValue._2+" paramId : "+ paramId)
      // dequeue all with key == paramId  dequeueAll(x=> boolean)
      //      training
      //val data_train = this.dataQPull.dequeueAll(x=> x._2==paramId)
      this.eta = paramValue._1
      //      val data_train = this.dataFit.dequeueAll(x=> true)
      //      if(data_train.size>0){
      ////        todo
      //
      //
      //        val preplexity = 0
      //        val data_train2 = data_train.map(x=> x._1).toVector.toArray
      //        //preplexity,
      //        var grad = streamVI.find_lambda(data_train2,  ASYVI_PS.scale_of_step, etap=this.eta.data().asDouble() )
      //        ps.push(paramId, (grad.data().asDouble(), data_train.size))
      ////        val update = (Nd4j.rand(3),0)
      ////        ps.push(paramId,update)
      //      }



    }//onPull
  }//Worker
  //-----------------------------------------------------SERVER-----------------------------------------
  val ASYVIParameterServerLogic = new ParameterServerLogic[P, String] {
    val params = new mutable.HashMap[Int, P]()
    var count =0

    var TopicKnum = ASYVI_PS.topickK

    //    def setFeatureDimension(dismension : Int) : Int = {
    //      this.featuredimension = dismension
    //      this.featuredimension
    //    }

    /**
      * On Pull Receive this method searches for the current weight of this problem id and send them
      * to the worker
      * @param id problem id
      * @param workerPartitionIndex the worker id to remember where the answer needs to be send to
      * @param ps parameter server
      */
    override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, String]): Unit = {
      val param = this.params.getOrElseUpdate(id, (DenseMatrix.rand[Double](TopicKnum, ASYVI_PS.Vocab.vocab.size),0))
      import runtime.ScalaRunTime.stringOf
      println("server onPullRecv "+id+" workerPIndex: "+workerPartitionIndex)
      //      println(param._1.mkString(" "))
      //      ps.output(stringOf(param._1.mkString(" ")))

      println("param._2: " + param._2)

      ps.output(stringOf(param._2))
      //      ps.answerPull(1,param,0)
      //      ps.answerPull(0,param,0)
      //      ps.answerPull(1,param,1)
      //ps.answerPull(id, param, workerPartitionIndex)
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
      count +=1
      ps.output("Server onPushRecv, count "+String.valueOf(count))
      println("Server onPushRecv, count "+String.valueOf(count))
      //      ps.output(deltaUpdate)
      if(deltaUpdate._2 >= 0){
        val param = this.params.getOrElseUpdate(id, (DenseMatrix.rand[Double](TopicKnum, ASYVI_PS.Vocab.vocab.size), 0))

        val newdeltaUpdate = (deltaUpdate._1, deltaUpdate._2 + param._2 )
        this.params.update(id, newdeltaUpdate)
        ps.output("server current seemed exmaples: " + newdeltaUpdate._2)
        if( (newdeltaUpdate._2 % (ASYVI_PS._Nb_documents / 5)) ==0){
          ps.output("\nlambda<&%$#,"+newdeltaUpdate._1.toArray.mkString(" ")+",&%$#lambda>,"+TopicKnum+","+ASYVI_PS.Vocab.vocab.size+"\n")

        }

        println("server current seemed exmaples: " + newdeltaUpdate._2)

      }
    }//onPushRecv
  }//PS
  // equals constructor
  def apply(dictFile:String="src/main/resources/20news.dict") : ASYVI_PS = new ASYVI_PS()
  ASYVI_PS.initVocab()

} // end class

 // end class


// Single instance
object ASYVI_PS extends Serializable{




   case object Vocab  extends Serializable{
     var vocabInited:Boolean = false
     private[this] var _vocab: mutable.Map[String, Long] = mutable.Map[String,Long]()

     def vocab: mutable.Map[String, Long] = _vocab

     def vocab_=(value: mutable.Map[String, Long]): Unit = {
       _vocab = value
     }


   }

  def initVocab(dictFile:String="src/main/resources/20news.dict"): Unit ={

    if(this.Vocab.vocabInited){
      return
    }

    println("Begin initVocab: "+ dictFile)
    var vocab = mutable.Map[String,Long]()
    // /share/flink/onlineML/svi20news.txt
    //val filename = "src/main/resources/20news.dict"
    //val filename = "/share/flink/onlineML/20news.dict"
    var filename = dictFile

    var index  = 0
    for (line <- Source.fromFile(filename).getLines()) {
      vocab += (line -> index)
      index +=1

    }
    //val W = vocab.size
    //vocab2 = vocab
    this.Vocab.vocab = vocab
    this.Vocab.vocabInited=true
    println("Build vocab done!")

  }

  var _Nb_documents: Int = 1500

  var _max_itr: Int = 5

  def max_itr: Int = _max_itr

  def max_itr_=(value: Int): Unit = {
    _max_itr = value
  }

  def Nb_documents: Int = _Nb_documents

  def Nb_documents_=(value: Int): Unit = {
    _Nb_documents = value
  }


  private[this] var _scale_of_step: Double = Nb_documents / 100


  private[this] var _windowSize: Int = 100


  private[this] var _topickK: Int = 20




  //////////////////Getter and Setter



  def scale_of_step: Double = _scale_of_step

  def scale_of_step_=(value: Double): Unit = {
    _scale_of_step = value
  }
  def getWindowSize: Int = _windowSize

  def setWindowSize_=(value: Int): Unit = {
    _windowSize = value
  }

  def topickK: Int = _topickK

  def topickK_=(value: Int): Unit = {
    _topickK = value
  }
  //////////////////Getter and Setter





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
    private val DefaultPSParallelism: Int = 2
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
}

