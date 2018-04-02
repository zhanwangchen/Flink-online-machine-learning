// The implementation is based on a paper A. Bouchachia, and C. Vanaret, "GT2FC: An online growing interval type-2
// self-learning fuzzy classifier." IEEE Transactions on Fuzzy Systems 22.4, 999-1018, 2014

package eu.proteus.solma.ggmm

import breeze.linalg.eigSym.EigSym
import breeze.linalg.{DenseMatrix, det, eigSym, inv, max, norm, pinv, rank, sum, trace}
import breeze.numerics.{abs, exp, log, sqrt}
import hu.sztaki.ilab.ps.FlinkParameterServer._
import hu.sztaki.ilab.ps._
import hu.sztaki.ilab.ps.entities.PullAnswer
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.ml.common.{WithParameters, _}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{DenseVector, SparseVector}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import eu.proteus.solma.pipeline.{PredictDataStreamOperation, StreamEstimator, StreamFitOperation, StreamPredictor}

import scala.math._
import scala.collection.mutable

/**
  * A class for containing information of one Gaussian
  * @param id Model number.
  * @param tau Weight of the Gaussian in the mixture model.
  * @param c Sum of the expected posteriors of the Gaussian.
  * @param mu Centers of the Gaussian.
  * @param sigma Covariance matrix of the Gaussian.
  * @param Label of the Gaussian.
  */
case class GGaussian(id: Long, tau: Double,c:Double, mu: breeze.linalg.DenseVector[Double], sigma: DenseMatrix[Double],Label: Int)

class GGMM extends WithParameters with Serializable
  with StreamEstimator[GGMM]
  with StreamPredictor[GGMM]{
  type P = (Seq[GGaussian], Int)

  // Number of clusters.
  val K = 3

  // Learning rate;
  val alpha = 0.5
  val k0 = 2

  // (id, worker id, parameters)
  type WorkerIn = (Int, Int, P)
  type WorkerOut = (Boolean, Array[Int], P)
  type WOut = (LabeledVector, Int)

  def fitAndPredict(evalDataStream: DataStream[LabeledVector]): DataStream[String] = {

    // add a "problem" id
    val partitionedStream = evalDataStream.map(x => (x, 0))

    // send the stream to the parameter server, define communication and Worker and Server instances
    val outputDS = transform(
      partitionedStream,
      GGMMWorkerLogic,
      GGMMParameterServerLogic,
      (x: (WorkerOut)) => x match {
        case (true, Array(_, id), _) => Math.abs(id.hashCode())
        case (false, Array(_, id), _) => Math.abs(id.hashCode())
      },
      (x: (WorkerIn)) => x._2,
      this.getWorkerParallelism,
      this.getPSParallelism,
      new WorkerReceiver[WorkerIn, P] {
        override def onPullAnswerRecv(msg: WorkerIn, pullHandler: PullAnswer[P] => Unit): Unit = {
          pullHandler(PullAnswer(msg._1, msg._3))
        }
      },
      new WorkerSender[WorkerOut, P] {
        // output of Worker
        override def onPull(id: Int, collectAnswerMsg: WorkerOut => Unit, partitionId: Int): Unit = {
          collectAnswerMsg((true, Array(partitionId, id), ((for (i <- 1 to K) yield GGaussian(i, 0.5, 1.0, breeze.linalg.DenseVector.rand(2),
            DenseMatrix.rand(2, 2), 0)), 0)))
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
      this.getIterationWaitTime
    )
    val y = outputDS.flatMap(new FlatMapFunction[Either[WOut, String], String] {
      override def flatMap(t: Either[WOut, String], collector: Collector[String]): Unit = {
        if (getLabeledVectorOutput && t.isLeft) collector.collect(t.toString)
        if (!getLabeledVectorOutput && t.isRight) collector.collect(t.toString)
      }
    })
    y
  }

  // -------------- GETTER AND SETTER ----------------------------------------------------------------

  /** Get the WorkerParallelism value as Int
    *
    * @return The number of worker parallelism
    */
  def getWorkerParallelism: Int = {
    this.parameters.get(GGMM.WorkerParallism).get
  }

  /**
    * Set the WorkerParallelism value as Int
    *
    * @param workerParallelism workerParallelism as Int
    * @return
    */
  def setWorkerParallelism(workerParallelism: Int): GGMM = {
    this.parameters.add(GGMM.WorkerParallism, workerParallelism)
    this
  }

  /**
    * Get the ServerParallelism value as Int
    *
    * @return The number of server parallelism
    */
  def getPSParallelism: Int = {
    this.parameters.get(GGMM.PSParallelism).get
  }

  /**
    * Set the ServerParallelism value as Int
    *
    * @param psParallelism server parallelism as Int
    * @return
    */
  def setPSParallelism(psParallelism: Int): GGMM = {
    parameters.add(GGMM.PSParallelism, psParallelism)
    this
  }

  /**
    * Variable to stop the calculation in stream environment
    *
    * @param iterationWaitTime time as ms (long)
    * @return
    */
  def setIterationWaitTime(iterationWaitTime: Long): GGMM = {
    parameters.add(GGMM.IterationWaitTime, iterationWaitTime)
    this
  }

  /** Variable to stop the calculation in stream environment
    * get the waiting time
    *
    * @return The waiting time as Long
    */
  def getIterationWaitTime: Long = {
    this.parameters.get(GGMM.IterationWaitTime).get
  }

  /**
    * Set a new window size
    *
    * @param windowsize Window size.
    */
  def setWindowSize(windowsize: Int): GGMM = {
    this.parameters.add(GGMM.WindowSize, windowsize)
    GGMMWorkerLogic.setWindowSize(this.getWindowSize)
    this
  }

  /**
    * Get the Windowsize
    *
    * @return The windowsize
    */
  def getWindowSize: Int = {
    this.parameters.get(GGMM.WindowSize).get
  }

  /**
    * Get the Default Label which is used for unlabeled data points as double
    *
    * @return The defaultLabel
    */
  def getDefaultLabel: Double = {
    this.parameters.get(GGMM.DefaultLabel).get
  }

  /** Set the Default Label
    * Variable to define the unlabeled observation that can be predicted later
    *
    * @param label the value of the defaul label
    * @return
    */
  def setDefaultLabel(label: Double): GGMM = {
    parameters.add(GGMM.DefaultLabel, label)
    GGMMWorkerLogic.setLabel(label)
    this
  }

  /**
    * Set the required output format
    *
    * @return either the Labeled data points (true) or the model (false)
    */
  def setLabeledVectorOutput(yes: Boolean): GGMM = {
    this.parameters.add(GGMM.LabeledVectorOutput, yes)
    this
  }

  /**
    * Get the current output format
    *
    * @return
    */
  def getLabeledVectorOutput: Boolean = {
    this.parameters.get(GGMM.LabeledVectorOutput).get
  }

  // ---------------------------------WORKER--------------------------------
  val GGMMWorkerLogic = new WorkerLogic[(LabeledVector, Int), P, WOut] {
    private var window = 50
    private var label = -1.0
    private var doLabeling = true
    private val dataFit = new mutable.Queue[(LabeledVector, Int)]()
    private val dataPredict = new mutable.Queue[(DenseVector, Int)]()
    private val dataPredictPull = new mutable.Queue[(DenseVector, Int)]()

    // free parameters
    private val dataQPull = new mutable.Queue[(LabeledVector, Int)]()

    def setWindowSize(windowsize: Int): Int = {
      this.window = windowsize
      this.window
    }

    /** Set the Default Label
      * Variable to define the unlabeled observation that can be predicted later
      *
      * @param label the value of the default label
      * @return the default label
      */
    def setLabel(label: Double): Double = {
      this.label = label
      this.label
    }

    /**
      * Set to true if prediction is required,else false
      *
      * @param predictionOutput Flag
      * @return
      */
    def setDoLabeling(predictionOutput: Boolean): Boolean = {
      this.doLabeling = predictionOutput
      this.doLabeling
    }

    override def onRecv(data: WOut, ps: ParameterServerClient[P, WOut]): Unit = {

      // if for unlabeled data
      if (data._1.label == this.label && this.doLabeling) {
        val dataDenseVector = data._1.vector match {
          case d: DenseVector => d
          case s: SparseVector => s.toDenseVector
        }
        this.dataPredict.enqueue((dataDenseVector, data._2))
        if (this.dataPredict.size >= this.window * 0.5) {

          // already collected data is put into dataQPull-Queue so dataQ starts again with 0
          val dataPart = this.dataPredict.dequeueAll(x => x._2 == data._2)
          dataPart.foreach(x => this.dataPredictPull.enqueue(x))

          // sent pull request
          ps.pull(data._2)
        }
      }

      // for labeled data
      else {
        if (data._1.label != this.label) this.dataFit.enqueue(data)
        if (this.dataFit.lengthCompare(this.window) >= 0) {

          // already collected data is put into dataQPull-Queue so dataQ starts again with 0
          val dataPart = dataFit.dequeueAll(x => x._2 == data._2)
          dataPart.foreach(x => dataQPull.enqueue(x))
          ps.pull(data._2)
        }
      }
    }
    def get_invertible_ggausian(i:Int):GGaussian={
      while(true){
        var invertible = DenseMatrix.rand[Double](2,2)
        invertible = max(invertible,invertible.t)
        if(rank(invertible) == 2){
          return GGaussian(i, 0.5, 1.0, breeze.linalg.DenseVector.rand(2),invertible, 0)
        }
      }
      GGaussian(i, 0.5, 1.0, breeze.linalg.DenseVector.rand(2), DenseMatrix.rand(2, 2), 0)
    }
    override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {

      // dequeue all with key == paramId  dequeueAll(x=> boolean)
      var localParameter = paramValue._1
      if (localParameter.isEmpty) {
//        val k = 10
        val temparray = (for (i <- 1 to K) yield get_invertible_ggausian(i)).toSeq
        localParameter = temparray
      }

      //      training
      val data_train = this.dataQPull.dequeueAll(x => x._2 == paramId)
      if (data_train.nonEmpty) {
        val p = calculateMatch(data_train, localParameter)
        val updatedGaussians = update(p, localParameter)
        //        I think here is the count of train sample. maybe not use, just follow pattern.
        ps.push(paramId, (updatedGaussians, data_train.size))
      }
      // && paramValue._2> 1000
      print("++++++####"+paramValue._2)
      if (this.dataPredictPull.nonEmpty && paramValue._2> 1000) {
        val xPredict = this.dataPredictPull.dequeueAll(x => x._2 == paramId)
        val xs_Sized = xPredict.splitAt(this.window)
        xs_Sized._2.foreach(x => this.dataPredictPull.enqueue(x))
        if (xs_Sized._1.nonEmpty) {
          xs_Sized._1.foreach { x =>

            /*
               TODO predict a class of a new data point. Done!
               This actually falls under first situation of an update(), but update() takes only LabeledVector
             */
            var pMax = -1.0
            var prediction = -1
            for (i <- localParameter.iterator) {
              var p = 0.0
              p = getGaussianProbability1(x._1, i.mu, i.sigma)
//              A trick....
              if (p > pMax && i.Label != 0) {
                pMax = p
                prediction = i.Label
              }
            }
            ps.output(LabeledVector(prediction, x._1), paramId)
          }
        }
      }
      else {
        val release = this.dataPredictPull.dequeueAll(x => x._2 == paramId)
        release.foreach(x => this.dataPredict.enqueue(x))
      }
    }

    /**
      * Used to determine a matching Gaussian for an input point.
      *
      * @param tuples    a sequence of input data;
      * @param gaussians a sequence of available Gaussians against which the input data is compared.
      * @return An array of tuples. First element of a tuple denotes what kind situation in terms of labels of
      *         data and Gaussians we have. In detail:
      *         - 1 - If the highest matching Gaussian is unlabelled, label it with the label of the input.
      *         - 2 - the input point and the Gaussian it matches with have different labels;
      *         - 3 - all other situations.
      *         The second element of a tuple is the label of a matching gaussian.
      *         Third - a labeled datapoint.
      *         Fourth - probabilities of matching with each Gaussian for a single point.
      */
    def calculateMatch(tuples: mutable.Seq[(LabeledVector, Int)], gaussians: Seq[GGaussian]):
    Array[(Int, Int, (LabeledVector, Int), Array[Double])] = {

      // An array that contains probabilities of matching with each Gaussian



      // The closeness threshold.
      val tSigma = 5.0
      var k = 0
      val flag = tuples.map { x =>
        var allProbabilities = new Array[Double](gaussians.size)
        var pMax = 0.0
        var matchingGaussian = 0
        var matchingID = 0
        for (i <- gaussians.iterator) {
          var p = 0.0
          if (getMahalanobis(x._1, i.mu, i.sigma) < tSigma){
            p = getGaussianProbability(x._1, i.mu, i.sigma)
          }
          if (p > pMax) {
            pMax = p
            matchingGaussian = i.Label
            matchingID = i.id.toInt
          }
          allProbabilities(k) = p
          k = k + 1
        }
        k = 0
        var situation = 3

        // TODO: here I assume that 0 means no label, as we firtsly initialise all, discuss later
        // Gaussians we such label (at temparray)
        if (matchingGaussian == 0) {
          situation = 1
        }
        else if (x._1.label != matchingGaussian.toDouble) {
          situation = 2
        }
        (situation, matchingID, x, allProbabilities)
      }
      flag.toArray
    }

    /**
      * Computes the density of a Gaussian at a given point (2).
      *
      * @param x     a point;
      * @param mu    mean of a Gaussian;
      * @param sigma a covariance matrix of a Gaussian.
      * @return A density of a Gaussian at a given point.
      */
    def getGaussianProbability(x: LabeledVector, mu: breeze.linalg.DenseVector[Double], sigma: DenseMatrix[Double]): Double = {
      val xDense = x.vector.asBreeze.toDenseVector
      val res = (xDense - mu).t * pinv(sigma)
      val res2 = res * (xDense - mu)
      val res3 = -0.5 * res2
      val numerator = math.exp(res3)
//      to the power of feature dimension, need abs for determinant
      val denominator = math.sqrt(pow(2 * Math.PI,2) *abs(det( sigma)))
      val pro = numerator / denominator
      return pro
    }

    def getGaussianProbability1(x: DenseVector, mu: breeze.linalg.DenseVector[Double], sigma: DenseMatrix[Double]): Double = {
      val xDense = x.asBreeze.toDenseVector
      val res = (xDense - mu).t * pinv(sigma)
      val res2 = res * (xDense - mu)
      val res3 = -0.5 * res2
      val numerator = exp(res3)
      val denominator = math.sqrt(pow(2 * Math.PI,2) *abs(det( sigma)))
      numerator / denominator
    }

    /**
      * Computes a Mahalanobis distance (36) between a point and a Gaussian.
      *
      * @param x     input point;
      * @param mu    mean of a Gaussian;
      * @param sigma covariance matrix of a Gaussian.
      * @return A distance.
      */
    def getMahalanobis(x: LabeledVector, mu: breeze.linalg.DenseVector[Double], sigma: DenseMatrix[Double]): Double = {
      val xDense = x.vector.asBreeze.toDenseVector
      sqrt(abs((xDense - mu).t * pinv(sigma) * (xDense - mu)))
    }

    def update(ints: Array[(Int, Int, (LabeledVector, Int), Array[Double])], gaussians: Seq[GGaussian]):
    Seq[GGaussian] = {
      var out = gaussians.toArray
      ints.map { flag =>

        // unlabel gaussian model
        if (flag._1 == 1) {
          for (i <- gaussians.iterator) {
            if (i.id == flag._2) {
              val forupdate = GGaussian(i.id, i.tau, i.c, i.mu, i.sigma, flag._3._1.label.toInt)
              scala.util.control.Exception.ignoring(classOf[java.lang.IndexOutOfBoundsException]) {
                out = out.updated(flag._2-1, forupdate)
              }
            }
          }
        }

        // different label, create a new one
        // need add global learning rate alpha
        else if (flag._1 == 2) {
          var num = 0

          // decay the weight
          for (i <- gaussians.iterator) {
            val forupdate = GGaussian(i.id, (1 - 0.5) * i.tau, i.c, i.mu, i.sigma, flag._3._2)
            out = out.updated(num, forupdate)
            num = num + 1
          }

          // remove the less contributing one and create new one
          var min = Double.MaxValue
          var min_index = (0, 0L)
          num = 0
          for (i <- gaussians.iterator) {
            if (min > i.tau) {
              min = i.tau
              min_index = (num, i.id)
            }
            num = num + 1
          }
          val tmu = flag._3._1.vector.asBreeze.toDenseVector
          val forupdate = GGaussian(min_index._2, 0.5, 1, tmu, DenseMatrix.eye[Double](2), 0)
          out = out.updated(min_index._1, forupdate)
        }

        // match, add contribution by using EM
        else {
          var num = 0
          for (i <- out.iterator) {
            val qj = flag._4(num) / sum(flag._4)
            val eata = qj * ((1 - 0.5) / i.c + 0.5)
            val xtodensevector = flag._3._1.vector.asBreeze.toDenseVector
            val tmu = (1 - eata) * i.mu + eata * xtodensevector
            val tsigma = (1 - eata) * i.sigma + eata * (xtodensevector - i.mu).dot(xtodensevector - i.mu)
            val forupdate = GGaussian(i.id, (1 - 0.5) * i.tau + 0.5 * qj, i.c + qj, tmu, tsigma, i.Label)
            out = out.updated(num, forupdate)
            num = num + 1
          }
        }
        0
      }
      out.toSeq
    }
  }

  //-----------------------------------------------------SERVER-----------------------------------------
  val GGMMParameterServerLogic = new ParameterServerLogic[P, String] {
    val params = new mutable.HashMap[Int, (Seq[GGaussian], Int)]()

    /**
      * On Pull Receive this method searches for the current weight of this problem id and send them
      * to the worker
      *
      * @param id                   problem id
      * @param workerPartitionIndex the worker id to remember where the answer needs to be send to
      * @param ps                   parameter server
      */
    override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, String]): Unit = {
      //    because we put initial in the worker onPullrece, here just inital the param.
      val param = this.params.getOrElseUpdate(id, (Seq[GGaussian](), 0))
      ps.answerPull(id, param, workerPartitionIndex)
    }

    /**
      * this method merges the the current weights with the updated weights with regards to the number of
      * observation forming each weight
      *
      * @param id          problem id
      * @param deltaUpdate updated weights from a worker
      * @param ps          parameter server
      */
    override def onPushRecv(id: Int, deltaUpdate: P, ps: ParameterServer[P, String]): Unit = {
      if (deltaUpdate._2 >= 0) {
        val param = this.params.getOrElseUpdate(id, (Seq[GGaussian](), 0))
        val count = param._2 + deltaUpdate._2
        if (param._2 > 0) {
          val update_merge = merge(deltaUpdate._1, 0.01)
          val update_merge_split = split(update_merge, 1.0, 0.8)
          //          I think this threshold need define in pipeline
          this.params.update(id, (update_merge_split, count))
          ps.output(this.params(id).toString)
        }
        else {
          this.params.update(id, deltaUpdate)
          ps.output(this.params(id).toString)
        }
      }
    }

    /**
      * In order to reduce the complexity, Gaussians of the same label can be merged. The merge operation combines the
      * two closest Gaussians into one single Gaussian. One pair with smallest SKLD (40) is merged.
      *
      * @param gaussians a sequence of Guassians, among which the merge candidates are sought;
      * @param tMerge    merge threshold. Merge only if KL-divergence between two Gaussians is smaller than this.
      * @return A new sequence of Gaussians with a newly created Gaussian instead of a pair of merge candidates. If
      *         no candidates were found, the initial sequence is returned.
      */
    def merge(gaussians: Seq[GGaussian], tMerge: Double): Seq[GGaussian] = {
      val gaussianPairs = gaussians.combinations(2)
      var minSkld = 0.0
      var mergeCandidates = Seq.empty[GGaussian]
      var doMerge = false
      for (gaussianPair <- gaussianPairs) {
        if (gaussianPair(0).Label == gaussianPair(1).Label
          && gaussianPair(0).Label != 0
          && gaussianPair(1).Label != 0) {
          val skld = 0.5 * (kld(gaussianPair(0), gaussianPair(1)) + kld(gaussianPair(1), gaussianPair(0)))
          if (minSkld == 0 & skld < tMerge) {
            minSkld = skld
            mergeCandidates = gaussianPair
            doMerge = true
          }
          else if (skld < minSkld & skld < tMerge) {
            minSkld = skld
            mergeCandidates = gaussianPair
          }
        }
      }
      if (doMerge) {

        // Construct a new Gaussian from a pair of Gaussians according to (41).
        val f1 = mergeCandidates(0).tau / (mergeCandidates(0).tau + mergeCandidates(1).tau)
        val f2 = mergeCandidates(1).tau / (mergeCandidates(0).tau + mergeCandidates(1).tau)
        val tau = mergeCandidates(0).tau + mergeCandidates(1).tau
        val mu = f1 * mergeCandidates(0).mu + f2 * mergeCandidates(1).mu
        val sigma = f1 * mergeCandidates(0).sigma + f2 * mergeCandidates(1).sigma +
          f1 * f2 * (mergeCandidates(0).mu - mergeCandidates(1).mu) * (mergeCandidates(0).mu - mergeCandidates(1).mu).t
        val id1 = mergeCandidates(0).id
        val id2 = mergeCandidates(1).id

        // TODO: replace 0.5 with variable c
        val newGaussian = GGaussian(mergeCandidates(0).id, tau, 0.5, mu, sigma, mergeCandidates(0).Label)

        // Delete merge candidates from a sequence and add a merged Gaussian.
        return gaussians.filter(_.id != id1).filter(_.id != id2) :+ newGaussian
      }
      gaussians
    }

    /**
      * Splits the largest Gaussian into smaller ones based on a size. The Gaussian is split into two Gaussians
      * (φ1(τ1;µ1;Σ1), φ2(τ2;µ2;Σ2)) along its dominant principal component.
      *
      * @param gaussians Gaussians, among which we seek for too large candidates;
      * @param tSplit    splitting criterion. We only split if the volume of a Gaussion is above this threshold;
      * @param a         a split factor that determines how far the centers of the two new Gaussians are apart from each other.
      * @return A new sequence of Gaussians, that contains two newly created Gaussians instead of an old big one.
      */
    def split(gaussians: Seq[GGaussian], tSplit: Double, a: Double): Seq[GGaussian] = {
      var maxVolume = 0.0
      var doSplit = false
      var splitCandidate = GGaussian(0, 0.5, 1.0, breeze.linalg.DenseVector.rand(2), DenseMatrix.rand(2, 2), 0)
      for (gaussian <- gaussians.iterator) {
        val V = det(gaussian.sigma)
        if (V > tSplit && V > maxVolume) {
          maxVolume = V
          splitCandidate = gaussian
          doSplit = true
        }
      }
      if (doSplit) {

        // Split according to (38).
        val (lambda, v) = decompose(splitCandidate.sigma)
        val vDelta = v * sqrt(a * lambda)
        val tau1, tau2 = splitCandidate.tau / 2
        val mu1 = splitCandidate.mu + vDelta
        val mu2 = splitCandidate.mu - vDelta
        val sigma1, sigma2 = splitCandidate.sigma - vDelta * vDelta.t
        val id1 = splitCandidate.id
        val id2 = findId(gaussians, 0)
        val label = splitCandidate.Label

        // TODO: replace 0.5 with c1 and c2
        val newGaussian1 = GGaussian(id1, tau1, 0.5, mu1, sigma1, label)
        val newGaussian2 = GGaussian(id2, tau2, 0.5, mu2, sigma2, label)
        return gaussians.filter(_.id != id1) :+ newGaussian1 :+ newGaussian2
      }
      gaussians
    }

    /**
      * Computes Kullback-Leibler divergence between two Gaussians according to (39).
      *
      * @param gaussian1 first Gaussian;
      * @param gaussian2 second Gaussian.
      * @return Kullback-Leibler divergence between two D-dimensional Gaussians.
      */
    def kld(gaussian1: GGaussian, gaussian2: GGaussian): Double = {
      val sigma1 = gaussian1.sigma
      val sigma2 = gaussian2.sigma
      val mu1 = gaussian1.mu
      val mu2 = gaussian2.mu
      val D = sigma1.cols
      return log(det(sigma2) / det(sigma1)) + trace(inv(sigma2) * sigma1) +
        (mu2 - mu1).t * inv(sigma1) * (mu2 - mu1) - D
    }

    /**
      * Computes eigenvalue/eigenvector decomposition of a matrix.
      *
      * @param m a matrix to compute a decomposition of.
      * @return The largest eigenvalue and corresponding normalized eigenvector.
      */
    def decompose(m: DenseMatrix[Double]): (Double, breeze.linalg.DenseVector[Double]) = {
      val EigSym(lambda, evs) = eigSym(m)
      val maxEigenvalue = max(lambda)
      val maxEigenvector = evs(::, lambda.findAll(_ == maxEigenvalue)).flatten().toDenseVector
      val maxEigenvectorNormalized = maxEigenvector / norm(maxEigenvalue)
      return (maxEigenvalue, maxEigenvectorNormalized)
    }

    /**
      * Finds the smallest id, which is not represented yet.
      *
      * @param gaussians a sequence of Gaussians, among which to seek an id;
      * @param id        a candidate id, which may be created if there is no such id yet.
      * @return A smallest unrepresented id.
      */
    def findId(gaussians: Seq[GGaussian], id: Long): Long = {
      var tempId = id
      for (gaussian <- gaussians.iterator) {
        if (gaussian.id == id) {
          tempId = tempId + 1
          return findId(gaussians, tempId)
        }
      }
      tempId
    }

    // equals constructor
    def apply(): GGMM = new GGMM()
  }
}

// Single instance
object GGMM {
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
  implicit val fit =  new StreamFitOperation[GGMM,LabeledVector,String] {
    override def fit(instance: GGMM,
                     fitParameters: ParameterMap,
                     input: DataStream[LabeledVector]): DataStream[String] = {
      instance.fitAndPredict(input)
    }
  }
  implicit val prediction = new PredictDataStreamOperation[GGMM,LabeledVector, String] {
    override def predictDataStream(instance: GGMM,
                                   predictParameters: ParameterMap,
                                   input: DataStream[LabeledVector])
    : DataStream[String] = {
      instance.fitAndPredict(input)
    }
  }
}

