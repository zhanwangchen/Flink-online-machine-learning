/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.proteus.solma.pipeline

import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.streaming.api.scala._

case class ChainedStreamPredictor[
    T <: StreamTransformer[T],
    P <: StreamPredictor[P]]
    (transformer: T, predictor: P)
  extends StreamPredictor[ChainedStreamPredictor[T, P]]{}

object ChainedStreamPredictor {
  implicit def chainedPredictOperation[
      T <: StreamTransformer[T],
      P <: StreamPredictor[P],
      Testing,
      Intermediate,
      Prediction](
      implicit transformOperation: TransformDataStreamOperation[T, Testing, Intermediate],
      predictOperation: PredictDataStreamOperation[P, Intermediate, Prediction])
    : PredictDataStreamOperation[ChainedStreamPredictor[T, P], Testing, Prediction] = {

    new PredictDataStreamOperation[ChainedStreamPredictor[T, P], Testing, Prediction] {
      override def predictDataStream(
          instance: ChainedStreamPredictor[T, P],
          predictParameters: ParameterMap,
          input: DataStream[Testing])
        : DataStream[Prediction] = {
        val testing = instance.transformer.transform(input, predictParameters)
        instance.predictor.predict(testing, predictParameters)
      }
    }
  }

  implicit def chainedFitOperation[L <: StreamTransformer[L], R <: StreamPredictor[R], I, T, M]
    (implicit fitOperation: StreamFitOperation[L, I, M],
    transformOperation: TransformDataStreamOperation[L, I, T],
    predictorFitOperation: StreamFitOperation[R, T, M])
  : StreamFitOperation[ChainedStreamPredictor[L, R], I, M] = {
    new StreamFitOperation[ChainedStreamPredictor[L, R], I, M] {
      override def fit(
          instance: ChainedStreamPredictor[L, R],
          fitParameters: ParameterMap,
          input: DataStream[I])
        : DataStream[M] = {
        instance.transformer.train(input, fitParameters)
        val intermediateResult = instance.transformer.transform(input, fitParameters)
        instance.predictor.train(intermediateResult, fitParameters)
      }
    }
  }
}
