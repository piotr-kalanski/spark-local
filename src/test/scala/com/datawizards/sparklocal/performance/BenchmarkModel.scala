package com.datawizards.sparklocal.performance

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.rdd.RDDAPI

object BenchmarkModel {
  case class InputDataSets[T](
                               scalaEagerImpl: DataSetAPI[T],
                               scalaLazyImpl: DataSetAPI[T],
                               scalaParallelImpl: DataSetAPI[T],
                               scalaParallelLazyImpl: DataSetAPI[T],
                               sparkImpl: DataSetAPI[T]
                             )

  case class InputRDDs[T](
                           scalaEagerImpl: RDDAPI[T],
                           scalaLazyImpl: RDDAPI[T],
                           scalaParallelImpl: RDDAPI[T],
                           scalaParallelLazyImpl: RDDAPI[T],
                           sparkImpl: RDDAPI[T]
                             )

  case class DataSetBenchmarkSetting[T](
                                  operationName: String,
                                  dataSets: InputDataSets[T],
                                  op: DataSetAPI[T] => Unit
                                )

  def dataSetBenchmarkSetting[T](operationName: String, dataSets: InputDataSets[T])(op: DataSetAPI[T] => Unit): DataSetBenchmarkSetting[T] =
    DataSetBenchmarkSetting(operationName, dataSets, op)

  case class RDDBenchmarkSetting[T](
                                     operationName: String,
                                     rdds: InputRDDs[T],
                                     op: RDDAPI[T] => Unit
                                   )

  def rddBenchmarkSetting[T](operationName: String, rdds: InputRDDs[T])(op: RDDAPI[T] => Unit): RDDBenchmarkSetting[T] =
    RDDBenchmarkSetting(operationName, rdds, op)

  case class BenchmarkResult(
                              collection: String,
                              operationName: String,
                              sampleSize: Long,
                              scalaEagerTime: Double,
                              scalaLazyTime: Double,
                              scalaParallelTime: Double,
                              scalaParallelLazyTime: Double,
                              sparkTime: Double
                            )

  case class BenchmarkResultNarrow(
                                    collection: String,
                                    operationName: String,
                                    sampleSize: Long,
                                    engine: String,
                                    time: Double
                                  )

}
