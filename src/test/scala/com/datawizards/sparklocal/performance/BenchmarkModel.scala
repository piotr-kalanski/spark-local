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
                                  operationCategory: String,
                                  operationName: String,
                                  dataSets: InputDataSets[T],
                                  op: DataSetAPI[T] => Unit
                                )

  def dataSetBenchmarkSetting[T](operationCategory: String, operationName: String, dataSets: InputDataSets[T])(op: DataSetAPI[T] => Unit): DataSetBenchmarkSetting[T] =
    DataSetBenchmarkSetting(operationCategory, operationName, dataSets, op)

  def dataSetBenchmarkSettings[T](operationCategory: String, operationName: String, dataSets: Iterable[InputDataSets[T]])(op: DataSetAPI[T] => Unit): Iterable[DataSetBenchmarkSetting[T]] =
    dataSets
      .map(d => DataSetBenchmarkSetting(operationCategory, operationName, d, op))

  case class RDDBenchmarkSetting[T](
                                     operationCategory: String,
                                     operationName: String,
                                     rdds: InputRDDs[T],
                                     op: RDDAPI[T] => Unit
                                   )

  def rddBenchmarkSetting[T](operationCategory: String, operationName: String, rdds: InputRDDs[T])(op: RDDAPI[T] => Unit): RDDBenchmarkSetting[T] =
    RDDBenchmarkSetting(operationCategory, operationName, rdds, op)

  def rddBenchmarkSettings[T](operationCategory: String, operationName: String, rdds: Iterable[InputRDDs[T]])(op: RDDAPI[T] => Unit): Iterable[RDDBenchmarkSetting[T]] =
    rdds
      .map(rdd => RDDBenchmarkSetting(operationCategory, operationName, rdd, op))

  case class BenchmarkResult(
                              collection: String,
                              operationCategory: String,
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
                                    operationCategory: String,
                                    operationName: String,
                                    sampleSize: Long,
                                    engine: String,
                                    time: Double
                                  )

}
