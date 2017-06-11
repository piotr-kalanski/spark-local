package com.datawizards.sparklocal.performance

import org.scalameter._
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.performance.BenchmarkModel._
import com.datawizards.sparklocal.rdd.RDDAPI

object BenchmarksRunner extends App {

  def runDataSetBenchmarks[T](benchmarks: Iterable[DataSetBenchmarkSetting[T]]): Iterable[BenchmarkResult] = {
    for(b <- benchmarks)
      yield benchmarkDataSetOperation(b.operationName, b.dataSets)(b.op)
  }

  def runRDDBenchmarks[T](benchmarks: Iterable[RDDBenchmarkSetting[T]]): Iterable[BenchmarkResult] = {
    for(b <- benchmarks)
      yield benchmarkRDDOperation(b.operationName, b.rdds)(b.op)
  }

  private def benchmarkDataSetOperation[T](operationName: String, dataSets: InputDataSets[T])(op: DataSetAPI[T] => Unit): BenchmarkResult =
    BenchmarkResult(
      collection = "DataSet",
      operationName = operationName,
      sampleSize = dataSets.scalaEagerImpl.count(),
      scalaEagerTime = measureDataSetOperation(op, dataSets.scalaEagerImpl),
      scalaLazyTime = measureDataSetOperation(op, dataSets.scalaLazyImpl),
      scalaParallelTime = measureDataSetOperation(op, dataSets.scalaParallelImpl),
      scalaParallelLazyTime = measureDataSetOperation(op, dataSets.scalaParallelLazyImpl),
      sparkTime = measureDataSetOperation(op, dataSets.sparkImpl)
    )

  private def benchmarkRDDOperation[T](operationName: String, rdds: InputRDDs[T])(op: RDDAPI[T] => Unit): BenchmarkResult =
    BenchmarkResult(
      collection = "RDD",
      operationName = operationName,
      sampleSize = rdds.scalaEagerImpl.count(),
      scalaEagerTime = measureRDDOperation(op, rdds.scalaEagerImpl),
      scalaLazyTime = measureRDDOperation(op, rdds.scalaLazyImpl),
      scalaParallelTime = measureRDDOperation(op, rdds.scalaParallelImpl),
      scalaParallelLazyTime = measureRDDOperation(op, rdds.scalaParallelLazyImpl),
      sparkTime = measureRDDOperation(op, rdds.sparkImpl)
    )

  private def measureDataSetOperation[T](op: DataSetAPI[T] => Unit, ds: DataSetAPI[T]): Double = {
    val time = standardConfig measure op(ds)
    time.value
  }

  private def measureRDDOperation[T](op: RDDAPI[T] => Unit, rdd: RDDAPI[T]): Double = {
    val time = standardConfig measure op(rdd)
    time.value
  }

  private lazy val standardConfig = config(
    Key.exec.minWarmupRuns -> 20,
    Key.exec.maxWarmupRuns -> 40,
    Key.exec.benchRuns -> 80,
    Key.verbose -> true
  ) withWarmer new Warmer.Default

}
