package com.datawizards.sparklocal.performance

import com.datawizards.csv2class.parseCSV
import com.datawizards.class2csv.writeCSV
import com.datawizards.sparklocal.performance.BenchmarkModel.BenchmarkResult
import com.datawizards.sparklocal.performance.BenchmarkModel.BenchmarkResultNarrow

object BenchmarkResultsSerialization {
  def writeBenchmarkResults(results: Iterable[BenchmarkResult]): Unit = {
    val resultsNarrow = results.flatMap(r => Seq(
      BenchmarkResultNarrow(r.collection, r.operationName, r.sampleSize, "ScalaEager", r.scalaEagerTime),
      BenchmarkResultNarrow(r.collection, r.operationName, r.sampleSize, "ScalaLazy", r.scalaLazyTime),
      BenchmarkResultNarrow(r.collection, r.operationName, r.sampleSize, "ScalaParallel", r.scalaParallelTime),
      BenchmarkResultNarrow(r.collection, r.operationName, r.sampleSize, "ScalaParallelLazy", r.scalaParallelLazyTime),
      BenchmarkResultNarrow(r.collection, r.operationName, r.sampleSize, "Spark", r.sparkTime)
    ))
    writeCSV(results, benchmarkResultsFile)
    writeCSV(resultsNarrow, benchmarkNarrowResultsFile)
  }

  def readBenchmarkResults(): (Iterable[BenchmarkResult], Iterable[BenchmarkResultNarrow]) =
    (
      parseCSV[BenchmarkResult](benchmarkResultsFile)._1,
      parseCSV[BenchmarkResultNarrow](benchmarkNarrowResultsFile)._1
    )

  private val benchmarkResultsFile = "benchmarks/benchmarkResults.csv"
  private val benchmarkNarrowResultsFile = "benchmarks/benchmarkResultsNarrow.csv"
}
