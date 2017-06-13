package com.datawizards.sparklocal.performance

import java.io.{File, PrintWriter}

import com.datawizards.csv2class._
import com.datawizards.class2csv._
import com.datawizards.sparklocal.performance.BenchmarkModel.BenchmarkResult
import com.datawizards.sparklocal.performance.BenchmarkModel.BenchmarkResultNarrow

object BenchmarkResultsSerialization {
  def writeBenchmarkResults(results: Iterable[BenchmarkResult]): Unit = {
    for(b <- results)
      writeCSV(Seq(b), benchmarkRawResultsDir + uuid + ".csv", ',', header = false)
    val resultsNarrow = convertBenchmarkResults(results)
    for(b <- resultsNarrow)
      writeCSV(Seq(b), benchmarkNarrowResultsDir + uuid + ".csv", ',', header = false)
  }

  def readBenchmarkResults(): (Iterable[BenchmarkResult], Iterable[BenchmarkResultNarrow]) = {
    mergeBenchmarkResults()
    (
      aggregateBenchmarkResults(parseCSV[BenchmarkResult](benchmarkResultsFile, header = false)._1),
      aggregateBenchmarkNarrowResults(parseCSV[BenchmarkResultNarrow](benchmarkNarrowResultsFile, header = false)._1)
    )
  }

  private def convertBenchmarkResults(results: Iterable[BenchmarkResult]): Iterable[BenchmarkResultNarrow] =
    results.flatMap(r => Seq(
      BenchmarkResultNarrow(r.collection, r.operationCategory, r.operationName, r.sampleSize, "ScalaEager", r.scalaEagerTime),
      BenchmarkResultNarrow(r.collection, r.operationCategory, r.operationName, r.sampleSize, "ScalaLazy", r.scalaLazyTime),
      BenchmarkResultNarrow(r.collection, r.operationCategory, r.operationName, r.sampleSize, "ScalaParallel", r.scalaParallelTime),
      BenchmarkResultNarrow(r.collection, r.operationCategory, r.operationName, r.sampleSize, "ScalaParallelLazy", r.scalaParallelLazyTime),
      BenchmarkResultNarrow(r.collection, r.operationCategory, r.operationName, r.sampleSize, "Spark", r.sparkTime)
    ))

  private def uuid: String = java.util.UUID.randomUUID().toString

  private def mergeBenchmarkResults(): Unit = {
    mergeCSVFilesInDirectory(benchmarkRawResultsDir, benchmarkResultsFile)
    mergeCSVFilesInDirectory(benchmarkNarrowResultsDir, benchmarkNarrowResultsFile)
  }

  private def mergeCSVFilesInDirectory(dir: String, outputFile: String): Unit = {
    val csvFiles = new File(dir)
      .listFiles()
      .filter(f => f.getName.endsWith(".csv"))

    val allLines = (for(f <- csvFiles) yield scala.io.Source.fromFile(f).getLines().toSeq).flatten

    val pw = new PrintWriter(outputFile)
    for(line <- allLines)
      pw.write(line + "\n")
    pw.close()
  }

  private def aggregateBenchmarkResults(results: Iterable[BenchmarkResult]): Iterable[BenchmarkResult] =
    results
      .toSeq
      .groupBy(r => (r.operationCategory, r.operationName, r.sampleSize, r.collection))
      .map{case ((operationCategory, operationName, sampleSize, collection), resultsWithinGroup) =>
        val size = resultsWithinGroup.size
        BenchmarkResult(
          collection = collection,
          operationCategory = operationCategory,
          operationName = operationName,
          sampleSize = sampleSize,
          scalaEagerTime = resultsWithinGroup.map(_.scalaEagerTime).sum / size,
          scalaLazyTime = resultsWithinGroup.map(_.scalaLazyTime).sum / size,
          scalaParallelTime = resultsWithinGroup.map(_.scalaParallelTime).sum / size,
          scalaParallelLazyTime = resultsWithinGroup.map(_.scalaParallelLazyTime).sum / size,
          sparkTime = resultsWithinGroup.map(_.sparkTime).sum / size
        )
      }

  private def aggregateBenchmarkNarrowResults(results: Iterable[BenchmarkResultNarrow]): Iterable[BenchmarkResultNarrow] =
    results
      .toSeq
      .groupBy(r => (r.operationCategory, r.operationName, r.sampleSize, r.collection, r.engine))
      .map{case ((operationCategory, operationName, sampleSize, collection, engine), resultsWithinGroup) =>
        val size = resultsWithinGroup.size
        BenchmarkResultNarrow(
          collection = collection,
          operationCategory = operationCategory,
          operationName = operationName,
          sampleSize = sampleSize,
          engine = engine,
          time = resultsWithinGroup.map(_.time).sum / size
        )
      }

  private val benchmarkResultDir = "benchmarks/"
  private val benchmarkRawResultsDir = benchmarkResultDir + "results/"
  private val benchmarkNarrowResultsDir = benchmarkResultDir + "narrow/"
  private val benchmarkResultsFile = benchmarkResultDir + "benchmarkResults.csv"
  private val benchmarkNarrowResultsFile = benchmarkResultDir + "benchmarkResultsNarrow.csv"
}
