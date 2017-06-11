package com.datawizards.sparklocal.performance

import com.datawizards.sparklocal.performance.BenchmarkModel.{BenchmarkResult, BenchmarkResultNarrow}
import com.datawizards.splot.api.implicits._
import com.datawizards.splot.model.ImageFormats

import scala.collection.mutable.ListBuffer

object ResultsVisualization {
  def visualizeResults(results: (Iterable[BenchmarkResult], Iterable[BenchmarkResultNarrow])): Unit = {
    val file = "benchmarks/Benchmarks.md"
    import java.io._
    val pw = new PrintWriter(file)
    pw.write(generateHeader())
    pw.write(generateMarkdownTables(results._1))
    pw.write(generateMarkdownImages(generateCharts(results._2)))
    pw.close()
  }

  private def generateHeader(): String = {
    s"""# Benchmark results
       |
       |""".stripMargin
  }

  private def generateCharts(results: Iterable[BenchmarkResultNarrow]): Iterable[ChartMetaData] = {
    val charts = new ListBuffer[ChartMetaData]

    results
      .groupBy(r => (r.collection, r.operationName, r.sampleSize))
      .foreach{ case ((collection, operationName, sampleSize), resultsInGroup) =>
        val file = s"${collection}_${operationName}_$sampleSize.png"

        resultsInGroup
          .buildPlot()
          .bar(_.engine, _.time)
          .size(800, 200)
          .title(s"$collection.$operationName() operation time by engine [ms] - sample size: $sampleSize")
          .legendVisible(false)
          .save("benchmarks/" + file , ImageFormats.PNG)

        charts += ChartMetaData(file, collection, operationName, sampleSize)
      }

    charts.toList
  }

  private def generateMarkdownImages(charts: Iterable[ChartMetaData]): String = {
    val buffer = new StringBuilder()
    buffer ++= "\n# Images\n"

    charts
      .groupBy(_.collection)
      .foreach{case(collection, resultsWithinCollection) =>
        buffer ++= s"\n## $collection\n"
        resultsWithinCollection
          .groupBy(_.sampleSize)
          .foreach{case (sampleSize, resultsWithinGroup) =>
            buffer ++= s"\n### Sample size = $sampleSize elements\n\n"
            for(chart <- resultsWithinGroup)
              buffer ++= s"![](${chart.file})\n\n"
          }
      }

    buffer.toString()
  }

  private def generateMarkdownTables(results: Iterable[BenchmarkResult]): String = {
    val buffer = new StringBuilder()
    buffer ++= "# Raw data\n"
      results
      .groupBy(_.collection)
      .foreach{case (collection, resultsWithinCollection) =>
        buffer ++= s"\n## $collection\n"
        resultsWithinCollection
          .groupBy(_.sampleSize)
          .foreach{case (sampleSize, resultsWithinGroup) =>
            buffer ++= s"\n### Sample size = $sampleSize elements\n\n"
            buffer ++= "|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|\n"
            buffer ++= "|--|--|--|--|--|--|\n"
            for(r <- resultsWithinGroup)
              buffer ++= s"|${r.collection}.${r.operationName}()|${round(r.scalaEagerTime)}|${round(r.scalaLazyTime)}|${round(r.scalaParallelTime)}|${round(r.scalaParallelLazyTime)}|${round(r.sparkTime)}|\n"
          }
      }

    def round(d: Double): String = (math.round(d*1000) / 1000.0).toString

    buffer.toString()
  }

  case class ChartMetaData(file: String, collection: String, operationName: String, sampleSize: Long)
}
