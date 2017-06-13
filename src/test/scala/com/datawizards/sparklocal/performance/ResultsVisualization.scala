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
    pw.write(generateSummary(results._2))
    pw.write(generateMarkdownTables(results._1))
    pw.write(generateMarkdownImages(generateCharts(results._2)))
    pw.close()
  }

  private def generateHeader(): String = {
    s"""# Benchmark results
       |
       |""".stripMargin
  }

  private def generateSummary(results: Iterable[BenchmarkResultNarrow]): String = {
    val summaryFile = "benchmarks/summary"

    val buffer = new StringBuilder()
    buffer ++= "# Summary\n"

    results
      .groupBy(_.sampleSize)
      .foreach{case (sampleSize, resultsWithinGroup) =>
        buffer ++= s"\n## Sample size = $sampleSize elements\n\n"
        val totalTimeByEngine = resultsWithinGroup
            .groupBy(_.engine)
            .mapValues(v => v.map(_.time).sum)
            .toSeq
            .sortBy(_._1)
        val file = summaryFile + "_" + sampleSize + ".png"
        totalTimeByEngine
          .buildPlot()
          .bar(_._1, _._2)
          .size(800, 200)
          .titles(s"Total operations time by engine [ms] - sample size: $sampleSize", "", "")
          .legendVisible(false)
          .showAnnotations(true)
          .save(file , ImageFormats.PNG)
        buffer ++= s"![]($file)\n"
      }

    buffer.toString
  }

  private def generateCharts(results: Iterable[BenchmarkResultNarrow]): Iterable[ChartMetaData] = {
    val charts = new ListBuffer[ChartMetaData]

    results
      .groupBy(r => (r.collection, r.operationCategory, r.operationName, r.sampleSize))
      .foreach{ case ((collection, operationCategory, operationName, sampleSize), resultsInGroup) =>
        val file = s"${collection}_${operationName}_$sampleSize.png"

        resultsInGroup
          .filter(_.engine != "Spark")
          .buildPlot()
          .bar(_.engine, _.time)
          .size(800, 200)
          .titles(s"$collection.$operationName() operation time by engine [ms] - sample size: $sampleSize", "", "")
          .legendVisible(false)
          .showAnnotations(true)
          .save("benchmarks/" + file , ImageFormats.PNG)

        charts += ChartMetaData(file, collection, operationCategory, operationName, sampleSize)
      }

    charts.toList
  }

  private def generateMarkdownImages(charts: Iterable[ChartMetaData]): String = {
    val buffer = new StringBuilder()
    buffer ++= "\n# Comparing Scala implementations\n"

    charts
      .groupBy(_.collection)
      .foreach{case(collection, resultsWithinCollection) =>
        buffer ++= s"\n## $collection\n"
        resultsWithinCollection
          .groupBy(_.sampleSize)
          .toSeq
          .sortBy(_._1)
          .foreach{case (sampleSize, resultsWithinSampleSizeGroup) =>
            buffer ++= s"\n### Sample size = $sampleSize elements\n\n"
            resultsWithinSampleSizeGroup
              .groupBy(_.operationCategory)
              .foreach{case (operationCategory, resultsWithinGroup) =>
                buffer ++= s"\n#### Operations: $operationCategory\n"
                for(chart <- resultsWithinGroup)
                  buffer ++= s"![](${chart.file})\n\n"
              }

          }
      }

    buffer.toString()
  }

  private def generateMarkdownTables(results: Iterable[BenchmarkResult]): String = {
    val buffer = new StringBuilder()
    buffer ++= "\n# Raw data\n"
      results
      .groupBy(_.collection)
      .foreach{case (collection, resultsWithinCollection) =>
        buffer ++= s"\n## $collection\n"
        resultsWithinCollection
          .groupBy(_.sampleSize)
          .toSeq
          .sortBy(_._1)
          .foreach{case (sampleSize, resultsWithinSampleSizeGroup) =>
            buffer ++= s"\n### Sample size = $sampleSize elements\n\n"
            resultsWithinSampleSizeGroup
              .groupBy(_.operationCategory)
              .foreach{case (operationCategory, resultsWithinGroup) =>
                buffer ++= s"\n#### Operations: $operationCategory\n"
                buffer ++= "|Operation|ScalaEager|ScalaLazy|ScalaParallel|ScalaParallelLazy|Spark|\n"
                buffer ++= "|--|--|--|--|--|--|\n"
                for(r <- resultsWithinGroup)
                  buffer ++= s"|${r.collection}.${r.operationName}()|${round(r.scalaEagerTime, r.sparkTime)}|${round(r.scalaLazyTime, r.sparkTime)}|${round(r.scalaParallelTime, r.sparkTime)}|${round(r.scalaParallelLazyTime, r.sparkTime)}|${round(r.sparkTime, r.sparkTime)}|\n"
              }
          }
      }

    def round(time: Double, sparkTime: Double): String =
      s"${math.round(time*1000) / 1000.0}<br/>(${math.round(time / sparkTime * 100)})%"

    buffer.toString()
  }

  case class ChartMetaData(file: String, collection: String, operationCategory: String, operationName: String, sampleSize: Long)
}
