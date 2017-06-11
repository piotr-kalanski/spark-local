package com.datawizards.sparklocal.performance

import com.datawizards.sparklocal.performance.BenchmarkModel._
import com.datawizards.sparklocal.performance.BenchmarkTestData._
import com.datawizards.sparklocal.implicits._

object Benchmarks {
  val peopleDataSetBenchmarks = Seq(
    dataSetBenchmarkSetting("map", dataSets10Elements) {
      ds => ds.map(p => p.age + 1).collect()
    },
    dataSetBenchmarkSetting("count", dataSets10Elements) {
      ds => ds.count()
    },
    dataSetBenchmarkSetting("filter", dataSets10Elements) {
      ds => ds.filter(p => p.age > 5).collect()
    },
    dataSetBenchmarkSetting("union", dataSets10Elements) {
      ds => ds.union(ds).collect()
    },
    dataSetBenchmarkSetting("flatMap", dataSets10Elements) {
      ds => ds.flatMap(p => Seq(p,p)).collect()
    },
    dataSetBenchmarkSetting("limit", dataSets10Elements) {
      ds => ds.limit(5).collect()
    },
    dataSetBenchmarkSetting("take", dataSets10Elements) {
      ds => ds.take(5)
    },
    dataSetBenchmarkSetting("distinct", dataSets10Elements) {
      ds => ds.distinct().collect()
    },
    dataSetBenchmarkSetting("map().reduce", dataSets10Elements) {
      ds => ds.map(p => p.age).reduce(_ + _)
    }
  )

  val peopleRDDBenchmarks = Seq(
    rddBenchmarkSetting("map", rdds10Elements) {
      rdd => rdd.map(p => p.age + 1).collect()
    },
    rddBenchmarkSetting("count", rdds10Elements) {
      rdd => rdd.count()
    },
    rddBenchmarkSetting("filter", rdds10Elements) {
      rdd => rdd.filter(p => p.age > 5).collect()
    },
    rddBenchmarkSetting("union", rdds10Elements) {
      rdd => rdd.union(rdd).collect()
    },
    rddBenchmarkSetting("flatMap", rdds10Elements) {
      rdd => rdd.flatMap(p => Seq(p,p)).collect()
    },
    rddBenchmarkSetting("take", rdds10Elements) {
      rdd => rdd.take(5)
    },
    rddBenchmarkSetting("distinct", rdds10Elements) {
      rdd => rdd.distinct().collect()
    },
    rddBenchmarkSetting("map().reduce", rdds10Elements) {
      rdd => rdd.map(p => p.age).reduce(_ + _)
    }
  )

}
