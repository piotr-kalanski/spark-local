package com.datawizards.sparklocal.performance

import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.performance.BenchmarkModel._
import com.datawizards.sparklocal.performance.BenchmarkTestData._
import com.datawizards.sparklocal.implicits._

object Benchmarks {

  private lazy val inputDataSets = Seq(
    dataSets10Elements,
    dataSets100Elements,
    dataSets1000Elements
  )

  lazy val peopleDataSetBenchmarks: Seq[DataSetBenchmarkSetting[Person]] = Seq(
    dataSetBenchmarkSettings("map", inputDataSets) {
      ds => ds.map(p => p.age + 1).collect()
    },
    dataSetBenchmarkSettings("count", inputDataSets) {
      ds => ds.count()
    },
    dataSetBenchmarkSettings("filter", inputDataSets) {
      ds => ds.filter(p => p.age > 5).collect()
    },
    dataSetBenchmarkSettings("union", inputDataSets) {
      ds => ds.union(ds).collect()
    },
    dataSetBenchmarkSettings("flatMap", inputDataSets) {
      ds => ds.flatMap(p => Seq(p,p)).collect()
    },
    dataSetBenchmarkSettings("limit", inputDataSets) {
      ds => ds.limit(5).collect()
    },
    dataSetBenchmarkSettings("take", inputDataSets) {
      ds => ds.take(5)
    },
    dataSetBenchmarkSettings("distinct", inputDataSets) {
      ds => ds.distinct().collect()
    },
    dataSetBenchmarkSettings("map().reduce", inputDataSets) {
      ds => ds.map(p => p.age).reduce(_ + _)
    },
    dataSetBenchmarkSettings("groupByKey().count", inputDataSets) {
      ds => ds.groupByKey(p => p.age).count().collect()
    },
    dataSetBenchmarkSettings("groupByKey().mapGroups", inputDataSets) {
      ds => ds
        .groupByKey(p => p.name)
        .mapGroups{case (name, people) => (name, people.size)}
        .collect()
    }
  )
    .flatten
    .sortBy(b => (b.dataSets.scalaEagerImpl.count(), b.operationName))

  private lazy val inputRDDs = Seq(
    rdds10Elements,
    rdds100Elements,
    rdds1000Elements
  )

  lazy val peopleRDDBenchmarks: Seq[RDDBenchmarkSetting[Person]] = Seq(
    rddBenchmarkSettings("map", inputRDDs) {
      rdd => rdd.map(p => p.age + 1).collect()
    },
    rddBenchmarkSettings("count", inputRDDs) {
      rdd => rdd.count()
    },
    rddBenchmarkSettings("filter", inputRDDs) {
      rdd => rdd.filter(p => p.age > 5).collect()
    },
    rddBenchmarkSettings("union", inputRDDs) {
      rdd => rdd.union(rdd).collect()
    },
    rddBenchmarkSettings("flatMap", inputRDDs) {
      rdd => rdd.flatMap(p => Seq(p,p)).collect()
    },
    rddBenchmarkSettings("take", inputRDDs) {
      rdd => rdd.take(5)
    },
    rddBenchmarkSettings("distinct", inputRDDs) {
      rdd => rdd.distinct().collect()
    },
    rddBenchmarkSettings("map().reduce", inputRDDs) {
      rdd => rdd.map(p => p.age).reduce(_ + _)
    },
    rddBenchmarkSettings("zip", inputRDDs) {
      rdd => rdd.zip(rdd).collect()
    },
    rddBenchmarkSettings("zipWithIndex", inputRDDs) {
      rdd => rdd.zipWithIndex().collect()
    },
    rddBenchmarkSettings("sortBy", inputRDDs) {
      rdd => rdd.sortBy(p => p.age).collect()
    }
  )
    .flatten
    .sortBy(b => (b.rdds.scalaEagerImpl.count(), b.operationName))

}
