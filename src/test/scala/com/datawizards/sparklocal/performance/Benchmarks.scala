package com.datawizards.sparklocal.performance

import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.performance.BenchmarkModel._
import com.datawizards.sparklocal.performance.BenchmarkTestData._
import com.datawizards.sparklocal.implicits._

object Benchmarks {

  private val BASIC = "basic"
  private val COMPLEX = "complex"

  private lazy val inputDataSets = Seq(
    dataSets10Elements,
    dataSets100Elements,
    dataSets1000Elements,
    dataSets100000Elements
  )

  lazy val peopleDataSetBenchmarks: Seq[DataSetBenchmarkSetting[Person]] = Seq(
    dataSetBenchmarkSettings(BASIC, "map", inputDataSets) {
      ds => ds.map(p => p.age + 1).collect()
    },
    dataSetBenchmarkSettings(BASIC, "count", inputDataSets) {
      ds => ds.count()
    },
    dataSetBenchmarkSettings(BASIC, "filter", inputDataSets) {
      ds => ds.filter(p => p.age > 5).collect()
    },
    dataSetBenchmarkSettings(BASIC, "union", inputDataSets) {
      ds => ds.union(ds).collect()
    },
    dataSetBenchmarkSettings(BASIC, "flatMap", inputDataSets) {
      ds => ds.flatMap(p => Seq(p,p)).collect()
    },
    dataSetBenchmarkSettings(BASIC, "limit", inputDataSets) {
      ds => ds.limit(5).collect()
    },
    dataSetBenchmarkSettings(BASIC, "take", inputDataSets) {
      ds => ds.take(5)
    },
    dataSetBenchmarkSettings(BASIC, "distinct", inputDataSets) {
      ds => ds.distinct().collect()
    },
    dataSetBenchmarkSettings(COMPLEX, "map().map().map", inputDataSets) {
      ds => ds
        .map(p => p.age)
        .map(x => x+1)
        .map(x => x * 2)
        .collect()
    },
    dataSetBenchmarkSettings(COMPLEX, "map().reduce", inputDataSets) {
      ds => ds.map(p => p.age).reduce(_ + _)
    },
    dataSetBenchmarkSettings(COMPLEX, "groupByKey().count", inputDataSets) {
      ds => ds.groupByKey(p => p.age).count().collect()
    },
    dataSetBenchmarkSettings(COMPLEX, "groupByKey().mapGroups", inputDataSets) {
      ds => ds
        .groupByKey(p => p.name)
        .mapGroups{case (name, people) => (name, people.size)}
        .collect()
    },
    dataSetBenchmarkSettings(COMPLEX, "map().filter().groupByKey().mapGroups", inputDataSets) {
      ds => ds
        .map(p => Person(p.name, p.age*2))
        .filter(_.age > 5)
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
    rdds1000Elements,
    rdds100000Elements
  )

  lazy val peopleRDDBenchmarks: Seq[RDDBenchmarkSetting[Person]] = Seq(
    rddBenchmarkSettings(BASIC, "map", inputRDDs) {
      rdd => rdd.map(p => p.age + 1).collect()
    },
    rddBenchmarkSettings(BASIC, "count", inputRDDs) {
      rdd => rdd.count()
    },
    rddBenchmarkSettings(BASIC, "filter", inputRDDs) {
      rdd => rdd.filter(p => p.age > 5).collect()
    },
    rddBenchmarkSettings(BASIC, "union", inputRDDs) {
      rdd => rdd.union(rdd).collect()
    },
    rddBenchmarkSettings(BASIC, "flatMap", inputRDDs) {
      rdd => rdd.flatMap(p => Seq(p,p)).collect()
    },
    rddBenchmarkSettings(BASIC, "take", inputRDDs) {
      rdd => rdd.take(5)
    },
    rddBenchmarkSettings(BASIC, "distinct", inputRDDs) {
      rdd => rdd.distinct().collect()
    },
    rddBenchmarkSettings(BASIC, "zip", inputRDDs) {
      rdd => rdd.zip(rdd).collect()
    },
    rddBenchmarkSettings(BASIC, "zipWithIndex", inputRDDs) {
      rdd => rdd.zipWithIndex().collect()
    },
    rddBenchmarkSettings(BASIC, "sortBy", inputRDDs) {
      rdd => rdd.sortBy(p => p.age).collect()
    },
    rddBenchmarkSettings(COMPLEX, "map().reduce", inputRDDs) {
      rdd => rdd.map(p => p.age).reduce(_ + _)
    },
    rddBenchmarkSettings(COMPLEX, "map().map().map", inputRDDs) {
      rdd => rdd
        .map(p => p.age)
        .map(x => x+1)
        .map(x => x * 2)
        .collect()
    }
  )
    .flatten
    .sortBy(b => (b.rdds.scalaEagerImpl.count(), b.operationName))

}
