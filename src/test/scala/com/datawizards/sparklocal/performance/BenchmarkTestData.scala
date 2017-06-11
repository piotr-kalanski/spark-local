package com.datawizards.sparklocal.performance

import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.performance.BenchmarkModel.{InputDataSets, InputRDDs}
import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}
import org.scalacheck.Arbitrary
import org.scalacheck.Shapeless._
import com.datawizards.sparklocal.implicits._

object BenchmarkTestData {
  lazy val dataSets10Elements = InputDataSets(
    scalaEagerImpl = scalaEagerSession.createDataset(people10Elements),
    scalaLazyImpl = scalaLazySession.createDataset(people10Elements),
    scalaParallelImpl = scalaParallelSession.createDataset(people10Elements),
    scalaParallelLazyImpl = scalaParallelLazySession.createDataset(people10Elements),
    sparkImpl = sparkSession.createDataset(people10Elements)
  )

  lazy val rdds10Elements = InputRDDs(
    scalaEagerImpl = scalaEagerSession.createRDD(people10Elements),
    scalaLazyImpl = scalaLazySession.createRDD(people10Elements),
    scalaParallelImpl = scalaParallelSession.createRDD(people10Elements),
    scalaParallelLazyImpl = scalaParallelLazySession.createRDD(people10Elements),
    sparkImpl = sparkSession.createRDD(people10Elements)
  )

  private lazy val scalaEagerSession = SparkSessionAPI.builder(ExecutionEngine.ScalaEager).master("local").getOrCreate()
  private lazy val scalaLazySession = SparkSessionAPI.builder(ExecutionEngine.ScalaLazy).master("local").getOrCreate()
  private lazy val scalaParallelSession = SparkSessionAPI.builder(ExecutionEngine.ScalaParallel).master("local").getOrCreate()
  private lazy val scalaParallelLazySession = SparkSessionAPI.builder(ExecutionEngine.ScalaParallelLazy).master("local").getOrCreate()
  private lazy val sparkSession = SparkSessionAPI.builder(ExecutionEngine.Spark).master("local").getOrCreate()

  private lazy val peopleGenerator = implicitly[Arbitrary[Person]].arbitrary

  private lazy val people10Elements = for(i <- 1 to 10) yield peopleGenerator.sample.get
}
