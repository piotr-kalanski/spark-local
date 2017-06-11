package com.datawizards.sparklocal.performance

import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.performance.BenchmarkModel.{InputDataSets, InputRDDs}
import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}
import com.datawizards.sparklocal.implicits._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalacheck.Arbitrary
import org.scalacheck.Shapeless._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object BenchmarkTestData {
  lazy val dataSets10Elements: InputDataSets[Person] = createInputDataSets(people10Elements)
  lazy val dataSets100Elements: InputDataSets[Person] = createInputDataSets(people100Elements)
  lazy val dataSets1000Elements: InputDataSets[Person] = createInputDataSets(people1000Elements)
  lazy val dataSets100000Elements: InputDataSets[Person] = createInputDataSets(people100000Elements)
  lazy val rdds10Elements: InputRDDs[Person] = createInputRDDs(people10Elements)
  lazy val rdds100Elements: InputRDDs[Person] = createInputRDDs(people100Elements)
  lazy val rdds1000Elements: InputRDDs[Person] = createInputRDDs(people1000Elements)
  lazy val rdds100000Elements: InputRDDs[Person] = createInputRDDs(people100000Elements)

  def createInputDataSets[T: ClassTag: TypeTag](data: Seq[T]): InputDataSets[T] = {
    implicit val encoder = ExpressionEncoder[T]()

    InputDataSets(
      scalaEagerImpl = scalaEagerSession.createDataset(data),
      scalaLazyImpl = scalaLazySession.createDataset(data),
      scalaParallelImpl = scalaParallelSession.createDataset(data),
      scalaParallelLazyImpl = scalaParallelLazySession.createDataset(data),
      sparkImpl = sparkSession.createDataset(data)
    )
  }

  private def createInputRDDs[T: ClassTag](data: Seq[T]): InputRDDs[T] =
    InputRDDs(
      scalaEagerImpl = scalaEagerSession.createRDD(data),
      scalaLazyImpl = scalaLazySession.createRDD(data),
      scalaParallelImpl = scalaParallelSession.createRDD(data),
      scalaParallelLazyImpl = scalaParallelLazySession.createRDD(data),
      sparkImpl = sparkSession.createRDD(data)
    )

  private lazy val scalaEagerSession = SparkSessionAPI.builder(ExecutionEngine.ScalaEager).master("local").getOrCreate()
  private lazy val scalaLazySession = SparkSessionAPI.builder(ExecutionEngine.ScalaLazy).master("local").getOrCreate()
  private lazy val scalaParallelSession = SparkSessionAPI.builder(ExecutionEngine.ScalaParallel).master("local").getOrCreate()
  private lazy val scalaParallelLazySession = SparkSessionAPI.builder(ExecutionEngine.ScalaParallelLazy).master("local").getOrCreate()
  private lazy val sparkSession = SparkSessionAPI.builder(ExecutionEngine.Spark).master("local").getOrCreate()

  private lazy val peopleGenerator = implicitly[Arbitrary[Person]].arbitrary

  private lazy val people10Elements = for(i <- 1 to 10) yield peopleGenerator.sample.get
  private lazy val people100Elements = for(i <- 1 to 100) yield peopleGenerator.sample.get
  private lazy val people1000Elements = for(i <- 1 to 1000) yield peopleGenerator.sample.get
  private lazy val people100000Elements = for(i <- 1 to 100000) yield peopleGenerator.sample.get
}
