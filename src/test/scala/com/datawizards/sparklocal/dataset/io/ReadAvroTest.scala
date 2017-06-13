package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.datastore.AvroDataStore
import com.datawizards.sparklocal.impl.scala.`lazy`.dataset.io.ReaderScalaLazyImpl
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaEagerImpl
import com.datawizards.sparklocal.impl.scala.parallel.dataset.io.ReaderScalaParallelImpl
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReadAvroTest extends SparkLocalBaseTest {

  private val peopleAvro = AvroDataStore(getClass.getResource("/people.avro").getPath)

  test("Read avro - result") {
    assertDatasetOperationResult(ReaderScalaEagerImpl.read[Person](peopleAvro)) {
      Array(
        Person("p1", 10),
        Person("p2", 20),
        Person("p3", 30),
        Person("p,4", 40)
      )
    }
  }

  test("Read avro - equals") {
    //access lazy val spark just to init SparkContext
    spark
    val scalaEagerDs = ReaderScalaEagerImpl.read[Person](peopleAvro)
    val scalaLazyDs = ReaderScalaLazyImpl.read[Person](peopleAvro)
    val scalaParallelDs = ReaderScalaParallelImpl.read[Person](peopleAvro)
    val sparkDs = ReaderSparkImpl.read[Person](peopleAvro)

    assertDatasetEquals(scalaEagerDs, scalaLazyDs)
    assertDatasetEquals(scalaEagerDs, scalaParallelDs)
    assertDatasetEquals(scalaEagerDs, sparkDs)
  }

}