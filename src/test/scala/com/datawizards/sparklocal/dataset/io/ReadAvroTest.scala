package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.datastore.AvroDataStore
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReadAvroTest extends SparkLocalBaseTest {

  private val peopleAvro = AvroDataStore(getClass.getResource("/people.avro").getPath)

  test("Read avro - result") {
    assertDatasetOperationResult(ReaderScalaImpl.read[Person](peopleAvro)) {
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
    assert(ReaderScalaImpl.read[Person](peopleAvro) == ReaderSparkImpl.read[Person](peopleAvro))
  }

}