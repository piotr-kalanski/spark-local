package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.PersonBigInt
import com.datawizards.sparklocal.datastore.JsonDataStore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReadJsonTest extends SparkLocalBaseTest {

  private val peopleJson = JsonDataStore(getClass.getResource("/people.json").getPath)

  test("Read JSON - result") {
    assertDatasetOperationResult(ReaderScalaImpl.read[PersonBigInt](peopleJson)) {
      Array(
        PersonBigInt("p1", 10),
        PersonBigInt("p2", 20),
        PersonBigInt("p3", 30),
        PersonBigInt("p,4", 40)
      )
    }
  }

  test("Read JSON - equals") {
    //access lazy val spark just to init SparkContext
    spark
    assert(ReaderScalaImpl.read[PersonBigInt](peopleJson) == ReaderSparkImpl.read[PersonBigInt](peopleJson))
  }

}