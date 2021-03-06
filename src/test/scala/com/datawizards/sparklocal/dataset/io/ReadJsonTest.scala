package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel._
import com.datawizards.sparklocal.datastore.JsonDataStore
import com.datawizards.sparklocal.impl.scala.`lazy`.dataset.io.ReaderScalaLazyImpl
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaEagerImpl
import com.datawizards.sparklocal.impl.scala.parallel.dataset.io.ReaderScalaParallelImpl
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReadJsonTest extends SparkLocalBaseTest {

  private val peopleJson = JsonDataStore(getClass.getResource("/people.json").getPath)

  test("Read JSON - result") {
    assertDatasetOperationResult(ReaderScalaEagerImpl.read[PersonBigInt](peopleJson)) {
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
    val scalaEagerDs = ReaderScalaEagerImpl.read[PersonBigInt](peopleJson)
    val scalaLazyDs = ReaderScalaLazyImpl.read[PersonBigInt](peopleJson)
    val scalaParallelDs = ReaderScalaParallelImpl.read[PersonBigInt](peopleJson)
    val sparkDs = ReaderSparkImpl.read[PersonBigInt](peopleJson)

    assertDatasetEquals(scalaEagerDs, scalaLazyDs)
    assertDatasetEquals(scalaEagerDs, scalaParallelDs)
    assertDatasetEquals(scalaEagerDs, sparkDs)
  }

}