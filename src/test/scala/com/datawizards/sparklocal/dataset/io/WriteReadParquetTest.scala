package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore.ParquetDataStore
import com.datawizards.sparklocal.implicits._
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WriteReadParquetTest extends SparkLocalBaseTest {

  test("Writing and reading parquet file produces the same result - Scala") {
    val file = "target/foo_scala.parquet"
    val expected = DataSetAPI(Seq(
      Person("p1", 10),
      Person("p2", 20),
      Person("p3", 30),
      Person("p,4", 40)
    ))

    val dataStore = ParquetDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    val result = ReaderScalaImpl.read[Person](dataStore)

    assertResult(expected) {
      result
    }
  }

  test("Writing and reading parquet file produces the same result - Spark") {
    import spark.implicits._

    val file = "target/foo_spark.parquet"
    val expected = DataSetAPI(Seq(
      Person("p1", 10),
      Person("p2", 20),
      Person("p3", 30),
      Person("p,4", 40)
    ).toDS)

    val dataStore = ParquetDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    val result = ReaderSparkImpl.read[Person](dataStore)

    assertResult(expected) {
      result
    }
  }

}