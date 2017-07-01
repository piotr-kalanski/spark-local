package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.PersonUppercase
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore.H2DataStore
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaEagerImpl
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.implicits._
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WriteReadJdbcTest extends SparkLocalBaseTest {

  val connectionString = "jdbc:h2:mem:test"
  val data = Seq(
    PersonUppercase("p1", 10),
    PersonUppercase("p2", 20),
    PersonUppercase("p3", 30),
    PersonUppercase("p4", 40)
  )

  test("Writing and reading JDBC table produces the same result - Scala") {
    val dataStore = H2DataStore(connectionString, "public", "PEOPLE_SCALA", new java.util.Properties())
    val expected = DataSetAPI(data)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderScalaEagerImpl.read[PersonUppercase](dataStore)
    }
  }

  test("Writing and reading JDBC table produces the same result - Spark") {
    val dataStore = H2DataStore(connectionString, "public", "PEOPLE_SPARK", new java.util.Properties())
    spark // just to initialize SparkSession
    val expected = DataSetAPI(data.toDS())
    expected.write.jdbc(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderSparkImpl.read[PersonUppercase].jdbc(dataStore)
    }
  }

}